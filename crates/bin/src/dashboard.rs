use hickory_proto::op::{Message, ResponseCode};
use redns_core::{DnsHandler, PluginResult, QueryMeta, UpstreamMetrics, UpstreamWrapper};
use rusqlite::{Connection, params};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

#[derive(Debug, Clone, Serialize)]
pub struct GeoIpData {
    pub city: Option<String>,
    pub asn: Option<String>,
    pub isp: Option<String>,
    pub proxy: Option<bool>,
    pub hosting: Option<bool>,
}

type DynError = Box<dyn std::error::Error + Send + Sync>;

const DNS_LOG_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);
const DNS_LOG_PRUNE_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Debug, Clone, Serialize)]
pub struct DnsLogEntry {
    pub id: u64,
    pub ts_unix_ms: u64,
    pub client_ip: String,
    pub protocol: String,
    pub qname: String,
    pub qtype: String,
    pub rcode: String,
    pub upstreams: Vec<String>,
    pub result: String,
    pub result_rows: Vec<String>,
    pub latency_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct LogSummary {
    pub total_items: u64,
    pub unique_clients: u64,
    pub non_noerror: u64,
    pub avg_latency_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct PaginatedLogsResponse {
    pub items: Vec<DnsLogEntry>,
    pub page: u64,
    pub page_size: u64,
    pub total_items: u64,
    pub total_pages: u64,
    pub summary: LogSummary,
}

#[derive(Debug, Clone, Serialize)]
pub struct ClientStatsEntry {
    pub ip: String,
    pub query_total: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ClientStatsResponse {
    pub items: Vec<ClientStatsEntry>,
    pub total_clients: u64,
    pub total_queries: u64,
    pub top_client: Option<String>,
    pub top_volume: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ClearLogsResponse {
    pub ok: bool,
}

#[derive(Debug, Clone)]
struct NewDnsLogEntry {
    ts_unix_ms: u64,
    client_ip: String,
    protocol: String,
    qname: String,
    qtype: String,
    rcode: String,
    upstreams_json: String,
    result: String,
    result_rows_json: String,
    latency_ms: u64,
}

#[derive(Debug, Clone)]
pub struct LogsQuery {
    pub page: u64,
    pub page_size: u64,
    pub filter: String,
}

impl Default for LogsQuery {
    fn default() -> Self {
        Self {
            page: 1,
            page_size: 25,
            filter: String::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DashboardStore {
    db_path: Arc<String>,
    log_tx: tokio::sync::mpsc::Sender<NewDnsLogEntry>,
}

impl DashboardStore {
    pub fn new(db_path: impl Into<String>) -> Result<Self, DynError> {
        let db_path = db_path.into();
        ensure_sqlite_file_exists(&db_path)?;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<NewDnsLogEntry>(10240);
        let store = Self {
            db_path: Arc::new(db_path.clone()),
            log_tx: tx,
        };
        store.init()?;

        let path = store.db_path.clone();
        tokio::spawn(async move {
            let mut batch = Vec::new();
            loop {
                let entry_opt = rx.recv().await;
                match entry_opt {
                    Some(e) => {
                        batch.push(e);
                        while batch.len() < 100 {
                            if let Ok(e) = rx.try_recv() {
                                batch.push(e);
                            } else {
                                break;
                            }
                        }
                    }
                    None => break,
                }

                let entries = std::mem::take(&mut batch);
                let path_clone = path.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    if let Ok(mut conn) = Self::open_connection(&path_clone) {
                        if let Ok(tx) = conn.transaction() {
                            for entry in entries {
                                let _ = tx.execute(
                                    "INSERT INTO dns_logs (
                                        ts_unix_ms, client_ip, protocol, qname, qtype, rcode, result, result_rows_json, upstreams_json, latency_ms
                                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                                    params![
                                        entry.ts_unix_ms as i64,
                                        entry.client_ip,
                                        entry.protocol,
                                        entry.qname,
                                        entry.qtype,
                                        entry.rcode,
                                        entry.result,
                                        entry.result_rows_json,
                                        entry.upstreams_json,
                                        entry.latency_ms as i64,
                                    ],
                                );
                            }
                            let _ = tx.commit();
                        }
                    }
                }).await;
            }
        });

        Ok(store)
    }

    fn init(&self) -> Result<(), DynError> {
        let conn = Self::open_connection(&self.db_path)?;
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS dns_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_unix_ms INTEGER NOT NULL,
                client_ip TEXT NOT NULL,
                protocol TEXT NOT NULL,
                qname TEXT NOT NULL,
                qtype TEXT NOT NULL,
                rcode TEXT NOT NULL,
                result TEXT NOT NULL,
                result_rows_json TEXT NOT NULL DEFAULT '[]',
                upstreams_json TEXT NOT NULL DEFAULT '[]',
                latency_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_dns_logs_ts ON dns_logs(ts_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_dns_logs_client ON dns_logs(client_ip);
            CREATE INDEX IF NOT EXISTS idx_dns_logs_qname ON dns_logs(qname);

            CREATE TABLE IF NOT EXISTS geoip_cache (
                ip TEXT PRIMARY KEY,
                city TEXT,
                asn TEXT,
                isp TEXT,
                proxy INTEGER,
                hosting INTEGER,
                expires_at INTEGER NOT NULL
            );
            ",
        )?;
        Self::ensure_migrations(&conn)?;
        Ok(())
    }

    fn ensure_migrations(conn: &Connection) -> Result<(), DynError> {
        if !Self::has_column(conn, "dns_logs", "result_rows_json")? {
            conn.execute(
                "ALTER TABLE dns_logs ADD COLUMN result_rows_json TEXT NOT NULL DEFAULT '[]'",
                [],
            )?;
        }
        if !Self::has_column(conn, "dns_logs", "upstreams_json")? {
            conn.execute(
                "ALTER TABLE dns_logs ADD COLUMN upstreams_json TEXT NOT NULL DEFAULT '[]'",
                [],
            )?;
        }
        if !Self::has_column(conn, "geoip_cache", "isp")? {
            conn.execute("ALTER TABLE geoip_cache ADD COLUMN isp TEXT", [])?;
        }
        if !Self::has_column(conn, "geoip_cache", "proxy")? {
            conn.execute("ALTER TABLE geoip_cache ADD COLUMN proxy INTEGER", [])?;
        }
        if !Self::has_column(conn, "geoip_cache", "hosting")? {
            conn.execute("ALTER TABLE geoip_cache ADD COLUMN hosting INTEGER", [])?;
        }
        Ok(())
    }

    fn has_column(conn: &Connection, table: &str, column: &str) -> Result<bool, DynError> {
        let sql = format!("PRAGMA table_info({})", table);
        let mut stmt = conn.prepare(&sql)?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let name: String = row.get(1)?;
            if name == column {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn open_connection(path: &str) -> Result<Connection, DynError> {
        ensure_sqlite_file_exists(path)?;
        let conn = Connection::open(path)?;
        conn.busy_timeout(Duration::from_secs(3))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        Ok(conn)
    }

    async fn record(&self, entry: NewDnsLogEntry) -> Result<(), DynError> {
        self.log_tx
            .try_send(entry)
            .map_err(|e| -> DynError { format!("dashboard log channel error: {e}").into() })
    }

    pub async fn fetch_logs(&self, query: LogsQuery) -> Result<PaginatedLogsResponse, DynError> {
        let path = self.db_path.clone();
        tokio::task::spawn_blocking(move || -> Result<PaginatedLogsResponse, DynError> {
            let conn = Self::open_connection(&path)?;
            let page_size = query.page_size.clamp(1, 200);
            let page = query.page.max(1);
            let pattern = like_pattern(&query.filter);

            let summary = conn.query_row(
                "SELECT
                    COUNT(*),
                    COUNT(DISTINCT client_ip),
                    COALESCE(SUM(CASE WHEN lower(rcode) <> 'noerror' THEN 1 ELSE 0 END), 0),
                    AVG(latency_ms)
                 FROM dns_logs
                 WHERE client_ip LIKE ?1 OR protocol LIKE ?1 OR qname LIKE ?1 OR qtype LIKE ?1 OR rcode LIKE ?1 OR result LIKE ?1 OR upstreams_json LIKE ?1",
                params![pattern.clone()],
                |row| {
                    let avg: Option<f64> = row.get(3)?;
                    Ok(LogSummary {
                        total_items: row.get::<_, i64>(0)? as u64,
                        unique_clients: row.get::<_, i64>(1)? as u64,
                        non_noerror: row.get::<_, i64>(2)? as u64,
                        avg_latency_ms: avg.unwrap_or(0.0).ceil() as u64,
                    })
                },
            )?;

            let total_pages = if summary.total_items == 0 {
                1
            } else {
                summary.total_items.div_ceil(page_size)
            };
            let bounded_page = page.min(total_pages);
            let bounded_offset = (bounded_page - 1) * page_size;

            let mut stmt = conn.prepare(
                "SELECT id, ts_unix_ms, client_ip, protocol, qname, qtype, rcode, result, result_rows_json, upstreams_json, latency_ms
                 FROM dns_logs
                 WHERE client_ip LIKE ?1 OR protocol LIKE ?1 OR qname LIKE ?1 OR qtype LIKE ?1 OR rcode LIKE ?1 OR result LIKE ?1 OR upstreams_json LIKE ?1
                 ORDER BY id DESC
                 LIMIT ?2 OFFSET ?3",
            )?;
            let rows = stmt.query_map(
                params![pattern, page_size as i64, bounded_offset as i64],
                |row| {
                    Ok(DnsLogEntry {
                        id: row.get::<_, i64>(0)? as u64,
                        ts_unix_ms: row.get::<_, i64>(1)? as u64,
                        client_ip: row.get(2)?,
                        protocol: row.get(3)?,
                        qname: row.get(4)?,
                        qtype: row.get(5)?,
                        rcode: row.get(6)?,
                        result: row.get(7)?,
                        result_rows: parse_json_string_vec(&row.get::<_, String>(8)?),
                        upstreams: parse_json_string_vec(&row.get::<_, String>(9)?),
                        latency_ms: row.get::<_, i64>(10)? as u64,
                    })
                },
            )?;

            let mut items = Vec::new();
            for row in rows {
                items.push(row?);
            }

            Ok(PaginatedLogsResponse {
                items,
                page: bounded_page,
                page_size,
                total_items: summary.total_items,
                total_pages,
                summary,
            })
        })
        .await
        .map_err(|e| -> DynError { format!("dashboard sqlite task join failed: {e}").into() })?
    }

    pub async fn fetch_clients(&self) -> Result<ClientStatsResponse, DynError> {
        let path = self.db_path.clone();
        tokio::task::spawn_blocking(move || -> Result<ClientStatsResponse, DynError> {
            let conn = Self::open_connection(&path)?;
            let total_queries: i64 =
                conn.query_row("SELECT COUNT(*) FROM dns_logs", [], |row| row.get(0))?;
            let total_clients: i64 = conn.query_row(
                "SELECT COUNT(DISTINCT client_ip) FROM dns_logs",
                [],
                |row| row.get(0),
            )?;

            let mut stmt = conn.prepare(
                "SELECT client_ip, COUNT(*) AS query_total
                 FROM dns_logs
                 GROUP BY client_ip
                 ORDER BY query_total DESC, client_ip ASC",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok(ClientStatsEntry {
                    ip: row.get(0)?,
                    query_total: row.get::<_, i64>(1)? as u64,
                })
            })?;

            let mut items = Vec::new();
            for row in rows {
                items.push(row?);
            }

            let top_client = items.first().map(|item| item.ip.clone());
            let top_volume = items.first().map(|item| item.query_total).unwrap_or(0);

            Ok(ClientStatsResponse {
                items,
                total_clients: total_clients as u64,
                total_queries: total_queries as u64,
                top_client,
                top_volume,
            })
        })
        .await
        .map_err(|e| -> DynError { format!("dashboard sqlite task join failed: {e}").into() })?
    }

    pub async fn clear_logs(&self) -> Result<(), DynError> {
        let path = self.db_path.clone();
        tokio::task::spawn_blocking(move || -> Result<(), DynError> {
            let conn = Self::open_connection(&path)?;
            conn.execute("DELETE FROM dns_logs", [])?;
            conn.execute("DELETE FROM sqlite_sequence WHERE name='dns_logs'", [])?;
            Ok(())
        })
        .await
        .map_err(|e| -> DynError { format!("dashboard sqlite task join failed: {e}").into() })?
    }

    pub async fn prune_expired_logs(&self) -> Result<u64, DynError> {
        self.prune_expired_logs_at(SystemTime::now()).await
    }

    async fn prune_expired_logs_at(&self, now: SystemTime) -> Result<u64, DynError> {
        let path = self.db_path.clone();
        let cutoff_ms = dns_log_retention_cutoff_ms(now);
        tokio::task::spawn_blocking(move || -> Result<u64, DynError> {
            let conn = Self::open_connection(&path)?;
            let deleted = conn.execute(
                "DELETE FROM dns_logs WHERE ts_unix_ms < ?1",
                params![cutoff_ms as i64],
            )? as u64;
            if deleted > 0 {
                conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE);")?;
            }
            Ok(deleted)
        })
        .await
        .map_err(|e| -> DynError { format!("dashboard sqlite task join failed: {e}").into() })?
    }

    pub async fn get_geoip(&self, ip: &str) -> Result<GeoIpData, DynError> {
        let path = self.db_path.clone();
        let ip_owned = ip.to_string();

        let cached: Option<GeoIpData> = tokio::task::spawn_blocking({
            let path = path.clone();
            let ip_owned = ip_owned.clone();
            move || -> Result<Option<GeoIpData>, DynError> {
                let conn = Self::open_connection(&path)?;
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64;

                let mut stmt = conn.prepare("SELECT city, asn, isp, proxy, hosting, expires_at FROM geoip_cache WHERE ip = ?1")?;
                let mut rows = stmt.query(params![ip_owned])?;

                if let Some(row) = rows.next()? {
                    let expires_at: i64 = row.get(5)?;
                    if now < expires_at {
                        let city: Option<String> = row.get(0)?;
                        let asn: Option<String> = row.get(1)?;
                        let isp: Option<String> = row.get(2)?;
                        let proxy: Option<bool> = row.get(3)?;
                        let hosting: Option<bool> = row.get(4)?;
                        return Ok(Some(GeoIpData { city, asn, isp, proxy, hosting }));
                    }
                }
                Ok(None)
            }
        })
        .await
        .map_err(|e| -> DynError { format!("geoip sqlite read join failed: {e}").into() })??;

        if let Some(res) = cached {
            return Ok(res);
        }

        // Cache miss or expired, fetch from API
        let url = format!(
            "http://ip-api.com/json/{}?fields=status,message,country,countryCode,region,regionName,city,zip,lat,lon,timezone,isp,org,as,query,proxy,hosting",
            ip
        );
        let resp = reqwest::get(&url).await?;
        let text = resp.text().await?;
        let json: serde_json::Value = serde_json::from_str(&text)?;

        let mut location = None;
        let city = json.get("city").and_then(|v| v.as_str());
        let region = json.get("region").and_then(|v| v.as_str());
        let country = json.get("country").and_then(|v| v.as_str());
        let mut parts = Vec::new();
        if let Some(name) = city {
            let trimmed = name.trim();
            if !trimmed.is_empty() {
                parts.push(trimmed);
            }
        }
        if let Some(name) = region {
            let trimmed = name.trim();
            if !trimmed.is_empty() && !parts.contains(&trimmed) {
                parts.push(trimmed);
            }
        }
        if let Some(name) = country {
            let trimmed = name.trim();
            if !trimmed.is_empty() && !parts.contains(&trimmed) {
                parts.push(trimmed);
            }
        }
        if !parts.is_empty() {
            location = Some(parts.join(", "));
        }

        let asn = json
            .get("as")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let isp = json
            .get("isp")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let proxy = json.get("proxy").and_then(|v| v.as_bool());
        let hosting = json.get("hosting").and_then(|v| v.as_bool());

        let res = GeoIpData {
            city: location,
            asn,
            isp,
            proxy,
            hosting,
        };

        // Save to cache (30 days expiration)
        let c_res = res.clone();
        let ip_owned_2 = ip_owned.clone();
        tokio::task::spawn_blocking(move || -> Result<(), DynError> {
            let conn = Self::open_connection(&path)?;
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            let expires_at = now + (30 * 24 * 60 * 60);

            conn.execute(
                "INSERT INTO geoip_cache (ip, city, asn, isp, proxy, hosting, expires_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(ip) DO UPDATE SET
                 city = excluded.city,
                 asn = excluded.asn,
                 isp = excluded.isp,
                 proxy = excluded.proxy,
                 hosting = excluded.hosting,
                 expires_at = excluded.expires_at",
                params![
                    ip_owned_2,
                    c_res.city,
                    c_res.asn,
                    c_res.isp,
                    c_res.proxy,
                    c_res.hosting,
                    expires_at
                ],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| -> DynError { format!("geoip sqlite write join failed: {e}").into() })??;

        Ok(res)
    }
}

pub async fn run_log_retention(store: Arc<DashboardStore>, cancel: CancellationToken) {
    match store.prune_expired_logs().await {
        Ok(deleted) if deleted > 0 => {
            info!(
                deleted,
                "pruned expired DNS log rows from dashboard sqlite store"
            );
        }
        Ok(_) => {}
        Err(error) => warn!(error = %error, "failed to prune expired DNS log rows"),
    }

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(DNS_LOG_PRUNE_INTERVAL) => {
                match store.prune_expired_logs().await {
                    Ok(deleted) if deleted > 0 => {
                        info!(deleted, "pruned expired DNS log rows from dashboard sqlite store");
                    }
                    Ok(_) => {}
                    Err(error) => warn!(error = %error, "failed to prune expired DNS log rows"),
                }
            }
        }
    }
}

pub fn default_sqlite_path(config_file: &str) -> String {
    let config_path = Path::new(config_file);
    let base_dir = config_path.parent().unwrap_or_else(|| Path::new("."));
    base_dir.join("redns.db").to_string_lossy().into_owned()
}

pub struct DashboardDnsHandler {
    inner: Arc<dyn DnsHandler>,
    store: Arc<DashboardStore>,
}

impl DashboardDnsHandler {
    pub fn new(inner: Arc<dyn DnsHandler>, store: Arc<DashboardStore>) -> Self {
        Self { inner, store }
    }
}

#[async_trait::async_trait]
impl DnsHandler for DashboardDnsHandler {
    async fn handle(&self, query: Message, meta: QueryMeta) -> PluginResult<Message> {
        let (qname, qtype) = query
            .queries()
            .first()
            .map(|q| (q.name().to_ascii(), format!("{:?}", q.query_type())))
            .unwrap_or_else(|| (String::new(), String::new()));

        let client_ip = meta
            .client_addr
            .map(|ip| ip.to_string())
            .unwrap_or_else(|| "unknown".to_string());

        let protocol = meta.protocol.clone().unwrap_or_else(|| {
            if meta.from_udp {
                "udp".to_string()
            } else {
                "tcp".to_string()
            }
        });

        let selected_upstreams = Arc::new(Mutex::new(Vec::<String>::new()));
        let mut meta = meta;
        meta.selected_upstreams = Some(selected_upstreams.clone());

        let start = Instant::now();
        let result = self.inner.handle(query, meta).await;
        let elapsed = start.elapsed();

        let (rcode, result_summary, result_rows) = match &result {
            Ok(resp) => {
                let rcode = resp.response_code();
                let (summary, rows) = persisted_dns_result(resp, &qname);
                (format!("{:?}", rcode), summary, rows)
            }
            Err(e) => (
                "ERROR".to_string(),
                e.to_string(),
                vec![format!("error: {}", e)],
            ),
        };

        let upstreams = selected_upstreams
            .lock()
            .map(|items| dedupe_keep_order(items.clone()))
            .unwrap_or_default();

        let entry = NewDnsLogEntry {
            ts_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            client_ip,
            protocol,
            qname,
            qtype,
            rcode,
            upstreams_json: serde_json::to_string(&upstreams).unwrap_or_else(|_| "[]".to_string()),
            result: result_summary,
            result_rows_json: serde_json::to_string(&result_rows)
                .unwrap_or_else(|_| "[]".to_string()),
            latency_ms: latency_ms_ceil(elapsed),
        };
        if let Err(e) = self.store.record(entry).await {
            warn!(error = %e, "dashboard failed to persist dns log entry");
        }

        result
    }
}

#[derive(Clone)]
pub struct DashboardState {
    pub api_http: Option<String>,
    pub upstreams: Arc<[Arc<UpstreamWrapper>]>,
    pub store: Arc<DashboardStore>,
    pub static_dir: String,
}

pub async fn serve_dashboard(
    listener: TcpListener,
    state: DashboardState,
    cancel: CancellationToken,
) {
    let state = Arc::new(state);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            result = listener.accept() => {
                match result {
                    Ok((stream, _peer)) => {
                        let state = state.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_dashboard_request(stream, state).await {
                                warn!(error = %e, "dashboard request error");
                            }
                        });
                    }
                    Err(e) => {
                        warn!(error = %e, "dashboard accept error");
                    }
                }
            }
        }
    }
}

async fn handle_dashboard_request(
    mut stream: TcpStream,
    state: Arc<DashboardState>,
) -> Result<(), DynError> {
    let mut buf = vec![0u8; 8192];
    let n = stream.read(&mut buf).await?;
    if n == 0 {
        return Ok(());
    }

    let request = String::from_utf8_lossy(&buf[..n]);
    let first_line = request.lines().next().unwrap_or("");
    let parts: Vec<&str> = first_line.split_whitespace().collect();
    let method = parts.first().copied().unwrap_or("");
    let target = parts.get(1).copied().unwrap_or("/");
    let path = target.split('?').next().unwrap_or(target);
    let query = parse_query_string(target);

    match (method, path) {
        ("GET", "/api/upstreams") => {
            let body = upstream_metrics_json(&state).await?;
            write_response(
                &mut stream,
                "200 OK",
                "application/json; charset=utf-8",
                body.as_bytes(),
            )
            .await?;
        }
        ("GET", "/api/cache") => {
            let body = cache_metrics_json(&state).await?;
            write_response(
                &mut stream,
                "200 OK",
                "application/json; charset=utf-8",
                body.as_bytes(),
            )
            .await?;
        }
        ("GET", "/api/geoip") => {
            let ip_str = query.get("ip").map(|s| s.as_str()).unwrap_or("");
            if let Ok(ip) = ip_str.parse::<std::net::IpAddr>() {
                let mut geoip = GeoIpData {
                    city: None,
                    asn: None,
                    isp: None,
                    proxy: None,
                    hosting: None,
                };

                let is_private = match ip {
                    std::net::IpAddr::V4(v4) => v4.is_private() || v4.is_loopback(),
                    std::net::IpAddr::V6(v6) => {
                        (v6.segments()[0] & 0xfe00) == 0xfc00 || v6.is_loopback()
                    }
                };

                if is_private {
                    geoip.city = Some("Private Network".to_string());
                } else {
                    match state.store.get_geoip(&ip.to_string()).await {
                        Ok(g) => {
                            geoip = g;
                        }
                        Err(e) => {
                            warn!(error = %e, ip = %ip_str, "failed to get geoip");
                        }
                    }
                }

                let body = serde_json::json!(geoip);
                write_response(
                    &mut stream,
                    "200 OK",
                    "application/json; charset=utf-8",
                    &serde_json::to_vec(&body)?,
                )
                .await?;
            } else {
                write_response(&mut stream, "400 Bad Request", "text/plain", b"Invalid IP").await?;
            }
        }
        ("GET", "/api/logs") => {
            let logs_query = logs_query_from_params(&query);
            let body = serde_json::to_vec(&state.store.fetch_logs(logs_query).await?)?;
            write_response(
                &mut stream,
                "200 OK",
                "application/json; charset=utf-8",
                &body,
            )
            .await?;
        }
        ("GET", "/api/clients") => {
            let body = serde_json::to_vec(&state.store.fetch_clients().await?)?;
            write_response(
                &mut stream,
                "200 OK",
                "application/json; charset=utf-8",
                &body,
            )
            .await?;
        }
        ("POST", "/api/logs/clear") => {
            state.store.clear_logs().await?;
            let body = serde_json::to_vec(&ClearLogsResponse { ok: true })?;
            write_response(
                &mut stream,
                "200 OK",
                "application/json; charset=utf-8",
                &body,
            )
            .await?;
        }
        ("GET", _) => {
            let mut file_path = PathBuf::from(&state.static_dir);
            let mut rel_path = path.trim_start_matches('/');
            if rel_path.is_empty() {
                rel_path = "index.html";
            }
            file_path.push(rel_path);

            let mut file_contents = vec![];
            let content_type = mime_guess::from_path(&file_path)
                .first_or_octet_stream()
                .to_string();

            match tokio::fs::File::open(&file_path).await {
                Ok(mut file) => {
                    file.read_to_end(&mut file_contents).await?;
                    write_response(&mut stream, "200 OK", &content_type, &file_contents).await?;
                }
                Err(_) => {
                    // SPA fallback: Serve index.html if file isn't found
                    let mut index_path = PathBuf::from(&state.static_dir);
                    index_path.push("index.html");
                    if let Ok(mut file) = tokio::fs::File::open(&index_path).await {
                        file.read_to_end(&mut file_contents).await?;
                        write_response(
                            &mut stream,
                            "200 OK",
                            "text/html; charset=utf-8",
                            &file_contents,
                        )
                        .await?;
                    } else {
                        write_response(
                            &mut stream,
                            "404 Not Found",
                            "application/json; charset=utf-8",
                            b"{\"error\":\"not found\"}",
                        )
                        .await?;
                    }
                }
            }
        }
        _ => {
            write_response(
                &mut stream,
                "404 Not Found",
                "application/json; charset=utf-8",
                b"{\"error\":\"not found\"}",
            )
            .await?;
        }
    }

    Ok(())
}

async fn upstream_metrics_json(state: &DashboardState) -> Result<String, DynError> {
    if let Some(api_addr) = &state.api_http {
        match fetch_upstreams_from_api(api_addr).await {
            Ok(body) => return Ok(body),
            Err(e) => {
                warn!(error = %e, addr = %api_addr, "failed to fetch upstream metrics from API");
            }
        }
    }

    let metrics: Vec<UpstreamMetrics> = state.upstreams.iter().map(|u| u.snapshot()).collect();
    Ok(serde_json::to_string(&metrics)?)
}

async fn cache_metrics_json(state: &DashboardState) -> Result<String, DynError> {
    if let Some(api_addr) = &state.api_http {
        match fetch_cache_from_api(api_addr).await {
            Ok(body) => return Ok(body),
            Err(e) => {
                warn!(error = %e, addr = %api_addr, "failed to fetch cache metrics from API");
            }
        }
    }

    let metrics = redns_executables::cache::cache_registry_snapshot().await;
    Ok(serde_json::to_string(&metrics)?)
}

async fn fetch_upstreams_from_api(api_addr: &str) -> Result<String, DynError> {
    let mut stream = TcpStream::connect(api_addr)
        .await
        .map_err(|e| -> DynError {
            format!("failed to connect to API {}: {}", api_addr, e).into()
        })?;

    let req = format!(
        "GET /metrics/upstreams HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        api_addr
    );
    stream
        .write_all(req.as_bytes())
        .await
        .map_err(|e| -> DynError { format!("failed to send API request: {}", e).into() })?;

    let mut bytes = Vec::new();
    stream
        .read_to_end(&mut bytes)
        .await
        .map_err(|e| -> DynError { format!("failed to read API response: {}", e).into() })?;

    let response = String::from_utf8_lossy(&bytes);
    let (headers, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| -> DynError { "invalid API response format".into() })?;
    let status_line = headers.lines().next().unwrap_or("");
    if !status_line.contains(" 200 ") {
        return Err(format!("API returned non-200 status: {}", status_line).into());
    }

    Ok(body.to_string())
}

async fn fetch_cache_from_api(api_addr: &str) -> Result<String, DynError> {
    let mut stream = TcpStream::connect(api_addr)
        .await
        .map_err(|e| -> DynError {
            format!("failed to connect to API {}: {}", api_addr, e).into()
        })?;

    let req = format!(
        "GET /metrics/cache HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        api_addr
    );
    stream
        .write_all(req.as_bytes())
        .await
        .map_err(|e| -> DynError { format!("failed to send API request: {}", e).into() })?;

    let mut bytes = Vec::new();
    stream
        .read_to_end(&mut bytes)
        .await
        .map_err(|e| -> DynError { format!("failed to read API response: {}", e).into() })?;

    let response = String::from_utf8_lossy(&bytes);
    let (headers, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| -> DynError { "invalid API response format".into() })?;
    let status_line = headers.lines().next().unwrap_or("");
    if !status_line.contains(" 200 ") {
        return Err(format!("API returned non-200 status: {}", status_line).into());
    }

    Ok(body.to_string())
}

async fn write_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &[u8],
) -> Result<(), DynError> {
    let resp = format!(
        "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        status,
        content_type,
        body.len()
    );
    stream.write_all(resp.as_bytes()).await?;
    stream.write_all(body).await?;
    Ok(())
}

fn summarize_dns_result(resp: &Message, qname: &str) -> (String, Vec<String>) {
    let answers = resp.answers();
    if answers.is_empty() {
        let row = format!("rcode={:?}, answers=0", resp.response_code());
        return (String::new(), vec![row]);
    }

    let mut rows = Vec::new();
    for answer in answers {
        let ans_name = answer.name().to_ascii();
        let name_disp = if ans_name.eq_ignore_ascii_case(qname) {
            "@"
        } else {
            &ans_name
        };
        rows.push(format!(
            "{} {:?} {}",
            name_disp,
            answer.record_type(),
            answer.data()
        ));
    }

    // Return empty summary to save disk space, we will just rely on result_rows_json.
    (String::new(), rows)
}

fn persisted_dns_result(resp: &Message, qname: &str) -> (String, Vec<String>) {
    if resp.response_code() == ResponseCode::NXDomain
        || (resp.response_code() == ResponseCode::NoError && resp.answers().is_empty())
    {
        (String::new(), Vec::new())
    } else {
        summarize_dns_result(resp, qname)
    }
}

fn latency_ms_ceil(elapsed: Duration) -> u64 {
    let micros = elapsed.as_micros();
    if micros == 0 {
        0
    } else {
        ((micros + 999) / 1000).min(u64::MAX as u128) as u64
    }
}

fn logs_query_from_params(query: &HashMap<String, String>) -> LogsQuery {
    let page = query
        .get("page")
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(1);
    let page_size = query
        .get("page_size")
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(25);
    let filter = query.get("q").cloned().unwrap_or_default();
    LogsQuery {
        page: page.max(1),
        page_size: page_size.clamp(1, 200),
        filter,
    }
}

fn like_pattern(filter: &str) -> String {
    if filter.trim().is_empty() {
        "%".to_string()
    } else {
        format!("%{}%", filter.trim())
    }
}

fn parse_query_string(target: &str) -> HashMap<String, String> {
    let mut params = HashMap::new();
    let Some((_, query)) = target.split_once('?') else {
        return params;
    };

    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        params.insert(percent_decode(key), percent_decode(value));
    }
    params
}

fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                if let (Some(h), Some(l)) = (hex_value(bytes[i + 1]), hex_value(bytes[i + 2])) {
                    out.push((h << 4) | l);
                    i += 3;
                } else {
                    out.push(bytes[i]);
                    i += 1;
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_value(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(10 + (b - b'a')),
        b'A'..=b'F' => Some(10 + (b - b'A')),
        _ => None,
    }
}

fn ensure_sqlite_file_exists(path: &str) -> Result<(), DynError> {
    if path == ":memory:" {
        return Ok(());
    }

    let path = PathBuf::from(path);
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    if !path.exists() {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
    }
    Ok(())
}

fn dns_log_retention_cutoff_ms(now: SystemTime) -> u64 {
    now.checked_sub(DNS_LOG_RETENTION)
        .and_then(|cutoff| cutoff.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

fn parse_json_string_vec(text: &str) -> Vec<String> {
    serde_json::from_str(text).unwrap_or_default()
}

fn dedupe_keep_order(items: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::new();
    for item in items {
        if seen.insert(item.clone()) {
            deduped.push(item);
        }
    }
    deduped
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_db_path(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("redns-{name}-{}-{unique}.db", std::process::id()))
    }

    fn sample_entry(ts_unix_ms: u64, qname: &str) -> NewDnsLogEntry {
        NewDnsLogEntry {
            ts_unix_ms,
            client_ip: "127.0.0.1".to_string(),
            protocol: "udp".to_string(),
            qname: qname.to_string(),
            qtype: "A".to_string(),
            rcode: "NOERROR".to_string(),
            upstreams_json: "[\"upstream-a\"]".to_string(),
            result: "ok".to_string(),
            result_rows_json: "[]".to_string(),
            latency_ms: 12,
        }
    }

    fn cleanup_db(path: &Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
    }

    #[tokio::test]
    async fn prune_expired_logs_removes_rows_older_than_24_hours() {
        let path = temp_db_path("retention");
        let path_str = path.to_string_lossy().into_owned();
        let store = DashboardStore::new(path_str.clone()).expect("create dashboard store");

        let now_ms = 3 * DNS_LOG_RETENTION.as_millis() as u64;
        let cutoff_ms = now_ms - DNS_LOG_RETENTION.as_millis() as u64;

        store
            .record(sample_entry(cutoff_ms - 1, "old.example"))
            .await
            .expect("insert old row");
        store
            .record(sample_entry(cutoff_ms, "boundary.example"))
            .await
            .expect("insert boundary row");
        store
            .record(sample_entry(cutoff_ms + 1, "new.example"))
            .await
            .expect("insert new row");

        // Wait for background worker to process the logs
        tokio::time::sleep(Duration::from_millis(100)).await;

        let deleted = store
            .prune_expired_logs_at(UNIX_EPOCH + Duration::from_millis(now_ms))
            .await
            .expect("prune logs");

        assert_eq!(deleted, 1);

        let logs = store
            .fetch_logs(LogsQuery::default())
            .await
            .expect("fetch logs");
        let qnames: Vec<_> = logs.items.into_iter().map(|item| item.qname).collect();
        assert_eq!(qnames.len(), 2);
        assert!(qnames.iter().any(|qname| qname == "boundary.example"));
        assert!(qnames.iter().any(|qname| qname == "new.example"));

        cleanup_db(&path);
    }

    #[test]
    fn persisted_dns_result_elides_nxdomain_and_empty_noerror_only() {
        let qname = "example.com.";
        let mut noerror = Message::new();
        noerror.set_response_code(ResponseCode::NoError);
        let (summary, rows) = persisted_dns_result(&noerror, qname);
        assert!(summary.is_empty());
        assert!(rows.is_empty());

        let mut servfail = Message::new();
        servfail.set_response_code(ResponseCode::ServFail);
        let (summary, rows) = persisted_dns_result(&servfail, qname);
        assert_eq!(summary, ""); // We are no longer returning summary text
        assert_eq!(rows, vec!["rcode=ServFail, answers=0".to_string()]);

        let mut nxdomain = Message::new();
        nxdomain.set_response_code(ResponseCode::NXDomain);
        let (summary, rows) = persisted_dns_result(&nxdomain, qname);
        assert!(summary.is_empty());
        assert!(rows.is_empty());
    }
}
