// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! redns CLI entry point.

#[cfg(unix)]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod dashboard;
mod http;

use clap::{Parser, Subcommand};
use hickory_proto::op::Message;
use redns_core::chain_builder::ChainBuilder;
use redns_core::config::parse_rule_args;
use redns_core::redns::{Redns, find_and_load_config, load_config_file};
use redns_core::server::{DnsHandler, EntryHandler, QueryMeta};
use redns_core::upstream::UpstreamWrapper;
use redns_core::{PluginRegistry, Sequence};
use redns_executables::forward::{
    REDNS_FORWARD_PARENT_PID_ENV, REDNS_FORWARD_SOCKET_ENV, REDNS_FORWARD_SUFFIX_ENV,
};
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket, UnixListener};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

/// `"<version> (<short-commit>)"` — shown by `--version`, the `version`
/// subcommand, and the dashboard. Assembled at compile time from the package
/// version and the short git commit hash captured by `build.rs`.
const FULL_VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), " (", env!("GIT_COMMIT_SHORT"), ")");

#[derive(Debug, Clone, PartialEq, Eq)]
struct CacheBuildConfig {
    size: usize,
    cache_file: Option<String>,
    dump_interval_secs: u64,
}

#[derive(Parser)]
#[command(name = "redns", about = "A DNS forwarder", version = FULL_VERSION)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start redns main program.
    Start {
        /// Config file path.
        #[arg(short, long)]
        config: Option<String>,

        /// Working directory.
        #[arg(short, long)]
        dir: Option<String>,

        /// UDP backend: "epoll" (default) or "io-uring" (Linux-only, requires io-uring feature).
        #[arg(long, default_value = None)]
        udp_backend: Option<String>,
    },

    /// Print version info and exit.
    Version,
}

#[tokio::main]
async fn main() {
    // Install rustls provider early to avoid runtime provider auto-detection panics.
    redns_core::install_rustls_crypto_provider();

    let cli = Cli::parse();

    match cli.command {
        Commands::Version => println!("redns {FULL_VERSION}"),
        Commands::Start {
            config,
            dir,
            udp_backend,
        } => {
            if let Err(e) = run_server(config, dir, udp_backend).await {
                eprintln!("server failed: {e}");
                std::process::exit(1);
            }
        }
    }
}

fn parse_listen_addr(addr: &str) -> io::Result<SocketAddr> {
    addr.parse().map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid socket address '{addr}': {e}"),
        )
    })
}

fn bind_udp_socket(addr: &str) -> io::Result<UdpSocket> {
    let addr = parse_listen_addr(addr)?;
    let socket = Socket::new(Domain::for_address(addr), Type::DGRAM, Some(Protocol::UDP))?;

    if addr.is_ipv6() {
        socket.set_only_v6(true)?;
    }

    socket.bind(&SockAddr::from(addr))?;
    socket.set_nonblocking(true)?;

    UdpSocket::from_std(socket.into())
}

fn bind_tcp_listener(addr: &str) -> io::Result<TcpListener> {
    let addr = parse_listen_addr(addr)?;
    let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;

    #[cfg(not(windows))]
    socket.set_reuse_address(true)?;

    if addr.is_ipv6() {
        socket.set_only_v6(true)?;
    }

    socket.bind(&SockAddr::from(addr))?;
    socket.listen(1024)?;
    socket.set_nonblocking(true)?;

    TcpListener::from_std(socket.into())
}

fn parse_cache_args(args: &str) -> CacheBuildConfig {
    #[derive(serde::Deserialize)]
    struct CacheArgs {
        #[serde(default)]
        size: Option<usize>,
        #[serde(default)]
        cache_file: Option<String>,
        #[serde(default)]
        dump_interval: Option<u64>,
    }

    let default = CacheBuildConfig {
        size: 0,
        cache_file: None,
        dump_interval_secs: 300,
    };

    let args = args.trim();
    if args.is_empty() {
        return default;
    }

    if let Ok(size) = args.parse::<usize>() {
        return CacheBuildConfig { size, ..default };
    }

    if let Ok(cfg) = redns_core::config::deserialize_yaml_str::<CacheArgs>(args) {
        return CacheBuildConfig {
            size: cfg.size.unwrap_or(default.size),
            cache_file: cfg.cache_file,
            dump_interval_secs: cfg.dump_interval.unwrap_or(default.dump_interval_secs),
        };
    }

    default
}

/// URL of the default ASN database used when the config references the `asn`
/// matcher without an explicit `asn_db`. A MaxMind DB of origin ASNs
/// published by sapics/ip-location-db (~10 MB).
const DEFAULT_ASN_DB_URL: &str =
    "https://github.com/sapics/ip-location-db/releases/download/latest/origin-asn.mmdb";
/// Local cache file name for the auto-downloaded default ASN database.
const DEFAULT_ASN_DB_FILE: &str = "origin-asn.mmdb";

/// Resolves the cache path for the auto-downloaded default ASN database:
/// `$XDG_CACHE_HOME/redns/origin-asn.mmdb`, falling back to
/// `~/.cache/redns/origin-asn.mmdb`, then `./origin-asn.mmdb`.
fn default_asn_db_cache_path() -> PathBuf {
    asn_db_cache_path(
        std::env::var("XDG_CACHE_HOME").ok().as_deref(),
        std::env::var("HOME").ok().as_deref(),
    )
}

/// Pure helper for [`default_asn_db_cache_path`], split out for testing.
fn asn_db_cache_path(xdg_cache_home: Option<&str>, home: Option<&str>) -> PathBuf {
    if let Some(dir) = xdg_cache_home.filter(|d| !d.trim().is_empty()) {
        return PathBuf::from(dir).join("redns").join(DEFAULT_ASN_DB_FILE);
    }
    if let Some(dir) = home.filter(|d| !d.trim().is_empty()) {
        return PathBuf::from(dir)
            .join(".cache")
            .join("redns")
            .join(DEFAULT_ASN_DB_FILE);
    }
    PathBuf::from(DEFAULT_ASN_DB_FILE)
}

/// Returns `true` when the config references the `asn` matcher anywhere:
/// as a named plugin (`type: asn`) or inside a sequence rule
/// (`matches: asn ...` / `matches: !asn ...`).
fn config_uses_asn(cfg: &redns_core::config::Config) -> bool {
    cfg.plugins.iter().any(|plugin| {
        if plugin.plugin_type == "asn" {
            return true;
        }
        if plugin.plugin_type != "sequence" {
            return false;
        }
        let rule_args: Vec<redns_core::RuleArgs> =
            redns_core::config::deserialize_yaml_str(&plugin.args).unwrap_or_default();
        rule_args
            .iter()
            .map(redns_core::config::parse_rule_args)
            .any(|rc| rc.matches.iter().any(|m| m.match_type == "asn"))
    })
}

/// Loads the ASN database backing the `asn` matcher.
///
/// When the user configured `asn_db`, those MaxMind DB files are loaded.
/// Otherwise, if the config uses the `asn` matcher, the default
/// `origin-asn.mmdb` database is downloaded from GitHub on first use and
/// cached on disk (see [`default_asn_db_cache_path`]); later starts reuse the
/// cache. Returns `None` when no `asn` matcher is used and no database is
/// configured.
async fn load_asn_db(
    cfg: &redns_core::config::Config,
) -> Result<Option<Arc<redns_matchers::AsnDb>>, redns_core::PluginError> {
    let mut db = redns_matchers::AsnDb::new();
    if !cfg.asn_db.is_empty() {
        db.load_files(&cfg.asn_db)?;
        info!(files = ?cfg.asn_db, "ASN database loaded");
        return Ok(Some(Arc::new(db)));
    }
    if !config_uses_asn(cfg) {
        return Ok(None);
    }

    let cache_path = default_asn_db_cache_path();
    if !cache_path.exists() {
        download_default_asn_db(&cache_path).await?;
    }
    db.load_file(&cache_path.to_string_lossy())?;
    info!(path = %cache_path.display(), "default ASN database loaded");
    Ok(Some(Arc::new(db)))
}

/// Downloads the default ASN database from GitHub releases into `path`.
///
/// The download is written to a temporary sibling file first and renamed
/// into place, so an interrupted download never leaves a truncated database
/// that would be silently reused on the next start.
async fn download_default_asn_db(path: &std::path::Path) -> Result<(), redns_core::PluginError> {
    info!(url = DEFAULT_ASN_DB_URL, "downloading default ASN database");
    let client = reqwest::Client::builder()
        .user_agent(concat!("redns/", env!("CARGO_PKG_VERSION")))
        .connect_timeout(Duration::from_secs(30))
        .timeout(Duration::from_secs(120))
        .build()?;
    let resp = client
        .get(DEFAULT_ASN_DB_URL)
        .send()
        .await
        .map_err(|e| -> redns_core::PluginError {
            format!("failed to download default ASN database: {e}").into()
        })?;
    if !resp.status().is_success() {
        return Err(format!(
            "failed to download default ASN database: HTTP {}",
            resp.status()
        )
        .into());
    }
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| -> redns_core::PluginError {
            format!("failed to read default ASN database download: {e}").into()
        })?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| -> redns_core::PluginError {
            format!(
                "failed to create cache directory '{}': {e}",
                parent.display()
            )
            .into()
        })?;
    }
    let tmp = path.with_extension("mmdb.tmp");
    std::fs::write(&tmp, &bytes).map_err(|e| -> redns_core::PluginError {
        format!("failed to write '{}': {e}", tmp.display()).into()
    })?;
    std::fs::rename(&tmp, path).map_err(|e| -> redns_core::PluginError {
        format!("failed to move '{}' into place: {e}", tmp.display()).into()
    })?;
    info!(
        path = %path.display(),
        size = bytes.len(),
        "default ASN database downloaded"
    );
    Ok(())
}

/// Registers all built-in matcher and executor factories on the given builder.
fn register_builtins(builder: &mut ChainBuilder, asn_db: Option<Arc<redns_matchers::AsnDb>>) {
    use redns_core::built_in::*;
    use redns_core::plugin::{Executable, Matcher, RecursiveExecutable};
    use redns_executables::*;
    use redns_matchers::*;

    // ── Built-in flow-control ────────────────────────────────────
    builder.register_rec_exec(
        "accept",
        Box::new(|_| Ok(Box::new(ActionAccept) as Box<dyn RecursiveExecutable>)),
    );
    builder.register_rec_exec(
        "reject",
        Box::new(|args| {
            Ok(Box::new(ActionReject::from_str_args(args)?) as Box<dyn RecursiveExecutable>)
        }),
    );

    // ── Simple Executables ───────────────────────────────────────
    builder.register_exec(
        "black_hole",
        Box::new(|args: &str| Ok(Box::new(BlackHole::from_str_args(args)?) as Box<dyn Executable>)),
    );
    builder.register_exec(
        "ttl",
        Box::new(|args: &str| Ok(Box::new(Ttl::from_str_args(args)?) as Box<dyn Executable>)),
    );
    builder.register_exec(
        "sleep",
        Box::new(|args: &str| Ok(Box::new(Sleep::from_str_args(args)?) as Box<dyn Executable>)),
    );
    builder.register_exec(
        "debug_print",
        Box::new(|_| Ok(Box::new(DebugPrint) as Box<dyn Executable>)),
    );
    builder.register_exec(
        "drop_resp",
        Box::new(|_| Ok(Box::new(DropResp) as Box<dyn Executable>)),
    );
    builder.register_exec(
        "shuffle",
        Box::new(|_| {
            Ok(Box::new(Shuffle {
                answer: true,
                ns: true,
                extra: true,
            }) as Box<dyn Executable>)
        }),
    );
    builder.register_exec(
        "hosts",
        Box::new(|args: &str| Ok(Box::new(Hosts::from_lines(args)?) as Box<dyn Executable>)),
    );
    // ── Recursive Executables ────────────────────────────────────
    builder.register_rec_exec(
        "cache",
        Box::new(|args: &str| {
            let cfg = parse_cache_args(args);
            let persist =
                cfg.cache_file
                    .map(|file_path| redns_executables::cache::CachePersistConfig {
                        file_path,
                        dump_interval: std::time::Duration::from_secs(cfg.dump_interval_secs),
                    });
            Ok(Box::new(Cache::new(
                cfg.size,
                std::time::Duration::from_secs(30),
                persist,
            )) as Box<dyn RecursiveExecutable>)
        }),
    );
    builder.register_rec_exec(
        "redirect",
        Box::new(|args: &str| {
            Ok(Box::new(Redirect::from_lines(args)?) as Box<dyn RecursiveExecutable>)
        }),
    );
    builder.register_rec_exec(
        "ecs",
        Box::new(|args: &str| {
            let cfg = redns_executables::ecs_handler::EcsConfig::from_str_args(args)?;
            Ok(Box::new(EcsHandler::new(cfg)) as Box<dyn RecursiveExecutable>)
        }),
    );
    builder.register_rec_exec(
        "ecs_handler",
        Box::new(|args: &str| {
            let cfg = redns_executables::ecs_handler::EcsConfig::from_str_args(args)?;
            Ok(Box::new(EcsHandler::new(cfg)) as Box<dyn RecursiveExecutable>)
        }),
    );
    builder.register_rec_exec(
        "reverse_lookup",
        Box::new(|args: &str| {
            let cfg = redns_executables::reverse_lookup::ReverseLookupConfig::from_str_args(args)?;
            Ok(Box::new(ReverseLookup::new(cfg)) as Box<dyn RecursiveExecutable>)
        }),
    );

    // ── Matchers ─────────────────────────────────────────────────
    builder.register_matcher(
        "has_resp",
        Box::new(|_| Ok(Box::new(HasResp) as Box<dyn Matcher>)),
    );
    builder.register_matcher(
        "has_wanted_ans",
        Box::new(|_| Ok(Box::new(HasWantedAns) as Box<dyn Matcher>)),
    );
    builder.register_matcher(
        "qtype",
        Box::new(|args: &str| Ok(Box::new(QTypeMatcher::from_str_args(args)?) as Box<dyn Matcher>)),
    );
    builder.register_matcher(
        "qclass",
        Box::new(
            |args: &str| Ok(Box::new(QClassMatcher::from_str_args(args)?) as Box<dyn Matcher>),
        ),
    );
    builder.register_matcher(
        "qname",
        Box::new(|args: &str| Ok(Box::new(QnameMatcher::from_str_args(args)?) as Box<dyn Matcher>)),
    );
    builder.register_matcher(
        "client_ip",
        Box::new(|args: &str| {
            Ok(Box::new(ClientIpMatcher::from_str_args(args)?) as Box<dyn Matcher>)
        }),
    );
    builder.register_matcher(
        "resp_ip",
        Box::new(
            |args: &str| Ok(Box::new(RespIpMatcher::from_str_args(args)?) as Box<dyn Matcher>),
        ),
    );
    builder.register_matcher(
        "cname",
        Box::new(|args: &str| Ok(Box::new(CnameMatcher::from_str_args(args)) as Box<dyn Matcher>)),
    );
    builder.register_matcher(
        "rcode",
        Box::new(|args: &str| Ok(Box::new(RcodeMatcher::from_str_args(args)?) as Box<dyn Matcher>)),
    );
    builder.register_matcher(
        "random",
        Box::new(
            |args: &str| Ok(Box::new(RandomMatcher::from_str_args(args)?) as Box<dyn Matcher>),
        ),
    );
    builder.register_matcher(
        "env",
        Box::new(|args: &str| Ok(Box::new(EnvMatcher::from_str_args(args)) as Box<dyn Matcher>)),
    );
    builder.register_matcher(
        "ptr_ip",
        Box::new(|args: &str| Ok(Box::new(PtrIpMatcher::from_str_args(args)?) as Box<dyn Matcher>)),
    );
    builder.register_matcher(
        "string_exp",
        Box::new(|args: &str| {
            Ok(Box::new(StringExpMatcher::from_str_args(args)?) as Box<dyn Matcher>)
        }),
    );
    builder.register_matcher(
        "asn",
        Box::new(move |args: &str| {
            let db = asn_db.clone().ok_or_else(
                || -> Box<dyn std::error::Error + Send + Sync> {
                    "asn matcher requires the top-level `asn_db` config key (path to a MaxMind DB file) — automatic download of the default database is unavailable".into()
                },
            )?;
            Ok(Box::new(AsnMatcher::from_str_args(args, db)?) as Box<dyn Matcher>)
        }),
    );
    builder.register_rec_exec(
        "use-answer-of",
        Box::new(|args: &str| {
            Ok(Box::new(UseAnswerOf::from_str_args(args)?) as Box<dyn RecursiveExecutable>)
        }),
    );

    // ── Data providers (registered as matchers) ──────────────────
    builder.register_matcher(
        "domain_set",
        Box::new(|args: &str| Ok(Box::new(DomainSet::from_str_args(args)?) as Box<dyn Matcher>)),
    );
    builder.register_matcher(
        "ip_set",
        Box::new(|args: &str| Ok(Box::new(IpSet::from_str_args(args)?) as Box<dyn Matcher>)),
    );
}

async fn run_server(
    config_path: Option<String>,
    working_dir: Option<String>,
    cli_udp_backend: Option<String>,
) -> Result<(), redns_core::PluginError> {
    // ── Forward-helper mode ─────────────────────────────────────
    // The forward plugin (SubprocessUpstream) spawns us as its out-of-process
    // upstream by setting REDNS_FORWARD_SOCKET_ENV. In that mode we rename
    // ourselves to `redns<suffix>` (e.g. `redns-direct`), skip the network
    // listeners/dashboard/API, and serve queries over the Unix socket instead.
    let subprocess_socket = std::env::var(REDNS_FORWARD_SOCKET_ENV)
        .ok()
        .filter(|s| !s.is_empty());
    let subprocess_suffix = std::env::var(REDNS_FORWARD_SUFFIX_ENV)
        .ok()
        .filter(|s| !s.is_empty());
    let subprocess_parent_pid = std::env::var(REDNS_FORWARD_PARENT_PID_ENV)
        .ok()
        .and_then(|s| s.parse::<i32>().ok())
        .filter(|&p| p > 0);
    let subprocess_mode = subprocess_socket.is_some();

    if subprocess_mode {
        set_process_name(&format!("redns{}", subprocess_suffix.as_deref().unwrap_or("")));
    }

    if let Some(ref dir) = working_dir {
        std::env::set_current_dir(dir).map_err(|e| -> redns_core::PluginError {
            format!("failed to change working directory to {}: {}", dir, e).into()
        })?;
        info!(path = %dir, "working directory changed");
    }

    let (cfg, file_used) = if let Some(path) = config_path {
        let cfg = load_config_file(&path)?;
        (cfg, path)
    } else {
        find_and_load_config()?
    };

    // Re-initialize tracing with the config's log level.
    // RUST_LOG env var takes precedence; otherwise use config; otherwise "info".
    let log_level = cfg.log.level.as_deref().unwrap_or("info");
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(log_level));
    // Replace the global subscriber with one that uses the config level.
    // If a log file is configured, write to that file instead of stdout.
    if let Some(ref log_file) = cfg.log.file {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            // The helper must append: truncating would clobber the main
            // process's log right after it was opened.
            .truncate(!subprocess_mode)
            .open(log_file)
            .unwrap_or_else(|e| panic!("failed to open log file '{}': {}", log_file, e));
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(file)
            .with_ansi(false)
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("failed to set global tracing subscriber");
    } else {
        let subscriber = tracing_subscriber::fmt().with_env_filter(filter).finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("failed to set global tracing subscriber");
    };

    info!(file = %file_used, level = %log_level, "config loaded");

    let registry = PluginRegistry::new();
    let _redns = Redns::new(cfg.clone(), registry)?;

    let asn_db = load_asn_db(&cfg).await?;

    let mut builder = ChainBuilder::new();
    register_builtins(&mut builder, asn_db);

    // Register forward plugin with upstream collection for metrics API.
    // Uses a Mutex during startup only; frozen into Arc<[]> before serving.
    let upstreams_collector: Arc<parking_lot::Mutex<Vec<Arc<UpstreamWrapper>>>> =
        Arc::new(parking_lot::Mutex::new(Vec::new()));
    {
        let collector = upstreams_collector.clone();
        builder.register_exec(
            "forward",
            Box::new(move |args: &str| {
                use redns_core::plugin::Executable;
                use redns_executables::Forward;
                use redns_executables::forward::ForwardConfig;
                let cfg = if let Ok(yaml_cfg) = ForwardConfig::from_yaml_str(args) {
                    yaml_cfg
                } else {
                    ForwardConfig::from_str_args(args)
                };
                let fwd = Forward::new(cfg, "forward")?;
                {
                    let mut guard = collector.lock();
                    guard.extend(fwd.upstreams().iter().cloned());
                }
                Ok(Box::new(fwd) as Box<dyn Executable>)
            }),
        );
    }

    // ── Phase 1: Build non-sequence plugins and register by tag ──
    // Two passes: first register all normal plugins, then resolve
    // inter-plugin references like `fallback`.
    let mut deferred_fallbacks = Vec::new();

    for plugin in &cfg.plugins {
        if plugin.plugin_type == "sequence" || plugin.tag.is_empty() {
            continue;
        }

        // Defer fallback plugins — they need primary/secondary to be registered first.
        if plugin.plugin_type == "fallback" {
            deferred_fallbacks.push(plugin.clone());
            continue;
        }

        let args_str: String = plugin.args.clone();

        match builder.build_and_register(&plugin.tag, &plugin.plugin_type, &args_str) {
            Ok(()) => {
                info!(tag = %plugin.tag, plugin_type = %plugin.plugin_type, "registered named plugin")
            }
            Err(e) => {
                warn!(tag = %plugin.tag, plugin_type = %plugin.plugin_type, error = %e, "failed to build plugin")
            }
        }
    }

    // ── Phase 1.5: Build sequence plugins (first pass) ───────────
    // Some sequences may fail if they reference not-yet-built plugins
    // like $fallback. Track failures for retry after fallback is built.
    let mut failed_sequences: Vec<usize> = Vec::new();

    for (idx, plugin) in cfg.plugins.iter().enumerate() {
        if plugin.plugin_type != "sequence" {
            continue;
        }

        let rule_args: Vec<redns_core::RuleArgs> =
            redns_core::config::deserialize_yaml_str(&plugin.args).unwrap_or_default();
        let rule_configs: Vec<_> = rule_args.iter().map(parse_rule_args).collect();
        match builder.build_chain(&rule_configs) {
            Ok(chain) => {
                let tag = if plugin.tag.is_empty() {
                    "(anonymous)"
                } else {
                    &plugin.tag
                };
                info!(tag = %tag, rules = chain.len(), "sequence built");

                if !plugin.tag.is_empty() {
                    builder.add_named_exec(&plugin.tag, Arc::new(Sequence::new(chain)));
                }
            }
            Err(_) => {
                // Defer — might succeed after fallback is registered.
                failed_sequences.push(idx);
            }
        }
    }

    // ── Phase 2: Build fallback plugins (depend on sequences) ───
    for plugin in &deferred_fallbacks {
        use redns_executables::fallback::{Fallback, FallbackArgs};

        let args: FallbackArgs = match serde_saphyr::from_str(&plugin.args) {
            Ok(a) => a,
            Err(e) => {
                warn!(tag = %plugin.tag, error = %e, "fallback: invalid args");
                continue;
            }
        };

        let primary = match builder.get_named_exec(&args.primary) {
            Some(e) => e,
            None => {
                warn!(tag = %plugin.tag, primary = %args.primary, "fallback: primary exec not found");
                continue;
            }
        };
        let secondary = match builder.get_named_exec(&args.secondary) {
            Some(e) => e,
            None => {
                warn!(tag = %plugin.tag, secondary = %args.secondary, "fallback: secondary exec not found");
                continue;
            }
        };

        let threshold = if args.threshold > 0 {
            std::time::Duration::from_millis(args.threshold)
        } else {
            std::time::Duration::from_millis(500)
        };

        let fb = Fallback::new(primary, secondary, threshold, args.always_standby);
        builder.add_named_exec(&plugin.tag, std::sync::Arc::new(fb));
        info!(tag = %plugin.tag, "registered fallback plugin");
    }

    // ── Phase 2.5: Retry failed sequences (now $fallback etc. exist) ──
    for idx in &failed_sequences {
        let plugin = &cfg.plugins[*idx];
        let rule_args: Vec<redns_core::RuleArgs> =
            redns_core::config::deserialize_yaml_str(&plugin.args).unwrap_or_default();
        let rule_configs: Vec<_> = rule_args.iter().map(parse_rule_args).collect();
        match builder.build_chain(&rule_configs) {
            Ok(chain) => {
                let tag = if plugin.tag.is_empty() {
                    "(anonymous)"
                } else {
                    &plugin.tag
                };
                info!(tag = %tag, rules = chain.len(), "sequence built (retry)");

                if !plugin.tag.is_empty() {
                    builder.add_named_exec(&plugin.tag, Arc::new(Sequence::new(chain)));
                }
            }
            Err(e) => {
                warn!(tag = %plugin.tag, error = %e, "failed to build sequence, skipping");
            }
        }
    }

    // ── Phase 3: Start servers from config ──────────────────────
    // Collect server configs from both `servers:` section and
    // `udp_server`/`tcp_server` plugin entries.
    let mut servers: Vec<redns_core::config::ServerConfig> = cfg.servers.clone();

    // Extract server configs from plugin entries.
    for plugin in &cfg.plugins {
        match plugin.plugin_type.as_str() {
            "udp_server" | "tcp_server" => {
                #[derive(serde::Deserialize, Default)]
                struct ServerPluginArgs {
                    #[serde(default)]
                    entry: String,
                    #[serde(default = "default_listen")]
                    listen: String,
                    #[serde(default)]
                    udp_workers: Option<usize>,
                    #[serde(default)]
                    udp_max_inflight: Option<usize>,
                }
                fn default_listen() -> String {
                    "127.0.0.1:53".into()
                }

                let args: ServerPluginArgs =
                    serde_saphyr::from_str(&plugin.args).unwrap_or_default();
                let proto = if plugin.plugin_type == "udp_server" {
                    "udp"
                } else {
                    "tcp"
                };
                servers.push(redns_core::config::ServerConfig {
                    protocol: proto.into(),
                    addr: args.listen,
                    entry: args.entry,
                    udp_backend: cli_udp_backend.clone(),
                    udp_workers: args.udp_workers,
                    udp_max_inflight: args.udp_max_inflight,
                });
            }
            _ => {}
        }
    }

    let cancel = tokio_util::sync::CancellationToken::new();

    if subprocess_mode {
        // Forward-helper mode: no UDP/TCP listeners, no dashboard, no API —
        // the entry sequence is served over the Unix socket instead.
        let entry = servers
            .iter()
            .find(|s| !s.entry.is_empty())
            .map(|s| s.entry.clone())
            .ok_or_else(|| -> redns_core::PluginError {
                error!("forward subprocess: no server entry sequence configured");
                "forward subprocess: no server entry sequence configured".into()
            })?;
        let entry_exec = builder.get_named_exec(&entry).ok_or_else(|| -> redns_core::PluginError {
            error!(entry = %entry, "forward subprocess: entry sequence not found");
            format!("forward subprocess: entry sequence '{}' not found", entry).into()
        })?;
        let handler: Arc<dyn DnsHandler> =
            Arc::new(EntryHandler::with_best_effort(entry_exec, cfg.best_effort));

        // Exit when the parent process dies so we never linger as an orphan.
        if let Some(ppid) = subprocess_parent_pid {
            let c = cancel.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    if !process_alive(ppid) {
                        info!(parent_pid = ppid, "forward subprocess: parent exited, shutting down");
                        c.cancel();
                        return;
                    }
                }
            });
        }

        let socket_path = subprocess_socket.clone().ok_or_else(|| -> redns_core::PluginError {
            "forward subprocess: socket path missing".into()
        })?;
        let c = cancel.clone();
        tokio::spawn(async move {
            if let Err(e) = serve_forward_subprocess(&socket_path, handler, c.clone()).await {
                error!(error = %e, "forward subprocess server error");
                // A fatal serve error (e.g. bind failure) means we can never
                // do our job — exit instead of idling until the parent kills us.
                c.cancel();
            }
        });

        return finish_shutdown(cancel).await;
    }

    if servers.is_empty() {
        error!("no servers configured");
        return Err("no servers configured".into());
    }

    let sqlite_path = cfg
        .dashboard
        .sqlite
        .clone()
        .unwrap_or_else(|| dashboard::default_sqlite_path(&file_used));
    info!(
        path = %sqlite_path,
        persist = cfg.dashboard.persist,
        "dashboard sqlite path selected"
    );
    let dashboard_store = Arc::new(dashboard::DashboardStore::new(
        !cfg.dashboard.persist,
        sqlite_path,
        cfg.dashboard.dhcp_leases.clone(),
    )?);
    {
        let store = dashboard_store.clone();
        let c = cancel.clone();
        tokio::spawn(async move {
            dashboard::run_log_retention(store, c).await;
        });
    }

    for srv in &servers {
        if srv.entry.is_empty() {
            error!(addr = %srv.addr, "server has no entry sequence configured");
            return Err(format!("server {} has no entry sequence configured", srv.addr).into());
        }
        let entry_exec = match builder.get_named_exec(&srv.entry) {
            Some(e) => e,
            None => {
                error!(entry = %srv.entry, addr = %srv.addr, "entry sequence not found");
                return Err(format!("entry sequence '{}' not found", srv.entry).into());
            }
        };
        let handler: Arc<dyn redns_core::DnsHandler> =
            Arc::new(EntryHandler::with_best_effort(entry_exec, cfg.best_effort));
        let handler: Arc<dyn redns_core::DnsHandler> = Arc::new(
            dashboard::DashboardDnsHandler::new(handler, dashboard_store.clone()),
        );

        let addr = &srv.addr;
        let proto = &srv.protocol;
        let udp_options = redns_core::udp_server::UdpServerOptions {
            worker_count: srv.udp_workers,
            max_inflight_handlers: srv.udp_max_inflight,
        };

        if proto.contains("udp") || proto == "udp+tcp" {
            match bind_udp_socket(addr) {
                Ok(socket) => {
                    info!(addr = %addr, "UDP server listening");
                    let h = handler.clone();
                    let s = Arc::new(socket);
                    let c = cancel.clone();

                    // Determine which UDP backend to use
                    let use_io_uring = srv.udp_backend.as_deref() == Some("io-uring");

                    #[cfg(all(target_os = "linux", feature = "io-uring"))]
                    if use_io_uring {
                        // Check if io_uring is available
                        if redns_core::udp_server_uring::is_uring_available() {
                            info!(addr = %addr, "using io_uring UDP backend");
                            tokio::spawn(async move {
                                if let Err(e) =
                                    redns_core::udp_server_uring::serve_udp_uring_with_options(
                                        s,
                                        h,
                                        c,
                                        udp_options,
                                    )
                                    .await
                                {
                                    error!(error = %e, "io_uring UDP server error");
                                }
                            });
                        } else {
                            warn!(addr = %addr, "io_uring requested but not available, falling back to epoll");
                            tokio::spawn(async move {
                                if let Err(e) = redns_core::udp_server::serve_udp_with_options(
                                    s,
                                    h,
                                    c,
                                    udp_options,
                                )
                                .await
                                {
                                    error!(error = %e, "UDP server error");
                                }
                            });
                        }
                    } else {
                        tokio::spawn(async move {
                            if let Err(e) =
                                redns_core::udp_server::serve_udp_with_options(s, h, c, udp_options)
                                    .await
                            {
                                error!(error = %e, "UDP server error");
                            }
                        });
                    }

                    #[cfg(not(all(target_os = "linux", feature = "io-uring")))]
                    {
                        if use_io_uring {
                            warn!(addr = %addr, "io_uring not supported (requires Linux + io-uring feature), using epoll");
                        }
                        tokio::spawn(async move {
                            if let Err(e) =
                                redns_core::udp_server::serve_udp_with_options(s, h, c, udp_options)
                                    .await
                            {
                                error!(error = %e, "UDP server error");
                            }
                        });
                    }
                }
                Err(e) => warn!(error = %e, addr = %addr, "failed to bind UDP"),
            }
        }

        if proto.contains("tcp") || proto == "udp+tcp" {
            match bind_tcp_listener(addr) {
                Ok(listener) => {
                    info!(addr = %addr, "TCP server listening");
                    let h = handler.clone();
                    let c = cancel.clone();
                    tokio::spawn(async move {
                        if let Err(e) = redns_core::tcp_server::serve_tcp(listener, h, c).await {
                            error!(error = %e, "TCP server error");
                        }
                    });
                }
                Err(e) => warn!(error = %e, addr = %addr, "failed to bind TCP"),
            }
        }
    }

    // ── Phase 4: Start API HTTP server ──────────────────────────
    // Freeze the upstreams collection into an immutable Arc slice (no more Mutex).
    let all_upstreams: Arc<[Arc<UpstreamWrapper>]> = {
        let guard = upstreams_collector.lock();
        guard.clone().into()
    };

    // Eagerly probe upstream capabilities (e.g. RFC 7766 pipelining for DoT).
    for uw in all_upstreams.iter() {
        uw.probe().await;
    }

    if let Some(ref api_addr) = cfg.api.http {
        match bind_tcp_listener(api_addr) {
            Ok(listener) => {
                info!(addr = %api_addr, "API HTTP server listening");
                let upstreams = all_upstreams.clone();
                let c = cancel.clone();
                tokio::spawn(async move {
                    serve_api(listener, upstreams, c).await;
                });
            }
            Err(e) => warn!(error = %e, addr = %api_addr, "failed to bind API HTTP"),
        }
    }

    // ── Phase 5: Start Dashboard HTTP server ────────────────────
    if let Some(ref dashboard_addr) = cfg.dashboard.http {
        match bind_tcp_listener(dashboard_addr) {
            Ok(listener) => {
                info!(addr = %dashboard_addr, "Dashboard HTTP server listening");
                let static_dir = cfg.dashboard.static_dir.clone().unwrap_or_else(|| {
                    if let Some(dir) = &working_dir {
                        format!("{}/dashboard/dist", dir)
                    } else {
                        "dashboard/dist".to_string()
                    }
                });
                let state = dashboard::DashboardState {
                    api_http: cfg.api.http.clone(),
                    upstreams: all_upstreams.clone(),
                    store: dashboard_store.clone(),
                    static_dir,
                    version: FULL_VERSION,
                };
                let c = cancel.clone();
                tokio::spawn(async move {
                    dashboard::serve_dashboard(listener, state, c).await;
                });
            }
            Err(e) => warn!(error = %e, addr = %dashboard_addr, "failed to bind Dashboard HTTP"),
        }
    }

    finish_shutdown(cancel).await
}

/// Shared shutdown tail for both the main server and the forward-helper
/// process: log startup, wait for a shutdown signal (the helper also exits
/// when its parent dies, via the cancellation token), then cancel all tasks.
async fn finish_shutdown(cancel: CancellationToken) -> Result<(), redns_core::PluginError> {
    info!("redns started");
    tokio::select! {
        _ = wait_for_shutdown_signal() => {}
        _ = cancel.cancelled() => {
            info!("shutdown requested via cancellation");
        }
    }
    info!("shutting down...");
    cancel.cancel();
    // Give servers time to clean up.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    info!("redns stopped");
    Ok(())
}

/// Waits for a shutdown signal.
///
/// On Unix this resolves on either SIGTERM (sent by `systemctl stop` and
/// `docker stop`) or SIGINT (Ctrl-C). Handling SIGTERM is essential: its
/// default disposition is immediate termination, which would bypass the
/// cancellation token and the graceful-shutdown paths (e.g. cache dumping).
/// On non-Unix platforms it falls back to Ctrl-C only.
async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        // If signal registration fails, fall back to never resolving for that
        // source rather than panicking; the other source still works.
        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "failed to install SIGTERM handler");
                return;
            }
        };
        let mut sigint = match signal(SignalKind::interrupt()) {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "failed to install SIGINT handler");
                return;
            }
        };

        tokio::select! {
            _ = sigterm.recv() => info!("received SIGTERM"),
            _ = sigint.recv() => info!("received SIGINT"),
        }
    }

    #[cfg(not(unix))]
    {
        if let Err(e) = tokio::signal::ctrl_c().await {
            warn!(error = %e, "failed to listen for ctrl-c");
        }
    }
}

// ── Forward-helper mode (out-of-process upstream) ────────────────
//
// The forward plugin's `subprocess_suffix` option spawns a second copy of the
// current executable as an out-of-process upstream. That copy detects the
// role via the REDNS_FORWARD_* environment variables (see forward.rs), renames
// itself to `redns<suffix>`, and serves the configured entry sequence over a
// Unix socket instead of binding UDP/TCP listeners.

/// Best-effort rename so the forward helper shows up as `redns<suffix>`
/// (e.g. `redns-direct`) in `ps`/`top` rather than a second `redns`.
fn set_process_name(name: &str) {
    #[cfg(target_os = "linux")]
    {
        // PR_SET_NAME updates /proc/self/comm (truncated to 15 chars).
        let Ok(cname) = std::ffi::CString::new(name) else {
            return;
        };
        let ret = unsafe {
            libc::prctl(libc::PR_SET_NAME, cname.as_ptr() as libc::c_ulong, 0, 0, 0)
        };
        if ret != 0 {
            warn!(
                name = %name,
                error = %std::io::Error::last_os_error(),
                "failed to set process name"
            );
        }
    }
    #[cfg(target_os = "macos")]
    {
        // pthread_setname_np names the calling thread; visible in some tools.
        let Ok(cname) = std::ffi::CString::new(name) else {
            return;
        };
        let ret = unsafe { libc::pthread_setname_np(cname.as_ptr()) };
        if ret != 0 {
            warn!(
                name = %name,
                error = %std::io::Error::last_os_error(),
                "failed to set process name"
            );
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        debug!(name = %name, "process rename not supported on this platform");
    }
}

/// Returns true if a process with the given PID exists.
#[cfg(unix)]
fn process_alive(pid: i32) -> bool {
    // kill(pid, 0) probes existence without delivering a signal.
    let ret = unsafe { libc::kill(pid, 0) };
    if ret == 0 {
        return true;
    }
    // EPERM means the process exists but belongs to another user.
    std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(not(unix))]
fn process_alive(_pid: i32) -> bool {
    true
}

/// Serves DNS queries over a Unix domain socket in forward-helper mode.
///
/// The forward plugin's `SubprocessUpstream` is the client: it opens a
/// connection per query, writes a 2-byte big-endian length followed by the
/// query wire, and reads a length-prefixed response wire (the same framing as
/// TCP DNS). Runs until the cancellation token fires.
async fn serve_forward_subprocess(
    socket_path: &str,
    handler: Arc<dyn DnsHandler>,
    cancel: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Remove a stale socket left by a crashed run. Socket paths embed the
    // parent PID, so this can never clobber a live helper.
    let _ = std::fs::remove_file(socket_path);
    let listener = UnixListener::bind(socket_path)
        .map_err(|e| format!("forward subprocess: failed to bind '{socket_path}': {e}"))?;
    info!(socket = %socket_path, "forward subprocess listening");

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, _peer) = match result {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(error = %e, "forward subprocess: accept failed");
                        continue;
                    }
                };
                let handler = handler.clone();
                let cancel = cancel.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_forward_subprocess_conn(stream, handler, cancel).await
                    {
                        debug!(error = %e, "forward subprocess: connection error");
                    }
                });
            }
            _ = cancel.cancelled() => {
                debug!("forward subprocess shutting down");
                break;
            }
        }
    }

    let _ = std::fs::remove_file(socket_path);
    Ok(())
}

/// Serves one Unix socket connection: length-prefixed query in, response out.
async fn handle_forward_subprocess_conn(
    mut stream: tokio::net::UnixStream,
    handler: Arc<dyn DnsHandler>,
    cancel: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // The parent enforces its own exchange timeout, so this read timeout is
    // only a guard against a stuck peer pinning the task forever.
    const READ_TIMEOUT: Duration = Duration::from_secs(10);

    loop {
        if cancel.is_cancelled() {
            return Ok(());
        }

        let mut len_buf = [0u8; 2];
        match tokio::time::timeout(READ_TIMEOUT, stream.read_exact(&mut len_buf)).await {
            Ok(Ok(_)) => {}
            _ => return Ok(()), // EOF or timeout — connection finished.
        }
        let msg_len = u16::from_be_bytes(len_buf) as usize;
        if msg_len == 0 {
            return Ok(());
        }

        let mut msg_buf = vec![0u8; msg_len];
        match tokio::time::timeout(READ_TIMEOUT, stream.read_exact(&mut msg_buf)).await {
            Ok(Ok(_)) => {}
            _ => return Ok(()),
        }

        let query = match Message::from_vec(&msg_buf) {
            Ok(q) => q,
            Err(e) => {
                debug!(error = %e, "forward subprocess: invalid DNS query");
                return Ok(());
            }
        };

        let meta = QueryMeta {
            protocol: Some("unix".to_string()),
            from_udp: false,
            client_addr: None,
            url_path: None,
            server_name: None,
            selected_upstreams: None,
            query_wire: Some(Arc::new(msg_buf)),
        };

        let resp_bytes = handler.handle_tcp(query, meta).await?;
        let len = (resp_bytes.len() as u16).to_be_bytes();
        stream.write_all(&len).await?;
        stream.write_all(&resp_bytes).await?;
    }
}

/// Simple API HTTP server.
async fn serve_api(
    listener: TcpListener,
    upstreams: Arc<[Arc<UpstreamWrapper>]>,
    cancel: tokio_util::sync::CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            result = listener.accept() => {
                match result {
                    Ok((stream, _peer)) => {
                        let upstreams = upstreams.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_api_request(stream, upstreams).await {
                                warn!(error = %e, "API request error");
                            }
                        });
                    }
                    Err(e) => {
                        warn!(error = %e, "API accept error");
                    }
                }
            }
        }
    }
}

async fn handle_api_request(
    mut stream: tokio::net::TcpStream,
    upstreams: Arc<[Arc<UpstreamWrapper>]>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use tokio::io::AsyncWriteExt;

    let Some((head, _body)) = crate::http::read_request_head(&mut stream).await? else {
        return Ok(());
    };
    let request = String::from_utf8_lossy(&head);

    // Parse the request line.
    let first_line = request.lines().next().unwrap_or("");
    let parts: Vec<&str> = first_line.split_whitespace().collect();
    let (method, path) = if parts.len() >= 2 {
        (parts[0], parts[1])
    } else {
        ("", "")
    };

    if method == "GET" && path == "/metrics/upstreams" {
        let metrics: Vec<redns_core::UpstreamMetrics> =
            upstreams.iter().map(|u| u.snapshot()).collect();
        let body = serde_json::to_string_pretty(&metrics)?;
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(resp.as_bytes()).await?;
    } else if method == "GET" && path == "/metrics/cache" {
        let metrics = redns_executables::cache::cache_registry_snapshot().await;
        let body = serde_json::to_string_pretty(&metrics)?;
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(resp.as_bytes()).await?;
    } else {
        let body = "{\"error\":\"not found\"}";
        let resp = format!(
            "HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(resp.as_bytes()).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        CacheBuildConfig, asn_db_cache_path, bind_tcp_listener, bind_udp_socket, config_uses_asn,
        parse_cache_args,
    };
    use std::path::PathBuf;

    #[test]
    fn cache_size_parses_plain_integer_arg() {
        assert_eq!(
            parse_cache_args("16384"),
            CacheBuildConfig {
                size: 16384,
                cache_file: None,
                dump_interval_secs: 300,
            }
        );
    }

    #[test]
    fn cache_size_parses_yaml_mapping_arg() {
        assert_eq!(
            parse_cache_args("size: 16384"),
            CacheBuildConfig {
                size: 16384,
                cache_file: None,
                dump_interval_secs: 300,
            }
        );
    }

    #[test]
    fn cache_size_defaults_when_arg_is_invalid() {
        assert_eq!(
            parse_cache_args("size: nope"),
            CacheBuildConfig {
                size: 0,
                cache_file: None,
                dump_interval_secs: 300,
            }
        );
    }

    #[tokio::test]
    async fn udp_ipv4_and_ipv6_any_can_share_a_port() {
        let ipv4 = bind_udp_socket("0.0.0.0:0").expect("bind IPv4 UDP listener");
        let port = ipv4.local_addr().expect("read IPv4 UDP addr").port();
        let ipv6 = bind_udp_socket(&format!("[::]:{port}")).expect("bind IPv6 UDP listener");

        assert_eq!(ipv6.local_addr().expect("read IPv6 UDP addr").port(), port);
    }

    #[tokio::test]
    async fn tcp_ipv4_and_ipv6_any_can_share_a_port() {
        let ipv4 = bind_tcp_listener("0.0.0.0:0").expect("bind IPv4 TCP listener");
        let port = ipv4.local_addr().expect("read IPv4 TCP addr").port();
        let ipv6 = bind_tcp_listener(&format!("[::]:{port}")).expect("bind IPv6 TCP listener");

        assert_eq!(ipv6.local_addr().expect("read IPv6 TCP addr").port(), port);
    }

    #[test]
    fn asn_db_cache_path_prefers_xdg() {
        assert_eq!(
            asn_db_cache_path(Some("/tmp/xdg"), Some("/home/u")),
            PathBuf::from("/tmp/xdg/redns/origin-asn.mmdb")
        );
    }

    #[test]
    fn asn_db_cache_path_falls_back_to_home() {
        assert_eq!(
            asn_db_cache_path(None, Some("/home/u")),
            PathBuf::from("/home/u/.cache/redns/origin-asn.mmdb")
        );
    }

    #[test]
    fn asn_db_cache_path_ignores_blank_dirs_and_defaults_to_cwd() {
        assert_eq!(
            asn_db_cache_path(Some("   "), None),
            PathBuf::from("origin-asn.mmdb")
        );
        assert_eq!(asn_db_cache_path(None, None), PathBuf::from("origin-asn.mmdb"));
    }

    #[test]
    fn config_uses_asn_detects_sequence_rule() {
        let cfg: redns_core::Config = serde_saphyr::from_str(
            "plugins:\n  - type: sequence\n    args:\n      - matches: asn 13335\n        exec: accept\n",
        )
        .unwrap();
        assert!(config_uses_asn(&cfg));
    }

    #[test]
    fn config_uses_asn_detects_reverse_rule() {
        let cfg: redns_core::Config = serde_saphyr::from_str(
            "plugins:\n  - type: sequence\n    args:\n      - matches: '!asn 13335'\n        exec: accept\n",
        )
        .unwrap();
        assert!(config_uses_asn(&cfg));
    }

    #[test]
    fn config_uses_asn_detects_named_plugin() {
        let cfg: redns_core::Config = serde_saphyr::from_str(
            "plugins:\n  - tag: my_asn\n    type: asn\n    args: '13335'\n",
        )
        .unwrap();
        assert!(config_uses_asn(&cfg));
    }

    #[test]
    fn config_uses_asn_false_without_asn() {
        let cfg: redns_core::Config = serde_saphyr::from_str(
            "plugins:\n  - type: sequence\n    args:\n      - matches: qtype 1\n        exec: accept\n",
        )
        .unwrap();
        assert!(!config_uses_asn(&cfg));

        let empty: redns_core::Config = serde_saphyr::from_str("log:\n  level: info\n").unwrap();
        assert!(!config_uses_asn(&empty));
    }
}
