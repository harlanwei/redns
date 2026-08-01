// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! ASN matcher — checks A/AAAA response addresses against an ASN database.
//!
//! # Configuration
//!
//! The matcher needs an ASN → network database in MaxMind DB format. When the
//! config uses the `asn` matcher but does **not** set `asn_db`, redns
//! downloads the default `origin-asn.mmdb` database (sapics/ip-location-db)
//! on first use and caches it under `$XDG_CACHE_HOME/redns/` (falling back
//! to `~/.cache/redns/`). Alternatively, point the top-level `asn_db` config
//! key at one or more MaxMind DB files:
//!
//! ```yaml
//! asn_db:
//!   - /etc/redns/origin-asn.mmdb
//!
//! plugins:
//!   - type: sequence
//!     args:
//!       - exec: $forward
//!       - matches: asn 13335
//!         exec: use-answer-of www.example.com
//!       - exec: $forward
//! ```
//!
//! `matches: asn 13335` is `true` when at least one A/AAAA answer in the
//! current response belongs to AS 13335. Multiple ASNs may be listed
//! (`asn 13335 15169`); any listed ASN owning any answer matches. When the
//! query has no response yet, or all A/AAAA answers belong to other ASNs, the
//! matcher is `false`.

use maxminddb::geoip2;
use redns_core::plugin::PluginResult;
use redns_core::{Context, Matcher};
use std::net::IpAddr;
use std::sync::Arc;
use tracing::info;

/// Byte sequence that marks the metadata section of a MaxMind DB file.
const MMDB_MARKER: [u8; 14] = *b"\xab\xcd\xefMaxMind.com";

/// In-memory ASN → network database.
///
/// Holds one or more MaxMind DB files (e.g. sapics/ip-location-db's
/// `origin-asn.mmdb`) queried via [`maxminddb::Reader`] on demand. Multiple
/// loaded files are combined with union semantics. Each lookup is a tree
/// walk — no linear scan over the whole database.
#[derive(Default)]
pub struct AsnDb {
    mmdb: Vec<maxminddb::Reader<Vec<u8>>>,
}

impl AsnDb {
    pub fn new() -> Self {
        Self::default()
    }

    /// Loads every file in `paths`. Errors if a file cannot be read or is
    /// not a valid MaxMind DB.
    pub fn load_files(
        &mut self,
        paths: &[String],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for path in paths {
            self.load_file(path)?;
        }
        Ok(())
    }

    /// Loads a single MaxMind DB file. The database must carry ASN records
    /// (`autonomous_system_number`), like any GeoIP2/GeoLite2 or sapics
    /// `*-asn` database does.
    pub fn load_file(
        &mut self,
        path: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let bytes = std::fs::read(path).map_err(
            |e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("failed to read ASN database '{path}': {e}").into()
            },
        )?;
        if !is_maxmind_db(&bytes) && !path.ends_with(".mmdb") {
            return Err(format!(
                "'{path}' is not a MaxMind DB file — the `asn` matcher requires a MaxMind DB such as origin-asn.mmdb from sapics/ip-location-db"
            )
            .into());
        }
        self.load_mmdb_bytes(bytes)
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("ASN database '{path}': {e}").into()
            })
    }

    /// Loads a MaxMind DB from its raw bytes (e.g. `origin-asn.mmdb` from
    /// sapics/ip-location-db). The database must carry ASN records
    /// (`autonomous_system_number`), like any GeoIP2/GeoLite2 or sapics
    /// `*-asn` database does.
    pub fn load_mmdb_bytes(
        &mut self,
        bytes: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let reader = maxminddb::Reader::from_source(bytes)
            .map_err(|e| format!("invalid MaxMind DB data: {e}"))?;
        info!(
            database_type = %reader.metadata().database_type,
            ip_version = reader.metadata().ip_version,
            "ASN database loaded from MaxMind DB"
        );
        self.mmdb.push(reader);
        Ok(())
    }

    /// Returns `true` when `addr` belongs to a network owned by `asn`
    /// according to any of the loaded MaxMind DB files.
    pub fn contains(&self, asn: u32, addr: IpAddr) -> bool {
        self.mmdb.iter().any(|reader| {
            match reader.lookup(addr) {
                Ok(result) => match result.decode::<geoip2::Asn>() {
                    Ok(Some(record)) => record.autonomous_system_number == Some(asn),
                    _ => false,
                },
                Err(_) => false,
            }
        })
    }
}

/// Returns `true` when the byte slice looks like a MaxMind DB (the metadata
/// marker appears somewhere in the file, normally right before the trailing
/// metadata section).
fn is_maxmind_db(bytes: &[u8]) -> bool {
    bytes.windows(14).any(|w| w == &MMDB_MARKER[..])
}

/// Matches when any A/AAAA answer in the response belongs to one of the
/// configured ASNs.
pub struct AsnMatcher {
    db: Arc<AsnDb>,
    asns: Vec<u32>,
}

impl AsnMatcher {
    pub fn new(db: Arc<AsnDb>, asns: Vec<u32>) -> Self {
        Self { db, asns }
    }

    /// Parses matcher args: whitespace-separated ASN numbers, optionally with
    /// an `AS` prefix (e.g. `"13335 15169"` or `"AS13335"`).
    pub fn from_str_args(s: &str, db: Arc<AsnDb>) -> PluginResult<Self> {
        let s = s.trim();
        if s.is_empty() {
            return Err(
                "asn matcher expects at least one ASN number, e.g. `asn 13335`".into(),
            );
        }
        let mut asns = Vec::new();
        for token in s.split_whitespace() {
            let asn = parse_asn(token).ok_or_else(
                || -> Box<dyn std::error::Error + Send + Sync> {
                    format!("asn matcher: invalid ASN '{token}'").into()
                },
            )?;
            asns.push(asn);
        }
        Ok(Self { db, asns })
    }
}

impl Matcher for AsnMatcher {
    fn match_ctx(&self, ctx: &Context) -> PluginResult<bool> {
        let resp = match ctx.response() {
            Some(r) => r,
            None => return Ok(false),
        };
        for rr in &resp.answers {
            let ip: Option<IpAddr> = match rr.data {
                hickory_proto::rr::RData::A(a) => Some(IpAddr::V4(a.0)),
                hickory_proto::rr::RData::AAAA(aaaa) => Some(IpAddr::V6(aaaa.0)),
                _ => None,
            };
            if let Some(ip) = ip
                && self.asns.iter().any(|&asn| self.db.contains(asn, ip))
            {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// Parses an ASN token: `13335` or `AS13335` (case-insensitive prefix).
fn parse_asn(s: &str) -> Option<u32> {
    let s = s.trim();
    let s = s
        .strip_prefix("AS")
        .or_else(|| s.strip_prefix("as"))
        .unwrap_or(s);
    s.parse::<u32>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode, Query, ResponseCode};
    use hickory_proto::rr::{Name, RData, Record, RecordType};

    fn make_ctx_with_resp(ips: &[IpAddr]) -> Context {
        let mut msg = Message::new(1, MessageType::Query, OpCode::Query);
        msg.add_query({
            let mut q = Query::new();
            q.set_name(Name::from_ascii("example.com.").unwrap())
                .set_query_type(RecordType::A);
            q
        });
        let mut ctx = Context::new(msg);
        let mut resp = Message::response(1, OpCode::Query);
        resp.metadata.response_code = ResponseCode::NoError;
        for ip in ips {
            let rdata = match ip {
                IpAddr::V4(v4) => RData::A((*v4).into()),
                IpAddr::V6(v6) => RData::AAAA((*v6).into()),
            };
            resp.add_answer(Record::from_rdata(
                Name::from_ascii("example.com.").unwrap(),
                300,
                rdata,
            ));
        }
        ctx.set_response(Some(resp));
        ctx
    }

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    /// Builds a minimal valid MaxMind DB (record_size 24, node_count 1) whose
    /// single data record is `{"autonomous_system_number": 13335}`. Every
    /// address (v4 and v6) resolves to that record.
    ///
    /// Layout: 6-byte search tree, 16-byte separator, 12-byte data section,
    /// then the metadata marker and map. Both tree records hold the data
    /// pointer 17 = node_count(1) + 16 (separator) + 0 (data offset).
    fn tiny_asn_mmdb() -> Vec<u8> {
        let mut out = Vec::new();
        // Search tree: one node; record 0 (bit 0) and record 1 (bit 1) both
        // point at the single data record.
        out.extend_from_slice(&[0x00, 0x00, 0x11, 0x00, 0x00, 0x11]);
        // Data section separator.
        out.extend_from_slice(&[0x00; 8]);
        out.extend_from_slice(&[0xff; 8]);
        // Data section: map { "autonomous_system_number": 13335 }.
        out.push(0xE1); // map, 1 entry
        out.push(0x58); // utf8 string, 24 bytes
        out.extend_from_slice(b"autonomous_system_number");
        out.push(0xC4); // uint32, 4 bytes
        out.extend_from_slice(&13335u32.to_be_bytes());
        // Metadata section.
        out.extend_from_slice(b"\xab\xcd\xefMaxMind.com");
        out.push(0xE9); // map, 9 entries
        // Keys must appear in struct declaration order (serde derive reads
        // fields in order from the map).
        out.push(0x5B); // utf8, 27 bytes
        out.extend_from_slice(b"binary_format_major_version");
        out.extend_from_slice(&[0xA2, 0x00, 0x02]); // uint16, 2 bytes → 2
        out.push(0x5B); // utf8, 27 bytes
        out.extend_from_slice(b"binary_format_minor_version");
        out.extend_from_slice(&[0xA1, 0x00]); // uint16, 1 byte → 0
        out.push(0x4B); // utf8, 11 bytes
        out.extend_from_slice(b"build_epoch");
        out.extend_from_slice(&[0x08, 0x02]); // extended type 9 (uint64), 8 bytes
        out.extend_from_slice(&[0u8; 8]); // 0
        out.push(0x4D); // utf8, 13 bytes
        out.extend_from_slice(b"database_type");
        out.push(0x43); // utf8, 3 bytes
        out.extend_from_slice(b"asn");
        out.push(0x4B); // utf8, 11 bytes
        out.extend_from_slice(b"description");
        out.push(0xE0); // map, 0 entries
        out.push(0x4A); // utf8, 10 bytes
        out.extend_from_slice(b"ip_version");
        out.extend_from_slice(&[0xA1, 0x06]); // uint16, 1 byte → 6
        out.push(0x49); // utf8, 9 bytes
        out.extend_from_slice(b"languages");
        out.extend_from_slice(&[0x00, 0x04]); // extended type 11 (array), 0 entries
        out.push(0x4A); // utf8, 10 bytes
        out.extend_from_slice(b"node_count");
        out.extend_from_slice(&[0xC4, 0x00, 0x00, 0x00, 0x01]); // uint32, 4 bytes → 1
        out.push(0x4B); // utf8, 11 bytes
        out.extend_from_slice(b"record_size");
        out.extend_from_slice(&[0xA1, 0x18]); // uint16, 1 byte → 24
        out
    }

    #[test]
    fn marks_asn_owner_for_v4_and_v6() {
        let mut db = AsnDb::new();
        db.load_mmdb_bytes(tiny_asn_mmdb()).unwrap();
        // Every address maps to ASN 13335.
        assert!(db.contains(13335, ip("1.2.3.4")));
        assert!(db.contains(13335, ip("8.8.8.8")));
        assert!(db.contains(13335, ip("2001:db8::1")));
        assert!(db.contains(13335, ip("::1")));
        assert!(db.contains(13335, ip("0.0.0.0")));
        // Other ASNs never match.
        assert!(!db.contains(15169, ip("1.2.3.4")));
        assert!(!db.contains(99999, ip("::1")));
    }

    #[test]
    fn multiple_databases_union() {
        let mut db = AsnDb::new();
        db.load_mmdb_bytes(tiny_asn_mmdb()).unwrap();
        db.load_mmdb_bytes(tiny_asn_mmdb()).unwrap();
        // Both loaded copies agree; unknown ASNs still never match.
        assert!(db.contains(13335, ip("1.2.3.4")));
        assert!(!db.contains(15169, ip("1.2.3.4")));
    }

    #[test]
    fn rejects_garbage_bytes() {
        let mut db = AsnDb::new();
        assert!(db.load_mmdb_bytes(vec![0xde, 0xad, 0xbe, 0xef]).is_err());
    }

    #[test]
    fn load_file_loads_mmdb_and_rejects_tsv() {
        let dir = std::env::temp_dir().join(format!("redns-asn-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mmdb_path = dir.join("asn.mmdb");
        std::fs::write(&mmdb_path, tiny_asn_mmdb()).unwrap();
        let tsv_path = dir.join("asn.tsv");
        std::fs::write(&tsv_path, "1.0.0.0 1.0.0.255 13335\n").unwrap();

        let mut db = AsnDb::new();
        db.load_file(&mmdb_path.to_string_lossy()).unwrap();
        assert!(db.contains(13335, ip("9.9.9.9")));
        // TSV files are no longer supported and must be rejected loudly.
        assert!(db.load_file(&tsv_path.to_string_lossy()).is_err());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn matcher_matches_answers_in_asn() {
        let mut db = AsnDb::new();
        db.load_mmdb_bytes(tiny_asn_mmdb()).unwrap();
        let db = Arc::new(db);
        // The tiny database maps every address to ASN 13335.
        let m = AsnMatcher::from_str_args("13335", db.clone()).unwrap();
        assert!(m.match_ctx(&make_ctx_with_resp(&[ip("1.0.0.1")])).unwrap());
        assert!(m.match_ctx(&make_ctx_with_resp(&[ip("8.8.8.8")])).unwrap());
        // An ASN absent from the database never matches.
        let m2 = AsnMatcher::from_str_args("15169", db).unwrap();
        assert!(!m2.match_ctx(&make_ctx_with_resp(&[ip("1.0.0.1")])).unwrap());
    }

    #[test]
    fn matcher_accepts_as_prefix() {
        let mut db = AsnDb::new();
        db.load_mmdb_bytes(tiny_asn_mmdb()).unwrap();
        let m = AsnMatcher::from_str_args("AS13335", Arc::new(db)).unwrap();
        assert!(m.match_ctx(&make_ctx_with_resp(&[ip("1.0.0.1")])).unwrap());
    }

    #[test]
    fn matcher_matches_any_listed_asn() {
        let mut db = AsnDb::new();
        db.load_mmdb_bytes(tiny_asn_mmdb()).unwrap();
        let db = Arc::new(db);
        // IP owned by the first listed ASN matches.
        let m = AsnMatcher::from_str_args("13335 15169", db.clone()).unwrap();
        assert!(m.match_ctx(&make_ctx_with_resp(&[ip("1.0.0.1")])).unwrap());
        // None of the listed ASNs is present in the database: no match.
        let m2 = AsnMatcher::from_str_args("15169 99999", db).unwrap();
        assert!(!m2.match_ctx(&make_ctx_with_resp(&[ip("9.9.9.9")])).unwrap());
    }

    #[test]
    fn matcher_matches_ipv6_answers() {
        let mut db = AsnDb::new();
        db.load_mmdb_bytes(tiny_asn_mmdb()).unwrap();
        let db = Arc::new(db);
        let m = AsnMatcher::from_str_args("13335", db.clone()).unwrap();
        assert!(m.match_ctx(&make_ctx_with_resp(&[ip("2001:db8::1")])).unwrap());
        let m2 = AsnMatcher::from_str_args("15169", db).unwrap();
        assert!(!m2.match_ctx(&make_ctx_with_resp(&[ip("2001:db8::1")])).unwrap());
    }

    #[test]
    fn matcher_no_response_is_false() {
        let m = AsnMatcher::from_str_args("13335", Arc::new(AsnDb::new())).unwrap();
        let mut msg = Message::new(1, MessageType::Query, OpCode::Query);
        msg.add_query({
            let mut q = Query::new();
            q.set_name(Name::from_ascii("example.com.").unwrap())
                .set_query_type(RecordType::A);
            q
        });
        let ctx = Context::new(msg);
        assert!(!m.match_ctx(&ctx).unwrap());
    }

    #[test]
    fn matcher_rejects_bad_args() {
        assert!(AsnMatcher::from_str_args("", Arc::new(AsnDb::new())).is_err());
        assert!(AsnMatcher::from_str_args("abc", Arc::new(AsnDb::new())).is_err());
        assert!(AsnMatcher::from_str_args("13335 abc", Arc::new(AsnDb::new())).is_err());
    }
}
