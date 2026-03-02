// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Qname matcher — matches query name against a domain set.

use crate::domain_set::DomainSet;
use redns_core::plugin::PluginResult;
use redns_core::{Context, Matcher};

/// Matches if the query name is in the domain set.
///
/// Delegates to [`DomainSet`] for actual matching.
pub struct QnameMatcher {
    ds: DomainSet,
}

impl QnameMatcher {
    pub fn new() -> Self {
        Self {
            ds: DomainSet::new(),
        }
    }

    /// Parse from a quick-setup string.
    ///
    /// Format:
    /// - `&path` — load domain list from file
    /// - plain expression — domain expression (passed to `DomainSet::add_expression`)
    ///
    /// Examples:
    /// ```text
    /// "google.com facebook.com"          — subdomain match (default)
    /// "domain:google.com full:exact.com" — typed expressions
    /// "&blocklist.txt domain:extra.com"  — file + inline
    /// ```
    pub fn from_str_args(s: &str) -> PluginResult<Self> {
        let mut ds = DomainSet::new();
        for part in s.split_whitespace() {
            if let Some(path) = part.strip_prefix('&') {
                ds.load_file(path)?;
            } else {
                if let Err(e) = ds.add_expression(part) {
                    tracing::warn!(error = %e, exp = part, "skipping invalid domain expression");
                }
            }
        }
        Ok(Self { ds })
    }
}

impl Default for QnameMatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl Matcher for QnameMatcher {
    fn match_ctx(&self, ctx: &Context) -> PluginResult<bool> {
        self.ds.match_ctx(ctx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode, Query};
    use hickory_proto::rr::{Name, RecordType};

    fn make_ctx(name: &str) -> Context {
        let mut msg = Message::new();
        msg.set_id(1)
            .set_message_type(MessageType::Query)
            .set_op_code(OpCode::Query);
        msg.add_query({
            let mut q = Query::new();
            q.set_name(Name::from_ascii(name).unwrap())
                .set_query_type(RecordType::A);
            q
        });
        Context::new(msg)
    }

    #[test]
    fn plain_domain_is_subdomain_match() {
        let m = QnameMatcher::from_str_args("google.com").unwrap();
        // Matches the domain itself and subdomains.
        assert!(m.match_ctx(&make_ctx("google.com.")).unwrap());
        assert!(m.match_ctx(&make_ctx("www.google.com.")).unwrap());
        assert!(m.match_ctx(&make_ctx("sub.www.google.com.")).unwrap());
        // Does not match unrelated domains.
        assert!(!m.match_ctx(&make_ctx("notgoogle.com.")).unwrap());
    }

    #[test]
    fn typed_expressions_work() {
        let m = QnameMatcher::from_str_args("full:exact.com keyword:goo").unwrap();
        // full: only matches exact
        assert!(m.match_ctx(&make_ctx("exact.com.")).unwrap());
        assert!(!m.match_ctx(&make_ctx("sub.exact.com.")).unwrap());
        // keyword: matches substring
        assert!(m.match_ctx(&make_ctx("www.google.com.")).unwrap());
    }

    #[test]
    fn multiple_domains() {
        let m = QnameMatcher::from_str_args("google.com facebook.com").unwrap();
        assert!(m.match_ctx(&make_ctx("www.google.com.")).unwrap());
        assert!(m.match_ctx(&make_ctx("facebook.com.")).unwrap());
        assert!(!m.match_ctx(&make_ctx("twitter.com.")).unwrap());
    }

    #[test]
    fn file_loading() {
        use std::io::Write;
        let dir = std::env::temp_dir().join("redns_qname_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("domains.txt");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "# comment").unwrap();
        writeln!(f, "example.com").unwrap();
        writeln!(f, "domain:test.org").unwrap();
        drop(f);

        let arg = format!("&{}", path.display());
        let m = QnameMatcher::from_str_args(&arg).unwrap();
        assert!(m.match_ctx(&make_ctx("example.com.")).unwrap());
        assert!(m.match_ctx(&make_ctx("sub.example.com.")).unwrap());
        assert!(m.match_ctx(&make_ctx("test.org.")).unwrap());
        assert!(!m.match_ctx(&make_ctx("other.com.")).unwrap());

        std::fs::remove_dir_all(&dir).ok();
    }
}
