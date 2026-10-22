// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! CNAME matcher — matches CNAME targets in DNS response against domain set.

use crate::domain_set::DomainTrie;
use redns_core::plugin::PluginResult;
use redns_core::{Context, Matcher};
use std::collections::HashSet;

/// Matches CNAME record targets in the DNS response.
#[derive(Debug, Clone)]
pub struct CnameMatcher {
    exact: HashSet<Box<str>>,
    /// Suffix patterns (leading `.`), lowercased and trimmed of trailing
    /// dots at load; matched on label boundaries via the domain trie.
    suffixes: DomainTrie,
    /// A pattern consisting only of dots (e.g. `.`) matches every name.
    match_all: bool,
}

impl CnameMatcher {
    pub fn new() -> Self {
        Self {
            exact: HashSet::new(),
            suffixes: DomainTrie::new(),
            match_all: false,
        }
    }

    pub fn add_pattern(&mut self, pattern: &str) {
        let p = pattern.trim().to_lowercase();
        if p.is_empty() {
            return;
        }
        if let Some(stripped) = p.strip_prefix('.') {
            let suffix = stripped.trim_end_matches('.');
            if suffix.is_empty() {
                self.match_all = true;
            } else {
                self.suffixes.insert(suffix);
            }
        } else {
            self.exact.insert(p.into_boxed_str());
        }
    }

    pub fn from_str_args(s: &str) -> Self {
        let mut m = Self::new();
        for part in s.split_whitespace() {
            m.add_pattern(part);
        }
        m
    }

    fn matches_name(&self, name: &hickory_proto::rr::Name) -> bool {
        if self.match_all {
            return true;
        }
        if self.suffixes.matches_labels(name.iter()) {
            return true;
        }
        // Exact patterns compare whole strings; only build the lowercase
        // name when exact patterns are configured.
        if !self.exact.is_empty() {
            let name_lower = name.to_ascii().to_lowercase();
            if self.exact.contains(name_lower.as_str()) {
                return true;
            }
        }
        false
    }
}

impl Default for CnameMatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl Matcher for CnameMatcher {
    fn match_ctx(&self, ctx: &Context) -> PluginResult<bool> {
        let resp = match ctx.response() {
            Some(r) => r,
            None => return Ok(false),
        };
        for rr in &resp.answers {
            if rr.record_type() == hickory_proto::rr::RecordType::CNAME {
                let rdata = &rr.data;
                if let hickory_proto::rr::RData::CNAME(cname) = rdata
                    && self.matches_name(&cname.0)
                {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode, Query, ResponseCode};
    use hickory_proto::rr::{Name, RData, Record, RecordType};

    fn make_ctx_with_cname(target: &str) -> Context {
        let mut msg = Message::new(1, MessageType::Query, OpCode::Query);
        msg.add_query({
            let mut q = Query::new();
            q.set_name(Name::from_ascii("alias.com.").unwrap())
                .set_query_type(RecordType::A);
            q
        });
        let mut ctx = Context::new(msg);
        let mut resp = Message::response(1, OpCode::Query);
        resp.metadata.response_code = ResponseCode::NoError;
        resp.add_answer(Record::from_rdata(
            Name::from_ascii("alias.com.").unwrap(),
            300,
            RData::CNAME(hickory_proto::rr::rdata::CNAME(
                Name::from_ascii(target).unwrap(),
            )),
        ));
        ctx.set_response(Some(resp));
        ctx
    }

    #[test]
    fn exact_cname_match() {
        let m = CnameMatcher::from_str_args("cdn.example.com.");
        assert!(
            m.match_ctx(&make_ctx_with_cname("cdn.example.com."))
                .unwrap()
        );
        assert!(!m.match_ctx(&make_ctx_with_cname("other.com.")).unwrap());
    }

    #[test]
    fn suffix_cname_match() {
        let m = CnameMatcher::from_str_args(".cdn.example.com.");
        assert!(
            m.match_ctx(&make_ctx_with_cname("a.cdn.example.com."))
                .unwrap()
        );
        assert!(!m.match_ctx(&make_ctx_with_cname("other.com.")).unwrap());
    }

    #[test]
    fn no_response_returns_false() {
        let m = CnameMatcher::from_str_args("anything.com.");
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
    fn suffix_match_respects_label_boundaries() {
        let m = CnameMatcher::from_str_args(".cdn.example.com.");
        assert!(
            m.match_ctx(&make_ctx_with_cname("cdn.example.com."))
                .unwrap()
        );
        assert!(
            m.match_ctx(&make_ctx_with_cname("a.cdn.example.com."))
                .unwrap()
        );
        // A bare string suffix without a label boundary must not match.
        assert!(
            !m.match_ctx(&make_ctx_with_cname("xcdn.example.com."))
                .unwrap()
        );
    }

    #[test]
    fn dot_only_pattern_matches_everything() {
        // Legacy behavior: a `.` pattern trims to an empty suffix, which
        // every name ends with.
        let m = CnameMatcher::from_str_args(".");
        assert!(m.match_ctx(&make_ctx_with_cname("anything.net.")).unwrap());
    }
}
