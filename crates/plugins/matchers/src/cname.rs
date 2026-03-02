// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! CNAME matcher — matches CNAME targets in DNS response against domain set.

use redns_core::plugin::PluginResult;
use redns_core::{Context, Matcher};
use std::collections::HashSet;

/// Matches CNAME record targets in the DNS response.
#[derive(Debug, Clone)]
pub struct CnameMatcher {
    exact: HashSet<String>,
    suffixes: Vec<String>,
}

impl CnameMatcher {
    pub fn new() -> Self {
        Self {
            exact: HashSet::new(),
            suffixes: Vec::new(),
        }
    }

    pub fn add_pattern(&mut self, pattern: &str) {
        let p = pattern.trim().to_lowercase();
        if p.is_empty() {
            return;
        }
        if p.starts_with('.') {
            self.suffixes.push(p[1..].to_string());
        } else {
            self.exact.insert(p);
        }
    }

    pub fn from_str_args(s: &str) -> Self {
        let mut m = Self::new();
        for part in s.split_whitespace() {
            m.add_pattern(part);
        }
        m
    }

    fn matches_name(&self, name: &str) -> bool {
        let name_lower = name.to_lowercase();
        if self.exact.contains(&name_lower) {
            return true;
        }
        for suffix in &self.suffixes {
            if name_lower.ends_with(suffix)
                || name_lower
                    .trim_end_matches('.')
                    .ends_with(suffix.trim_end_matches('.'))
            {
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
        for rr in resp.answers() {
            if rr.record_type() == hickory_proto::rr::RecordType::CNAME {
                let rdata = rr.data();
                if let hickory_proto::rr::RData::CNAME(cname) = rdata {
                    if self.matches_name(&cname.0.to_ascii()) {
                        return Ok(true);
                    }
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
        let mut msg = Message::new();
        msg.set_id(1)
            .set_message_type(MessageType::Query)
            .set_op_code(OpCode::Query);
        msg.add_query({
            let mut q = Query::new();
            q.set_name(Name::from_ascii("alias.com.").unwrap())
                .set_query_type(RecordType::A);
            q
        });
        let mut ctx = Context::new(msg);
        let mut resp = Message::new();
        resp.set_id(1)
            .set_message_type(MessageType::Response)
            .set_response_code(ResponseCode::NoError);
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
        let mut msg = Message::new();
        msg.set_id(1)
            .set_message_type(MessageType::Query)
            .set_op_code(OpCode::Query);
        msg.add_query({
            let mut q = Query::new();
            q.set_name(Name::from_ascii("example.com.").unwrap())
                .set_query_type(RecordType::A);
            q
        });
        let ctx = Context::new(msg);
        assert!(!m.match_ctx(&ctx).unwrap());
    }
}
