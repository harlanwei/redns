// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Environment variable matcher.
//!
//! Checks if an env var exists (or has a specific value) at build time.
//! Always returns true or false — evaluated eagerly when the matcher is created.

use redns_core::plugin::PluginResult;
use redns_core::{Context, Matcher};

/// Env matcher — checks environment variables.
///
/// Evaluated at construction time, not at match time.
pub struct EnvMatcher {
    result: bool,
}

impl EnvMatcher {
    /// Create from string args: `KEY` or `KEY VALUE`.
    ///
    /// - `KEY` → match if env var is set (any value)
    /// - `KEY VALUE` → match if env var equals VALUE
    pub fn from_str_args(s: &str) -> Self {
        let fields: Vec<&str> = s.split_whitespace().collect();
        let result = match fields.len() {
            0 => false,
            1 => std::env::var(fields[0]).is_ok(),
            _ => std::env::var(fields[0])
                .map(|v| v == fields[1])
                .unwrap_or(false),
        };
        Self { result }
    }
}

impl Matcher for EnvMatcher {
    fn match_ctx(&self, _ctx: &Context) -> PluginResult<bool> {
        Ok(self.result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode, Query};
    use hickory_proto::rr::{Name, RecordType};

    fn make_ctx() -> Context {
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
        Context::new(msg)
    }

    #[test]
    fn env_not_set_returns_false() {
        let m = EnvMatcher::from_str_args("redns_TEST_NONEXISTENT_VAR_12345");
        assert!(!m.match_ctx(&make_ctx()).unwrap());
    }

    #[test]
    fn env_set_returns_true() {
        // PATH should exist on all systems.
        let m = EnvMatcher::from_str_args("PATH");
        assert!(m.match_ctx(&make_ctx()).unwrap());
    }

    #[test]
    fn empty_args_returns_false() {
        let m = EnvMatcher::from_str_args("");
        assert!(!m.match_ctx(&make_ctx()).unwrap());
    }
}
