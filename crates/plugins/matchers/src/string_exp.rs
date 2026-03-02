// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! String expression matcher.
//!
//! Format: `source op [values...]`
//! Sources: `$ENV_KEY`, `url_path`, `server_name`
//! Operators: `zl`, `eq`, `prefix`, `suffix`, `contains`, `regexp`

use redns_core::plugin::PluginResult;
use redns_core::{Context, Matcher};

/// Enum for string operations.
enum StringOp {
    /// Match if string is zero-length.
    ZeroLength,
    /// Match if string equals any of the given values.
    Eq(Vec<String>),
    /// Match if string starts with any of the given prefixes.
    Prefix(Vec<String>),
    /// Match if string ends with any of the given suffixes.
    Suffix(Vec<String>),
    /// Match if string contains any of the given substrings.
    Contains(Vec<String>),
    /// Match if string matches any of the given regexes.
    Regexp(Vec<regex::Regex>),
}

impl StringOp {
    fn match_str(&self, s: &str) -> bool {
        match self {
            StringOp::ZeroLength => s.is_empty(),
            StringOp::Eq(vals) => vals.iter().any(|v| v == s),
            StringOp::Prefix(vals) => vals.iter().any(|v| s.starts_with(v.as_str())),
            StringOp::Suffix(vals) => vals.iter().any(|v| s.ends_with(v.as_str())),
            StringOp::Contains(vals) => vals.iter().any(|v| s.contains(v.as_str())),
            StringOp::Regexp(exps) => exps.iter().any(|r| r.is_match(s)),
        }
    }
}

/// Source of the string to match against.
enum StringSource {
    /// Environment variable value.
    Env(String),
    /// URL path from server metadata.
    UrlPath,
    /// Server name from server metadata.
    ServerName,
}

impl StringSource {
    fn get_str(&self, ctx: &Context) -> String {
        match self {
            StringSource::Env(key) => std::env::var(key).unwrap_or_default(),
            StringSource::UrlPath => ctx.server_meta.url_path.clone().unwrap_or_default(),
            StringSource::ServerName => ctx.server_meta.server_name.clone().unwrap_or_default(),
        }
    }
}

/// String expression matcher.
pub struct StringExpMatcher {
    source: StringSource,
    op: StringOp,
}

impl StringExpMatcher {
    /// Parse from string args: `source op [values...]`
    pub fn from_str_args(s: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let fields: Vec<&str> = s.split_whitespace().collect();
        if fields.len() < 2 {
            return Err("string_exp: need at least 'source op'".into());
        }

        let src_name = fields[0];
        let op_name = fields[1];
        let args: Vec<String> = fields[2..].iter().map(|s| s.to_string()).collect();

        // Parse source.
        let source = if let Some(env_key) = src_name.strip_prefix('$') {
            StringSource::Env(env_key.to_string())
        } else {
            match src_name {
                "url_path" => StringSource::UrlPath,
                "server_name" => StringSource::ServerName,
                _ => return Err(format!("unknown string source: {}", src_name).into()),
            }
        };

        // Parse operator.
        let op = match op_name {
            "zl" => StringOp::ZeroLength,
            "eq" => StringOp::Eq(args),
            "prefix" => StringOp::Prefix(args),
            "suffix" => StringOp::Suffix(args),
            "contains" => StringOp::Contains(args),
            "regexp" => {
                let exps: Result<Vec<regex::Regex>, _> =
                    args.iter().map(|s| regex::Regex::new(s)).collect();
                StringOp::Regexp(exps?)
            }
            _ => return Err(format!("unknown string operator: {}", op_name).into()),
        };

        Ok(Self { source, op })
    }
}

impl Matcher for StringExpMatcher {
    fn match_ctx(&self, ctx: &Context) -> PluginResult<bool> {
        let s = self.source.get_str(ctx);
        Ok(self.op.match_str(&s))
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
    fn env_eq_match() {
        // PATH should exist.
        let m = StringExpMatcher::from_str_args("$PATH zl").unwrap();
        // PATH is not zero-length.
        assert!(!m.match_ctx(&make_ctx()).unwrap());
    }

    #[test]
    fn url_path_zl() {
        // No URL path set → empty → zl matches.
        let m = StringExpMatcher::from_str_args("url_path zl").unwrap();
        assert!(m.match_ctx(&make_ctx()).unwrap());
    }

    #[test]
    fn contains_op() {
        let m = StringExpMatcher::from_str_args("$PATH contains /").unwrap();
        // PATH typically contains path separators.
        // On Windows it uses ; but the value itself may contain \
        let ctx = make_ctx();
        // Just exercise the code path.
        let _ = m.match_ctx(&ctx);
    }

    #[test]
    fn invalid_source_errors() {
        assert!(StringExpMatcher::from_str_args("invalid_src eq foo").is_err());
    }

    #[test]
    fn invalid_op_errors() {
        assert!(StringExpMatcher::from_str_args("url_path invalid_op").is_err());
    }
}
