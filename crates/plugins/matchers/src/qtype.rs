// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.
//
// redns is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// redns is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

//! Matches on DNS query type (e.g. A, AAAA, MX).

use redns_core::plugin::PluginResult;
use redns_core::{Context, Matcher};
use std::collections::HashSet;

/// Matches if the query type is in the allowed set.
#[derive(Debug, Clone)]
pub struct QTypeMatcher {
    allowed: HashSet<u16>,
}

impl QTypeMatcher {
    /// Creates a new matcher from a set of allowed record type values.
    pub fn new(types: impl IntoIterator<Item = u16>) -> Self {
        Self {
            allowed: types.into_iter().collect(),
        }
    }

    /// Parses a space-separated string of integer type codes.
    pub fn from_str_args(s: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let types = s
            .split_whitespace()
            .map(|t| t.parse::<u16>())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self::new(types))
    }
}

impl Matcher for QTypeMatcher {
    fn match_ctx(&self, ctx: &Context) -> PluginResult<bool> {
        if let Some(q) = ctx.question() {
            Ok(self.allowed.contains(&q.query_type().into()))
        } else {
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode, Query};
    use hickory_proto::rr::{Name, RecordType};

    fn make_query(rtype: RecordType) -> Context {
        let mut msg = Message::new();
        msg.set_id(1)
            .set_message_type(MessageType::Query)
            .set_op_code(OpCode::Query);
        msg.add_query({
            let mut q = Query::new();
            q.set_name(Name::from_ascii("example.com.").unwrap())
                .set_query_type(rtype);
            q
        });
        Context::new(msg)
    }

    #[test]
    fn matches_allowed_type() {
        let m = QTypeMatcher::new([1]); // A = 1
        let ctx = make_query(RecordType::A);
        assert!(m.match_ctx(&ctx).unwrap());
    }

    #[test]
    fn rejects_disallowed_type() {
        let m = QTypeMatcher::new([28]); // AAAA = 28
        let ctx = make_query(RecordType::A);
        assert!(!m.match_ctx(&ctx).unwrap());
    }

    #[test]
    fn from_str_args() {
        let m = QTypeMatcher::from_str_args("1 28").unwrap();
        assert!(m.match_ctx(&make_query(RecordType::A)).unwrap());
        assert!(m.match_ctx(&make_query(RecordType::AAAA)).unwrap());
    }
}
