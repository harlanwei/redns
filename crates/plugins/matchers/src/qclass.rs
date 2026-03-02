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

//! Matches on DNS query class (e.g. IN, CH, HS).

use redns_core::plugin::PluginResult;
use redns_core::{Context, Matcher};
use std::collections::HashSet;

/// Matches if the query class is in the allowed set.
#[derive(Debug, Clone)]
pub struct QClassMatcher {
    allowed: HashSet<u16>,
}

impl QClassMatcher {
    pub fn new(classes: impl IntoIterator<Item = u16>) -> Self {
        Self {
            allowed: classes.into_iter().collect(),
        }
    }

    /// Parses a space-separated string of integer class codes.
    pub fn from_str_args(s: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let classes = s
            .split_whitespace()
            .map(|c| c.parse::<u16>())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self::new(classes))
    }
}

impl Matcher for QClassMatcher {
    fn match_ctx(&self, ctx: &Context) -> PluginResult<bool> {
        if let Some(q) = ctx.question() {
            Ok(self.allowed.contains(&q.query_class().into()))
        } else {
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode, Query};
    use hickory_proto::rr::{DNSClass, Name, RecordType};

    fn make_query_class(class: DNSClass) -> Context {
        let mut msg = Message::new();
        msg.set_id(1)
            .set_message_type(MessageType::Query)
            .set_op_code(OpCode::Query);
        msg.add_query({
            let mut q = Query::new();
            q.set_name(Name::from_ascii("example.com.").unwrap())
                .set_query_type(RecordType::A)
                .set_query_class(class);
            q
        });
        Context::new(msg)
    }

    #[test]
    fn matches_in_class() {
        let m = QClassMatcher::new([1]); // IN = 1
        let ctx = make_query_class(DNSClass::IN);
        assert!(m.match_ctx(&ctx).unwrap());
    }

    #[test]
    fn rejects_wrong_class() {
        let m = QClassMatcher::new([3]); // CH = 3
        let ctx = make_query_class(DNSClass::IN);
        assert!(!m.match_ctx(&ctx).unwrap());
    }
}
