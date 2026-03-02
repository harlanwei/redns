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

//! Matches on the response code of a DNS response.

use redns_core::plugin::PluginResult;
use redns_core::{Context, Matcher};
use std::collections::HashSet;

/// Matches if the response rcode is in the allowed set.
/// Returns `false` if there is no response yet.
#[derive(Debug, Clone)]
pub struct RcodeMatcher {
    allowed: HashSet<u16>,
}

impl RcodeMatcher {
    pub fn new(codes: impl IntoIterator<Item = u16>) -> Self {
        Self {
            allowed: codes.into_iter().collect(),
        }
    }

    /// Parses a space-separated string of integer rcode values.
    pub fn from_str_args(s: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let codes = s
            .split_whitespace()
            .map(|c| c.parse::<u16>())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self::new(codes))
    }
}

impl Matcher for RcodeMatcher {
    fn match_ctx(&self, ctx: &Context) -> PluginResult<bool> {
        if let Some(resp) = ctx.response() {
            Ok(self.allowed.contains(&resp.response_code().into()))
        } else {
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode, Query, ResponseCode};
    use hickory_proto::rr::{Name, RecordType};

    fn make_ctx_with_resp(rcode: ResponseCode) -> Context {
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
        let mut ctx = Context::new(msg);
        let mut resp = Message::new();
        resp.set_response_code(rcode);
        ctx.set_response(Some(resp));
        ctx
    }

    #[test]
    fn matches_rcode() {
        let m = RcodeMatcher::new([0]); // NOERROR = 0
        let ctx = make_ctx_with_resp(ResponseCode::NoError);
        assert!(m.match_ctx(&ctx).unwrap());
    }

    #[test]
    fn no_response_returns_false() {
        let m = RcodeMatcher::new([0]);
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

    #[test]
    fn rejects_wrong_rcode() {
        let m = RcodeMatcher::new([0]); // NOERROR
        let ctx = make_ctx_with_resp(ResponseCode::ServFail);
        assert!(!m.match_ctx(&ctx).unwrap());
    }
}
