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

//! `has_resp` matcher — returns `true` when the [`Context`] already
//! contains a response.

use redns_core::plugin::PluginResult;
use redns_core::{Context, Matcher};

/// A zero-sized matcher that checks whether a response has been set on the
/// query context.
#[derive(Debug, Clone, Copy, Default)]
pub struct HasResp;

impl Matcher for HasResp {
    fn match_ctx(&self, ctx: &Context) -> PluginResult<bool> {
        Ok(ctx.response().is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode, Query};
    use hickory_proto::rr::{Name, RecordType};

    fn make_query() -> Message {
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
        msg
    }

    #[test]
    fn no_response_does_not_match() {
        let ctx = Context::new(make_query());
        let matcher = HasResp;
        assert!(!matcher.match_ctx(&ctx).unwrap());
    }

    #[test]
    fn with_response_matches() {
        let mut ctx = Context::new(make_query());
        ctx.set_response(Some(Message::new()));
        let matcher = HasResp;
        assert!(matcher.match_ctx(&ctx).unwrap());
    }
}
