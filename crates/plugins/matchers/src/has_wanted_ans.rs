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

//! Checks if the response contains answer records.

use redns_core::plugin::PluginResult;
use redns_core::{Context, Matcher};

/// Matches if the response has at least one answer record.
#[derive(Debug, Clone, Copy, Default)]
pub struct HasWantedAns;

impl Matcher for HasWantedAns {
    fn match_ctx(&self, ctx: &Context) -> PluginResult<bool> {
        if let Some(resp) = ctx.response() {
            Ok(!resp.answers().is_empty())
        } else {
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode, Query};
    use hickory_proto::rr::{Name, RData, Record, RecordType};
    use std::net::Ipv4Addr;

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
    fn no_response_returns_false() {
        let m = HasWantedAns;
        let ctx = make_ctx();
        assert!(!m.match_ctx(&ctx).unwrap());
    }

    #[test]
    fn empty_answer_returns_false() {
        let m = HasWantedAns;
        let mut ctx = make_ctx();
        ctx.set_response(Some(Message::new()));
        assert!(!m.match_ctx(&ctx).unwrap());
    }

    #[test]
    fn with_answer_returns_true() {
        let m = HasWantedAns;
        let mut ctx = make_ctx();
        let mut resp = Message::new();
        let record = Record::from_rdata(
            Name::from_ascii("example.com.").unwrap(),
            300,
            RData::A(Ipv4Addr::new(1, 2, 3, 4).into()),
        );
        resp.add_answer(record);
        ctx.set_response(Some(resp));
        assert!(m.match_ctx(&ctx).unwrap());
    }
}
