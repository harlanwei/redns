// Copyright (C) 2025, Wei Chen
//
// This file is part of redns.

//! Shuffles answer RRs in the DNS response.

use async_trait::async_trait;
use redns_core::plugin::PluginResult;
use redns_core::{Context, Executable};

#[derive(Debug, Clone, Copy)]
pub struct Shuffle {
    pub answer: bool,
    pub ns: bool,
    pub extra: bool,
}

impl Shuffle {
    pub fn default_answer_only() -> Self {
        Self {
            answer: true,
            ns: false,
            extra: false,
        }
    }
}
impl Default for Shuffle {
    fn default() -> Self {
        Self::default_answer_only()
    }
}

#[async_trait]
impl Executable for Shuffle {
    async fn exec(&self, ctx: &mut Context) -> PluginResult<()> {
        if let Some(resp) = ctx.response_mut() {
            if self.answer {
                shuffle_slice(resp.answers_mut());
            }
            if self.ns {
                shuffle_slice(resp.name_servers_mut());
            }
        }
        Ok(())
    }
}

fn shuffle_slice<T>(slice: &mut [T]) {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    let len = slice.len();
    if len <= 1 {
        return;
    }
    let state = RandomState::new();
    for i in (1..len).rev() {
        let mut hasher = state.build_hasher();
        hasher.write_usize(i);
        let j = (hasher.finish() as usize) % (i + 1);
        slice.swap(i, j);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode, Query};
    use hickory_proto::rr::{Name, RData, Record, RecordType};
    use std::net::Ipv4Addr;

    #[tokio::test]
    async fn shuffle_preserves_count() {
        let s = Shuffle::default();
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
        for i in 0..5 {
            resp.add_answer(Record::from_rdata(
                Name::from_ascii("example.com.").unwrap(),
                300,
                RData::A(Ipv4Addr::new(10, 0, 0, i).into()),
            ));
        }
        ctx.set_response(Some(resp));
        s.exec(&mut ctx).await.unwrap();
        assert_eq!(ctx.response().unwrap().answers().len(), 5);
    }
}
