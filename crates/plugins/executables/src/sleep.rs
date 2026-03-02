// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Delays execution for a configured duration (async).

use async_trait::async_trait;
use redns_core::plugin::PluginResult;
use redns_core::{Context, Executable};
use std::time::Duration;

/// Sleeps for the configured duration using tokio::time::sleep.
#[derive(Debug, Clone, Copy)]
pub struct Sleep {
    duration: Duration,
}

impl Sleep {
    pub fn new(duration: Duration) -> Self {
        Self { duration }
    }

    /// Parses duration in milliseconds from a string.
    pub fn from_str_args(s: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let ms: u64 = s.trim().parse()?;
        Ok(Self::new(Duration::from_millis(ms)))
    }
}

#[async_trait]
impl Executable for Sleep {
    async fn exec(&self, _ctx: &mut Context) -> PluginResult<()> {
        tokio::time::sleep(self.duration).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode, Query};
    use hickory_proto::rr::{Name, RecordType};
    use std::time::Instant;

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

    #[tokio::test]
    async fn sleep_delays_execution() {
        let s = Sleep::new(Duration::from_millis(50));
        let mut ctx = make_ctx();
        let start = Instant::now();
        s.exec(&mut ctx).await.unwrap();
        assert!(start.elapsed() >= Duration::from_millis(40));
    }

    #[test]
    fn from_str_args_valid() {
        let s = Sleep::from_str_args("100").unwrap();
        assert_eq!(s.duration, Duration::from_millis(100));
    }
}
