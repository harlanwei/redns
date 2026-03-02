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

//! Matches randomly with a configurable probability.

use redns_core::plugin::PluginResult;
use redns_core::{Context, Matcher};

/// Matches with the given probability (0.0 = never, 1.0 = always).
#[derive(Debug, Clone, Copy)]
pub struct RandomMatcher {
    /// Probability of matching, in the range [0.0, 1.0].
    probability: f64,
}

impl RandomMatcher {
    /// Creates a new random matcher.
    ///
    /// # Panics
    /// Panics if `probability` is not in `[0.0, 1.0]`.
    pub fn new(probability: f64) -> Self {
        assert!(
            (0.0..=1.0).contains(&probability),
            "probability must be in [0.0, 1.0], got {probability}"
        );
        Self { probability }
    }

    /// Parses a probability from a string like "0.5".
    pub fn from_str_args(s: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let p: f64 = s.trim().parse()?;
        if !(0.0..=1.0).contains(&p) {
            return Err(format!("probability must be in [0.0, 1.0], got {p}").into());
        }
        Ok(Self::new(p))
    }
}

impl Matcher for RandomMatcher {
    fn match_ctx(&self, _ctx: &Context) -> PluginResult<bool> {
        if self.probability >= 1.0 {
            return Ok(true);
        }
        if self.probability <= 0.0 {
            return Ok(false);
        }
        Ok(rand_f64() < self.probability)
    }
}

/// Simple pseudo-random float in [0.0, 1.0) using thread-local state.
fn rand_f64() -> f64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u64(std::time::Instant::now().elapsed().as_nanos() as u64);
    (hasher.finish() as f64) / (u64::MAX as f64)
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
    fn always_matches_at_1() {
        let m = RandomMatcher::new(1.0);
        let ctx = make_ctx();
        assert!(m.match_ctx(&ctx).unwrap());
    }

    #[test]
    fn never_matches_at_0() {
        let m = RandomMatcher::new(0.0);
        let ctx = make_ctx();
        assert!(!m.match_ctx(&ctx).unwrap());
    }

    #[test]
    fn from_str_args_valid() {
        let m = RandomMatcher::from_str_args("0.5").unwrap();
        assert!((0.0..=1.0).contains(&m.probability));
    }

    #[test]
    fn from_str_args_invalid() {
        assert!(RandomMatcher::from_str_args("1.5").is_err());
        assert!(RandomMatcher::from_str_args("-0.1").is_err());
    }
}
