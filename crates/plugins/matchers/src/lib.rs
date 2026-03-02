// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

pub mod client_ip;
pub mod cname;
pub mod domain_set;
pub mod env;
pub mod has_resp;
pub mod has_wanted_ans;
pub mod ip_set;
pub mod ptr_ip;
pub mod qclass;
pub mod qname;
pub mod qtype;
pub mod random;
pub mod rate_limiter;
pub mod rcode;
pub mod resp_ip;
pub mod string_exp;

// Re-export matchers for convenience.
pub use client_ip::ClientIpMatcher;
pub use cname::CnameMatcher;
pub use domain_set::DomainSet;
pub use env::EnvMatcher;
pub use has_resp::HasResp;
pub use has_wanted_ans::HasWantedAns;
pub use ip_set::IpSet;
pub use ptr_ip::PtrIpMatcher;
pub use qclass::QClassMatcher;
pub use qname::QnameMatcher;
pub use qtype::QTypeMatcher;
pub use random::RandomMatcher;
pub use rate_limiter::RateLimiter;
pub use rcode::RcodeMatcher;
pub use resp_ip::RespIpMatcher;
pub use string_exp::StringExpMatcher;
