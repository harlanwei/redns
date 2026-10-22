// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Longest-prefix-match sets shared by IP-set style data providers
//! (`ip_set`, `asn`).

use std::collections::HashSet;
use std::net::IpAddr;

/// Longest-prefix-match set for a single address family.
///
/// Networks are bucketed by prefix length: `buckets[len]` holds every network
/// of that length as its masked network address. A membership test masks the
/// query address to each *present* prefix length and probes that bucket's hash
/// set, so a lookup is O(distinct prefix lengths) (≤ 33 for v4, ≤ 129 for v6)
/// regardless of how many networks are loaded — not the O(n) linear scan a
/// `Vec<IpNet>` would require for large block/allow lists.
#[derive(Debug, Clone, Default)]
pub(crate) struct PrefixSet<T> {
    /// Indexed by prefix length; `None` for lengths with no networks.
    buckets: Vec<Option<HashSet<T>>>,
    /// Distinct prefix lengths present, kept sorted for deterministic probing.
    lengths: Vec<u8>,
}

impl<T: Eq + std::hash::Hash> PrefixSet<T> {
    fn insert_masked(&mut self, addr: T, prefix_len: u8) {
        let idx = prefix_len as usize;
        if self.buckets.len() <= idx {
            self.buckets.resize_with(idx + 1, || None);
        }
        if self.buckets[idx].is_none() {
            let pos = self.lengths.partition_point(|&len| len < prefix_len);
            self.lengths.insert(pos, prefix_len);
            self.buckets[idx] = Some(HashSet::new());
        }
        self.buckets[idx].as_mut().unwrap().insert(addr);
    }

    fn contains_masked(&self, mask: impl Fn(u8) -> T) -> bool {
        self.lengths.iter().any(|&len| {
            self.buckets[len as usize]
                .as_ref()
                .is_some_and(|set| set.contains(&mask(len)))
        })
    }
}

impl PrefixSet<u32> {
    pub(crate) fn insert(&mut self, addr: u32, prefix_len: u8) {
        self.insert_masked(mask_v4(addr, prefix_len), prefix_len);
    }

    pub(crate) fn contains(&self, addr: u32) -> bool {
        self.contains_masked(|len| mask_v4(addr, len))
    }
}

impl PrefixSet<u128> {
    pub(crate) fn insert(&mut self, addr: u128, prefix_len: u8) {
        self.insert_masked(mask_v6(addr, prefix_len), prefix_len);
    }

    pub(crate) fn contains(&self, addr: u128) -> bool {
        self.contains_masked(|len| mask_v6(addr, len))
    }
}

/// Parse a CIDR string (or bare IP, which becomes /32 or /128) into an
/// address and prefix length.
pub(crate) fn parse_cidr(
    s: &str,
) -> Result<(IpAddr, u8), Box<dyn std::error::Error + Send + Sync>> {
    if let Some((addr_str, len_str)) = s.split_once('/') {
        let network: IpAddr = addr_str.parse()?;
        let prefix_len: u8 = len_str.parse()?;
        let max_len = match network {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        };
        if prefix_len > max_len {
            return Err(format!(
                "prefix length {prefix_len} is too large for {network} (max {max_len})"
            )
            .into());
        }
        Ok((network, prefix_len))
    } else {
        // Single IP — /32 or /128.
        let network: IpAddr = s.parse()?;
        let prefix_len = match network {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        };
        Ok((network, prefix_len))
    }
}

/// Mask an IPv4 address (as `u32`) to the given prefix length.
fn mask_v4(addr: u32, prefix_len: u8) -> u32 {
    if prefix_len == 0 {
        0
    } else if prefix_len >= 32 {
        addr
    } else {
        addr & (!0u32 << (32 - prefix_len))
    }
}

/// Mask an IPv6 address (as `u128`) to the given prefix length.
fn mask_v6(addr: u128, prefix_len: u8) -> u128 {
    if prefix_len == 0 {
        0
    } else if prefix_len >= 128 {
        addr
    } else {
        addr & (!0u128 << (128 - prefix_len))
    }
}
