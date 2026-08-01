// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Longest-prefix-match sets shared by IP-set style data providers
//! (`ip_set`, `asn`).

use std::collections::{BTreeSet, HashMap, HashSet};

/// Longest-prefix-match set for a single address family.
///
/// Networks are bucketed by prefix length: `buckets[len]` holds every network
/// of that length as its masked network address. A membership test masks the
/// query address to each *present* prefix length and probes that bucket's hash
/// set, so a lookup is O(distinct prefix lengths) (≤ 33 for v4, ≤ 129 for v6)
/// regardless of how many networks are loaded — not the O(n) linear scan a
/// `Vec<IpNet>` would require for large block/allow lists.
#[derive(Default)]
pub(crate) struct PrefixSet<T> {
    /// prefix length → set of masked network addresses at that length.
    buckets: HashMap<u8, HashSet<T>>,
    /// Distinct prefix lengths present, kept sorted for deterministic probing.
    lengths: BTreeSet<u8>,
}

impl PrefixSet<u32> {
    pub(crate) fn insert(&mut self, addr: u32, prefix_len: u8) {
        let masked = mask_v4(addr, prefix_len);
        self.buckets.entry(prefix_len).or_default().insert(masked);
        self.lengths.insert(prefix_len);
    }

    pub(crate) fn contains(&self, addr: u32) -> bool {
        self.lengths.iter().any(|&len| {
            self.buckets
                .get(&len)
                .is_some_and(|set| set.contains(&mask_v4(addr, len)))
        })
    }
}

impl PrefixSet<u128> {
    pub(crate) fn insert(&mut self, addr: u128, prefix_len: u8) {
        let masked = mask_v6(addr, prefix_len);
        self.buckets.entry(prefix_len).or_default().insert(masked);
        self.lengths.insert(prefix_len);
    }

    pub(crate) fn contains(&self, addr: u128) -> bool {
        self.lengths.iter().any(|&len| {
            self.buckets
                .get(&len)
                .is_some_and(|set| set.contains(&mask_v6(addr, len)))
        })
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
