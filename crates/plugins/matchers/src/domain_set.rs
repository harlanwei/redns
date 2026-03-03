// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Domain set data provider — loads domain lists for matching.
//!
//! Domain expressions support type prefixes:
//! - `full:example.com` — exact match only
//! - `domain:example.com` — subdomain match (default)
//! - `keyword:google` — keyword substring match
//! - `regexp:.*google.*` — regex match
//!
//! Without a prefix, defaults to subdomain matching.

use redns_core::plugin::PluginResult;
use redns_core::{Context, Matcher};
use regex::Regex;
use std::collections::{HashMap, HashSet};
use tracing::warn;

// ── Domain Suffix Trie ──────────────────────────────────────────

/// A trie node keyed by domain labels in reverse order (TLD first).
///
/// For example, inserting "google.com" stores labels ["com", "google"].
/// Lookup for "www.google.com" walks: root → "com" → "google" (terminal) → match.
#[derive(Default)]
struct TrieNode {
    children: HashMap<Box<str>, TrieNode>,
    /// If true, this node represents a terminal domain entry.
    terminal: bool,
}

/// A domain suffix trie for efficient subdomain matching.
///
/// Replaces `HashSet<String>` with O(labels) single-pass lookup
/// instead of O(labels) hash lookups with substring slicing.
struct DomainTrie {
    root: TrieNode,
}

impl DomainTrie {
    fn new() -> Self {
        Self {
            root: TrieNode::default(),
        }
    }

    /// Insert a domain (e.g. "google.com") into the trie.
    fn insert(&mut self, domain: &str) {
        let mut node = &mut self.root;
        for label in domain.rsplit('.') {
            node = node.children.entry(label.into()).or_default();
        }
        node.terminal = true;
    }

    /// Check if a domain or any of its parent domains is in the trie.
    fn matches(&self, domain: &str) -> bool {
        let mut node = &self.root;
        for label in domain.rsplit('.') {
            match node.children.get(label) {
                Some(child) => {
                    node = child;
                    if node.terminal {
                        return true;
                    }
                }
                None => return false,
            }
        }
        false
    }
}

// ── DomainSet ───────────────────────────────────────────────────

/// A domain set that matches query names against loaded domain patterns.
///
/// Supports loading from inline expressions (`exps`) and files (`files`).
pub struct DomainSet {
    /// Full/exact match domains (lowercased, without trailing dot).
    full: HashSet<String>,
    /// Subdomain match via suffix trie — matches the domain itself
    /// and all subdomains in a single traversal.
    domain: DomainTrie,
    /// Keyword substring matches.
    keywords: Vec<String>,
    /// Regex matches.
    regexes: Vec<Regex>,
}

/// YAML args for domain_set plugin.
#[derive(Debug, Clone, serde::Deserialize, Default)]
pub struct DomainSetArgs {
    #[serde(default)]
    pub exps: Vec<String>,
    #[serde(default)]
    pub files: Vec<String>,
}

impl DomainSet {
    pub fn new() -> Self {
        Self {
            full: HashSet::new(),
            domain: DomainTrie::new(),
            keywords: Vec::new(),
            regexes: Vec::new(),
        }
    }

    /// Normalize a domain: lowercase, strip leading/trailing dots.
    fn normalize(s: &str) -> String {
        s.trim()
            .to_lowercase()
            .trim_start_matches('.')
            .trim_end_matches('.')
            .to_string()
    }

    /// Add a domain expression with optional type prefix.
    pub fn add_expression(
        &mut self,
        exp: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let exp = exp.trim();
        if exp.is_empty() || exp.starts_with('#') {
            return Ok(());
        }

        if let Some((typ, pattern)) = exp.split_once(':') {
            let pattern = Self::normalize(pattern);
            if pattern.is_empty() {
                return Ok(());
            }
            match typ {
                "full" => {
                    self.full.insert(pattern);
                }
                "domain" => {
                    self.domain.insert(&pattern);
                }
                "keyword" => {
                    self.keywords.push(pattern);
                }
                "regexp" => {
                    let re = Regex::new(&pattern).map_err(
                        |e| -> Box<dyn std::error::Error + Send + Sync> {
                            format!("invalid regex '{}': {}", pattern, e).into()
                        },
                    )?;
                    self.regexes.push(re);
                }
                _ => {
                    // Unknown type prefix — treat as default (domain/subdomain).
                    let full_pattern = Self::normalize(exp);
                    self.domain.insert(&full_pattern);
                }
            }
        } else {
            // No prefix — default to subdomain matching.
            let pattern = Self::normalize(exp);
            if !pattern.is_empty() {
                self.domain.insert(&pattern);
            }
        }
        Ok(())
    }

    /// Load expressions from a file (one per line).
    pub fn load_file(
        &mut self,
        path: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let content = std::fs::read_to_string(path).map_err(
            |e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("failed to read domain file {}: {}", path, e).into()
            },
        )?;
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            self.add_expression(line)?;
        }
        Ok(())
    }

    /// Create from a YAML string.
    pub fn from_yaml_str(s: &str) -> PluginResult<Self> {
        let args: DomainSetArgs =
            serde_saphyr::from_str(s).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("domain_set: invalid args: {e}").into()
            })?;
        Self::from_args(&args)
    }

    /// Create from parsed args.
    pub fn from_args(args: &DomainSetArgs) -> PluginResult<Self> {
        let mut ds = Self::new();
        for exp in &args.exps {
            ds.add_expression(exp)?;
        }
        for file in &args.files {
            ds.load_file(file)?;
        }
        Ok(ds)
    }

    /// Create from a string (space-separated or newline-separated expressions).
    ///
    /// Supports `&path` to load domain lists from files.
    pub fn from_str_args(s: &str) -> PluginResult<Self> {
        // Try YAML struct deserialization first.
        if let Ok(ds) = Self::from_yaml_str(s) {
            return Ok(ds);
        }
        // Fall back to newline/space-separated expressions.
        let mut ds = Self::new();
        for part in s.split_whitespace() {
            if let Some(path) = part.strip_prefix('&') {
                if let Err(e) = ds.load_file(path) {
                    warn!(error = %e, file = path, "failed to load domain file");
                }
            } else if let Err(e) = ds.add_expression(part) {
                warn!(error = %e, exp = part, "skipping invalid domain expression");
            }
        }
        Ok(ds)
    }

    /// Check if a domain name matches any pattern in this set.
    pub fn matches_domain(&self, name: &str) -> bool {
        let normalized = Self::normalize(name);

        // Full/exact match.
        if self.full.contains(&normalized) {
            return true;
        }

        // Subdomain match — single trie traversal.
        if self.domain.matches(&normalized) {
            return true;
        }

        // Keyword match.
        for kw in &self.keywords {
            if normalized.contains(kw.as_str()) {
                return true;
            }
        }

        // Regex match.
        for re in &self.regexes {
            if re.is_match(&normalized) {
                return true;
            }
        }

        false
    }
}

impl Matcher for DomainSet {
    fn match_ctx(&self, ctx: &Context) -> PluginResult<bool> {
        if let Some(q) = ctx.question() {
            let name = q.name().to_ascii();
            Ok(self.matches_domain(&name))
        } else {
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode, Query};
    use hickory_proto::rr::{Name, RecordType};

    fn make_ctx(name: &str) -> Context {
        let mut msg = Message::new();
        msg.set_id(1)
            .set_message_type(MessageType::Query)
            .set_op_code(OpCode::Query);
        msg.add_query({
            let mut q = Query::new();
            q.set_name(Name::from_ascii(name).unwrap())
                .set_query_type(RecordType::A);
            q
        });
        Context::new(msg)
    }

    #[test]
    fn domain_subdomain_match() {
        let mut ds = DomainSet::new();
        ds.add_expression("domain:google.com").unwrap();
        assert!(ds.matches_domain("google.com."));
        assert!(ds.matches_domain("www.google.com"));
        assert!(ds.matches_domain("sub.www.google.com"));
        assert!(!ds.matches_domain("notgoogle.com"));
    }

    #[test]
    fn full_exact_match() {
        let mut ds = DomainSet::new();
        ds.add_expression("full:www.google.com").unwrap();
        assert!(ds.matches_domain("www.google.com"));
        assert!(!ds.matches_domain("sub.www.google.com"));
        assert!(!ds.matches_domain("google.com"));
    }

    #[test]
    fn keyword_match() {
        let mut ds = DomainSet::new();
        ds.add_expression("keyword:google").unwrap();
        assert!(ds.matches_domain("www.google.com"));
        assert!(ds.matches_domain("google.cn"));
        assert!(!ds.matches_domain("example.com"));
    }

    #[test]
    fn default_is_subdomain() {
        let mut ds = DomainSet::new();
        ds.add_expression("baidu.com").unwrap();
        assert!(ds.matches_domain("www.baidu.com"));
        assert!(ds.matches_domain("baidu.com"));
    }

    #[test]
    fn matcher_trait() {
        let mut ds = DomainSet::new();
        ds.add_expression("example.com").unwrap();
        assert!(ds.match_ctx(&make_ctx("sub.example.com.")).unwrap());
        assert!(!ds.match_ctx(&make_ctx("other.com.")).unwrap());
    }
}
