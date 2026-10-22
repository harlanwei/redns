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

use aho_corasick::AhoCorasick;
use redns_core::plugin::PluginResult;
use redns_core::{Context, Matcher};
use regex::{Regex, RegexBuilder};
use std::collections::HashMap;
use std::sync::OnceLock;
use tracing::warn;

// ── Domain Suffix Trie ──────────────────────────────────────────

/// A trie node keyed by domain labels in reverse order (TLD first).
///
/// For example, inserting "google.com" stores labels ["com", "google"].
/// Lookup for "www.google.com" walks: root → "com" → "google" (terminal) → match.
#[derive(Debug, Clone, Default)]
struct TrieNode {
    children: HashMap<Box<str>, TrieNode>,
    /// If true, this node represents a terminal domain entry.
    terminal: bool,
}

impl TrieNode {
    /// Look up a child by label, case-insensitively (trie keys are
    /// lowercased at insert time). No allocation for DNS-sized labels.
    fn child(&self, label: &[u8]) -> Option<&TrieNode> {
        let mut buf = [0u8; 63];
        if label.len() <= buf.len() {
            buf[..label.len()].copy_from_slice(label);
            buf[..label.len()].make_ascii_lowercase();
            let key = std::str::from_utf8(&buf[..label.len()]).ok()?;
            self.children.get(key)
        } else {
            let key = String::from_utf8_lossy(label).to_lowercase();
            self.children.get(key.as_str())
        }
    }
}

/// A domain suffix trie for efficient subdomain matching.
///
/// Replaces `HashSet<String>` with O(labels) single-pass lookup
/// instead of O(labels) hash lookups with substring slicing.
#[derive(Debug, Clone, Default)]
pub(crate) struct DomainTrie {
    root: TrieNode,
}

impl DomainTrie {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.root.children.is_empty()
    }

    /// Insert a domain (e.g. "google.com") into the trie.
    /// The domain must already be normalized (lowercased, no outer dots).
    pub(crate) fn insert(&mut self, domain: &str) {
        let mut node = &mut self.root;
        for label in domain.rsplit('.') {
            node = node.children.entry(label.into()).or_default();
        }
        node.terminal = true;
    }

    /// Check if a domain or any of its parent domains is in the trie.
    ///
    /// Labels are given in wire order (leftmost first) and compared
    /// case-insensitively.
    pub(crate) fn matches_labels<'a>(
        &self,
        labels: impl DoubleEndedIterator<Item = &'a [u8]>,
    ) -> bool {
        let mut node = &self.root;
        for label in labels.rev() {
            match node.child(label) {
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

    /// Check if the exact domain (all labels) is in the trie.
    pub(crate) fn matches_exact_labels<'a>(
        &self,
        labels: impl DoubleEndedIterator<Item = &'a [u8]>,
    ) -> bool {
        let mut node = &self.root;
        for label in labels.rev() {
            match node.child(label) {
                Some(child) => node = child,
                None => return false,
            }
        }
        node.terminal
    }
}

// ── DomainSet ───────────────────────────────────────────────────

/// A domain set that matches query names against loaded domain patterns.
///
/// Supports loading from inline expressions (`exps`) and files (`files`).
pub struct DomainSet {
    /// Full/exact match domains (lowercased, without trailing dot).
    full: DomainTrie,
    /// Subdomain match via suffix trie — matches the domain itself
    /// and all subdomains in a single traversal.
    domain: DomainTrie,
    /// Keyword substring matches.
    keywords: Vec<String>,
    /// Aho-Corasick automaton over `keywords`, built on first use.
    keyword_automaton: OnceLock<Option<AhoCorasick>>,
    /// Regex matches.
    regexes: Vec<Regex>,
    /// `regexes` combined into a single alternation, built on first use.
    combined_regex: OnceLock<Option<Regex>>,
}

/// YAML args for domain_set plugin.
#[derive(Debug, Clone, serde::Deserialize, Default)]
pub struct DomainSetArgs {
    #[serde(default, deserialize_with = "string_or_vec")]
    pub exps: Vec<String>,
    #[serde(default, deserialize_with = "string_or_vec")]
    pub files: Vec<String>,
}

fn string_or_vec<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de;

    struct StringOrVec;

    impl<'de> de::Visitor<'de> for StringOrVec {
        type Value = Vec<String>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a string or a list of strings")
        }

        fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
            Ok(vec![value.to_string()])
        }

        fn visit_string<E: de::Error>(self, value: String) -> Result<Self::Value, E> {
            Ok(vec![value])
        }

        fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut values = Vec::new();
            while let Some(value) = seq.next_element::<String>()? {
                values.push(value);
            }
            Ok(values)
        }

        fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(Vec::new())
        }

        fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(Vec::new())
        }
    }

    deserializer.deserialize_any(StringOrVec)
}

impl DomainSet {
    pub fn new() -> Self {
        Self {
            full: DomainTrie::new(),
            domain: DomainTrie::new(),
            keywords: Vec::new(),
            keyword_automaton: OnceLock::new(),
            regexes: Vec::new(),
            combined_regex: OnceLock::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.full.is_empty()
            && self.domain.is_empty()
            && self.keywords.is_empty()
            && self.regexes.is_empty()
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
            if typ == "regexp" {
                // Regex source must not be domain-normalized: lowercasing
                // rewrites case-escaping escapes (`\D` → `\d`) and the
                // dot-stripping pass mangles quantifiers (`.*` → `*`).
                // The pattern is used verbatim; case handling is done by the
                // regex builder (names are matched lowercased).
                let pattern = pattern.trim();
                if pattern.is_empty() {
                    return Ok(());
                }
                let re = RegexBuilder::new(pattern)
                    .case_insensitive(true)
                    .build()
                    .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                        format!("invalid regex '{}': {}", pattern, e).into()
                    })?;
                self.regexes.push(re);
                return Ok(());
            }

            let pattern = Self::normalize(pattern);
            if pattern.is_empty() {
                return Ok(());
            }
            match typ {
                "full" => {
                    self.full.insert(&pattern);
                }
                "domain" => {
                    self.domain.insert(&pattern);
                }
                "keyword" => {
                    self.keywords.push(pattern);
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

    /// Create from a string (YAML args, or space-separated expressions).
    ///
    /// Supports `&path` to load domain lists from files.
    ///
    /// A YAML `files:` entry that cannot be read is a hard error: silently
    /// producing an empty domain set would make routing rules fail open.
    pub fn from_str_args(s: &str) -> PluginResult<Self> {
        let trimmed = s.trim();

        // Try YAML struct deserialization first. If it parses, file-loading
        // errors must propagate; do not fall back to treating the YAML text as
        // a quick-form expression list.
        if let Ok(args) = serde_saphyr::from_str::<DomainSetArgs>(s) {
            let ds = Self::from_args(&args)?;
            // `&path` can be interpreted by YAML as an anchor token and
            // deserialize into an empty struct; preserve quick-arg file
            // loading for that case.
            if !(trimmed.starts_with('&') && ds.is_empty()) {
                return Ok(ds);
            }
        }
        // Fall back to newline/space-separated expressions.
        let mut ds = Self::new();
        for part in trimmed.split_whitespace() {
            if let Some(path) = part.strip_prefix('&') {
                ds.load_file(path)?;
            } else if let Err(e) = ds.add_expression(part) {
                warn!(error = %e, exp = part, "skipping invalid domain expression");
            }
        }
        Ok(ds)
    }

    /// Check if a domain name matches any pattern in this set.
    pub fn matches_domain(&self, name: &str) -> bool {
        let normalized = Self::normalize(name);
        self.matches_normalized(&normalized)
    }

    /// Match against an already-normalized domain string.
    fn matches_normalized(&self, normalized: &str) -> bool {
        // Full/exact match.
        if self
            .full
            .matches_exact_labels(normalized.split('.').map(str::as_bytes))
        {
            return true;
        }

        // Subdomain match — single trie traversal.
        if self
            .domain
            .matches_labels(normalized.split('.').map(str::as_bytes))
        {
            return true;
        }

        self.matches_keywords(normalized) || self.matches_regexes(normalized)
    }

    /// Match a query name. Exact/suffix checks walk the name's labels
    /// directly without allocation; keywords and regexes need the normalized
    /// string form and are only evaluated when configured.
    fn matches_name(&self, name: &hickory_proto::rr::Name) -> bool {
        if self.full.matches_exact_labels(name.iter()) || self.domain.matches_labels(name.iter()) {
            return true;
        }
        if self.keywords.is_empty() && self.regexes.is_empty() {
            return false;
        }
        let normalized = Self::normalize(&name.to_ascii());
        self.matches_keywords(&normalized) || self.matches_regexes(&normalized)
    }

    /// Keyword match — a single Aho-Corasick pass over all keywords.
    fn matches_keywords(&self, name: &str) -> bool {
        if self.keywords.is_empty() {
            return false;
        }
        match self
            .keyword_automaton
            .get_or_init(|| AhoCorasick::new(&self.keywords).ok())
        {
            Some(ac) => ac.is_match(name),
            // Automaton construction can only fail for pathological patterns;
            // fall back to the linear scan rather than panicking.
            None => self.keywords.iter().any(|kw| name.contains(kw.as_str())),
        }
    }

    /// Regex match — a single pass of the combined alternation.
    fn matches_regexes(&self, name: &str) -> bool {
        if self.regexes.is_empty() {
            return false;
        }
        match self.combined_regex.get_or_init(|| {
            let pattern = self
                .regexes
                .iter()
                .map(|re| format!("(?:{})", re.as_str()))
                .collect::<Vec<_>>()
                .join("|");
            RegexBuilder::new(&pattern)
                .case_insensitive(true)
                .build()
                .ok()
        }) {
            Some(re) => re.is_match(name),
            None => self.regexes.iter().any(|re| re.is_match(name)),
        }
    }
}

impl Default for DomainSet {
    fn default() -> Self {
        Self::new()
    }
}

impl Matcher for DomainSet {
    fn match_ctx(&self, ctx: &Context) -> PluginResult<bool> {
        if let Some(q) = ctx.question() {
            Ok(self.matches_name(q.name()))
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
        let mut msg = Message::new(1, MessageType::Query, OpCode::Query);
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

    /// Regex source must be preserved verbatim: `.*google.*` must survive
    /// normalization (it would otherwise be corrupted into `*google.*` and
    /// rejected as invalid), and case-escapes like `\D` must keep their
    /// meaning (lowercasing the source would turn `\D` into `\d`).
    #[test]
    fn regexp_source_is_not_normalized() {
        let mut ds = DomainSet::new();
        ds.add_expression("regexp:.*google.*").unwrap();
        assert!(ds.matches_domain("www.google.com"));
        assert!(ds.matches_domain("notgoogle.cn"));

        let mut ds = DomainSet::new();
        ds.add_expression(r"regexp:^\D+\.example\.com$").unwrap();
        assert!(ds.matches_domain("mail.example.com"));
        assert!(!ds.matches_domain("123.example.com"));

        // Case-insensitive matching is handled by the builder, not by
        // lowercasing the source.
        let mut ds = DomainSet::new();
        ds.add_expression("regexp:^GOOGLE\\.COM$").unwrap();
        assert!(ds.matches_domain("google.com"));
    }

    #[test]
    fn matcher_trait() {
        let mut ds = DomainSet::new();
        ds.add_expression("example.com").unwrap();
        assert!(ds.match_ctx(&make_ctx("sub.example.com.")).unwrap());
        assert!(!ds.match_ctx(&make_ctx("other.com.")).unwrap());
    }

    #[test]
    fn missing_yaml_file_is_an_error() {
        let path = format!("/nonexistent/redns-domain-list-{}.txt", std::process::id());
        let args = format!("files:\n  - {path}\n");
        let err = match DomainSet::from_str_args(&args) {
            Ok(_) => panic!("missing file must fail"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("failed to read domain file"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn missing_quick_form_file_is_an_error() {
        let path = format!(
            "/nonexistent/redns-domain-list-quick-{}.txt",
            std::process::id()
        );
        assert!(DomainSet::from_str_args(&format!("&{path}")).is_err());
    }

    #[test]
    fn yaml_scalar_file_arg_loads_domains() {
        use std::io::Write;

        let dir = std::env::temp_dir().join("redns_domain_set_yaml_scalar_file");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("domains.txt");
        let mut file = std::fs::File::create(&path).unwrap();
        writeln!(file, "example.com").unwrap();
        drop(file);

        let args = format!("files: {}\n", path.display());
        let ds = DomainSet::from_str_args(&args).unwrap();
        assert!(ds.matches_domain("www.example.com."));

        std::fs::remove_dir_all(&dir).ok();
    }
}
