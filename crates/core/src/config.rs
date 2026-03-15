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

//! Configuration types for the YAML config layer.

use serde::Deserialize;

/// Top-level redns configuration.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct Config {
    #[serde(default)]
    pub log: LogConfig,
    #[serde(default)]
    pub include: Vec<String>,
    #[serde(default)]
    pub plugins: Vec<PluginConfig>,
    #[serde(default)]
    pub api: ApiConfig,
    #[serde(default)]
    pub dashboard: DashboardConfig,
    #[serde(default)]
    pub servers: Vec<ServerConfig>,
}

/// Server listener configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    /// Protocol: "udp", "tcp", or "udp+tcp" (default).
    #[serde(default = "default_protocol")]
    pub protocol: String,
    /// Listen address (e.g. "0.0.0.0:53").
    pub addr: String,
    /// Entry sequence tag to use for this server.
    #[serde(default)]
    pub entry: String,
}

fn default_protocol() -> String {
    "udp+tcp".to_string()
}

/// Logging configuration.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct LogConfig {
    /// Log level string (e.g. "info", "debug").
    #[serde(default)]
    pub level: Option<String>,
    /// Optional log file path.
    #[serde(default)]
    pub file: Option<String>,
}

/// A single plugin configuration entry.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct PluginConfig {
    /// Optional tag (unique name). If empty, an anonymous tag is generated.
    #[serde(default)]
    pub tag: String,
    /// The plugin type (e.g. "sequence", "forward").
    #[serde(default, rename = "type")]
    pub plugin_type: String,
    /// Plugin-specific arguments as a raw YAML string, deserialized further by the plugin itself.
    #[serde(default, deserialize_with = "value_to_string")]
    pub args: String,
}

/// API server configuration.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ApiConfig {
    /// HTTP listen address (e.g. "127.0.0.1:8080").
    #[serde(default)]
    pub http: Option<String>,
}

/// Dashboard server configuration.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct DashboardConfig {
    /// HTTP listen address for the dashboard (e.g. "127.0.0.1:9090").
    #[serde(default)]
    pub http: Option<String>,
    /// Optional SQLite path for dashboard data.
    #[serde(default)]
    pub sqlite: Option<String>,
    /// Directory to serve static frontend files from. Defaults to "./dashboard/dist".
    #[serde(default)]
    pub static_dir: Option<String>,
    /// Paths to DHCP lease files for resolving client IPs to hostnames.
    /// Supports dnsmasq lease format (`/tmp/dhcp.leases`) and
    /// hosts-file format (`/tmp/hosts/odhcpd`).
    #[serde(default)]
    pub dhcp_leases: Vec<String>,
}

// ── Sequence Rule Config ─────────────────────────────────────────

/// A rule as written by the user in YAML.
///
/// The `matches` field accepts both a single string and a list of strings.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct RuleArgs {
    #[serde(default, deserialize_with = "string_or_vec")]
    pub matches: Vec<String>,
    #[serde(default)]
    pub exec: String,
}

/// Deserializes a YAML value that can be either a string or a list of strings.
///
/// - `"qname $cn_domains"` → `vec!["qname $cn_domains"]`
/// - `["qname $cn_domains", "has_resp"]` → `vec!["qname $cn_domains", "has_resp"]`
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

        fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
            Ok(vec![v.to_string()])
        }

        fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut v = Vec::new();
            while let Some(s) = seq.next_element::<String>()? {
                v.push(s);
            }
            Ok(v)
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

/// A parsed rule ready for building chain nodes.
#[derive(Debug, Clone, Default)]
pub struct RuleConfig {
    pub matches: Vec<MatchConfig>,
    pub tag: String,
    pub plugin_type: String,
    pub args: String,
}

/// A parsed match directive.
#[derive(Debug, Clone, Default)]
pub struct MatchConfig {
    pub tag: String,
    pub match_type: String,
    pub args: String,
    pub reverse: bool,
}

/// Parses a [`RuleArgs`] into a [`RuleConfig`].
pub fn parse_rule_args(ra: &RuleArgs) -> RuleConfig {
    let mut rc = RuleConfig::default();
    for s in &ra.matches {
        rc.matches.push(parse_match(s));
    }
    let (tag, typ, args) = parse_exec(&ra.exec);
    rc.tag = tag;
    rc.plugin_type = typ;
    rc.args = args;
    rc
}

/// Parses a match string like `"!$my_matcher args"` or `"qtype 1 28"`.
pub fn parse_match(s: &str) -> MatchConfig {
    let mut mc = MatchConfig::default();
    let s = s.trim();
    let (s, reverse) = trim_prefix_field(s, "!");
    mc.reverse = reverse;

    let (prefix, args) = s.split_once(' ').unwrap_or((s, ""));
    mc.args = args.trim().to_string();

    if let Some(tag) = prefix.strip_prefix('$') {
        mc.tag = tag.trim().to_string();
    } else {
        mc.match_type = prefix.to_string();
    }
    mc
}

/// Parses an exec string like `"$forward_google"` or `"reject 5"`.
pub fn parse_exec(s: &str) -> (String, String, String) {
    let s = s.trim();
    let (prefix, args) = s.split_once(' ').unwrap_or((s, ""));
    let args = args.trim().to_string();

    if let Some(tag) = prefix.strip_prefix('$') {
        (tag.trim().to_string(), String::new(), args)
    } else {
        (String::new(), prefix.to_string(), args)
    }
}

/// Deserializes a YAML string into a concrete type.
///
/// This is a convenience wrapper so downstream crates don't need
/// to depend on serde_saphyr directly.
pub fn deserialize_yaml_str<T: serde::de::DeserializeOwned>(
    yaml: &str,
) -> Result<T, Box<dyn std::error::Error + Send + Sync>> {
    Ok(serde_saphyr::from_str(yaml)?)
}

/// Custom deserializer that captures any YAML value subtree as a string.
///
/// Scalar values are captured directly. Mappings and sequences are deserialized
/// into a generic container and re-serialized back to a YAML string, preserving
/// the structure for later on-demand deserialization by each plugin.
fn value_to_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de;

    /// A generic YAML value that can hold any YAML structure.
    /// Used only as a bridge to re-serialize complex args to a string.
    #[derive(serde::Deserialize, serde::Serialize)]
    #[serde(untagged)]
    enum GenericYaml {
        Null,
        Bool(bool),
        Int(i64),
        Float(f64),
        String(String),
        Seq(Vec<GenericYaml>),
        Map(std::collections::BTreeMap<String, GenericYaml>),
    }

    struct ValueVisitor;
    impl<'de> de::Visitor<'de> for ValueVisitor {
        type Value = String;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("any YAML value")
        }

        fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
            Ok(v.to_string())
        }

        fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
            Ok(v)
        }

        fn visit_bool<E: de::Error>(self, v: bool) -> Result<Self::Value, E> {
            Ok(v.to_string())
        }

        fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
            Ok(v.to_string())
        }

        fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
            Ok(v.to_string())
        }

        fn visit_f64<E: de::Error>(self, v: f64) -> Result<Self::Value, E> {
            Ok(v.to_string())
        }

        fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(String::new())
        }

        fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(String::new())
        }

        fn visit_seq<A: de::SeqAccess<'de>>(self, seq: A) -> Result<Self::Value, A::Error> {
            use serde::Deserialize;
            let values: Vec<GenericYaml> =
                Vec::deserialize(de::value::SeqAccessDeserializer::new(seq))?;
            serde_saphyr::to_string(&values).map_err(de::Error::custom)
        }

        fn visit_map<A: de::MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
            use serde::Deserialize;
            use std::collections::BTreeMap;
            let values: BTreeMap<String, GenericYaml> =
                BTreeMap::deserialize(de::value::MapAccessDeserializer::new(map))?;
            serde_saphyr::to_string(&values).map_err(de::Error::custom)
        }
    }
    deserializer.deserialize_any(ValueVisitor)
}

fn trim_prefix_field<'a>(s: &'a str, prefix: &str) -> (&'a str, bool) {
    if let Some(rest) = s.strip_prefix(prefix) {
        (rest.trim(), true)
    } else {
        (s, false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_match_type_with_args() {
        let mc = parse_match("qtype 1 28");
        assert_eq!(mc.match_type, "qtype");
        assert_eq!(mc.args, "1 28");
        assert!(!mc.reverse);
        assert!(mc.tag.is_empty());
    }

    #[test]
    fn parse_match_tag_with_reverse() {
        let mc = parse_match("! $my_matcher some_args");
        assert!(mc.reverse);
        assert_eq!(mc.tag, "my_matcher");
        assert_eq!(mc.args, "some_args");
    }

    #[test]
    fn parse_match_type_no_args() {
        let mc = parse_match("has_resp");
        assert_eq!(mc.match_type, "has_resp");
        assert!(mc.args.is_empty());
    }

    #[test]
    fn parse_exec_tag() {
        let (tag, typ, args) = parse_exec("$forward_google");
        assert_eq!(tag, "forward_google");
        assert!(typ.is_empty());
        assert!(args.is_empty());
    }

    #[test]
    fn parse_exec_type_with_args() {
        let (tag, typ, args) = parse_exec("reject 5");
        assert!(tag.is_empty());
        assert_eq!(typ, "reject");
        assert_eq!(args, "5");
    }

    #[test]
    fn parse_rule_args_full() {
        let ra = RuleArgs {
            matches: vec!["qtype 1".to_string(), "!has_resp".to_string()],
            exec: "$forward".to_string(),
        };
        let rc = parse_rule_args(&ra);
        assert_eq!(rc.matches.len(), 2);
        assert_eq!(rc.matches[0].match_type, "qtype");
        assert_eq!(rc.matches[0].args, "1");
        assert!(rc.matches[1].reverse);
        assert_eq!(rc.matches[1].match_type, "has_resp");
        assert_eq!(rc.tag, "forward");
    }
}

/// Loads data from a file or returns inline text.
///
/// If `s` starts with `file:`, the rest is treated as a file path and its
/// contents are loaded. Otherwise `s` is returned as-is.
///
/// This allows matchers to accept either inline data or file references:
/// ```text
/// qname "domain:example.com example.org"    # inline
/// qname "file:blocklist.txt"                # from file
/// ```
pub fn load_file_or_inline(s: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let s = s.trim();
    if let Some(path) = s.strip_prefix("file:") {
        let path = path.trim();
        std::fs::read_to_string(path).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            format!("failed to load file '{}': {}", path, e).into()
        })
    } else {
        Ok(s.to_string())
    }
}
