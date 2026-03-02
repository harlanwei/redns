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

//! The central runtime struct — owns plugins, config, and lifecycle.

use crate::config::{Config, PluginConfig};
use crate::plugin::PluginError;
use crate::registry::PluginRegistry;
use std::collections::HashMap;
use std::path::Path;
use tracing::info;

/// The central redns runtime.
pub struct Redns {
    /// All loaded plugins, keyed by their tag.
    plugins: HashMap<String, Box<dyn std::any::Any + Send + Sync>>,

    /// Plugin type registry.
    registry: PluginRegistry,
}

impl Redns {
    /// Creates a new redns instance from a config.
    pub fn new(cfg: Config, registry: PluginRegistry) -> Result<Self, PluginError> {
        let mut m = Self {
            plugins: HashMap::new(),
            registry,
        };

        // Load plugins from config.
        m.load_plugins_from_cfg(&cfg, 0)?;
        info!(count = m.plugins.len(), "all plugins loaded");
        Ok(m)
    }

    /// Returns a reference to a plugin by tag, downcast to `T`.
    pub fn get_plugin<T: 'static>(&self, tag: &str) -> Option<&T> {
        self.plugins.get(tag).and_then(|p| p.downcast_ref::<T>())
    }

    /// Returns a raw reference to a plugin by tag.
    pub fn get_plugin_any(&self, tag: &str) -> Option<&(dyn std::any::Any + Send + Sync)> {
        self.plugins.get(tag).map(|p| p.as_ref())
    }

    /// Returns the number of loaded plugins.
    pub fn plugin_count(&self) -> usize {
        self.plugins.len()
    }

    /// Returns all plugin tags.
    pub fn plugin_tags(&self) -> Vec<&str> {
        self.plugins.keys().map(|s| s.as_str()).collect()
    }

    fn load_plugins_from_cfg(&mut self, cfg: &Config, depth: usize) -> Result<(), PluginError> {
        const MAX_INCLUDE_DEPTH: usize = 8;
        if depth > MAX_INCLUDE_DEPTH {
            return Err("maximum include depth reached".into());
        }

        // Follow includes first.
        for include_path in &cfg.include {
            let sub_cfg = load_config_file(include_path)?;
            info!(file = %include_path, "loading included config");
            self.load_plugins_from_cfg(&sub_cfg, depth + 1)?;
        }

        // Load plugins.
        for (i, pc) in cfg.plugins.iter().enumerate() {
            self.new_plugin(i, pc)?;
        }
        Ok(())
    }

    fn new_plugin(&mut self, index: usize, pc: &PluginConfig) -> Result<(), PluginError> {
        let tag = if pc.tag.is_empty() {
            format!("__anon_{}_{}", pc.plugin_type, index)
        } else {
            pc.tag.clone()
        };

        // If plugin type is not in the registry, skip it — it will be
        // handled by ChainBuilder (e.g. "forward", "sequence", matchers).
        let factory = match self.registry.get(&pc.plugin_type) {
            Some(f) => f,
            None => return Ok(()),
        };

        let plugin = factory(&pc.args)?;
        info!(tag = %tag, plugin_type = %pc.plugin_type, "plugin loaded");
        self.plugins.insert(tag, plugin);
        Ok(())
    }
}

/// Loads a config from a YAML file.
pub fn load_config_file(path: impl AsRef<Path>) -> Result<Config, PluginError> {
    let path = path.as_ref();
    let content = std::fs::read_to_string(path).map_err(|e| -> PluginError {
        format!("failed to read config file {}: {}", path.display(), e).into()
    })?;
    let cfg: Config = serde_saphyr::from_str(&content).map_err(|e| -> PluginError {
        format!("failed to parse config file {}: {}", path.display(), e).into()
    })?;
    Ok(cfg)
}

/// Searches for a config file named "config.yaml" or "config.yml" in the
/// current directory. Returns the parsed config and the file path used.
pub fn find_and_load_config() -> Result<(Config, String), PluginError> {
    for name in &["config.yaml", "config.yml"] {
        if Path::new(name).exists() {
            let cfg = load_config_file(name)?;
            return Ok((cfg, name.to_string()));
        }
    }
    Err("no config file found (tried config.yaml, config.yml)".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redns_new_empty_config() {
        let cfg = Config::default();
        let reg = PluginRegistry::new();
        let m = Redns::new(cfg, reg).unwrap();
        assert_eq!(m.plugin_count(), 0);
    }

    #[test]
    fn redns_loads_plugin() {
        let mut reg = PluginRegistry::new();
        reg.register("test_plugin", Box::new(|_| Ok(Box::new(42u32))))
            .unwrap();

        let cfg = Config {
            plugins: vec![PluginConfig {
                tag: "my_test".into(),
                plugin_type: "test_plugin".into(),
                args: String::new(),
            }],
            ..Default::default()
        };

        let m = Redns::new(cfg, reg).unwrap();
        assert_eq!(m.plugin_count(), 1);
        assert_eq!(m.get_plugin::<u32>("my_test"), Some(&42));
    }

    #[test]
    fn redns_unknown_plugin_type_skipped() {
        let reg = PluginRegistry::new();
        let cfg = Config {
            plugins: vec![PluginConfig {
                tag: "unknown".into(),
                plugin_type: "nonexistent".into(),
                args: String::new(),
            }],
            ..Default::default()
        };
        // Unknown types are skipped (handled by ChainBuilder later).
        let m = Redns::new(cfg, reg).unwrap();
        assert_eq!(m.plugin_count(), 0);
    }
}
