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

//! Plugin type registry.

use crate::plugin::PluginError;
use std::collections::HashMap;

/// A factory function that creates a boxed plugin instance from its YAML args string.
pub type PluginFactory =
    Box<dyn Fn(&str) -> Result<Box<dyn std::any::Any + Send + Sync>, PluginError> + Send + Sync>;

/// Stores registered plugin types and their factory functions.
#[derive(Default)]
pub struct PluginRegistry {
    types: HashMap<String, PluginFactory>,
}

impl PluginRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a plugin type. Returns an error if the type is already registered.
    pub fn register(
        &mut self,
        type_name: impl Into<String>,
        factory: PluginFactory,
    ) -> Result<(), PluginError> {
        let name = type_name.into();
        if self.types.contains_key(&name) {
            return Err(format!("duplicate plugin type: {}", name).into());
        }
        self.types.insert(name, factory);
        Ok(())
    }

    /// Retrieves the factory for a plugin type.
    pub fn get(&self, type_name: &str) -> Option<&PluginFactory> {
        self.types.get(type_name)
    }

    /// Returns all registered type names.
    pub fn type_names(&self) -> Vec<&str> {
        self.types.keys().map(|s| s.as_str()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_get() {
        let mut reg = PluginRegistry::new();
        reg.register("test_type", Box::new(|_| Ok(Box::new(42u32))))
            .unwrap();
        assert!(reg.get("test_type").is_some());
        assert!(reg.get("nonexistent").is_none());
    }

    #[test]
    fn duplicate_registration_fails() {
        let mut reg = PluginRegistry::new();
        reg.register("dup", Box::new(|_| Ok(Box::new(1u32))))
            .unwrap();
        let result = reg.register("dup", Box::new(|_| Ok(Box::new(2u32))));
        assert!(result.is_err());
    }

    #[test]
    fn type_names_lists_all() {
        let mut reg = PluginRegistry::new();
        reg.register("alpha", Box::new(|_| Ok(Box::new(()))))
            .unwrap();
        reg.register("beta", Box::new(|_| Ok(Box::new(()))))
            .unwrap();
        let mut names = reg.type_names();
        names.sort();
        assert_eq!(names, vec!["alpha", "beta"]);
    }
}
