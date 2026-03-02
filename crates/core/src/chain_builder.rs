// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Chain builder — translates parsed YAML config into executable chains.
//!
//! Resolves plugin type strings to concrete `Matcher`, `Executable`, and
//! `RecursiveExecutable` instances. Uses factory closures registered per type.
//! Supports `$tag` references to pre-built named plugins.

use crate::config::{MatchConfig, RuleConfig};
use crate::plugin::{Executable, Matcher, PluginResult, RecursiveExecutable, ReverseMatcher};
use crate::sequence::{ChainNode, NodeExecutor};
use std::collections::HashMap;
use std::sync::Arc;

/// Factory for creating matchers from a type string and args.
pub type MatcherFactory = Box<dyn Fn(&str) -> PluginResult<Box<dyn Matcher>> + Send + Sync>;

/// Factory for creating executables.
pub type ExecFactory = Box<dyn Fn(&str) -> PluginResult<Box<dyn Executable>> + Send + Sync>;

/// Factory for creating recursive executables.
pub type RecExecFactory =
    Box<dyn Fn(&str) -> PluginResult<Box<dyn RecursiveExecutable>> + Send + Sync>;

/// What kind of executor a type string produces.
pub enum ExecKind {
    Simple(ExecFactory),
    Recursive(RecExecFactory),
}

/// A named plugin that was pre-built and can be referenced by `$tag`.
enum NamedPlugin {
    Exec(Arc<dyn Executable>),
    RecExec(Arc<dyn RecursiveExecutable>),
}

/// Builds chains from parsed config by resolving type names to plugin instances.
pub struct ChainBuilder {
    matcher_factories: HashMap<String, MatcherFactory>,
    exec_factories: HashMap<String, ExecKind>,
    /// Named plugins (by tag) — executables and recursive executables.
    named_plugins: HashMap<String, NamedPlugin>,
    /// Named matchers (by tag).
    named_matchers: HashMap<String, Arc<dyn Matcher>>,
}

impl ChainBuilder {
    pub fn new() -> Self {
        Self {
            matcher_factories: HashMap::new(),
            exec_factories: HashMap::new(),
            named_plugins: HashMap::new(),
            named_matchers: HashMap::new(),
        }
    }

    /// Registers a matcher factory for the given type name.
    pub fn register_matcher(&mut self, type_name: &str, factory: MatcherFactory) {
        self.matcher_factories
            .insert(type_name.to_string(), factory);
    }

    /// Registers an executable factory.
    pub fn register_exec(&mut self, type_name: &str, factory: ExecFactory) {
        self.exec_factories
            .insert(type_name.to_string(), ExecKind::Simple(factory));
    }

    /// Registers a recursive executable factory.
    pub fn register_rec_exec(&mut self, type_name: &str, factory: RecExecFactory) {
        self.exec_factories
            .insert(type_name.to_string(), ExecKind::Recursive(factory));
    }

    /// Adds a pre-built named executable (referenced by tag `$name`).
    pub fn add_named_exec(&mut self, tag: &str, exec: Arc<dyn Executable>) {
        self.named_plugins
            .insert(tag.to_string(), NamedPlugin::Exec(exec));
    }

    /// Adds a pre-built named recursive executable.
    pub fn add_named_rec_exec(&mut self, tag: &str, exec: Arc<dyn RecursiveExecutable>) {
        self.named_plugins
            .insert(tag.to_string(), NamedPlugin::RecExec(exec));
    }

    /// Adds a pre-built named matcher.
    pub fn add_named_matcher(&mut self, tag: &str, matcher: Arc<dyn Matcher>) {
        self.named_matchers.insert(tag.to_string(), matcher);
    }

    /// Returns a named executable by tag, if it exists.
    pub fn get_named_exec(&self, tag: &str) -> Option<Arc<dyn Executable>> {
        match self.named_plugins.get(tag)? {
            NamedPlugin::Exec(e) => Some(e.clone()),
            _ => None,
        }
    }

    /// Resolves a single match config into a `Box<dyn Matcher>`.
    fn resolve_matcher(&self, mc: &MatchConfig) -> PluginResult<Box<dyn Matcher>> {
        let matcher: Box<dyn Matcher> = if !mc.tag.is_empty() {
            // Tag reference — look up named matcher.
            let named = self.named_matchers.get(&mc.tag).ok_or_else(
                || -> Box<dyn std::error::Error + Send + Sync> {
                    format!("unknown matcher tag: ${}", mc.tag).into()
                },
            )?;
            Box::new(ArcMatcher(named.clone()))
        } else if !mc.match_type.is_empty() {
            let factory = self.matcher_factories.get(&mc.match_type).ok_or_else(
                || -> Box<dyn std::error::Error + Send + Sync> {
                    format!("unknown matcher type: {}", mc.match_type).into()
                },
            )?;

            // Handle `$tag` references in matcher args.
            // Split args on whitespace, resolve `$tag` tokens to named matchers,
            // and pass the remaining tokens to the factory. If both exist,
            // combine with OR logic.
            let mut tag_matchers: Vec<Box<dyn Matcher>> = Vec::new();
            let mut remaining_args: Vec<&str> = Vec::new();

            for token in mc.args.split_whitespace() {
                if let Some(tag) = token.strip_prefix('$') {
                    let named = self.named_matchers.get(tag).ok_or_else(
                        || -> Box<dyn std::error::Error + Send + Sync> {
                            format!("unknown matcher tag in args: ${}", tag).into()
                        },
                    )?;
                    tag_matchers.push(Box::new(ArcMatcher(named.clone())));
                } else {
                    remaining_args.push(token);
                }
            }

            if tag_matchers.is_empty() {
                // No $tag references — pass args directly to factory.
                factory(&mc.args)?
            } else {
                // Build a matcher from non-$tag args (if any) and combine.
                let joined = remaining_args.join(" ");
                if !joined.is_empty() {
                    let inline = factory(&joined)?;
                    tag_matchers.push(inline);
                }
                if tag_matchers.len() == 1 {
                    tag_matchers.into_iter().next().unwrap()
                } else {
                    Box::new(OrMatcher(tag_matchers))
                }
            }
        } else {
            return Err("match config has neither tag nor type".into());
        };

        if mc.reverse {
            Ok(Box::new(ReverseMatcher::new(BoxedMatcher(matcher))))
        } else {
            Ok(matcher)
        }
    }

    /// Resolves a single rule config into a `NodeExecutor`.
    fn resolve_executor(&self, rc: &RuleConfig) -> PluginResult<NodeExecutor> {
        if !rc.tag.is_empty() {
            // Tag reference — the user wrote `$some_plugin`.
            let named = self.named_plugins.get(&rc.tag).ok_or_else(
                || -> Box<dyn std::error::Error + Send + Sync> {
                    format!("unknown plugin tag: ${}", rc.tag).into()
                },
            )?;
            return match named {
                NamedPlugin::Exec(e) => Ok(NodeExecutor::Simple(Box::new(ArcExec(e.clone())))),
                NamedPlugin::RecExec(r) => {
                    Ok(NodeExecutor::Recursive(Box::new(ArcRecExec(r.clone()))))
                }
            };
        }

        if rc.plugin_type.is_empty() {
            return Err("rule has neither tag nor type for exec".into());
        }

        let factory = self.exec_factories.get(&rc.plugin_type).ok_or_else(
            || -> Box<dyn std::error::Error + Send + Sync> {
                format!("unknown exec type: {}", rc.plugin_type).into()
            },
        )?;

        match factory {
            ExecKind::Simple(f) => Ok(NodeExecutor::Simple(f(&rc.args)?)),
            ExecKind::Recursive(f) => Ok(NodeExecutor::Recursive(f(&rc.args)?)),
        }
    }

    /// Builds a plugin from its type name and args, then registers it by tag.
    ///
    /// This is used for non-sequence plugins referenced by `$tag` in sequences.
    /// Returns true if the plugin was successfully built and registered.
    pub fn build_and_register(
        &mut self,
        tag: &str,
        type_name: &str,
        args: &str,
    ) -> PluginResult<()> {
        // Check matcher factories first.
        if let Some(factory) = self.matcher_factories.get(type_name) {
            let m = factory(args)?;
            self.named_matchers.insert(tag.to_string(), Arc::from(m));
            return Ok(());
        }

        // We need to move the factory out temporarily to avoid borrow issues.
        // Instead, check if it exists and what kind it is, then build.
        let is_simple = matches!(
            self.exec_factories.get(type_name),
            Some(ExecKind::Simple(_))
        );
        let is_recursive = matches!(
            self.exec_factories.get(type_name),
            Some(ExecKind::Recursive(_))
        );

        if is_simple {
            if let Some(ExecKind::Simple(f)) = self.exec_factories.get(type_name) {
                let exec = f(args)?;
                self.named_plugins
                    .insert(tag.to_string(), NamedPlugin::Exec(Arc::from(exec)));
                return Ok(());
            }
        } else if is_recursive {
            if let Some(ExecKind::Recursive(f)) = self.exec_factories.get(type_name) {
                let exec = f(args)?;
                self.named_plugins
                    .insert(tag.to_string(), NamedPlugin::RecExec(Arc::from(exec)));
                return Ok(());
            }
        }

        Err(format!("unknown plugin type: {}", type_name).into())
    }

    /// Builds a chain of [`ChainNode`]s from a list of parsed rules.
    pub fn build_chain(&self, rules: &[RuleConfig]) -> PluginResult<Vec<ChainNode>> {
        let mut chain = Vec::with_capacity(rules.len());
        for rule in rules {
            let matchers: Vec<Box<dyn Matcher>> = rule
                .matches
                .iter()
                .map(|mc| self.resolve_matcher(mc))
                .collect::<PluginResult<Vec<_>>>()?;
            let executor = self.resolve_executor(rule)?;
            chain.push(ChainNode { matchers, executor });
        }
        Ok(chain)
    }
}

impl Default for ChainBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ── Arc wrapper types for shared named plugins ──────────────────

/// Wrapper to delegate `Matcher` calls through `Arc`.
struct ArcMatcher(Arc<dyn Matcher>);
impl Matcher for ArcMatcher {
    fn match_ctx(&self, ctx: &crate::context::Context) -> PluginResult<bool> {
        self.0.match_ctx(ctx)
    }
}

/// Wrapper to delegate `Executable` calls through `Arc`.
struct ArcExec(Arc<dyn Executable>);
#[async_trait::async_trait]
impl Executable for ArcExec {
    async fn exec(&self, ctx: &mut crate::context::Context) -> PluginResult<()> {
        self.0.exec(ctx).await
    }
}

/// Wrapper to delegate `RecursiveExecutable` calls through `Arc`.
struct ArcRecExec(Arc<dyn RecursiveExecutable>);
#[async_trait::async_trait]
impl RecursiveExecutable for ArcRecExec {
    async fn exec_recursive(
        &self,
        ctx: &mut crate::context::Context,
        next: crate::sequence::ChainWalker<'_>,
    ) -> PluginResult<()> {
        self.0.exec_recursive(ctx, next).await
    }
}

/// Wrapper to enable `ReverseMatcher<BoxedMatcher>`.
struct BoxedMatcher(Box<dyn Matcher>);
impl Matcher for BoxedMatcher {
    fn match_ctx(&self, ctx: &crate::context::Context) -> PluginResult<bool> {
        self.0.match_ctx(ctx)
    }
}

/// Combines multiple matchers with OR logic — true if any inner matcher matches.
struct OrMatcher(Vec<Box<dyn Matcher>>);
impl Matcher for OrMatcher {
    fn match_ctx(&self, ctx: &crate::context::Context) -> PluginResult<bool> {
        for m in &self.0 {
            if m.match_ctx(ctx)? {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

// Make wrappers Send + Sync (they wrap Send + Sync inner types).
unsafe impl Send for BoxedMatcher {}
unsafe impl Sync for BoxedMatcher {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{MatchConfig, RuleConfig};
    use crate::context::Context;
    use crate::plugin::Executable;
    use async_trait::async_trait;
    use hickory_proto::op::{Message, MessageType, OpCode, Query};
    use hickory_proto::rr::{Name, RecordType};

    fn make_query() -> Message {
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
        msg
    }

    struct SetMarkExec(u32);
    #[async_trait]
    impl Executable for SetMarkExec {
        async fn exec(&self, ctx: &mut Context) -> PluginResult<()> {
            ctx.set_mark(self.0);
            Ok(())
        }
    }

    struct AlwaysTrueMatcher;
    impl Matcher for AlwaysTrueMatcher {
        fn match_ctx(&self, _ctx: &Context) -> PluginResult<bool> {
            Ok(true)
        }
    }

    #[test]
    fn build_simple_chain() {
        let mut builder = ChainBuilder::new();
        builder.register_exec(
            "set_mark",
            Box::new(|args: &str| {
                let mark: u32 = args.parse().unwrap_or(0);
                Ok(Box::new(SetMarkExec(mark)) as Box<dyn Executable>)
            }),
        );
        builder.register_matcher(
            "always",
            Box::new(|_args: &str| Ok(Box::new(AlwaysTrueMatcher) as Box<dyn Matcher>)),
        );

        let rules = vec![RuleConfig {
            matches: vec![MatchConfig {
                match_type: "always".into(),
                ..Default::default()
            }],
            plugin_type: "set_mark".into(),
            args: "42".into(),
            ..Default::default()
        }];

        let chain = builder.build_chain(&rules).unwrap();
        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0].matchers.len(), 1);
    }

    #[test]
    fn unknown_type_errors() {
        let builder = ChainBuilder::new();
        let rules = vec![RuleConfig {
            plugin_type: "nonexistent".into(),
            ..Default::default()
        }];
        assert!(builder.build_chain(&rules).is_err());
    }

    #[test]
    fn reverse_matcher() {
        let mut builder = ChainBuilder::new();
        builder.register_matcher(
            "always",
            Box::new(|_: &str| Ok(Box::new(AlwaysTrueMatcher) as Box<dyn Matcher>)),
        );
        builder.register_exec(
            "nop",
            Box::new(|_: &str| Ok(Box::new(SetMarkExec(0)) as Box<dyn Executable>)),
        );

        let rules = vec![RuleConfig {
            matches: vec![MatchConfig {
                match_type: "always".into(),
                reverse: true,
                ..Default::default()
            }],
            plugin_type: "nop".into(),
            ..Default::default()
        }];

        let chain = builder.build_chain(&rules).unwrap();
        let ctx = Context::new(make_query());
        assert!(!chain[0].matchers[0].match_ctx(&ctx).unwrap());
    }

    #[test]
    fn tag_resolution() {
        let mut builder = ChainBuilder::new();
        builder.add_named_exec("my_exec", Arc::new(SetMarkExec(99)));

        let rules = vec![RuleConfig {
            tag: "my_exec".into(),
            ..Default::default()
        }];

        let chain = builder.build_chain(&rules).unwrap();
        assert_eq!(chain.len(), 1);
    }

    #[test]
    fn unknown_tag_errors() {
        let builder = ChainBuilder::new();
        let rules = vec![RuleConfig {
            tag: "nonexistent".into(),
            ..Default::default()
        }];
        assert!(builder.build_chain(&rules).is_err());
    }
}
