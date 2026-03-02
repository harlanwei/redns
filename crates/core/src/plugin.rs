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

use crate::context::Context;
use crate::sequence::ChainWalker;
use async_trait::async_trait;

/// Error type used throughout the plugin system.
pub type PluginError = Box<dyn std::error::Error + Send + Sync>;

/// Result type alias for plugin operations.
pub type PluginResult<T> = Result<T, PluginError>;

/// A matcher determines whether a [`Context`] matches a certain pattern.
///
/// Matchers remain **synchronous** since they do cheap in-memory lookups
/// with no I/O.
pub trait Matcher: Send + Sync {
    /// Returns `true` if the context matches this matcher's criteria.
    fn match_ctx(&self, ctx: &Context) -> PluginResult<bool>;
}

/// An executable plugin that can modify or act upon a [`Context`].
#[async_trait]
pub trait Executable: Send + Sync {
    /// Executes the plugin logic against the given context.
    async fn exec(&self, ctx: &mut Context) -> PluginResult<()>;
}

/// A recursive executable that receives the remaining chain walker,
/// allowing it to control whether downstream nodes execute.
#[async_trait]
pub trait RecursiveExecutable: Send + Sync {
    /// Executes with access to the remaining chain via `next`.
    async fn exec_recursive(&self, ctx: &mut Context, next: ChainWalker<'_>) -> PluginResult<()>;
}

/// A matcher that negates another matcher's result.
pub struct ReverseMatcher<M> {
    inner: M,
}

impl<M> ReverseMatcher<M> {
    pub fn new(inner: M) -> Self {
        Self { inner }
    }
}

impl<M: Matcher> Matcher for ReverseMatcher<M> {
    fn match_ctx(&self, ctx: &Context) -> PluginResult<bool> {
        self.inner.match_ctx(ctx).map(|ok| !ok)
    }
}
