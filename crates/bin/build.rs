// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Build script.
//!
//! Captures the short git commit hash at compile time and exposes it to the
//! crate via the `GIT_COMMIT_SHORT` environment variable (read with `env!`).
//! Falls back to `"unknown"` when the build is not inside a git work tree
//! (e.g. a source tarball) or `git` is unavailable, so builds never fail
//! because of version metadata.

use std::process::Command;

fn main() {
    let short = short_commit_hash().unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=GIT_COMMIT_SHORT={short}");
    // The commit hash is derived from the working tree state, not from
    // source files Cargo tracks; rebuilding when HEAD changes is enough.
    println!("cargo:rerun-if-changed=.git/HEAD");
}

/// Returns the short hash of `HEAD`, or `None` if it could not be determined.
fn short_commit_hash() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--short=8", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let s = String::from_utf8(output.stdout).ok()?;
    let trimmed = s.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}
