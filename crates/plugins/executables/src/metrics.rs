// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Metrics collector — lightweight query/error counting and latency tracking.
//!
//! Uses atomic counters instead of Prometheus for zero external dependencies.

use async_trait::async_trait;
use redns_core::plugin::PluginResult;
use redns_core::sequence::ChainWalker;
use redns_core::{Context, RecursiveExecutable};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tracing::info;

/// Metrics collector configuration.
#[derive(Debug, Clone)]
pub struct MetricsConfig {
    /// Name label for this collector instance.
    pub name: String,
}

impl MetricsConfig {
    pub fn from_str_args(s: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let name = s.trim().to_string();
        if name.is_empty() {
            return Err("metrics_collector requires a name".into());
        }
        Ok(Self { name })
    }
}

/// Lightweight metrics collector.
pub struct MetricsCollector {
    name: String,
    query_total: AtomicU64,
    err_total: AtomicU64,
    active_threads: AtomicU64,
    /// Sum of response latencies in microseconds (for average calculation).
    latency_sum_us: AtomicU64,
    latency_count: AtomicU64,
}

impl MetricsCollector {
    pub fn new(config: MetricsConfig) -> Self {
        Self {
            name: config.name,
            query_total: AtomicU64::new(0),
            err_total: AtomicU64::new(0),
            active_threads: AtomicU64::new(0),
            latency_sum_us: AtomicU64::new(0),
            latency_count: AtomicU64::new(0),
        }
    }

    /// Get current metrics snapshot.
    pub fn snapshot(&self) -> MetricsSnapshot {
        let count = self.latency_count.load(Ordering::Relaxed);
        let sum = self.latency_sum_us.load(Ordering::Relaxed);
        MetricsSnapshot {
            name: self.name.clone(),
            query_total: self.query_total.load(Ordering::Relaxed),
            err_total: self.err_total.load(Ordering::Relaxed),
            active_threads: self.active_threads.load(Ordering::Relaxed),
            avg_latency_ms: if count > 0 {
                (sum as f64 / count as f64) / 1000.0
            } else {
                0.0
            },
        }
    }
}

/// A point-in-time snapshot of metrics.
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub name: String,
    pub query_total: u64,
    pub err_total: u64,
    pub active_threads: u64,
    pub avg_latency_ms: f64,
}

#[async_trait]
impl RecursiveExecutable for MetricsCollector {
    async fn exec_recursive(&self, ctx: &mut Context, mut next: ChainWalker) -> PluginResult<()> {
        self.active_threads.fetch_add(1, Ordering::Relaxed);
        self.query_total.fetch_add(1, Ordering::Relaxed);

        let start = Instant::now();
        let result = next.exec_next(ctx).await;

        let elapsed = start.elapsed();
        self.latency_sum_us
            .fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);
        self.latency_count.fetch_add(1, Ordering::Relaxed);
        self.active_threads.fetch_sub(1, Ordering::Relaxed);

        if result.is_err() {
            self.err_total.fetch_add(1, Ordering::Relaxed);
        }

        // Log every 1000 queries.
        let total = self.query_total.load(Ordering::Relaxed);
        if total % 1000 == 0 {
            let snap = self.snapshot();
            info!(
                name = %snap.name,
                queries = snap.query_total,
                errors = snap.err_total,
                avg_latency_ms = format!("{:.2}", snap.avg_latency_ms),
                "metrics"
            );
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_str_args_valid() {
        let cfg = MetricsConfig::from_str_args("my_metrics").unwrap();
        assert_eq!(cfg.name, "my_metrics");
    }

    #[test]
    fn from_str_args_empty_errors() {
        assert!(MetricsConfig::from_str_args("").is_err());
    }

    #[test]
    fn snapshot_initial() {
        let c = MetricsCollector::new(MetricsConfig {
            name: "test".into(),
        });
        let s = c.snapshot();
        assert_eq!(s.query_total, 0);
        assert_eq!(s.err_total, 0);
        assert_eq!(s.active_threads, 0);
    }
}
