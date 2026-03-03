// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! redns CLI entry point.

use clap::{Parser, Subcommand};
use redns_core::chain_builder::ChainBuilder;
use redns_core::config::parse_rule_args;
use redns_core::redns::{Redns, find_and_load_config, load_config_file};
use redns_core::server::EntryHandler;
use redns_core::upstream::UpstreamWrapper;
use redns_core::{PluginRegistry, Sequence};
use std::sync::Arc;
use tokio::net::{TcpListener, UdpSocket};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Parser)]
#[command(name = "redns", about = "A DNS forwarder", version = VERSION)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start redns main program.
    Start {
        /// Config file path.
        #[arg(short, long)]
        config: Option<String>,

        /// Working directory.
        #[arg(short, long)]
        dir: Option<String>,
    },

    /// Print version info and exit.
    Version,
}

#[tokio::main]
async fn main() {
    // Tracing is initialized inside run_server after config is loaded,
    // so that `log.level` from the config file is respected.
    // Set up a minimal stderr fallback for early errors.
    let early_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let early_sub = tracing_subscriber::fmt()
        .with_env_filter(early_filter)
        .finish();
    let _guard = tracing::subscriber::set_default(early_sub);

    // Install rustls provider early to avoid runtime provider auto-detection panics.
    redns_core::install_rustls_crypto_provider();

    let cli = Cli::parse();

    match cli.command {
        Commands::Version => println!("redns {VERSION}"),
        Commands::Start { config, dir } => {
            if let Err(e) = run_server(config, dir).await {
                error!(error = %e, "server failed");
                std::process::exit(1);
            }
        }
    }
}

/// Registers all built-in matcher and executor factories on the given builder.
fn register_builtins(builder: &mut ChainBuilder) {
    use redns_core::built_in::*;
    use redns_core::plugin::{Executable, Matcher, RecursiveExecutable};
    use redns_executables::*;
    use redns_matchers::*;

    // ── Built-in flow-control ────────────────────────────────────
    builder.register_rec_exec(
        "accept",
        Box::new(|_| Ok(Box::new(ActionAccept) as Box<dyn RecursiveExecutable>)),
    );
    builder.register_rec_exec(
        "reject",
        Box::new(|_| Ok(Box::new(ActionReject) as Box<dyn RecursiveExecutable>)),
    );

    // ── Simple Executables ───────────────────────────────────────
    builder.register_exec(
        "black_hole",
        Box::new(|args: &str| Ok(Box::new(BlackHole::from_str_args(args)?) as Box<dyn Executable>)),
    );
    builder.register_exec(
        "ttl",
        Box::new(|args: &str| Ok(Box::new(Ttl::from_str_args(args)?) as Box<dyn Executable>)),
    );
    builder.register_exec(
        "arbitrary",
        Box::new(|_| Ok(Box::new(Arbitrary::new()) as Box<dyn Executable>)),
    );
    builder.register_exec(
        "sleep",
        Box::new(|args: &str| Ok(Box::new(Sleep::from_str_args(args)?) as Box<dyn Executable>)),
    );
    builder.register_exec(
        "debug_print",
        Box::new(|_| Ok(Box::new(DebugPrint) as Box<dyn Executable>)),
    );
    builder.register_exec(
        "drop_resp",
        Box::new(|_| Ok(Box::new(DropResp) as Box<dyn Executable>)),
    );
    builder.register_exec(
        "shuffle",
        Box::new(|_| {
            Ok(Box::new(Shuffle {
                answer: true,
                ns: true,
                extra: true,
            }) as Box<dyn Executable>)
        }),
    );
    builder.register_exec(
        "hosts",
        Box::new(|args: &str| Ok(Box::new(Hosts::from_lines(args)?) as Box<dyn Executable>)),
    );
    // ── Recursive Executables ────────────────────────────────────
    builder.register_rec_exec(
        "cache",
        Box::new(|args: &str| {
            let size: usize = args.trim().parse().unwrap_or(1024);
            Ok(
                Box::new(Cache::new(size, std::time::Duration::from_secs(30)))
                    as Box<dyn RecursiveExecutable>,
            )
        }),
    );
    builder.register_rec_exec(
        "redirect",
        Box::new(|args: &str| {
            Ok(Box::new(Redirect::from_lines(args)?) as Box<dyn RecursiveExecutable>)
        }),
    );
    builder.register_rec_exec(
        "ecs",
        Box::new(|args: &str| {
            let cfg = redns_executables::ecs_handler::EcsConfig::from_str_args(args)?;
            Ok(Box::new(EcsHandler::new(cfg)) as Box<dyn RecursiveExecutable>)
        }),
    );
    builder.register_rec_exec(
        "ecs_handler",
        Box::new(|args: &str| {
            let cfg = redns_executables::ecs_handler::EcsConfig::from_str_args(args)?;
            Ok(Box::new(EcsHandler::new(cfg)) as Box<dyn RecursiveExecutable>)
        }),
    );
    builder.register_rec_exec(
        "dual_selector",
        Box::new(|args: &str| {
            let prefer = if args.trim() == "ipv6" || args.trim() == "prefer_ipv6" {
                redns_executables::dual_selector::Prefer::Ipv6
            } else {
                redns_executables::dual_selector::Prefer::Ipv4
            };
            Ok(Box::new(DualSelector::new(prefer)) as Box<dyn RecursiveExecutable>)
        }),
    );
    builder.register_rec_exec(
        "prefer_ipv4",
        Box::new(|_| {
            Ok(Box::new(DualSelector::new(
                redns_executables::dual_selector::Prefer::Ipv4,
            )) as Box<dyn RecursiveExecutable>)
        }),
    );
    builder.register_rec_exec(
        "prefer_ipv6",
        Box::new(|_| {
            Ok(Box::new(DualSelector::new(
                redns_executables::dual_selector::Prefer::Ipv6,
            )) as Box<dyn RecursiveExecutable>)
        }),
    );
    builder.register_rec_exec(
        "reverse_lookup",
        Box::new(|args: &str| {
            let cfg = redns_executables::reverse_lookup::ReverseLookupConfig::from_str_args(args)?;
            Ok(Box::new(ReverseLookup::new(cfg)) as Box<dyn RecursiveExecutable>)
        }),
    );
    builder.register_rec_exec(
        "metrics_collector",
        Box::new(|args: &str| {
            let cfg = redns_executables::metrics::MetricsConfig::from_str_args(args)?;
            Ok(Box::new(MetricsCollector::new(cfg)) as Box<dyn RecursiveExecutable>)
        }),
    );

    // ── Matchers ─────────────────────────────────────────────────
    builder.register_matcher(
        "has_resp",
        Box::new(|_| Ok(Box::new(HasResp) as Box<dyn Matcher>)),
    );
    builder.register_matcher(
        "has_wanted_ans",
        Box::new(|_| Ok(Box::new(HasWantedAns) as Box<dyn Matcher>)),
    );
    builder.register_matcher(
        "qtype",
        Box::new(|args: &str| Ok(Box::new(QTypeMatcher::from_str_args(args)?) as Box<dyn Matcher>)),
    );
    builder.register_matcher(
        "qclass",
        Box::new(
            |args: &str| Ok(Box::new(QClassMatcher::from_str_args(args)?) as Box<dyn Matcher>),
        ),
    );
    builder.register_matcher(
        "qname",
        Box::new(|args: &str| Ok(Box::new(QnameMatcher::from_str_args(args)?) as Box<dyn Matcher>)),
    );
    builder.register_matcher(
        "client_ip",
        Box::new(|args: &str| {
            Ok(Box::new(ClientIpMatcher::from_str_args(args)?) as Box<dyn Matcher>)
        }),
    );
    builder.register_matcher(
        "resp_ip",
        Box::new(
            |args: &str| Ok(Box::new(RespIpMatcher::from_str_args(args)?) as Box<dyn Matcher>),
        ),
    );
    builder.register_matcher(
        "cname",
        Box::new(|args: &str| Ok(Box::new(CnameMatcher::from_str_args(args)) as Box<dyn Matcher>)),
    );
    builder.register_matcher(
        "rcode",
        Box::new(|args: &str| Ok(Box::new(RcodeMatcher::from_str_args(args)?) as Box<dyn Matcher>)),
    );
    builder.register_matcher(
        "random",
        Box::new(
            |args: &str| Ok(Box::new(RandomMatcher::from_str_args(args)?) as Box<dyn Matcher>),
        ),
    );
    builder.register_matcher(
        "rate_limiter",
        Box::new(|args: &str| {
            let cfg = redns_matchers::rate_limiter::RateLimiterConfig::from_str_args(args)?;
            Ok(Box::new(RateLimiter::new(cfg)) as Box<dyn Matcher>)
        }),
    );
    builder.register_matcher(
        "env",
        Box::new(|args: &str| Ok(Box::new(EnvMatcher::from_str_args(args)) as Box<dyn Matcher>)),
    );
    builder.register_matcher(
        "ptr_ip",
        Box::new(|args: &str| Ok(Box::new(PtrIpMatcher::from_str_args(args)?) as Box<dyn Matcher>)),
    );
    builder.register_matcher(
        "string_exp",
        Box::new(|args: &str| {
            Ok(Box::new(StringExpMatcher::from_str_args(args)?) as Box<dyn Matcher>)
        }),
    );

    // ── Data providers (registered as matchers) ──────────────────
    builder.register_matcher(
        "domain_set",
        Box::new(|args: &str| Ok(Box::new(DomainSet::from_str_args(args)?) as Box<dyn Matcher>)),
    );
    builder.register_matcher(
        "ip_set",
        Box::new(|args: &str| Ok(Box::new(IpSet::from_str_args(args)?) as Box<dyn Matcher>)),
    );
}

async fn run_server(
    config_path: Option<String>,
    working_dir: Option<String>,
) -> Result<(), redns_core::PluginError> {
    if let Some(dir) = working_dir {
        std::env::set_current_dir(&dir).map_err(|e| -> redns_core::PluginError {
            format!("failed to change working directory to {}: {}", dir, e).into()
        })?;
        info!(path = %dir, "working directory changed");
    }

    let (cfg, file_used) = if let Some(path) = config_path {
        let cfg = load_config_file(&path)?;
        (cfg, path)
    } else {
        find_and_load_config()?
    };

    // Re-initialize tracing with the config's log level.
    // RUST_LOG env var takes precedence; otherwise use config; otherwise "info".
    let log_level = cfg.log.level.as_deref().unwrap_or("info");
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(log_level));
    // Replace the global subscriber with one that uses the config level.
    // If a log file is configured, write to that file instead of stdout.
    if let Some(ref log_file) = cfg.log.file {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_file)
            .unwrap_or_else(|e| panic!("failed to open log file '{}': {}", log_file, e));
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(file)
            .with_ansi(false)
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("failed to set global tracing subscriber");
    } else {
        let subscriber = tracing_subscriber::fmt().with_env_filter(filter).finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("failed to set global tracing subscriber");
    };

    info!(file = %file_used, level = %log_level, "config loaded");

    let registry = PluginRegistry::new();
    let _redns = Redns::new(cfg.clone(), registry)?;

    let mut builder = ChainBuilder::new();
    register_builtins(&mut builder);

    // Register forward plugin with upstream collection for metrics API.
    let all_upstreams: Arc<std::sync::Mutex<Vec<Arc<UpstreamWrapper>>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    {
        let upstreams_collector = all_upstreams.clone();
        builder.register_exec(
            "forward",
            Box::new(move |args: &str| {
                use redns_core::plugin::Executable;
                use redns_executables::Forward;
                use redns_executables::forward::ForwardConfig;
                let cfg = if let Ok(yaml_cfg) = ForwardConfig::from_yaml_str(args) {
                    yaml_cfg
                } else {
                    ForwardConfig::from_str_args(args)
                };
                let fwd = Forward::new(cfg)?;
                if let Ok(mut guard) = upstreams_collector.lock() {
                    guard.extend(fwd.upstreams().iter().cloned());
                }
                Ok(Box::new(fwd) as Box<dyn Executable>)
            }),
        );
    }

    // ── Phase 1: Build non-sequence plugins and register by tag ──
    // Two passes: first register all normal plugins, then resolve
    // inter-plugin references like `fallback`.
    let mut deferred_fallbacks = Vec::new();

    for plugin in &cfg.plugins {
        if plugin.plugin_type == "sequence" || plugin.tag.is_empty() {
            continue;
        }

        // Defer fallback plugins — they need primary/secondary to be registered first.
        if plugin.plugin_type == "fallback" {
            deferred_fallbacks.push(plugin.clone());
            continue;
        }

        let args_str: String = plugin.args.clone();

        match builder.build_and_register(&plugin.tag, &plugin.plugin_type, &args_str) {
            Ok(()) => {
                info!(tag = %plugin.tag, plugin_type = %plugin.plugin_type, "registered named plugin")
            }
            Err(e) => {
                warn!(tag = %plugin.tag, plugin_type = %plugin.plugin_type, error = %e, "failed to build plugin")
            }
        }
    }

    // ── Phase 1.5: Build sequence plugins (first pass) ───────────
    // Some sequences may fail if they reference not-yet-built plugins
    // like $fallback. Track failures for retry after fallback is built.
    let mut failed_sequences: Vec<usize> = Vec::new();

    for (idx, plugin) in cfg.plugins.iter().enumerate() {
        if plugin.plugin_type != "sequence" {
            continue;
        }

        let rule_args: Vec<redns_core::RuleArgs> =
            redns_core::config::deserialize_yaml_str(&plugin.args).unwrap_or_default();
        let rule_configs: Vec<_> = rule_args.iter().map(|ra| parse_rule_args(ra)).collect();
        match builder.build_chain(&rule_configs) {
            Ok(chain) => {
                let tag = if plugin.tag.is_empty() {
                    "(anonymous)"
                } else {
                    &plugin.tag
                };
                info!(tag = %tag, rules = chain.len(), "sequence built");

                if !plugin.tag.is_empty() {
                    builder.add_named_exec(&plugin.tag, Arc::new(Sequence::new(chain)));
                }
            }
            Err(_) => {
                // Defer — might succeed after fallback is registered.
                failed_sequences.push(idx);
            }
        }
    }

    // ── Phase 2: Build fallback plugins (depend on sequences) ───
    for plugin in &deferred_fallbacks {
        use redns_executables::fallback::{Fallback, FallbackArgs};

        let args: FallbackArgs = match serde_saphyr::from_str(&plugin.args) {
            Ok(a) => a,
            Err(e) => {
                warn!(tag = %plugin.tag, error = %e, "fallback: invalid args");
                continue;
            }
        };

        let primary = match builder.get_named_exec(&args.primary) {
            Some(e) => e,
            None => {
                warn!(tag = %plugin.tag, primary = %args.primary, "fallback: primary exec not found");
                continue;
            }
        };
        let secondary = match builder.get_named_exec(&args.secondary) {
            Some(e) => e,
            None => {
                warn!(tag = %plugin.tag, secondary = %args.secondary, "fallback: secondary exec not found");
                continue;
            }
        };

        let threshold = if args.threshold > 0 {
            std::time::Duration::from_millis(args.threshold)
        } else {
            std::time::Duration::from_millis(500)
        };

        let fb = Fallback::new(primary, secondary, threshold, args.always_standby);
        builder.add_named_exec(&plugin.tag, std::sync::Arc::new(fb));
        info!(tag = %plugin.tag, "registered fallback plugin");
    }

    // ── Phase 2.5: Retry failed sequences (now $fallback etc. exist) ──
    for idx in &failed_sequences {
        let plugin = &cfg.plugins[*idx];
        let rule_args: Vec<redns_core::RuleArgs> =
            redns_core::config::deserialize_yaml_str(&plugin.args).unwrap_or_default();
        let rule_configs: Vec<_> = rule_args.iter().map(|ra| parse_rule_args(ra)).collect();
        match builder.build_chain(&rule_configs) {
            Ok(chain) => {
                let tag = if plugin.tag.is_empty() {
                    "(anonymous)"
                } else {
                    &plugin.tag
                };
                info!(tag = %tag, rules = chain.len(), "sequence built (retry)");

                if !plugin.tag.is_empty() {
                    builder.add_named_exec(&plugin.tag, Arc::new(Sequence::new(chain)));
                }
            }
            Err(e) => {
                warn!(tag = %plugin.tag, error = %e, "failed to build sequence, skipping");
            }
        }
    }

    // ── Phase 3: Start servers from config ──────────────────────
    // Collect server configs from both `servers:` section and
    // `udp_server`/`tcp_server` plugin entries.
    let mut servers: Vec<redns_core::config::ServerConfig> = cfg.servers.clone();

    // Extract server configs from plugin entries.
    for plugin in &cfg.plugins {
        match plugin.plugin_type.as_str() {
            "udp_server" | "tcp_server" => {
                #[derive(serde::Deserialize, Default)]
                struct ServerPluginArgs {
                    #[serde(default)]
                    entry: String,
                    #[serde(default = "default_listen")]
                    listen: String,
                }
                fn default_listen() -> String {
                    "127.0.0.1:53".into()
                }

                let args: ServerPluginArgs =
                    serde_saphyr::from_str(&plugin.args).unwrap_or_default();
                let proto = if plugin.plugin_type == "udp_server" {
                    "udp"
                } else {
                    "tcp"
                };
                servers.push(redns_core::config::ServerConfig {
                    protocol: proto.into(),
                    addr: args.listen,
                    entry: args.entry,
                });
            }
            _ => {}
        }
    }

    if servers.is_empty() {
        error!("no servers configured");
        return Err("no servers configured".into());
    }

    let cancel = tokio_util::sync::CancellationToken::new();

    for srv in &servers {
        if srv.entry.is_empty() {
            error!(addr = %srv.addr, "server has no entry sequence configured");
            return Err(format!("server {} has no entry sequence configured", srv.addr).into());
        }
        let entry_exec = match builder.get_named_exec(&srv.entry) {
            Some(e) => e,
            None => {
                error!(entry = %srv.entry, addr = %srv.addr, "entry sequence not found");
                return Err(format!("entry sequence '{}' not found", srv.entry).into());
            }
        };
        let handler: Arc<dyn redns_core::DnsHandler> = Arc::new(EntryHandler::new(entry_exec));

        let addr = &srv.addr;
        let proto = &srv.protocol;

        if proto.contains("udp") || proto == "udp+tcp" {
            match UdpSocket::bind(addr).await {
                Ok(socket) => {
                    info!(addr = %addr, "UDP server listening");
                    let h = handler.clone();
                    let s = Arc::new(socket);
                    let c = cancel.clone();
                    tokio::spawn(async move {
                        if let Err(e) = redns_core::udp_server::serve_udp(s, h, c).await {
                            error!(error = %e, "UDP server error");
                        }
                    });
                }
                Err(e) => warn!(error = %e, addr = %addr, "failed to bind UDP"),
            }
        }

        if proto.contains("tcp") || proto == "udp+tcp" {
            match TcpListener::bind(addr).await {
                Ok(listener) => {
                    info!(addr = %addr, "TCP server listening");
                    let h = handler.clone();
                    let c = cancel.clone();
                    tokio::spawn(async move {
                        if let Err(e) = redns_core::tcp_server::serve_tcp(listener, h, c).await {
                            error!(error = %e, "TCP server error");
                        }
                    });
                }
                Err(e) => warn!(error = %e, addr = %addr, "failed to bind TCP"),
            }
        }
    }

    // ── Phase 4: Start API HTTP server ──────────────────────────
    if let Some(ref api_addr) = cfg.api.http {
        match TcpListener::bind(api_addr).await {
            Ok(listener) => {
                info!(addr = %api_addr, "API HTTP server listening");
                let upstreams = all_upstreams.clone();
                let c = cancel.clone();
                tokio::spawn(async move {
                    serve_api(listener, upstreams, c).await;
                });
            }
            Err(e) => warn!(error = %e, addr = %api_addr, "failed to bind API HTTP"),
        }
    }

    info!("redns started");
    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for ctrl-c");
    info!("shutting down...");
    cancel.cancel();
    // Give servers time to clean up.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    info!("redns stopped");
    Ok(())
}

/// Simple API HTTP server.
async fn serve_api(
    listener: TcpListener,
    upstreams: Arc<std::sync::Mutex<Vec<Arc<UpstreamWrapper>>>>,
    cancel: tokio_util::sync::CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            result = listener.accept() => {
                match result {
                    Ok((stream, _peer)) => {
                        let upstreams = upstreams.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_api_request(stream, upstreams).await {
                                warn!(error = %e, "API request error");
                            }
                        });
                    }
                    Err(e) => {
                        warn!(error = %e, "API accept error");
                    }
                }
            }
        }
    }
}

async fn handle_api_request(
    mut stream: tokio::net::TcpStream,
    upstreams: Arc<std::sync::Mutex<Vec<Arc<UpstreamWrapper>>>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await?;
    if n == 0 {
        return Ok(());
    }
    let request = String::from_utf8_lossy(&buf[..n]);

    // Parse the request line.
    let first_line = request.lines().next().unwrap_or("");
    let parts: Vec<&str> = first_line.split_whitespace().collect();
    let (method, path) = if parts.len() >= 2 {
        (parts[0], parts[1])
    } else {
        ("", "")
    };

    if method == "GET" && path == "/metrics/upstreams" {
        let metrics: Vec<redns_core::UpstreamMetrics> = {
            let guard =
                upstreams
                    .lock()
                    .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                        format!("lock error: {e}").into()
                    })?;
            guard.iter().map(|u| u.snapshot()).collect()
        };
        let body = serde_json::to_string_pretty(&metrics)?;
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(resp.as_bytes()).await?;
    } else {
        let body = "{\"error\":\"not found\"}";
        let resp = format!(
            "HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(resp.as_bytes()).await?;
    }

    Ok(())
}
