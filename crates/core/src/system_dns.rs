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

//! System DNS fallback resolution via the upstream (Internet) interface.
//!
//! When the plugin chain produces a SERVFAIL response, this module forwards the
//! query to the DNS servers assigned to the WAN / Internet network interface —
//! the ones obtained over DHCP or PPP from the ISP.
//!
//! # Where the WAN servers live
//!
//! On OpenWrt (the target platform) `/etc/resolv.conf` points at the *local*
//! resolver — typically `nameserver 127.0.0.1`, which is this very program.
//! Reading it and forwarding there would loop straight back into ourselves. The
//! DHCP/PPP-assigned upstream servers are instead written to
//! `/tmp/resolv.conf.d/resolv.conf.auto`. We therefore read that file first and,
//! on any platform, skip loopback nameservers so the fallback can never query
//! itself.
//!
//! The query is sent with the OS picking the route, so it naturally egresses the
//! interface that owns the route to each WAN DNS server. No interface-name
//! guessing or source-address pinning is needed.

use crate::plugin::PluginResult;
use hickory_proto::op::{Message, ResponseCode};
use std::io::BufRead;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;
use tokio::net::UdpSocket;
use tracing::debug;

/// Default timeout for system DNS queries.
const SYSTEM_DNS_TIMEOUT: Duration = Duration::from_secs(5);

/// Maximum DNS UDP payload size for receiving.
const MAX_UDP_SIZE: usize = 4096;

/// Candidate `resolv.conf` files, in priority order.
///
/// On OpenWrt the WAN-assigned (DHCP/PPP) DNS servers are written to
/// `resolv.conf.auto`, while `/etc/resolv.conf` points at the local resolver
/// (`127.0.0.1`) — i.e. this program. We must read the WAN list, not the local
/// one, or the fallback would loop back into ourselves. `/etc/resolv.conf` is
/// kept last as a fallback for non-OpenWrt hosts; its loopback entries are
/// filtered out by [`system_nameservers`].
const RESOLV_CONF_PATHS: &[&str] = &[
    "/tmp/resolv.conf.d/resolv.conf.auto",
    "/tmp/resolv.conf.auto",
    "/etc/resolv.conf",
];

/// Parses a `resolv.conf`-format file and returns its `nameserver` IPs.
///
/// A missing file yields an empty list (expected — we probe several paths).
fn parse_resolv_conf(path: &str) -> Vec<IpAddr> {
    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) => {
            debug!(error = %e, path, "resolv.conf not readable");
            return Vec::new();
        }
    };

    std::io::BufReader::new(file)
        .lines()
        .filter_map(|line| {
            let line = line.ok()?;
            let line = line.trim();
            if !line.starts_with("nameserver") {
                return None;
            }
            let ip_str = line["nameserver".len()..].trim();
            ip_str.parse::<IpAddr>().ok()
        })
        .collect()
}

/// Returns the WAN/Internet-assigned DNS servers to use for the fallback.
///
/// Probes [`RESOLV_CONF_PATHS`] in order and returns the nameservers from the
/// first file that yields at least one usable (non-loopback) entry. Loopback
/// servers are dropped so the fallback never queries this resolver itself, and
/// duplicates are removed while preserving order.
fn system_nameservers() -> Vec<IpAddr> {
    for path in RESOLV_CONF_PATHS {
        let mut seen: Vec<IpAddr> = Vec::new();
        for ip in parse_resolv_conf(path) {
            // Skip loopback: on OpenWrt /etc/resolv.conf points at 127.0.0.1,
            // which is this program — forwarding there would loop.
            if ip.is_loopback() {
                continue;
            }
            if !seen.contains(&ip) {
                seen.push(ip);
            }
        }
        if !seen.is_empty() {
            debug!(path, count = seen.len(), "using WAN DNS servers for fallback");
            return seen;
        }
    }
    Vec::new()
}

/// Sends a DNS query wire message to a nameserver via UDP and returns the parsed
/// response.
///
/// The socket binds to the unspecified address (matching the nameserver's
/// family) and lets the OS route the datagram, so it egresses whichever
/// interface owns the route to `nameserver` — for WAN-assigned servers, the
/// Internet interface.
async fn resolve_via_udp(
    query_wire: &[u8],
    nameserver: IpAddr,
    timeout: Duration,
) -> PluginResult<Message> {
    let bind_addr: SocketAddr = if nameserver.is_ipv4() {
        "0.0.0.0:0"
            .parse()
            .expect("hardcoded IPv4 bind address is valid")
    } else {
        "[::]:0"
            .parse()
            .expect("hardcoded IPv6 bind address is valid")
    };
    let ns_addr = SocketAddr::new(nameserver, 53);

    let sock = UdpSocket::bind(bind_addr).await.map_err(
        |e| -> Box<dyn std::error::Error + Send + Sync> {
            format!("system dns: bind to {}: {e}", bind_addr).into()
        },
    )?;

    sock.connect(ns_addr).await.map_err(
        |e| -> Box<dyn std::error::Error + Send + Sync> {
            format!("system dns: connect to {}: {e}", ns_addr).into()
        },
    )?;

    sock.send(query_wire).await.map_err(
        |e| -> Box<dyn std::error::Error + Send + Sync> {
            format!("system dns: send to {}: {e}", ns_addr).into()
        },
    )?;

    let mut buf = vec![0u8; MAX_UDP_SIZE];
    let n = tokio::time::timeout(timeout, sock.recv(&mut buf))
        .await
        .map_err(|_| -> Box<dyn std::error::Error + Send + Sync> {
            "system dns: query timed out".into()
        })?
        .map_err(
            |e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("system dns: recv from {}: {e}", ns_addr).into()
            },
        )?;

    buf.truncate(n);
    Message::from_vec(&buf).map_err(
        |e| -> Box<dyn std::error::Error + Send + Sync> {
            format!("system dns: parse response: {e}").into()
        },
    )
}

/// Attempts to resolve a DNS query using the WAN/Internet-assigned DNS servers.
///
/// The servers come from the upstream interface's `resolv.conf` (DHCP/PPP),
/// preferring OpenWrt's `resolv.conf.auto` over `/etc/resolv.conf` and skipping
/// loopback entries (see [`system_nameservers`]).
///
/// Returns `Ok(Some(response))` if a nameserver returns a usable answer
/// (`NoError` or `NXDomain`), `Ok(None)` if no WAN nameserver is available, or
/// `Err` only if the query cannot be serialized.
pub async fn system_fallback_resolve(query: &Message) -> PluginResult<Option<Message>> {
    let query_wire = match query.to_vec() {
        Ok(w) => w,
        Err(e) => {
            debug!(error = %e, "failed to serialize query for system fallback");
            return Ok(None);
        }
    };

    let nameservers = system_nameservers();
    if nameservers.is_empty() {
        debug!("no WAN DNS servers available for system fallback");
        return Ok(None);
    }

    for ns in &nameservers {
        debug!(nameserver = %ns, "attempting system DNS fallback");
        match resolve_via_udp(&query_wire, *ns, SYSTEM_DNS_TIMEOUT).await {
            Ok(resp)
                if resp.response_code() == ResponseCode::NoError
                    || resp.response_code() == ResponseCode::NXDomain =>
            {
                debug!(nameserver = %ns, rcode = ?resp.response_code(), "system DNS fallback succeeded");
                return Ok(Some(resp));
            }
            Ok(resp) => {
                debug!(nameserver = %ns, rcode = ?resp.response_code(), "system DNS fallback returned error rcode");
            }
            Err(e) => {
                debug!(nameserver = %ns, error = %e, "system DNS fallback failed");
            }
        }
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Write `contents` to a unique temp file and return its path.
    fn temp_resolv(contents: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        let unique = format!(
            "redns-resolv-test-{}-{}.conf",
            std::process::id(),
            // A cheap per-call discriminator.
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        path.push(unique);
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(contents.as_bytes()).unwrap();
        path
    }

    #[test]
    fn parse_resolv_conf_valid() {
        let path = temp_resolv("# comment\nnameserver 8.8.8.8\nnameserver 8.8.4.4\n");
        let servers = parse_resolv_conf(path.to_str().unwrap());
        std::fs::remove_file(&path).ok();

        assert_eq!(servers.len(), 2);
        assert_eq!(servers[0], "8.8.8.8".parse::<IpAddr>().unwrap());
        assert_eq!(servers[1], "8.8.4.4".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn parse_resolv_conf_skips_comments_and_invalid() {
        let path = temp_resolv(
            "# nameserver 1.2.3.4\nsearch example.com\nnameserver not-an-ip\nnameserver 1.1.1.1\n",
        );
        let servers = parse_resolv_conf(path.to_str().unwrap());
        std::fs::remove_file(&path).ok();

        assert_eq!(servers.len(), 1);
        assert_eq!(servers[0], "1.1.1.1".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn parse_resolv_conf_missing_file_is_empty() {
        let servers = parse_resolv_conf("/nonexistent/redns/resolv.conf");
        assert!(servers.is_empty());
    }

    #[test]
    fn parse_resolv_conf_parses_ipv6() {
        let path = temp_resolv("nameserver 2001:4860:4860::8888\n");
        let servers = parse_resolv_conf(path.to_str().unwrap());
        std::fs::remove_file(&path).ok();

        assert_eq!(servers.len(), 1);
        assert_eq!(
            servers[0],
            "2001:4860:4860::8888".parse::<IpAddr>().unwrap()
        );
    }
}
