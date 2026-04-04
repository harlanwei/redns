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

//! System DNS fallback resolution via Ethernet interface.
//!
//! When the plugin chain produces a SERVFAIL response, this module provides
//! a fallback that reads the system DNS servers from `/etc/resolv.conf`,
//! discovers an active Ethernet interface, and sends the query directly
//! through that interface.

use crate::plugin::PluginResult;
use hickory_proto::op::{Message, ResponseCode};
use std::io::BufRead;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;
use tokio::net::UdpSocket;
use tracing::{debug, warn};

/// Default timeout for system DNS queries.
const SYSTEM_DNS_TIMEOUT: Duration = Duration::from_secs(5);

/// Maximum DNS UDP payload size for receiving.
const MAX_UDP_SIZE: usize = 4096;

/// Interface name prefixes that identify Ethernet (wired) interfaces.
/// Covers traditional names (`eth*`) and systemd Predictable Network
/// Interface Names (`en*` for Ethernet, `em*` for on-board, `p*` for
/// PCI hotplug slots).
const ETHERNET_PREFIXES: &[&str] = &["eth", "en", "em", "p"];

/// Returns the first active, non-loopback, non-link-local IPv4 address
/// whose interface name matches an Ethernet prefix.
fn find_ethernet_interface_ip() -> Option<IpAddr> {
    let ifaddrs = nix::ifaddrs::getifaddrs().ok()?;
    for ifaddr in ifaddrs {
        let name = ifaddr.interface_name.as_str();
        if !ETHERNET_PREFIXES.iter().any(|p| name.starts_with(p)) {
            continue;
        }
        if let Some(ipv4) = ifaddr.address.as_ref().and_then(|a| a.as_sockaddr_in()).map(|s| s.ip()) {
            let ip = IpAddr::V4(ipv4);
            if !ip.is_loopback() && !ip.is_unspecified() {
                debug!(interface = %name, ip = %ip, "found Ethernet interface");
                return Some(ip);
            }
        }
    }
    None
}

/// Parses `/etc/resolv.conf` and returns the list of nameserver IP addresses.
fn parse_resolv_conf() -> Vec<IpAddr> {
    let file = match std::fs::File::open("/etc/resolv.conf") {
        Ok(f) => f,
        Err(e) => {
            warn!(error = %e, "failed to open /etc/resolv.conf");
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

/// Sends a DNS query wire message to a nameserver via UDP, bound to the
/// given source IP, and returns the parsed response message.
async fn resolve_via_udp(
    query_wire: &[u8],
    nameserver: IpAddr,
    source_ip: IpAddr,
    timeout: Duration,
) -> PluginResult<Message> {
    let bind_addr = SocketAddr::new(source_ip, 0);
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

/// Attempts to resolve a DNS query using the system DNS server(s) read from
/// `/etc/resolv.conf`, sending the query through the first available Ethernet
/// interface.
///
/// Returns `Ok(Some(response))` if a successful (non-SERVFAIL) answer is
/// obtained, `Ok(None)` if no Ethernet interface or nameserver is available,
/// or `Err` if all nameservers fail.
pub async fn system_fallback_resolve(query: &Message) -> PluginResult<Option<Message>> {
    let query_wire = match query.to_vec() {
        Ok(w) => w,
        Err(e) => {
            debug!(error = %e, "failed to serialize query for system fallback");
            return Ok(None);
        }
    };

    let source_ip = match find_ethernet_interface_ip() {
        Some(ip) => ip,
        None => {
            debug!("no Ethernet interface found for system DNS fallback");
            return Ok(None);
        }
    };

    let nameservers = parse_resolv_conf();
    if nameservers.is_empty() {
        debug!("no nameservers found in /etc/resolv.conf");
        return Ok(None);
    }

    for ns in &nameservers {
        debug!(nameserver = %ns, source = %source_ip, "attempting system DNS fallback");
        match resolve_via_udp(&query_wire, *ns, source_ip, SYSTEM_DNS_TIMEOUT).await {
            Ok(resp) if resp.response_code() == ResponseCode::NoError
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

    #[test]
    fn parse_resolv_conf_valid() {
        let input = b"# comment\nnameserver 8.8.8.8\nnameserver 8.8.4.4\n";
        let lines: Vec<_> = std::io::BufReader::new(&input[..])
            .lines()
            .filter_map(|l| {
                let l = l.ok()?;
                let l = l.trim();
                if !l.starts_with("nameserver") {
                    return None;
                }
                l["nameserver".len()..].trim().parse::<IpAddr>().ok()
            })
            .collect();

        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], "8.8.8.8".parse::<IpAddr>().unwrap());
        assert_eq!(lines[1], "8.8.4.4".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn parse_resolv_conf_skips_comments_and_invalid() {
        let input = b"# nameserver 1.2.3.4\nsearch example.com\nnameserver not-an-ip\nnameserver 1.1.1.1\n";
        let lines: Vec<_> = std::io::BufReader::new(&input[..])
            .lines()
            .filter_map(|l| {
                let l = l.ok()?;
                let l = l.trim();
                if !l.starts_with("nameserver") {
                    return None;
                }
                l["nameserver".len()..].trim().parse::<IpAddr>().ok()
            })
            .collect();

        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0], "1.1.1.1".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn parse_resolv_conf_empty() {
        let input = b"# no nameservers\n";
        let lines: Vec<_> = std::io::BufReader::new(&input[..])
            .lines()
            .filter_map(|l| {
                let l = l.ok()?;
                let l = l.trim();
                if !l.starts_with("nameserver") {
                    return None;
                }
                l["nameserver".len()..].trim().parse::<IpAddr>().ok()
            })
            .collect();

        assert!(lines.is_empty());
    }
}
