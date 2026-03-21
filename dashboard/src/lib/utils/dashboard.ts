import type { UpstreamMetrics, UpstreamSortCol } from '../types/dashboard';

export function parseAnswer(row: string) {
  const parts = row.split(' ');
  // New format: "name ttl type data" (4+ parts, second part is numeric TTL)
  if (parts.length >= 4 && /^\d+$/.test(parts[1])) {
    return { type: parts[2], value: parts.slice(3).join(' '), ttl: parseInt(parts[1], 10) };
  }
  // Legacy format: "name type data" (3+ parts)
  if (parts.length >= 3) {
    return { type: parts[1], value: parts.slice(2).join(' '), ttl: undefined };
  }
  return { type: '-', value: row, ttl: undefined };
}

export function formatProtocol(protocol: string) {
  if (!protocol) return '-';
  const p = protocol.toLowerCase();
  if (p === 'udp') return 'UDP';
  if (p === 'tcp') return 'TCP';
  if (p === 'tls' || p === 'dot') return 'DoT';
  if (p === 'https' || p === 'doh') return 'DoH';
  if (p === 'quic' || p === 'doq') return 'DoQ';
  if (p === 'h3' || p === 'doh3') return 'DoH3';
  return p.toUpperCase();
}

export function formatRelativeTime(ts: number) {
  const diff = Date.now() - ts;
  const seconds = Math.floor(diff / 1000);
  if (seconds <= 2) return 'Now';
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  return `${Math.floor(hours / 24)}d ago`;
}

export function sortUpstreams(upstreams: UpstreamMetrics[], sortCol: UpstreamSortCol, sortAsc: boolean) {
  return [...upstreams].sort((a, b) => {
    let cmp = 0;
    if (sortCol === 'name' || sortCol === 'protocol') {
      cmp = String(a[sortCol] || '').localeCompare(String(b[sortCol] || ''));
    } else if (sortCol === 'avg_latency_ms') {
      const aVal = a.completed_total === 0 ? Infinity : Number(a.avg_latency_ms || 0);
      const bVal = b.completed_total === 0 ? Infinity : Number(b.avg_latency_ms || 0);
      cmp = aVal - bVal;
    } else {
      cmp = Number(a[sortCol] || 0) - Number(b[sortCol] || 0);
    }
    return sortAsc ? cmp : -cmp;
  });
}

export function formatUpstream(name: string): string {
  if (name === '__C__') return 'System Cache';
  return name;
}
