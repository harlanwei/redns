/**
 * Hash-based router that keeps dashboard view state (active tab, log filters,
 * upstream sort) in the URL so views are bookmarkable, shareable, and work with
 * browser back/forward.
 *
 * Format: `#/<tab>?q=…&page=…&size=…&range=…&sort=…&dir=…`
 * Only non-default values are written to keep URLs short.
 */
import type { TabId, TimeRange } from '../types/dashboard';

const VALID_TABS: TabId[] = ['logs', 'clients', 'cache', 'upstreams'];
const VALID_RANGES: TimeRange[] = ['1m', '5m', '1h', 'all'];
const VALID_SIZES = [25, 50, 100];

export type RouteState = {
  tab: TabId;
  q: string;
  page: number;
  size: number;
  range: TimeRange;
  sort: string;
  dir: 'asc' | 'desc';
};

export const DEFAULTS: RouteState = {
  tab: 'logs',
  q: '',
  page: 1,
  size: 50,
  range: 'all',
  sort: 'query_total',
  dir: 'desc',
};

function clampInt(raw: string | null, min: number, max: number, fallback: number): number {
  if (raw === null) return fallback;
  const n = parseInt(raw, 10);
  if (Number.isNaN(n)) return fallback;
  return Math.min(max, Math.max(min, n));
}

function parseHash(): RouteState {
  if (typeof window === 'undefined') return { ...DEFAULTS };
  const raw = window.location.hash.replace(/^#\/?/, '');
  const qIndex = raw.indexOf('?');
  const path = qIndex === -1 ? raw : raw.slice(0, qIndex);
  const query = qIndex === -1 ? '' : raw.slice(qIndex + 1);
  const params = new URLSearchParams(query);

  const tab = (VALID_TABS as string[]).includes(path) ? (path as TabId) : DEFAULTS.tab;

  const sizeRaw = clampInt(params.get('size'), 25, 100, DEFAULTS.size);
  const size = VALID_SIZES.includes(sizeRaw) ? sizeRaw : DEFAULTS.size;

  const rangeRaw = params.get('range');
  const range = rangeRaw && (VALID_RANGES as string[]).includes(rangeRaw)
    ? (rangeRaw as TimeRange)
    : DEFAULTS.range;

  return {
    tab,
    q: params.get('q') ?? DEFAULTS.q,
    page: clampInt(params.get('page'), 1, 1_000_000, DEFAULTS.page),
    size,
    range,
    sort: params.get('sort') ?? DEFAULTS.sort,
    dir: params.get('dir') === 'asc' ? 'asc' : 'desc',
  };
}

export const route = $state<RouteState>(parseHash());

function serialize(): string {
  const params = new URLSearchParams();
  if (route.q) params.set('q', route.q);
  if (route.page !== DEFAULTS.page) params.set('page', String(route.page));
  if (route.size !== DEFAULTS.size) params.set('size', String(route.size));
  if (route.range !== DEFAULTS.range) params.set('range', route.range);
  if (route.tab === 'upstreams') {
    if (route.sort !== DEFAULTS.sort) params.set('sort', route.sort);
    if (route.dir !== DEFAULTS.dir) params.set('dir', route.dir);
  }
  const qs = params.toString();
  return `#/${route.tab}${qs ? `?${qs}` : ''}`;
}

function sameState(a: RouteState, b: RouteState): boolean {
  return (
    a.tab === b.tab &&
    a.q === b.q &&
    a.page === b.page &&
    a.size === b.size &&
    a.range === b.range &&
    a.sort === b.sort &&
    a.dir === b.dir
  );
}

/** Merge a partial update into the route and reflect it in the URL hash. */
export function navigate(patch: Partial<RouteState>) {
  Object.assign(route, patch);
  if (typeof window === 'undefined') return;
  const target = serialize();
  if (window.location.hash !== target) {
    window.location.hash = target;
  }
}

let initialized = false;

/** Start listening for back/forward navigation. Safe to call multiple times. */
export function initRouter() {
  if (initialized || typeof window === 'undefined') return;
  initialized = true;
  window.addEventListener('hashchange', () => {
    const raw = window.location.hash;
    // Ignore in-page anchors (e.g. the skip link) — only '#/…' is a route.
    if (raw && !raw.startsWith('#/')) return;
    const next = parseHash();
    if (!sameState(next, route)) Object.assign(route, next);
  });
  // Normalize the hash on first load so the current view is immediately shareable.
  const target = serialize();
  if (window.location.hash !== target) {
    window.history.replaceState(null, '', target);
  }
}
