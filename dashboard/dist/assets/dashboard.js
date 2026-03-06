const PAGE_BY_PATH = {
  "/": "upstreams",
  "/upstreams": "upstreams",
  "/logs": "logs",
  "/clients": "clients",
};

const POLL_MS = 4000;

function esc(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function fmtNum(value) {
  return Number(value || 0).toLocaleString();
}

function fmtTime(value) {
  const n = Number(value || 0);
  return n > 0 ? new Date(n).toLocaleString() : "-";
}

function fmtPercent(value) {
  return Number(value || 0).toFixed(2);
}

function upstreamDisplay(upstreams, rcode) {
  if (Array.isArray(upstreams) && upstreams.length) {
    return upstreams.join(", ");
  }
  return "-";
}

function isRefusedRcode(rcode) {
  return String(rcode || "").toLowerCase() === "refused";
}

function parseLogAnswerRow(rowText) {
  const text = String(rowText || "").trim();
  if (!text) {
    return { type: "-", content: "-" };
  }

  const match = text.match(/^\S+\s+([A-Z][A-Za-z0-9_]*(?:\([0-9]+\))?)\s+(.+)$/);
  if (match) {
    return {
      type: match[1],
      content: match[2],
    };
  }

  return {
    type: "-",
    content: text,
  };
}

function badgeForRcode(rcode) {
  const value = String(rcode || "").toLowerCase();
  if (value === "noerror") {
    return "badge-success";
  }
  if (value === "servfail" || value === "refused") {
    return "badge-warn";
  }
  return "badge-danger";
}

function queryString(params) {
  const search = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") {
      search.set(key, String(value));
    }
  });
  const text = search.toString();
  return text ? `?${text}` : "";
}

function debounce(fn, wait) {
  let timer = 0;
  return (...args) => {
    window.clearTimeout(timer);
    timer = window.setTimeout(() => fn(...args), wait);
  };
}

class RednsDashboard extends HTMLElement {
  constructor() {
    super();
    this.page = PAGE_BY_PATH[window.location.pathname] || "upstreams";
    this.state = {
      loading: true,
      statusText: "Connecting...",
      upstreamSortKey: "query_total",
      upstreamSortDir: "desc",
      clientSortKey: "query_total",
      modalLogId: null,
      logs: {
        items: [],
        page: 1,
        pageSize: 25,
        totalItems: 0,
        totalPages: 1,
        summary: {
          total_items: 0,
          unique_clients: 0,
          non_noerror: 0,
          avg_latency_ms: 0,
        },
        filter: "",
      },
      upstreams: [],
      clients: {
        items: [],
        total_clients: 0,
        total_queries: 0,
        top_client: null,
        top_volume: 0,
      },
    };
    this.refreshTimer = null;
    this.handleInputFilter = debounce((value) => {
      this.state.logs.page = 1;
      this.state.logs.filter = value;
      this.refresh();
    }, 250);
  }

  connectedCallback() {
    this.render();
    this.bindEvents();
    this.refresh();
    this.refreshTimer = window.setInterval(() => this.refresh(false), POLL_MS);
  }

  disconnectedCallback() {
    if (this.refreshTimer) {
      window.clearInterval(this.refreshTimer);
      this.refreshTimer = null;
    }
  }

  bindEvents() {
    this.addEventListener("change", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }
      if (target.matches("[data-upstream-sort-key]")) {
        this.state.upstreamSortKey = target.value;
        this.refresh(false);
      } else if (target.matches("[data-upstream-sort-dir]")) {
        this.state.upstreamSortDir = target.value;
        this.refresh(false);
      } else if (target.matches("[data-client-sort-key]")) {
        this.state.clientSortKey = target.value;
        this.refresh(false);
      } else if (target.matches("[data-log-page-size]")) {
        this.state.logs.pageSize = Number(target.value || 25);
        this.state.logs.page = 1;
        this.refresh();
      }
    });

    this.addEventListener("input", (event) => {
      const target = event.target;
      if (target instanceof HTMLInputElement && target.matches("[data-log-filter]")) {
        this.handleInputFilter(target.value.trim());
      }
    });

    this.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }

      if (target.matches("[data-close-modal]") || target.closest("[data-close-modal]")) {
        this.state.modalLogId = null;
        this.render();
        return;
      }

      if (target.closest("[data-modal-card]")) {
        return;
      }

      const pageButton = target.closest("[data-log-page]");
      if (pageButton) {
        const nextPage = Number(pageButton.getAttribute("data-log-page") || "1");
        if (!Number.isNaN(nextPage) && nextPage !== this.state.logs.page) {
          this.state.logs.page = nextPage;
          this.refresh();
        }
        return;
      }

      if (target.matches("[data-clear-logs]") || target.closest("[data-clear-logs]")) {
        if (window.confirm("Clear all DNS logs and statistics from the database?")) {
          this.clearLogs();
        }
        return;
      }

      const row = target.closest("[data-log-id]");
      if (row) {
        const id = Number(row.getAttribute("data-log-id"));
        if (!Number.isNaN(id)) {
          this.state.modalLogId = id;
          this.render();
        }
      }
    });

    this.addEventListener("keydown", (event) => {
      if (event.key === "Escape" && this.state.modalLogId !== null) {
        this.state.modalLogId = null;
        this.render();
      }
    });
  }

  async refresh(updateStatus = true) {
    this.state.loading = true;
    this.render();

    try {
      if (this.page === "upstreams") {
        this.state.upstreams = await this.fetchJson("/api/upstreams");
      } else if (this.page === "logs") {
        const params = queryString({
          page: this.state.logs.page,
          page_size: this.state.logs.pageSize,
          q: this.state.logs.filter,
        });
        const data = await this.fetchJson(`/api/logs${params}`);
        this.state.logs = {
          ...this.state.logs,
          items: Array.isArray(data.items) ? data.items : [],
          page: data.page || this.state.logs.page,
          pageSize: data.page_size || this.state.logs.pageSize,
          totalItems: data.total_items || 0,
          totalPages: Math.max(1, data.total_pages || 1),
          summary: data.summary || this.state.logs.summary,
        };
      } else {
        this.state.clients = await this.fetchJson("/api/clients");
      }

      if (updateStatus) {
        this.state.statusText = `Updated ${new Date().toLocaleTimeString()}`;
      }
    } catch (error) {
      this.state.statusText = `Data unavailable: ${error}`;
    } finally {
      this.state.loading = false;
      this.render();
    }
  }

  async fetchJson(url) {
    const response = await fetch(url, { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    return response.json();
  }

  async clearLogs() {
    try {
      const response = await fetch("/api/logs/clear", {
        method: "POST",
        cache: "no-store",
      });
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      this.state.modalLogId = null;
      this.state.logs.page = 1;
      this.state.statusText = `Cleared ${new Date().toLocaleTimeString()}`;
      await this.refresh(false);
    } catch (error) {
      this.state.statusText = `Clear failed: ${error}`;
      this.render();
    }
  }

  render() {
    const page = this.page;
    const header = this.renderHeader();
    const summary = this.renderSummary();
    const body =
      page === "upstreams"
        ? this.renderUpstreams()
        : page === "logs"
          ? this.renderLogs()
          : this.renderClients();
    const modal = this.renderLogModal();

    this.innerHTML = `
      <main class="mx-auto w-full max-w-[1600px] px-6 pb-10 pt-8 sm:px-8 lg:px-12 lg:pt-10">
        ${header}
        ${summary}
        ${body}
      </main>
      ${modal}
    `;
  }

  renderHeader() {
    const nav = [
      ["upstreams", "/upstreams", "Upstream Metrics"],
      ["logs", "/logs", "DNS Logs"],
      ["clients", "/clients", "Client Statistics"],
    ]
      .map(([key, href, label]) => {
        const active = this.page === key ? "nav-pill nav-pill-active" : "nav-pill";
        return `<a class="${active}" href="${href}">${label}</a>`;
      })
      .join("");

    return `
      <section class="hero-panel">
        <div class="space-y-3">
          <p class="inline-flex w-fit items-center rounded-full border border-tide-100 bg-tide-50 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.24em] text-tide-700">DNS operations console</p>
          <h1 class="text-3xl font-semibold tracking-tight text-ink-900 sm:text-4xl">redns Dashboard</h1>
          <p class="max-w-4xl text-sm leading-6 text-ink-500 sm:text-base">Lightweight monitoring for upstream activity, paginated DNS logs, and SQLite-backed traffic statistics.</p>
          <nav class="flex flex-wrap gap-2">${nav}</nav>
        </div>
        <div class="status-panel">
          <div class="flex items-start gap-3">
            <span class="status-dot"></span>
            <div>
              <p class="text-[11px] font-semibold uppercase tracking-[0.18em] text-tide-700">Status</p>
              <p class="mt-1 text-sm text-ink-500">${esc(this.state.statusText)}</p>
            </div>
          </div>
        </div>
      </section>
    `;
  }

  renderSummary() {
    const cards = this.summaryCardsForPage();
    const html = cards
      .map(
        (card) => `
          <article class="stat-card">
            <p class="text-xs font-semibold uppercase tracking-[0.14em] text-ink-400">${esc(card.label)}</p>
            <div class="mt-2 flex items-end justify-between gap-4">
              <p class="text-2xl font-semibold text-ink-900 sm:text-3xl">${esc(card.value)}</p>
              <p class="text-right text-xs leading-5 text-ink-400">${esc(card.caption || "")}</p>
            </div>
          </article>
        `,
      )
      .join("");

    return `<section class="mb-4 grid w-full gap-3 md:grid-cols-2 xl:grid-cols-4">${html}</section>`;
  }

  summaryCardsForPage() {
    if (this.page === "upstreams") {
      const rows = this.sortedUpstreams();
      const totalQueries = rows.reduce((sum, row) => sum + Number(row.query_total || 0), 0);
      const errorTotal = rows.reduce((sum, row) => sum + Number(row.error_total || 0), 0);
      const inflightTotal = rows.reduce((sum, row) => sum + Number(row.inflight_total || 0), 0);
      return [
        { label: "Upstreams", value: fmtNum(rows.length) },
        {
          label: "Total Queries",
          value: fmtNum(totalQueries),
        },
        {
          label: "Inflight",
          value: fmtNum(inflightTotal),
        },
        {
          label: "Errors",
          value: fmtNum(errorTotal),
        },
      ];
    }

    if (this.page === "logs") {
      const summary = this.state.logs.summary;
      return [
        {
          label: "Matching Queries",
          value: fmtNum(summary.total_items),
        },
        {
          label: "Unique Clients",
          value: fmtNum(summary.unique_clients),
        },
        {
          label: "Non-NOERROR",
          value: fmtNum(summary.non_noerror),
        },
        {
          label: "Avg Latency",
          value: `${fmtNum(summary.avg_latency_ms)} ms`,
        },
      ];
    }

    const clients = Array.isArray(this.state.clients.items)
      ? this.state.clients.items
      : [];
    return [
      {
        label: "Clients",
        value: fmtNum(this.state.clients.total_clients),
      },
      {
        label: "Total Queries",
        value: fmtNum(this.state.clients.total_queries),
      },
      {
        label: "Top Client",
        value: this.state.clients.top_client || "-",
      },
      {
        label: "Top Volume",
        value: fmtNum(this.state.clients.top_volume),
        caption: clients.length ? "largest bucket" : "no data yet",
      },
    ];
  }

  sortedUpstreams() {
    const rows = Array.isArray(this.state.upstreams)
      ? this.state.upstreams.slice()
      : [];
    const key = this.state.upstreamSortKey;
    const desc = this.state.upstreamSortDir === "desc";
    rows.sort((a, b) => {
      const av = a[key];
      const bv = b[key];
      let diff;
      if (typeof av === "number" && typeof bv === "number") {
        diff = av - bv;
      } else {
        diff = String(av || "").localeCompare(String(bv || ""));
      }
      return desc ? -diff : diff;
    });
    return rows;
  }

  renderUpstreams() {
    const rows = this.sortedUpstreams();
      const controlBar = `
      <div class="mb-4 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h2 class="text-xl font-semibold text-ink-900">Upstream Metrics</h2>
          <p class="mt-1 text-sm text-ink-500">Live metrics mirrored from the API endpoint, with local sorting in the static client.</p>
        </div>
        <div class="flex flex-col gap-2 sm:flex-row">
          <select class="control" data-upstream-sort-key>
            ${this.selectOptions(this.state.upstreamSortKey, [
              ["query_total", "Sort by queries"],
              ["avg_latency_ms", "Sort by avg latency"],
              ["error_total", "Sort by errors"],
              ["final_selected_total", "Sort by selected"],
              ["name", "Sort by upstream name"],
            ])}
          </select>
          <select class="control" data-upstream-sort-dir>
            ${this.selectOptions(this.state.upstreamSortDir, [["desc", "Descending"], ["asc", "Ascending"]])}
          </select>
        </div>
      </div>
    `;

    if (!rows.length && !this.state.loading) {
      return `${controlBar}${this.emptyState("No upstream metrics yet.")}`;
    }

    const tableRows = rows
      .map(
        (row) => `
          <tr>
            <td class="truncate-cell font-mono text-xs sm:text-sm">${esc(row.name || "-")}</td>
            <td>${fmtNum(row.query_total)}</td>
            <td>${fmtNum(row.completed_total)}</td>
            <td>${fmtNum(row.inflight_total)}</td>
            <td>${fmtNum(row.error_total)}</td>
            <td>${fmtNum(row.final_selected_total)}</td>
            <td>${Number(row.avg_latency_ms || 0).toFixed(2)}</td>
          </tr>
        `,
      )
      .join("");

    return `
      <section class="glass-panel w-full p-4">
        ${controlBar}
        <div class="table-shell">
          <table class="table-base">
            <thead>
              <tr>
                <th class="w-[32%]">Upstream</th>
                <th class="w-[13%]">Queries</th>
                <th class="w-[13%]">Completed</th>
                <th class="w-[13%]">Inflight</th>
                <th class="w-[13%]">Errors</th>
                <th class="w-[16%]">Selected</th>
                <th class="w-[13%]">Avg Latency (ms)</th>
              </tr>
            </thead>
            <tbody>${tableRows}</tbody>
          </table>
        </div>
      </section>
    `;
  }

  renderLogs() {
    const logs = this.state.logs;
    const header = `
      <div class="mb-4 flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
        <div>
          <h2 class="text-xl font-semibold text-ink-900">DNS Logs</h2>
          <p class="mt-1 text-sm text-ink-500">Rows are fully fitted inside the table. Click non-REFUSED rows to view formatted answer records in a modal.</p>
        </div>
        <div class="flex flex-col gap-2 sm:flex-row sm:items-center">
          <input class="control min-w-[18rem]" data-log-filter type="search" value="${esc(logs.filter)}" placeholder="Search qname, client, rcode, upstream" />
          <select class="control" data-log-page-size>
            ${this.selectOptions(String(logs.pageSize), [["10", "10 rows"], ["25", "25 rows"], ["50", "50 rows"], ["100", "100 rows"]])}
          </select>
          <button class="pagination-button" data-clear-logs>Clear Logs</button>
        </div>
      </div>
    `;

    if (!logs.items.length && !this.state.loading) {
      return `
        <section class="glass-panel w-full p-4">
          ${header}
          ${this.emptyState(
            logs.filter
              ? "No DNS logs match the current search."
              : "No DNS logs yet.",
          )}
        </section>
      `;
    }

    const rows = logs.items
      .map((row) => {
        const upstreams = upstreamDisplay(row.upstreams, row.rcode);
        const refused = isRefusedRcode(row.rcode);
        const rowClass = refused ? "log-row-disabled" : "log-row";
        const rowAttr = refused ? "" : ` data-log-id="${row.id}"`;
        return `
          <tr class="${rowClass}"${rowAttr}>
            <td class="log-col-time truncate-cell">${esc(fmtTime(row.ts_unix_ms))}</td>
            <td class="log-col-client truncate-cell font-mono text-xs sm:text-sm">${esc(row.client_ip || "-")}</td>
            <td class="log-col-protocol truncate-cell">${esc(row.protocol || "-")}</td>
            <td class="log-col-qname truncate-cell font-mono text-xs sm:text-sm">${esc(row.qname || "-")}</td>
            <td class="log-col-qtype truncate-cell">${esc(row.qtype || "-")}</td>
            <td class="log-col-rcode"><span class="${badgeForRcode(row.rcode)}">${esc(row.rcode || "-")}</span></td>
            <td class="log-col-upstreams truncate-cell font-mono text-xs sm:text-sm">${esc(upstreams)}</td>
            <td class="log-col-latency">${fmtNum(row.latency_ms)}</td>
          </tr>
        `;
      })
      .join("");

    const startIndex = logs.totalItems === 0 ? 0 : (logs.page - 1) * logs.pageSize + 1;
    const endIndex = Math.min(logs.page * logs.pageSize, logs.totalItems);

    return `
      <section class="glass-panel w-full p-4">
        ${header}
        <div class="mb-3 flex flex-col gap-2 text-sm text-ink-500 sm:flex-row sm:items-center sm:justify-between">
          <p>Showing <span class="font-semibold text-ink-900">${fmtNum(startIndex)}</span> to <span class="font-semibold text-ink-900">${fmtNum(endIndex)}</span> of <span class="font-semibold text-ink-900">${fmtNum(logs.totalItems)}</span> matching queries.</p>
          <p>Page <span class="font-semibold text-ink-900">${fmtNum(logs.page)}</span> of <span class="font-semibold text-ink-900">${fmtNum(logs.totalPages)}</span></p>
        </div>
        <div class="table-shell">
          <table class="table-base logs-table">
            <thead>
              <tr>
                <th class="log-col-time">Time</th>
                <th class="log-col-client">Client IP</th>
                <th class="log-col-protocol">Protocol</th>
                <th class="log-col-qname">QName</th>
                <th class="log-col-qtype">QType</th>
                <th class="log-col-rcode">RCode</th>
                <th class="log-col-upstreams">Upstreams</th>
                <th class="log-col-latency">Latency (ms)</th>
              </tr>
            </thead>
            <tbody>${rows}</tbody>
          </table>
        </div>
        ${this.renderPagination(logs.page, logs.totalPages)}
      </section>
    `;
  }

  renderLogModal() {
    if (this.page !== "logs" || this.state.modalLogId === null) {
      return "";
    }

    const log = this.state.logs.items.find((item) => item.id === this.state.modalLogId);
    if (!log) {
      return "";
    }

    const rows = Array.isArray(log.result_rows) ? log.result_rows : [];
    const rendered = rows.length
      ? rows
          .map(
            (row) => {
              const parsed = parseLogAnswerRow(row);
              return `
            <tr>
              <td class="w-[18%] text-ink-400">${esc(parsed.type)}</td>
              <td class="truncate-cell font-mono text-xs sm:text-sm">${esc(parsed.content)}</td>
            </tr>
          `;
            },
          )
          .join("")
      : `
          <tr>
            <td colspan="2" class="text-ink-400">No answer rows captured for this query.</td>
          </tr>
        `;

    return `
      <div class="modal-overlay" data-close-modal>
        <div class="modal-card" data-modal-card>
          <div class="flex items-center justify-between border-b border-slate-200 px-5 py-4">
            <div>
              <h3 class="text-lg font-semibold text-ink-900">Query Result Details</h3>
              <p class="mt-1 text-sm text-ink-500">${esc(log.qname || "-")} (${esc(log.qtype || "-")})</p>
            </div>
            <button class="pagination-button" data-close-modal>Close</button>
          </div>
          <div class="space-y-3 px-5 py-4 text-sm text-ink-500">
            <div class="grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
              <div><span class="font-semibold text-ink-700">Client:</span> ${esc(log.client_ip || "-")}</div>
              <div><span class="font-semibold text-ink-700">Protocol:</span> ${esc(log.protocol || "-")}</div>
              <div><span class="font-semibold text-ink-700">RCode:</span> ${esc(log.rcode || "-")}</div>
              <div><span class="font-semibold text-ink-700">Latency:</span> ${fmtNum(log.latency_ms)} ms</div>
            </div>
            <div><span class="font-semibold text-ink-700">Upstreams:</span> ${esc(upstreamDisplay(log.upstreams, log.rcode))}</div>
          </div>
          <div class="px-5 pb-5">
            <div class="table-shell max-h-[22rem] overflow-y-auto">
              <table class="table-base">
                <thead>
                  <tr>
                    <th>Type</th>
                    <th>Content</th>
                  </tr>
                </thead>
                <tbody>${rendered}</tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    `;
  }

  renderClients() {
    const items = Array.isArray(this.state.clients.items)
      ? this.state.clients.items.slice()
      : [];
    const sortKey = this.state.clientSortKey;
    items.sort((a, b) => {
      if (sortKey === "ip") {
        return String(a.ip || "").localeCompare(String(b.ip || ""));
      }
      return Number(b.query_total || 0) - Number(a.query_total || 0);
    });

    const header = `
      <div class="mb-4 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h2 class="text-xl font-semibold text-ink-900">Client Statistics</h2>
          <p class="mt-1 text-sm text-ink-500">Source-IP aggregates computed directly from the SQLite log store.</p>
        </div>
        <select class="control" data-client-sort-key>
          ${this.selectOptions(this.state.clientSortKey, [["query_total", "Sort by query total"], ["ip", "Sort by IP address"]])}
        </select>
      </div>
    `;

    if (!items.length && !this.state.loading) {
      return `
        <section class="glass-panel w-full p-4">
          ${header}
          ${this.emptyState("No client statistics yet.")}
        </section>
      `;
    }

    const max = Math.max(...items.map((item) => Number(item.query_total || 0)), 1);
    const rows = items
      .map((item) => {
        const width = Math.max(3, Math.round((Number(item.query_total || 0) / max) * 100));
        return `
          <tr>
            <td class="truncate-cell font-mono text-xs sm:text-sm">${esc(item.ip || "-")}</td>
            <td>
              <div class="flex items-center gap-3">
                <div class="h-2.5 w-36 overflow-hidden rounded-full bg-ink-100">
                  <div class="h-full rounded-full bg-gradient-to-r from-tide-700 to-tide-500" style="width:${width}%"></div>
                </div>
                <span class="text-ink-700">${fmtNum(item.query_total)}</span>
              </div>
            </td>
            <td class="text-ink-400">${fmtPercent((Number(item.query_total || 0) / Math.max(this.state.clients.total_queries || 1, 1)) * 100)}%</td>
          </tr>
        `;
      })
      .join("");

    return `
      <section class="glass-panel w-full p-4">
        ${header}
        <div class="table-shell">
          <table class="table-base">
            <thead>
              <tr>
                <th>Client IP</th>
                <th>Query Volume</th>
                <th>Traffic Share</th>
              </tr>
            </thead>
            <tbody>${rows}</tbody>
          </table>
        </div>
      </section>
    `;
  }

  renderPagination(page, totalPages) {
    const prev = Math.max(1, page - 1);
    const next = Math.min(totalPages, page + 1);
    const buttons = [];
    const start = Math.max(1, page - 2);
    const end = Math.min(totalPages, page + 2);

    for (let i = start; i <= end; i += 1) {
      buttons.push(`
        <button class="pagination-button ${i === page ? "border-tide-700 bg-tide-50 text-tide-700 shadow-sm" : ""}" data-log-page="${i}">${fmtNum(i)}</button>
      `);
    }

    return `
      <div class="mt-4 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div class="flex flex-wrap items-center gap-2">
          <button class="pagination-button" data-log-page="1" ${page <= 1 ? "disabled" : ""}>First</button>
          <button class="pagination-button" data-log-page="${prev}" ${page <= 1 ? "disabled" : ""}>Previous</button>
          ${buttons.join("")}
          <button class="pagination-button" data-log-page="${next}" ${page >= totalPages ? "disabled" : ""}>Next</button>
          <button class="pagination-button" data-log-page="${totalPages}" ${page >= totalPages ? "disabled" : ""}>Last</button>
        </div>
      </div>
    `;
  }

  selectOptions(selectedValue, options) {
    return options
      .map(
        ([value, label]) =>
          `<option value="${esc(value)}" ${String(selectedValue) === String(value) ? "selected" : ""}>${esc(label)}</option>`,
      )
      .join("");
  }

  emptyState(message) {
    return `
      <div class="table-shell px-6 py-16 text-center text-sm text-ink-500">
        <p class="text-base font-medium text-ink-900">${esc(message)}</p>
      </div>
    `;
  }
}

customElements.define("redns-dashboard", RednsDashboard);
