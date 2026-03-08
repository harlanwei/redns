export type TabId = 'logs' | 'clients' | 'upstreams';

export type DnsLogEntry = {
  id: number;
  ts_unix_ms: number;
  client_ip: string;
  protocol: string;
  qname: string;
  qtype: string;
  rcode: string;
  upstreams: string[];
  result: string;
  result_rows: string[];
  latency_ms: number;
};

export type LogSummary = {
  total_items: number;
  unique_clients: number;
  non_noerror: number;
  avg_latency_ms: number;
};

export type PaginatedLogsResponse = {
  items: DnsLogEntry[];
  page: number;
  page_size: number;
  total_items: number;
  total_pages: number;
  summary: LogSummary;
};

export type ClientStatsEntry = {
  ip: string;
  query_total: number;
};

export type ClientStatsResponse = {
  items: ClientStatsEntry[];
  total_clients: number;
  total_queries: number;
  top_client: string | null;
  top_volume: number;
};

export type UpstreamMetrics = {
  name: string;
  protocol: string;
  query_total: number;
  completed_total: number;
  inflight_total: number;
  canceled_total: number;
  adopted_total: number;
  final_selected_total: number;
  rejected_rcode_total: number;
  error_total: number;
  avg_latency_ms: number;
};

export type UpstreamSortCol =
  | 'name'
  | 'protocol'
  | 'query_total'
  | 'completed_total'
  | 'error_total'
  | 'avg_latency_ms'
  | 'inflight_total'
  | 'canceled_total'
  | 'adopted_total'
  | 'final_selected_total'
  | 'rejected_rcode_total';
