/**
 * Cross-cutting view flags shared between the log explorer and the command
 * palette / keyboard shortcuts, so "toggle live tail" or "toggle auto-refresh"
 * can be driven from anywhere in the app.
 */
export const viewFlags = $state({
  autoRefresh: false,
  tailMode: false,
});
