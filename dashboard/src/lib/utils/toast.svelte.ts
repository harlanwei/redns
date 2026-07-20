/**
 * Lightweight global toast queue with optional action buttons (e.g. Undo).
 * Components call `pushToast`; a single `ToastHost` near the app root renders
 * the stack.
 */
import type { ToastKind } from '../types/dashboard';

export type Toast = {
  id: number;
  kind: ToastKind;
  message: string;
  actionLabel?: string;
  onAction?: () => void;
};

export const toasts = $state<Toast[]>([]);

let nextId = 1;

export function pushToast(opts: Omit<Toast, 'id'>): number {
  const id = nextId++;
  toasts.push({ ...opts, id });
  // Give action-able toasts longer to live so the user can act on them.
  const ttl = opts.onAction ? 8000 : 4500;
  setTimeout(() => dismissToast(id), ttl);
  return id;
}

export function dismissToast(id: number) {
  const i = toasts.findIndex((t) => t.id === id);
  if (i !== -1) toasts.splice(i, 1);
}
