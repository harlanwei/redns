/**
 * Global theme state for the dashboard.
 *
 * Uses a Svelte 5 module-level `$state` rune so any component that imports
 * `theme` reacts to changes. The `.svelte.ts` extension is required for runes
 * in a non-component module.
 */

export type Theme = "light" | "dark";

const STORAGE_KEY = "redns-theme";

function readStoredTheme(): Theme {
  if (typeof localStorage !== "undefined") {
    const saved = localStorage.getItem(STORAGE_KEY);
    if (saved === "light" || saved === "dark") return saved;
  }
  if (typeof window !== "undefined" && window.matchMedia) {
    return window.matchMedia("(prefers-color-scheme: dark)").matches
      ? "dark"
      : "light";
  }
  return "light";
}

function applyTheme(theme: Theme) {
  if (typeof document === "undefined") return;
  const root = document.documentElement;
  root.classList.toggle("dark", theme === "dark");
  root.style.colorScheme = theme;
}

export const theme = $state<{ value: Theme }>({ value: "light" });

let initialized = false;

/** Applies the persisted/preferred theme. Safe to call multiple times. */
export function initTheme() {
  if (initialized) return;
  initialized = true;
  theme.value = readStoredTheme();
  applyTheme(theme.value);
  if (typeof document !== "undefined") {
    document.documentElement.classList.add("ready");
  }
}

/** Toggles between light and dark, persisting the choice. */
export function toggleTheme() {
  theme.value = theme.value === "dark" ? "light" : "dark";
  applyTheme(theme.value);
  if (typeof localStorage !== "undefined") {
    try {
      localStorage.setItem(STORAGE_KEY, theme.value);
    } catch {
      /* ignore storage failures (private mode, etc.) */
    }
  }
}
