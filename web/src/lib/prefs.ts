import { useCallback, useSyncExternalStore } from "react";

// Theme and density are per-viewer conveniences, kept in localStorage.
// index.html applies them before the first paint.

type Prefs = { theme: "light" | "dark" | "system"; density: "comfortable" | "compact" };

function read(): Prefs {
  try {
    return {
      theme: (localStorage.getItem("openetl.theme") as Prefs["theme"]) ?? "system",
      density: (localStorage.getItem("openetl.density") as Prefs["density"]) ?? "comfortable",
    };
  } catch {
    return { theme: "system", density: "comfortable" };
  }
}

let prefs = read();
const listeners = new Set<() => void>();
const media = window.matchMedia("(prefers-color-scheme: dark)");

export const isDark = () => prefs.theme === "dark" || (prefs.theme === "system" && media.matches);

function apply() {
  document.documentElement.classList.toggle("dark", isDark());
  document.documentElement.dataset.density = prefs.density;
  listeners.forEach((l) => l());
}
media.addEventListener("change", apply);

function set(p: Partial<Prefs>) {
  prefs = { ...prefs, ...p };
  try {
    localStorage.setItem("openetl.theme", prefs.theme);
    localStorage.setItem("openetl.density", prefs.density);
  } catch {
    // The choice lasts until the page reloads.
  }
  apply();
}

export function usePrefs() {
  const p = useSyncExternalStore(
    (l) => (listeners.add(l), () => listeners.delete(l)),
    () => prefs,
  );
  const toggleTheme = useCallback(() => set({ theme: isDark() ? "light" : "dark" }), []);
  const toggleDensity = useCallback(
    () => set({ density: prefs.density === "compact" ? "comfortable" : "compact" }),
    [],
  );
  return { ...p, dark: isDark(), toggleTheme, toggleDensity };
}
