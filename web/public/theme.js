// Applies the viewer's theme and density before the first paint; prefs.ts
// keeps them after.
try {
  const t = localStorage.getItem("openetl.theme");
  const dark = t === "dark" || (t !== "light" && matchMedia("(prefers-color-scheme: dark)").matches);
  document.documentElement.classList.toggle("dark", dark);
  document.documentElement.dataset.density = localStorage.getItem("openetl.density") || "comfortable";
} catch {}
