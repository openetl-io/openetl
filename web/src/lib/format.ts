import { timestampDate, type Timestamp, type Duration } from "@bufbuild/protobuf/wkt";

export const dateOf = (t?: Timestamp) => (t ? timestampDate(t) : undefined);

const units: [Intl.RelativeTimeFormatUnit, number][] = [
  ["year", 365 * 86400],
  ["month", 30 * 86400],
  ["day", 86400],
  ["hour", 3600],
  ["minute", 60],
  ["second", 1],
];
const rtf = new Intl.RelativeTimeFormat("en", { numeric: "auto", style: "narrow" });

/** "4 min. ago", "in 2 hr." */
export function relative(d: Date, now = Date.now()) {
  const s = Math.round((d.getTime() - now) / 1000);
  if (Math.abs(s) < 10) return "just now";
  for (const [unit, size] of units) {
    if (Math.abs(s) >= size || unit === "second") return rtf.format(Math.round(s / size), unit);
  }
  return "";
}

const absoluteFmt = new Intl.DateTimeFormat("en", { dateStyle: "medium", timeStyle: "medium" });
export const absolute = (d: Date) => absoluteFmt.format(d);

const clockFmt = new Intl.DateTimeFormat("en", {
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  fractionalSecondDigits: 3,
  hour12: false,
});
export const clock = (d: Date) => clockFmt.format(d);

/** 950ms, 42s, 3m 05s, 2h 10m */
export function duration(ms: number) {
  if (ms < 1000) return `${Math.max(0, Math.round(ms))}ms`;
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${String(s % 60).padStart(2, "0")}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${String(m % 60).padStart(2, "0")}m`;
}

export const durationMs = (d?: Duration) => (d ? Number(d.seconds) * 1000 + d.nanos / 1e6 : 0);

const compactFmt = new Intl.NumberFormat("en", { notation: "compact", maximumFractionDigits: 1 });
const fullFmt = new Intl.NumberFormat("en");

/** 1.2M */
export const compact = (n: bigint | number) => compactFmt.format(typeof n === "bigint" ? Number(n) : n);
/** 1,234,567 */
export const full = (n: bigint | number) => fullFmt.format(n);

/** An enum value's name in sentence case: FULL_REFRESH → Full refresh; "" when unspecified. */
export function enumLabel(name: string | undefined) {
  if (!name || name === "UNSPECIFIED") return "";
  const words = name.toLowerCase().replaceAll("_", " ");
  return words.charAt(0).toUpperCase() + words.slice(1);
}

/** 1h, 1h 30m, 15m, 45s */
export function interval(ms: number) {
  const parts: string[] = [];
  let s = Math.round(ms / 1000);
  for (const [unit, size] of [
    ["d", 86400],
    ["h", 3600],
    ["m", 60],
    ["s", 1],
  ] as const) {
    if (s >= size) {
      parts.push(`${Math.floor(s / size)}${unit}`);
      s %= size;
    }
  }
  return parts.slice(0, 2).join(" ") || "0s";
}

/** The API's messages start lowercase, as Go's errors do. */
export const sentence = (s: string) => s.charAt(0).toUpperCase() + s.slice(1);

/** The cause at the end of a chain of Temporal activity errors, from its first line. */
export function errorSummary(e: string) {
  const first = e.split("\n")[0] ?? "";
  const i = first.lastIndexOf("): ");
  return first.startsWith("activity error") && i >= 0 ? first.slice(i + 3) : first;
}
