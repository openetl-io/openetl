import { JobSizeReason, type JobSize, type SyncAttempt } from "@/gen/openetl/api/v1/sync_pb";

/** 512 MiB, 1.5 GiB */
export function bytes(n: bigint) {
  const v = Number(n);
  if (v >= 2 ** 30) return `${(v / 2 ** 30).toLocaleString("en", { maximumFractionDigits: 1 })} GiB`;
  return `${Math.round(v / 2 ** 20).toLocaleString("en")} MiB`;
}

/** A job's size, by its preset's name if it has one: m, or 6 vCPU, 12 GiB. */
export function sizeLabel(s: JobSize | undefined) {
  if (!s) return "";
  return s.preset || `${Number(s.cpuMillis) / 1000} vCPU, ${bytes(s.memoryBytes)}`;
}

/** What a preset's name stands for: 2 vCPU, 4 GiB. */
export function sizeAmounts(s: JobSize | undefined) {
  return s ? `${Number(s.cpuMillis) / 1000} vCPU, ${bytes(s.memoryBytes)}` : "";
}

export const sizeReasonLabel: Record<JobSizeReason, string> = {
  [JobSizeReason.UNSPECIFIED]: "",
  [JobSizeReason.SPEC]: "set in the spec",
  [JobSizeReason.HINT]: "from its connectors' hints",
  [JobSizeReason.DEFAULT]: "the default",
  [JobSizeReason.HISTORY]: "from its recent peaks",
  [JobSizeReason.OUT_OF_MEMORY]: "after an attempt ran out of memory",
  [JobSizeReason.LARGEST_RUNNER]: "the most a runner can take",
  [JobSizeReason.RESCALE]: "rescaled until its next version",
  [JobSizeReason.AUTOSCALER]: "the lanes its job loop chose",
  [JobSizeReason.FINISH_WITHIN]: "the lanes to finish in time",
};

/** An attempt's lanes, and the share of their time each spent busy, waiting
 * for what's downstream, and waiting for rows; undefined without lanes. */
export function laneShares(a: SyncAttempt) {
  const l = a.lanes;
  const total = (l?.busySeconds ?? 0) + (l?.backpressuredSeconds ?? 0) + (l?.idleSeconds ?? 0);
  if (!l || total <= 0) return undefined;
  const pct = (v: number) => `${Math.round((100 * v) / total)}%`;
  return { lanes: l.lanes, busy: pct(l.busySeconds), backpressured: pct(l.backpressuredSeconds), idle: pct(l.idleSeconds) };
}

/** 45 s, 3.2 min, 1.5 h of CPU. */
export function cpuTime(seconds: number) {
  const fmt = (n: number) => n.toLocaleString("en", { maximumFractionDigits: 1 });
  if (seconds < 60) return `${fmt(seconds)} s`;
  if (seconds < 3600) return `${fmt(seconds / 60)} min`;
  return `${fmt(seconds / 3600)} h`;
}

/** An attempt's CPU seconds and peak memory, the runner's share included. */
export function attemptUse(a: SyncAttempt) {
  const u = a.usage;
  return { cpu: (u?.cpuSeconds ?? 0) + (u?.runnerCpuSeconds ?? 0), memory: (u?.peakMemoryBytes ?? 0n) + (u?.runnerMemoryBytes ?? 0n) };
}

/** A sync's use in all: its attempts' CPU, and the highest of their peaks. */
export function syncUse(attempts: SyncAttempt[]) {
  let cpu = 0;
  let memory = 0n;
  for (const a of attempts) {
    const u = attemptUse(a);
    cpu += u.cpu;
    if (u.memory > memory) memory = u.memory;
  }
  return { cpu, memory };
}
