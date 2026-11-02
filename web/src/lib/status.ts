import type { Pipeline } from "@/gen/openetl/api/v1/pipeline_pb";
import { SyncState, type Sync } from "@/gen/openetl/api/v1/sync_pb";

export type Status = "healthy" | "running" | "degraded" | "failed" | "paused" | "idle";

export const statusLabel: Record<Status, string> = {
  healthy: "Healthy",
  running: "Running",
  degraded: "Degraded",
  failed: "Failed",
  paused: "Paused",
  idle: "Never synced",
};

export function rows(s: Sync) {
  let all = 0n;
  let quarantined = 0n;
  for (const st of s.streams) {
    all += st.rows;
    quarantined += st.quarantinedRows;
  }
  return { all, quarantined };
}

/** A sync's status: degraded when it succeeded but quarantined rows. */
export function syncStatus(s: Sync): Status {
  switch (s.state) {
    case SyncState.RUNNING:
      return "running";
    case SyncState.FAILED:
      return "failed";
    case SyncState.SUCCEEDED:
      return rows(s).quarantined > 0n ? "degraded" : "healthy";
    default:
      return "idle";
  }
}

/** Paused wins over what the last sync did; a running sync shows as running. */
export function pipelineStatus(p: Pipeline): Status {
  if (p.paused) return "paused";
  const last = p.status?.lastSync;
  return last ? syncStatus(last) : "idle";
}
