import type { Pipeline } from "@/gen/openetl/api/v1/pipeline_pb";
import { SyncState, type Sync } from "@/gen/openetl/api/v1/sync_pb";

export type Status = "healthy" | "running" | "degraded" | "failed" | "paused" | "stopped" | "idle";

export const statusLabel: Record<Status, string> = {
  healthy: "Healthy",
  running: "Running",
  degraded: "Degraded",
  failed: "Failed",
  paused: "Paused",
  stopped: "Stopped",
  idle: "Never synced",
};

/** A sync's rows, and the columns its streams held at its latest commit. */
export function rows(s: Sync) {
  let all = 0n;
  let quarantined = 0n;
  let held = 0;
  for (const st of s.streams) {
    all += st.rows;
    quarantined += st.quarantinedRows;
    held += st.heldColumns.length;
  }
  return { all, quarantined, held };
}

export function heldLabel(columns: number) {
  return columns === 1 ? "1 column held" : `${columns} columns held`;
}

/** A sync's status: degraded when it succeeded but quarantined rows or ended holding columns. */
export function syncStatus(s: Sync): Status {
  switch (s.state) {
    case SyncState.RUNNING:
      return "running";
    case SyncState.FAILED:
      return "failed";
    case SyncState.SUCCEEDED: {
      const r = rows(s);
      return r.quarantined > 0n || r.held > 0 ? "degraded" : "healthy";
    }
    case SyncState.STOPPED:
      return "stopped";
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
