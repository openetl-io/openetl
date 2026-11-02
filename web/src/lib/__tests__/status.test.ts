import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import { PipelineSchema, PipelineStatusSchema } from "@/gen/openetl/api/v1/pipeline_pb";
import { SyncSchema, SyncState, SyncStreamSchema } from "@/gen/openetl/api/v1/sync_pb";
import { pipelineStatus } from "../status";

const sync = (state: SyncState, quarantined = 0n) =>
  create(SyncSchema, { state, streams: [create(SyncStreamSchema, { name: "a", rows: 10n, quarantinedRows: quarantined })] });

describe("pipelineStatus", () => {
  it("follows the last sync, and paused wins", () => {
    const p = (lastSync?: ReturnType<typeof sync>, paused = false) =>
      pipelineStatus(create(PipelineSchema, { paused, status: create(PipelineStatusSchema, { lastSync }) }));
    expect(p()).toBe("idle");
    expect(p(sync(SyncState.RUNNING))).toBe("running");
    expect(p(sync(SyncState.FAILED))).toBe("failed");
    expect(p(sync(SyncState.SUCCEEDED))).toBe("healthy");
    expect(p(sync(SyncState.SUCCEEDED, 3n))).toBe("degraded");
    expect(p(sync(SyncState.FAILED), true)).toBe("paused");
    expect(p(sync(SyncState.STOPPED))).toBe("stopped");
  });
});
