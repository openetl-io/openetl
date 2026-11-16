import { describe, expect, it } from "vitest";
import { create } from "@bufbuild/protobuf";
import { timestampFromMs } from "@bufbuild/protobuf/wkt";
import { OperatorStatsSchema } from "@/gen/openetl/api/v1/pipeline_pb";
import { requests, stateBytes, watermark, written } from "@/lib/operator-stats";

describe("operator stats", () => {
  it("writes sizes in binary units", () => {
    expect(stateBytes(0n)).toBe("0 B");
    expect(stateBytes(812n)).toBe("812 B");
    expect(stateBytes(4301n)).toBe("4.2 KiB");
    expect(stateBytes(48_234_496n)).toBe("46 MiB");
    expect(stateBytes(1_610_612_736n)).toBe("1.5 GiB");
  });

  it("says where the watermark is against the commit", () => {
    const at = Date.UTC(2026, 10, 16, 12, 0, 0);
    const s = create(OperatorStatsSchema, { watermark: timestampFromMs(at) });
    expect(watermark(s, timestampFromMs(at + 185_000))).toEqual({ at: "2026-11-16T12:00:00Z", behind: "3m 05s behind the commit" });
    expect(watermark(create(OperatorStatsSchema, {}), timestampFromMs(at)).at).toBe("None yet");
    expect(watermark(create(OperatorStatsSchema, { endOfInput: true })).at).toBe("End of input");
  });

  it("counts what was written and asked", () => {
    expect(written(create(OperatorStatsSchema, { inserts: 1200n, updates: 3n }))).toBe("1,200 new, 3 updated, 0 deleted");
    expect(requests(undefined)).toBe("0");
    expect(requests({ $typeName: "openetl.api.v1.StateRequests", count: 4n, bytes: 1_310_720n })).toBe("4 (1.3 MiB)");
  });
});
