import { create } from "@bufbuild/protobuf";
import { DurationSchema } from "@bufbuild/protobuf/wkt";
import { describe, expect, it } from "vitest";
import { ReadProgressSchema, SyncSchema } from "@/gen/openetl/api/v1/sync_pb";
import { lag } from "../pipeline";

describe("lag", () => {
  it("is the most of the streams' lags, in time if known, else in bytes", () => {
    const sync = (...progress: Parameters<typeof create<typeof ReadProgressSchema>>[1][]) =>
      create(SyncSchema, { progress: progress.map((p) => create(ReadProgressSchema, p)) });
    expect(lag(undefined)).toBeUndefined();
    expect(lag(sync())).toBeUndefined();
    expect(lag(sync({ stream: "a" }))).toBeUndefined();
    expect(lag(sync({ lagBytes: 1_500_000n }, { lagBytes: 20n }))).toBe("1.5MB");
    expect(
      lag(
        sync(
          { lagBytes: 9n, lag: create(DurationSchema, { seconds: 2n }) },
          { lag: create(DurationSchema, { seconds: 42n, nanos: 0 }) },
        ),
      ),
    ).toBe("42s");
  });
});
