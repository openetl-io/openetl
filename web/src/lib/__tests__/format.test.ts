import { describe, expect, it } from "vitest";
import { duration, enumLabel, errorSummary, relative } from "../format";

describe("format", () => {
  it("formats durations at each scale", () => {
    expect(duration(420)).toBe("420ms");
    expect(duration(42_000)).toBe("42s");
    expect(duration(185_000)).toBe("3m 05s");
    expect(duration(7_800_000)).toBe("2h 10m");
  });

  it("labels enum values in sentence case", () => {
    expect(enumLabel("FULL_REFRESH")).toBe("Full refresh");
    expect(enumLabel("UNSPECIFIED")).toBe("");
  });

  it("says just now for the last few seconds", () => {
    const now = Date.now();
    expect(relative(new Date(now - 3000), now)).toBe("just now");
    expect(relative(new Date(now - 5 * 60_000), now)).toMatch(/^5\s?m.* ago$/);
  });
});

describe("errorSummary", () => {
  it("keeps the innermost cause of an activity error", () => {
    const e =
      'activity error (type: RunSyncAttempt, scheduledEventID: 12): CONFIG_INVALID: runner: bad (type: CONFIG_INVALID, retryable: false): stream "users": column "score" is wrong\nmore';
    expect(errorSummary(e)).toBe('stream "users": column "score" is wrong');
    expect(errorSummary("TRANSIENT: boom\nmore")).toBe("TRANSIENT: boom");
  });
});
