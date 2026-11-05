import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import { HoldSchema, HoldWayOut_Action } from "@/gen/openetl/api/v1/pipeline_pb";
import { heldWhat, holdCommand, holdState, refusalNote } from "../holds";

const hold = (init: Parameters<typeof create<typeof HoldSchema>>[1] = {}) =>
  create(HoldSchema, { stream: "public.orders", column: "amount", whole: true, ...init });

describe("holdState", () => {
  it("is what openetl holds shows, a pending release before a refused one", () => {
    expect(holdState(hold())).toBe("held");
    expect(holdState(hold({ refused: { reason: "it doesn't change a column's type" } }))).toBe("refused");
    // Released again after a refusal: the new release waits for the next run.
    expect(holdState(hold({ refused: { reason: "x" }, pending: true }))).toBe("pending");
    expect(holdState(hold({ releasing: true, pending: true }))).toBe("releasing");
    expect(holdState(hold({ discarded: true, refused: { reason: "x" } }))).toBe("discarded");
  });
});

describe("heldWhat", () => {
  it("names a column or counts cells", () => {
    expect(heldWhat(hold())).toBe("Column");
    expect(heldWhat(hold({ whole: false, cells: 1n }))).toBe("1 cell");
    expect(heldWhat(hold({ whole: false, cells: 7n }))).toBe("7 cells");
  });
});

describe("refusalNote", () => {
  it("says the last release didn't clear the hold", () => {
    expect(refusalNote(hold())).toBeUndefined();
    expect(refusalNote(hold({ refused: { reason: "it doesn't change a column's type, which can rewrite the table." } }))).toBe(
      "Its last release didn't clear it: it doesn't change a column's type, which can rewrite the table. Release it again once that's fixed.",
    );
    expect(refusalNote(hold({ whole: false, refused: { reason: "2 released values still don't fit" } }))).toBe(
      "Its last release didn't clear it: 2 released values still don't fit. Release it again once that's fixed.",
    );
  });
});

describe("holdCommand", () => {
  it("names the workspace only when it isn't default", () => {
    const h = hold();
    expect(holdCommand(HoldWayOut_Action.RELEASE, "default", "shop", h)).toBe("openetl holds release shop public.orders amount");
    expect(holdCommand(HoldWayOut_Action.RELEASE_NEW_VERSION, "sales", "shop", h)).toBe(
      "openetl holds release --new-version --workspace sales shop public.orders amount",
    );
    expect(holdCommand(HoldWayOut_Action.DISCARD, "sales", "shop", h)).toBe("openetl holds discard --workspace sales shop public.orders amount");
    expect(holdCommand(HoldWayOut_Action.UNSPECIFIED, "default", "shop", h)).toBeUndefined();
  });
});
