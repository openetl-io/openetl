import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import { QuarantineFileSchema } from "@/gen/openetl/api/v1/pipeline_pb";
import { SyncSchema } from "@/gen/openetl/api/v1/sync_pb";
import { anyNotHeld, causeText, numbersRoundTrip, parseLine, rowsLabel, showCommand } from "../quarantine";
import { quarantinedStream } from "../status";

describe("causeText", () => {
  it("is what openetl quarantine prints", () => {
    expect(causeText({ code: "type", column: "amount", notHeld: "" })).toBe("type on amount");
    expect(causeText({ code: "type", column: "id", notHeld: "key" })).toBe("type on id, not held: key");
    expect(causeText({ code: "object", column: "", notHeld: "record" })).toBe("object, not held: record");
    expect(causeText({ code: "statement", column: "", notHeld: "" })).toBe("statement");
  });
});

describe("rowsLabel", () => {
  it("counts rows", () => {
    expect(rowsLabel(1n)).toBe("1 row");
    expect(rowsLabel(1312n)).toBe("1,312 rows");
  });
});

describe("anyNotHeld", () => {
  it("is whether a stream that holds values quarantined any of the rows", () => {
    const file = (notHeld: string) => create(QuarantineFileSchema, { causes: [{ code: "type", column: "amount", notHeld, rows: 2n }] });
    expect(anyNotHeld([])).toBe(false);
    expect(anyNotHeld([file("")])).toBe(false);
    expect(anyNotHeld([file(""), file("key")])).toBe(true);
  });
});

describe("showCommand", () => {
  it("puts flags before the names", () => {
    expect(showCommand("default", "shop", "3/17/orders.jsonl")).toBe("openetl quarantine show --limit 100 shop 3/17/orders.jsonl");
    expect(showCommand("sales", "shop", "3/17/orders.jsonl", 50)).toBe(
      "openetl quarantine show --workspace sales --limit 50 shop 3/17/orders.jsonl",
    );
  });
});

describe("parseLine", () => {
  it("gives the cause and reason, and the record pretty-printed where it's JSON", () => {
    const reason = `type: column "id" isn't a number`;
    const l = parseLine(JSON.stringify({ stream: "orders", code: "type", column: "id", notHeld: "key", reason, record: `{"id": "8a"}` }));
    expect(l).toEqual({ cause: "type on id, not held: key", reason, record: '{\n  "id": "8a"\n}' });
  });
  it("keeps a record that isn't JSON as it came", () => {
    expect(parseLine(JSON.stringify({ stream: "orders", code: "json", reason: "x", record: "{nope" })).record).toBe("{nope");
  });
  it("shows a line that isn't an object as it is", () => {
    expect(parseLine("not json")).toEqual({ record: "not json" });
    expect(parseLine("[1]")).toEqual({ record: "[1]" });
  });
});

describe("quarantinedStream", () => {
  it("is the one stream that quarantined rows", () => {
    const sync = (...q: bigint[]) => create(SyncSchema, { streams: q.map((n, i) => ({ name: `s${i}`, quarantinedRows: n })) });
    expect(quarantinedStream(sync(0n, 3n))).toBe("s1");
    expect(quarantinedStream(sync(1n, 3n))).toBeUndefined();
    expect(quarantinedStream(sync(0n))).toBeUndefined();
  });
});

describe("numbersRoundTrip", () => {
  it("is false for a number a double changes, and ignores strings", () => {
    expect(numbersRoundTrip('{"id": 7, "amount": -3.25, "n": [0, 12]}')).toBe(true);
    expect(numbersRoundTrip('{"id": 9007199254740993}')).toBe(false);
    expect(numbersRoundTrip('{"x": 1.0}')).toBe(false);
    expect(numbersRoundTrip('{"s": "9007199254740993", "t": "a\\"1.0"}')).toBe(true);
  });
});

describe("parseLine and large numbers", () => {
  it("shows a record whose numbers a double would change as it came", () => {
    const record = '{"id":9007199254740993}';
    expect(parseLine(JSON.stringify({ stream: "a", code: "type", reason: "r", record })).record).toBe(record);
    expect(parseLine(JSON.stringify({ stream: "a", code: "type", reason: "r", record: '{"id":7}' })).record).toBe('{\n  "id": 7\n}');
  });
});
