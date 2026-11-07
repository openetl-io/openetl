import { describe, expect, it } from "vitest";
import { listMatcher } from "../list-filter";

type Row = { name: string; roles: string[]; source: string };
const rows: Row[] = [
  { name: "postgres", roles: ["destination", "source"], source: "official" },
  { name: "kafka", roles: ["source"], source: "official" },
  { name: "s3", roles: ["destination"], source: "acme" },
];
const facets = { role: (r: Row) => r.roles, source: (r: Row) => r.source };
const match = (search: { q?: string; role?: string[]; source?: string[] }) => {
  const m = listMatcher({ rows, search, text: (r) => [r.name], facets });
  return { names: rows.filter((r) => m.matches(r)).map((r) => r.name), values: m.values, filtered: m.filtered };
};

describe("listMatcher", () => {
  it("matches a facet with several values when any of them is selected", () => {
    expect(match({ role: ["source"] }).names).toEqual(["postgres", "kafka"]);
    expect(match({ role: ["destination"] }).names).toEqual(["postgres", "s3"]);
    expect(match({ role: ["source", "destination"] }).names).toEqual(["postgres", "kafka", "s3"]);
  });

  it("counts each value given the other facets and the search", () => {
    expect(match({}).values("role")).toEqual([
      { value: "destination", count: 2 },
      { value: "source", count: 2 },
    ]);
    expect(match({ source: ["official"] }).values("role")).toEqual([
      { value: "destination", count: 1 },
      { value: "source", count: 2 },
    ]);
    expect(match({ role: ["source"] }).values("role")).toEqual([
      { value: "destination", count: 2 },
      { value: "source", count: 2 },
    ]);
    expect(match({ q: "kaf" }).values("source")).toEqual([
      { value: "acme", count: 0 },
      { value: "official", count: 1 },
    ]);
  });

  it("combines the search and the facets", () => {
    expect(match({ q: "s", role: ["destination"], source: ["acme"] }).names).toEqual(["s3"]);
    expect(match({ q: "s" }).filtered).toBe(true);
    expect(match({}).filtered).toBe(false);
  });
});
