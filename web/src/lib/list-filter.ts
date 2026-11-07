import { useMemo } from "react";

export type ListSearch<F extends string> = { q?: string } & Partial<Record<F, string[]>>;

type FilterArgs<T, F extends string> = {
  rows: T[];
  search: ListSearch<NoInfer<F>>;
  /** The strings the search text matches. */
  text: (row: T) => string[];
  /** A facet with several values for a row, such as a connector's roles, matches when any does. */
  facets: Record<F, (row: T) => string | string[]>;
};

/** Whether a row matches the search, but for one facet when except names it; and each facet value's count. */
export function listMatcher<T, F extends string>({ rows, search, text, facets }: FilterArgs<T, F>) {
  const keys = Object.keys(facets) as F[];
  const q = search.q?.toLowerCase();
  const valuesOf = (f: F, r: T) => [facets[f](r)].flat();
  const matches = (r: T, except?: F) =>
    (!q || text(r).some((v) => v.toLowerCase().includes(q))) &&
    keys.every((f) => f === except || !search[f]?.length || valuesOf(f, r).some((v) => search[f]!.includes(v)));

  /** Each value the facet takes in the rows, sorted, with its count. */
  const values = (f: F, order?: string[]) => {
    const all = [...new Set(rows.flatMap((r) => valuesOf(f, r)))].filter(Boolean);
    all.sort(order ? (a, b) => order.indexOf(a) - order.indexOf(b) : undefined);
    return all.map((value) => ({ value, count: rows.filter((r) => valuesOf(f, r).includes(value) && matches(r, f)).length }));
  };
  return { matches, values, filtered: !!q || keys.some((f) => search[f]?.length) };
}

/**
 * A list's rows filtered by its search text and facets, and how many rows
 * each facet value would show given the other filters.
 */
export function useListFilter<T, F extends string>(args: FilterArgs<T, F>) {
  const { matches, values, filtered } = listMatcher(args);
  const key = JSON.stringify(args.search);
  const visible = useMemo(() => args.rows.filter((r) => matches(r)), [args.rows, key]);
  return { visible, values, filtered, key };
}

/** A route's search params for a list: the search text and each facet's values. */
export function listSearch<const F extends string>(facets: readonly F[]) {
  const str = (v: unknown) => (typeof v === "string" && v !== "" ? v : undefined);
  const strs = (v: unknown) => {
    const l = (Array.isArray(v) ? v : [v]).filter((x): x is string => typeof x === "string" && x !== "");
    return l.length ? l : undefined;
  };
  return (s: Record<string, unknown>): ListSearch<F> => {
    const out = { q: str(s.q) } as ListSearch<F>;
    for (const f of facets) (out as Record<string, unknown>)[f] = strs(s[f]);
    return out;
  };
}
