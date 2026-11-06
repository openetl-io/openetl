import { useMemo } from "react";

export type ListSearch<F extends string> = { q?: string } & Partial<Record<F, string[]>>;

/**
 * A list's rows filtered by its search text and facets, and how many rows
 * each facet value would show given the other filters.
 */
export function useListFilter<T, F extends string>({
  rows,
  search,
  text,
  facets,
}: {
  rows: T[];
  search: ListSearch<NoInfer<F>>;
  /** The strings the search text matches. */
  text: (row: T) => string[];
  facets: Record<F, (row: T) => string>;
}) {
  const keys = Object.keys(facets) as F[];
  const q = search.q?.toLowerCase();
  const matches = (r: T, except?: F) =>
    (!q || text(r).some((v) => v.toLowerCase().includes(q))) &&
    keys.every((f) => f === except || !search[f]?.length || search[f]!.includes(facets[f](r)));

  const key = JSON.stringify(search);
  const visible = useMemo(() => rows.filter((r) => matches(r)), [rows, key]);

  /** Each value the facet takes in the rows, sorted, with its count. */
  const values = (f: F, order?: string[]) => {
    const all = [...new Set(rows.map(facets[f]))].filter(Boolean);
    all.sort(order ? (a, b) => order.indexOf(a) - order.indexOf(b) : undefined);
    return all.map((value) => ({ value, count: rows.filter((r) => facets[f](r) === value && matches(r, f)).length }));
  };

  return { visible, values, filtered: !!q || keys.some((f) => search[f]?.length), key };
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
