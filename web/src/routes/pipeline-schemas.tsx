import { useMemo } from "react";
import { useQuery } from "@connectrpc/connect-query";
import { getRouteApi, useNavigate } from "@tanstack/react-router";
import { Skeleton } from "@/components/ui/skeleton";
import { Empty, ErrorNote } from "@/components/empty";
import { RelativeTime } from "@/components/relative-time";
import { PipelineService, type SchemaColumn, type StreamSchema } from "@/gen/openetl/api/v1/pipeline_pb";
import { errorMessage } from "@/lib/api";
import { dateOf } from "@/lib/format";
import { pipelineName } from "@/lib/names";
import { cn } from "@/lib/utils";

const route = getRouteApi("/_app/workspaces/$workspace/pipelines/$pipeline");

type Change = "added" | "removed" | "changed" | "same";
const changeLabel: Record<Change, string> = { added: "Added", removed: "Removed", changed: "Changed", same: "" };

/** Each column of a version, and of the one before that it dropped. */
export function diff(prev: SchemaColumn[] | undefined, cur: SchemaColumn[]) {
  const before = new Map((prev ?? []).map((c) => [c.name, c]));
  const out: { col: SchemaColumn; change: Change; was?: SchemaColumn }[] = cur.map((c) => {
    const b = before.get(c.name);
    if (!prev) return { col: c, change: "same" };
    if (!b) return { col: c, change: "added" };
    return b.type !== c.type || b.nullable !== c.nullable ? { col: c, change: "changed", was: b } : { col: c, change: "same" };
  });
  const names = new Set(cur.map((c) => c.name));
  for (const c of prev ?? []) if (!names.has(c.name)) out.push({ col: c, change: "removed" });
  return out;
}

export function SchemasTab({ workspace, pipeline }: { workspace: string; pipeline: string }) {
  const { stream, version } = route.useSearch();
  const navigate = useNavigate({ from: "/workspaces/$workspace/pipelines/$pipeline" });
  const schemas = useQuery(PipelineService.method.listStreamSchemas, { parent: pipelineName(workspace, pipeline), pageSize: 500 });
  const byStream = useMemo(() => {
    const m = new Map<string, StreamSchema[]>();
    for (const s of schemas.data?.streamSchemas ?? []) m.set(s.stream, [...(m.get(s.stream) ?? []), s]);
    return m;
  }, [schemas.data]);

  if (schemas.error) return <ErrorNote title="Couldn't list schemas" message={errorMessage(schemas.error)} />;
  if (!schemas.data) return <Skeleton className="h-60" />;
  if (byStream.size === 0) {
    return <Empty title="No schemas yet">A sync records each stream's schema as it writes, and a new version whenever one changes.</Empty>;
  }
  const names = [...byStream.keys()];
  const selected = stream && byStream.has(stream) ? stream : names[0]!;
  const versions = byStream.get(selected)!;
  const n = Number(version);
  const shown = versions.find((v) => v.version === n) ?? versions[versions.length - 1]!;
  const prev = versions.find((v) => v.version === shown.version - 1);
  const rows = diff(prev?.columns, shown.columns);

  return (
    <div className="grid gap-6 md:grid-cols-[220px_1fr]">
      <nav aria-label="Streams" className="grid content-start gap-0.5">
        {names.map((s) => {
          const vs = byStream.get(s)!;
          return (
            <button
              key={s}
              onClick={() => navigate({ search: (q) => ({ ...q, stream: s, version: undefined }) })}
              aria-current={s === selected ? "true" : undefined}
              className={cn(
                "flex items-center justify-between gap-2 rounded-md px-3 py-1.5 text-left hover:bg-accent focus-visible:outline-2 focus-visible:outline-ring",
                s === selected && "bg-secondary font-medium",
              )}
            >
              <span className="truncate">{s}</span>
              <span className="font-mono text-xs text-muted-foreground">v{vs[vs.length - 1]!.version}</span>
            </button>
          );
        })}
      </nav>
      <div className="grid min-w-0 content-start gap-4">
        <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
          <h2 className="text-lg font-semibold tracking-tight">{selected}</h2>
          <span className="text-muted-foreground">
            into <code className="font-mono text-[0.8125rem] text-foreground">{shown.table}</code>
          </span>
          <div role="tablist" aria-label="Versions" className="flex flex-wrap gap-1 sm:ml-auto">
            {versions.map((v) => (
              <button
                key={v.version}
                role="tab"
                aria-selected={v.version === shown.version}
                onClick={() => navigate({ search: (q) => ({ ...q, stream: selected, version: String(v.version) }) })}
                className={cn(
                  "rounded-full border px-2.5 py-0.5 font-mono text-[0.8125rem] hover:bg-accent focus-visible:outline-2 focus-visible:outline-ring",
                  v.version === shown.version && "border-foreground bg-foreground text-background hover:bg-foreground",
                )}
              >
                v{v.version}
              </button>
            ))}
          </div>
        </div>
        <p className="text-muted-foreground">
          Version {shown.version}, first seen <RelativeTime date={dateOf(shown.createTime)!} />, at epoch{" "}
          <span className="font-mono tabular">{shown.epoch.toString()}</span> of generation{" "}
          <span className="font-mono tabular">{shown.generation.toString()}</span>.
          {prev ? ` Changes are against version ${prev.version}.` : ""}
        </p>
        <div className="overflow-x-auto rounded-lg border bg-card">
          <table className="w-full min-w-[560px] border-collapse">
            <thead>
              <tr className="border-b text-left text-[0.8125rem] text-muted-foreground">
                <th scope="col" className="px-4 py-2 font-normal">Column</th>
                <th scope="col" className="px-4 py-2 font-normal">Type</th>
                <th scope="col" className="px-4 py-2 font-normal">Nulls</th>
                <th scope="col" className="px-4 py-2 font-normal">From source column</th>
                <th scope="col" className="px-4 py-2 font-normal">Change</th>
              </tr>
            </thead>
            <tbody>
              {rows.map(({ col, change, was }) => (
                <tr
                  key={`${change}:${col.name}`}
                  className={cn(
                    "border-b last:border-b-0 [&>td:not(:last-child)]:decoration-failed/60",
                    change === "removed" && "text-muted-foreground [&>td:not(:last-child)]:line-through",
                    change === "added" && "bg-healthy/5",
                    change === "changed" && "bg-degraded/5",
                  )}
                >
                  <td className="h-row px-4 py-1.5 font-mono text-[0.8125rem]">{col.name}</td>
                  <td className="px-4 py-1.5 font-mono text-[0.8125rem]">
                    {was && was.type !== col.type && <span className="text-muted-foreground line-through">{was.type} </span>}
                    {col.type}
                  </td>
                  <td className="px-4 py-1.5 text-muted-foreground">{col.nullable ? "Allowed" : "Not null"}</td>
                  <td className="px-4 py-1.5 font-mono text-[0.8125rem] text-muted-foreground">{col.source}</td>
                  <td
                    className={cn(
                      "px-4 py-1.5 font-medium",
                      change === "added" && "text-healthy",
                      change === "removed" && "text-failed",
                      change === "changed" && "text-degraded",
                    )}
                  >
                    {changeLabel[change]}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
