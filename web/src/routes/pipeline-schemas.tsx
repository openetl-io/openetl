import { useMemo } from "react";
import { useQuery } from "@connectrpc/connect-query";
import { getRouteApi, useNavigate } from "@tanstack/react-router";
import type { ColumnDef } from "@tanstack/react-table";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { DataTable } from "@/components/data-table/data-table";
import { useDataTable } from "@/components/data-table/use-data-table";
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

type DiffRow = ReturnType<typeof diff>[number];

const columns: ColumnDef<DiffRow>[] = [
  {
    id: "column",
    header: "Column",
    cell: ({ row }) => <span className="font-mono text-[0.8125rem]">{row.original.col.name}</span>,
  },
  {
    id: "type",
    header: "Type",
    cell: ({ row }) => {
      const { col, was } = row.original;
      return (
        <span className="font-mono text-[0.8125rem]">
          {was && was.type !== col.type && <span className="text-muted-foreground line-through">{was.type} </span>}
          {col.type}
        </span>
      );
    },
  },
  {
    id: "nulls",
    header: "Nulls",
    cell: ({ row }) => <span className="text-muted-foreground">{row.original.col.nullable ? "Allowed" : "Not null"}</span>,
    meta: { className: "hidden sm:table-cell" },
  },
  {
    id: "source",
    header: "Renamed from",
    cell: ({ row }) => <span className="font-mono text-[0.8125rem] text-muted-foreground">{row.original.col.source}</span>,
    meta: { className: "hidden md:table-cell" },
  },
  {
    id: "change",
    header: "Change",
    cell: ({ row }) => {
      const c = row.original.change;
      return (
        <span className={cn("font-medium", c === "added" && "text-healthy", c === "removed" && "text-failed", c === "changed" && "text-degraded")}>
          {changeLabel[c]}
        </span>
      );
    },
  },
];

function ColumnsTable({ rows }: { rows: DiffRow[] }) {
  const table = useDataTable({ data: rows, columns, paged: false });
  return (
    <DataTable
      table={table}
      rowClassName={(r) =>
        cn(
          r.original.change === "removed" && "text-muted-foreground line-through decoration-failed/60",
          r.original.change === "added" && "bg-healthy/5",
          r.original.change === "changed" && "bg-degraded/5",
        )
      }
    />
  );
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

  return (
    <div className="grid gap-6 md:grid-cols-[220px_1fr]">
      <nav aria-label="Streams" className="grid content-start gap-0.5">
        {names.map((s) => {
          const vs = byStream.get(s)!;
          return (
            <Button
              key={s}
              variant={s === selected ? "secondary" : "ghost"}
              size="sm"
              onClick={() => navigate({ search: (q) => ({ ...q, stream: s, version: undefined }) })}
              aria-current={s === selected ? "true" : undefined}
              className="justify-between font-normal"
            >
              <span className="truncate">{s}</span>
              <span className="font-mono text-xs text-muted-foreground">v{vs[vs.length - 1]!.version}</span>
            </Button>
          );
        })}
      </nav>
      <div className="grid min-w-0 content-start gap-4">
        <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
          <h2 className="text-base font-semibold">{selected}</h2>
          <span className="text-muted-foreground">
            into <code className="font-mono text-[0.8125rem] text-foreground">{shown.table}</code>
          </span>
          <div role="tablist" aria-label="Versions" className="flex flex-wrap gap-1 sm:ml-auto">
            {versions.map((v) => (
              <Button
                key={v.version}
                role="tab"
                aria-selected={v.version === shown.version}
                variant={v.version === shown.version ? "secondary" : "ghost"}
                size="xs"
                onClick={() => navigate({ search: (q) => ({ ...q, stream: selected, version: String(v.version) }) })}
                className="font-mono"
              >
                v{v.version}
              </Button>
            ))}
          </div>
        </div>
        <p className="text-muted-foreground">
          Version {shown.version}, first seen <RelativeTime date={dateOf(shown.createTime)!} />, at epoch{" "}
          <span className="font-mono tabular-nums">{shown.epoch.toString()}</span> of generation{" "}
          <span className="font-mono tabular-nums">{shown.generation.toString()}</span>.
          {prev ? ` Changes are against version ${prev.version}.` : ""}
        </p>
        <ColumnsTable rows={diff(prev?.columns, shown.columns)} />
      </div>
    </div>
  );
}
