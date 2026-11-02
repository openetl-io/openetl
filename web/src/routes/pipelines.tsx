import { useMemo, useState } from "react";
import { useQuery } from "@connectrpc/connect-query";
import { Link, getRouteApi, useNavigate } from "@tanstack/react-router";
import {
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  useReactTable,
  type ColumnDef,
  type SortingState,
} from "@tanstack/react-table";
import { ArrowDown, ArrowUp, Search } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { Empty, ErrorNote } from "@/components/empty";
import { RelativeTime } from "@/components/relative-time";
import { StatusBadge } from "@/components/status";
import { SyncStrip } from "@/components/sync-strip";
import { DestinationService, SourceService } from "@/gen/openetl/api/v1/connection_pb";
import { PipelineService, type Pipeline } from "@/gen/openetl/api/v1/pipeline_pb";
import { SyncService } from "@/gen/openetl/api/v1/sync_pb";
import { errorMessage } from "@/lib/api";
import { compact, dateOf } from "@/lib/format";
import { lastSegment } from "@/lib/names";
import { scheduleLabel, stale } from "@/lib/pipeline";
import { pipelineStatus, rows, type Status } from "@/lib/status";
import { cn } from "@/lib/utils";
import type { PipelineFilter } from "@/router";

const route = getRouteApi("/_app/workspaces/$workspace/pipelines");

const severity: Record<Status, number> = { failed: 0, degraded: 1, running: 2, idle: 3, healthy: 4, paused: 5 };

const filters: { id: PipelineFilter; label: string; has: (s: Status) => boolean }[] = [
  { id: "all", label: "All", has: () => true },
  { id: "failing", label: "Failing", has: (s) => s === "failed" || s === "degraded" },
  { id: "running", label: "Running", has: (s) => s === "running" },
  { id: "paused", label: "Paused", has: (s) => s === "paused" },
];

function Strip({ workspace, pipeline, running }: { workspace: string; pipeline: string; running: boolean }) {
  const syncs = useQuery(
    SyncService.method.listSyncs,
    { parent: `workspaces/${workspace}/pipelines/${pipeline}`, pageSize: 24 },
    { refetchInterval: running ? 4_000 : 30_000 },
  );
  if (!syncs.data) return <Skeleton className="h-1.5 w-40" />;
  return <SyncStrip syncs={syncs.data.syncs} workspace={workspace} pipeline={pipeline} className="justify-end" />;
}

export function PipelinesPage() {
  const { workspace } = route.useParams();
  const { q, show = "all" } = route.useSearch();
  const navigate = useNavigate({ from: "/workspaces/$workspace/pipelines" });
  const parent = `workspaces/${workspace}`;
  const pipelines = useQuery(PipelineService.method.listPipelines, { parent, pageSize: 500 }, { refetchInterval: 10_000 });
  const sources = useQuery(SourceService.method.listSources, { parent, pageSize: 500 });
  const destinations = useQuery(DestinationService.method.listDestinations, { parent, pageSize: 500 });
  const connector = useMemo(() => {
    const m = new Map<string, string>();
    for (const s of sources.data?.sources ?? []) m.set(`s/${lastSegment(s.name)}`, s.spec?.connector ?? "");
    for (const d of destinations.data?.destinations ?? []) m.set(`d/${lastSegment(d.name)}`, d.spec?.connector ?? "");
    return m;
  }, [sources.data, destinations.data]);
  const [sorting, setSorting] = useState<SortingState>([{ id: "status", desc: false }]);

  const all = pipelines.data?.pipelines ?? [];
  const filter = filters.find((f) => f.id === show) ?? filters[0]!;
  const visible = all.filter(
    (p) => filter.has(pipelineStatus(p)) && (!q || lastSegment(p.name).toLowerCase().includes(q.toLowerCase())),
  );

  const columns = useMemo<ColumnDef<Pipeline>[]>(
    () => [
      {
        id: "status",
        header: "Status",
        accessorFn: (p) => severity[pipelineStatus(p)],
        cell: ({ row }) => <StatusBadge status={pipelineStatus(row.original)} />,
      },
      {
        id: "name",
        header: "Pipeline",
        accessorFn: (p) => lastSegment(p.name),
        cell: ({ row }) => {
          const p = row.original;
          const src = p.spec?.source?.ref ?? "";
          const dst = p.spec?.destination?.ref ?? "";
          return (
            <div className="grid min-w-0 gap-0.5">
              <Link
                to="/workspaces/$workspace/pipelines/$pipeline"
                params={{ workspace, pipeline: lastSegment(p.name) }}
                search={{ tab: undefined, stream: undefined, version: undefined }}
                className="truncate font-medium after:absolute after:inset-0 focus-visible:outline-none"
              >
                {lastSegment(p.name)}
              </Link>
              <span className="truncate text-[0.8125rem] text-muted-foreground">
                <span className="text-foreground/80">{src}</span>
                {connector.get(`s/${src}`) && ` (${connector.get(`s/${src}`)})`}
                <span aria-label="to"> → </span>
                <span className="text-foreground/80">{dst}</span>
                {connector.get(`d/${dst}`) && ` (${connector.get(`d/${dst}`)})`}
              </span>
            </div>
          );
        },
      },
      {
        id: "schedule",
        header: "Schedule",
        enableSorting: false,
        cell: ({ row }) => (
          <span className={cn("text-muted-foreground", row.original.spec?.schedule?.trigger.case === "cron" && "font-mono text-[0.8125rem]")}>
            {scheduleLabel(row.original.spec)}
          </span>
        ),
      },
      {
        id: "fresh",
        header: "Last success",
        accessorFn: (p) => dateOf(p.status?.lastSuccessTime)?.getTime() ?? 0,
        cell: ({ row }) => {
          const p = row.original;
          const t = dateOf(p.status?.lastSuccessTime);
          if (!t) return <span className="text-muted-foreground">Never</span>;
          return stale(p) ? (
            <Tooltip>
              <TooltipTrigger asChild>
                <span className="text-degraded">
                  <RelativeTime date={t} />
                </span>
              </TooltipTrigger>
              <TooltipContent>Older than two schedule intervals</TooltipContent>
            </Tooltip>
          ) : (
            <RelativeTime date={t} />
          );
        },
      },
      {
        id: "syncs",
        header: () => <span className="block text-right">Recent syncs</span>,
        enableSorting: false,
        cell: ({ row }) => (
          <div className="relative z-10 flex justify-end">
            <Strip
              workspace={workspace}
              pipeline={lastSegment(row.original.name)}
              running={pipelineStatus(row.original) === "running"}
            />
          </div>
        ),
      },
      {
        id: "rows",
        header: () => <span className="block text-right">Last sync</span>,
        accessorFn: (p) => (p.status?.lastSync ? Number(rows(p.status.lastSync).all) : -1),
        cell: ({ row }) => {
          const last = row.original.status?.lastSync;
          return (
            <span className="block text-right font-mono tabular text-[0.8125rem]">
              {last ? `${compact(rows(last).all)} rows` : "—"}
            </span>
          );
        },
      },
    ],
    [workspace, connector],
  );

  const table = useReactTable({
    data: visible,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
  });

  const counts = Object.fromEntries(filters.map((f) => [f.id, all.filter((p) => f.has(pipelineStatus(p))).length]));

  return (
    <div className="grid gap-5">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <h1 className="text-2xl font-semibold tracking-tight">Pipelines</h1>
        <div className="flex flex-wrap items-center gap-3">
          <div role="tablist" aria-label="Show" className="flex rounded-lg border bg-card p-0.5">
            {filters.map((f) => (
              <button
                key={f.id}
                role="tab"
                aria-selected={show === f.id}
                onClick={() => navigate({ search: (s) => ({ ...s, show: f.id === "all" ? undefined : f.id }) })}
                className={cn(
                  "flex items-center gap-1.5 rounded-md px-2.5 py-1 text-muted-foreground transition-colors hover:text-foreground focus-visible:outline-2 focus-visible:outline-ring",
                  show === f.id && "bg-secondary text-foreground",
                )}
              >
                {f.label}
                <span className="font-mono text-xs tabular opacity-70">{counts[f.id] ?? 0}</span>
              </button>
            ))}
          </div>
          <div className="relative">
            <Search className="pointer-events-none absolute top-1/2 left-2.5 size-4 -translate-y-1/2 text-muted-foreground" aria-hidden />
            <Input
              type="search"
              placeholder="Filter by name"
              aria-label="Filter by name"
              value={q ?? ""}
              onChange={(e) => navigate({ search: (s) => ({ ...s, q: e.target.value || undefined }), replace: true })}
              className="h-8 w-52 pl-8"
            />
          </div>
        </div>
      </div>

      {pipelines.error ? (
        <ErrorNote title="Couldn't list pipelines" message={errorMessage(pipelines.error)} />
      ) : pipelines.isPending ? (
        <div className="grid gap-2">
          {[0, 1, 2].map((i) => (
            <Skeleton key={i} className="h-row" />
          ))}
        </div>
      ) : all.length === 0 ? (
        <Empty title={`No pipelines in ${workspace} yet`} command="openetl apply -f pipeline.yaml">
          Pipelines, sources and destinations are YAML documents that the CLI applies. This view shows them once they exist.
        </Empty>
      ) : (
        <div className="overflow-x-auto rounded-lg border bg-card">
          <table className="w-full min-w-[860px] border-collapse">
            <thead>
              {table.getHeaderGroups().map((hg) => (
                <tr key={hg.id} className="border-b text-left text-[0.8125rem] text-muted-foreground">
                  {hg.headers.map((h) => {
                    const sorted = h.column.getIsSorted();
                    return (
                      <th
                        key={h.id}
                        scope="col"
                        aria-sort={sorted === "asc" ? "ascending" : sorted === "desc" ? "descending" : undefined}
                        className={cn("px-4 py-2 font-normal", h.id === "syncs" && "w-[260px]")}
                      >
                        {h.column.getCanSort() ? (
                          <button
                            onClick={h.column.getToggleSortingHandler()}
                            className="inline-flex items-center gap-1 hover:text-foreground"
                          >
                            {flexRender(h.column.columnDef.header, h.getContext())}
                            {sorted === "asc" ? <ArrowUp className="size-3" /> : sorted === "desc" ? <ArrowDown className="size-3" /> : null}
                          </button>
                        ) : (
                          flexRender(h.column.columnDef.header, h.getContext())
                        )}
                      </th>
                    );
                  })}
                </tr>
              ))}
            </thead>
            <tbody>
              {table.getRowModel().rows.map((r) => (
                <tr
                  key={r.id}
                  className="relative border-b transition-colors last:border-b-0 focus-within:bg-accent/60 hover:bg-accent/60"
                >
                  {r.getVisibleCells().map((c) => (
                    <td key={c.id} className="h-row px-4 py-2 align-middle">
                      {flexRender(c.column.columnDef.cell, c.getContext())}
                    </td>
                  ))}
                </tr>
              ))}
              {visible.length === 0 && (
                <tr>
                  <td colSpan={columns.length} className="px-4 py-8 text-center text-muted-foreground">
                    No pipelines match.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
