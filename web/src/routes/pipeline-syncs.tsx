import { useMemo } from "react";
import { useInfiniteQuery } from "@connectrpc/connect-query";
import { useNavigate } from "@tanstack/react-router";
import type { ColumnDef } from "@tanstack/react-table";
import { Button } from "@/components/ui/button";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { DataTable } from "@/components/data-table/data-table";
import { useDataTable } from "@/components/data-table/use-data-table";
import { Empty, ErrorNote } from "@/components/empty";
import { RelativeTime } from "@/components/relative-time";
import { StatusBadge } from "@/components/status";
import { SyncService, type Sync } from "@/gen/openetl/api/v1/sync_pb";
import { errorMessage } from "@/lib/api";
import { compact, dateOf, duration, errorSummary, full } from "@/lib/format";
import { lastSegment, pipelineName, shortDigest } from "@/lib/names";
import { heldLabel, rows, syncStatus } from "@/lib/status";

const columns: ColumnDef<Sync>[] = [
  {
    id: "status",
    header: "Status",
    cell: ({ row }) => <StatusBadge status={syncStatus(row.original)} />,
  },
  {
    id: "started",
    header: "Started",
    cell: ({ row }) => <RelativeTime date={dateOf(row.original.startTime)!} />,
  },
  {
    id: "took",
    header: "Took",
    cell: ({ row }) => {
      const start = dateOf(row.original.startTime)!;
      const end = dateOf(row.original.endTime);
      return <span className="tabular-nums">{duration((end?.getTime() ?? Date.now()) - start.getTime())}</span>;
    },
    meta: { align: "right", className: "hidden sm:table-cell" },
  },
  {
    id: "rows",
    header: "Rows",
    cell: ({ row }) => {
      const r = rows(row.original);
      const notes = [r.quarantined > 0n && `${compact(r.quarantined)} quarantined`, r.held > 0 && heldLabel(r.held)].filter(Boolean);
      return (
        <span className="tabular-nums">
          <span title={full(r.all)}>{compact(r.all)}</span>
          {notes.length > 0 && <span className="text-degraded"> ({notes.join(", ")})</span>}
        </span>
      );
    },
    meta: { align: "right", className: "hidden md:table-cell" },
  },
  {
    id: "version",
    header: "Version",
    cell: ({ row }) => <span className="font-mono text-[0.8125rem] text-muted-foreground">{shortDigest(row.original.version, 10)}</span>,
    meta: { className: "hidden lg:table-cell" },
  },
  {
    id: "error",
    header: "Error",
    cell: ({ row }) =>
      row.original.error && (
        <Tooltip>
          <TooltipTrigger asChild>
            <span className="block max-w-md truncate font-mono text-[0.8125rem] text-failed">{errorSummary(row.original.error)}</span>
          </TooltipTrigger>
          <TooltipContent className="max-w-lg font-mono whitespace-pre-wrap">{row.original.error}</TooltipContent>
        </Tooltip>
      ),
    meta: { className: "w-full hidden md:table-cell" },
  },
];

export function SyncsTab({ workspace, pipeline, running }: { workspace: string; pipeline: string; running: boolean }) {
  const navigate = useNavigate();
  const syncs = useInfiniteQuery(
    SyncService.method.listSyncs,
    { parent: pipelineName(workspace, pipeline), pageSize: 50, pageToken: "" },
    { pageParamKey: "pageToken", getNextPageParam: (r) => r.nextPageToken || undefined, refetchInterval: running ? 3_000 : 15_000 },
  );
  const all = useMemo(() => syncs.data?.pages.flatMap((p) => p.syncs) ?? [], [syncs.data]);
  const table = useDataTable({ data: all, columns, paged: false });

  if (syncs.error) return <ErrorNote title="Couldn't list syncs" message={errorMessage(syncs.error)} />;
  if (syncs.data && all.length === 0) {
    return <Empty title="No syncs yet" command={`openetl sync ${pipeline}`}>The schedule starts them, or you can start one now.</Empty>;
  }
  return (
    <div className="grid gap-3">
      <DataTable
        table={table}
        loading={syncs.isPending}
        onRowClick={(r) => navigate({ to: "/workspaces/$workspace/pipelines/$pipeline/syncs/$sync", params: { workspace, pipeline, sync: lastSegment(r.original.name) } })}
      />
      {syncs.hasNextPage && (
        <Button variant="outline" size="sm" className="w-fit" disabled={syncs.isFetchingNextPage} onClick={() => syncs.fetchNextPage()}>
          {syncs.isFetchingNextPage ? "Loading…" : "Load older syncs"}
        </Button>
      )}
    </div>
  );
}
