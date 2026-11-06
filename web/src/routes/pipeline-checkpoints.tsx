import { useMemo } from "react";
import { useInfiniteQuery } from "@connectrpc/connect-query";
import { Link } from "@tanstack/react-router";
import type { ColumnDef } from "@tanstack/react-table";
import { CircleCheckBig, Zap } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { DataTable } from "@/components/data-table/data-table";
import { useDataTable } from "@/components/data-table/use-data-table";
import { Empty, ErrorNote } from "@/components/empty";
import { RelativeTime } from "@/components/relative-time";
import { DeliveryLevel, PipelineService, type ListCheckpointsResponse } from "@/gen/openetl/api/v1/pipeline_pb";
import { errorMessage } from "@/lib/api";
import { compact, dateOf, enumLabel, full } from "@/lib/format";
import { lastSegment, pipelineName } from "@/lib/names";

type Checkpoint = ListCheckpointsResponse["checkpoints"][number];

function StreamChip({ s }: { s: Checkpoint["streams"][number] }) {
  return (
    <Badge variant="outline" className="gap-1.5 font-normal">
      <span>{s.name}</span>
      <span className="text-muted-foreground tabular-nums">{compact(s.rows)}</span>
      {s.quarantinedRows > 0n && <span className="text-degraded tabular-nums">{compact(s.quarantinedRows)} quarantined</span>}
      {s.heldCells > 0n && <span className="text-degraded tabular-nums">{compact(s.heldCells)} cells held</span>}
      {s.heldColumns.length > 0 && <span className="text-degraded">{s.heldColumns.join(", ")} held</span>}
      {s.collapsedRows > 0n && <span className="text-muted-foreground tabular-nums">{compact(s.collapsedRows)} collapsed</span>}
      {s.level !== DeliveryLevel.UNSPECIFIED && (
        <Tooltip>
          <TooltipTrigger className="text-degraded">{enumLabel(DeliveryLevel[s.level]).toLowerCase()}</TooltipTrigger>
          <TooltipContent>
            Below the planned {enumLabel(DeliveryLevel[s.plannedLevel]).toLowerCase()}: read again after a restore from a forced barrier, so
            its rows may repeat rows an earlier epoch committed
          </TooltipContent>
        </Tooltip>
      )}
      {s.schemaVersion > 0 && <span className="font-mono text-muted-foreground">v{s.schemaVersion}</span>}
      {s.forced && (
        <Tooltip>
          <TooltipTrigger aria-label="Forced barrier">
            <Zap className="size-3.5 text-degraded" />
          </TooltipTrigger>
          <TooltipContent>Forced barrier, without a resume point</TooltipContent>
        </Tooltip>
      )}
      {s.complete && (
        <Tooltip>
          <TooltipTrigger aria-label="Read complete">
            <CircleCheckBig className="size-3.5 text-healthy" />
          </TooltipTrigger>
          <TooltipContent>The stream's read ended in this epoch</TooltipContent>
        </Tooltip>
      )}
    </Badge>
  );
}

export function CheckpointsTab({ workspace, pipeline, running }: { workspace: string; pipeline: string; running: boolean }) {
  const cps = useInfiniteQuery(
    PipelineService.method.listCheckpoints,
    { parent: pipelineName(workspace, pipeline), pageSize: 50, pageToken: "" },
    { pageParamKey: "pageToken", getNextPageParam: (r) => r.nextPageToken || undefined, refetchInterval: running ? 3_000 : 30_000 },
  );
  const all = useMemo(() => cps.data?.pages.flatMap((p) => p.checkpoints) ?? [], [cps.data]);
  const columns = useMemo<ColumnDef<Checkpoint>[]>(
    () => [
      {
        id: "epoch",
        header: "Epoch",
        cell: ({ row }) => <span className="font-mono text-[0.8125rem] tabular-nums">{row.original.epoch.toString()}</span>,
      },
      {
        id: "generation",
        header: "Generation",
        cell: ({ row }) => <span className="font-mono text-[0.8125rem] text-muted-foreground tabular-nums">{row.original.generation.toString()}</span>,
        meta: { className: "hidden sm:table-cell" },
      },
      {
        id: "committed",
        header: "Committed",
        cell: ({ row }) => row.original.commitTime && <RelativeTime date={dateOf(row.original.commitTime)!} />,
      },
      {
        id: "rows",
        header: "Rows",
        cell: ({ row }) => {
          const total = row.original.streams.reduce((n, s) => n + s.rows, 0n);
          return <span className="tabular-nums" title={full(total)}>{compact(total)}</span>;
        },
        meta: { align: "right" },
      },
      {
        id: "streams",
        header: "Streams",
        cell: ({ row }) => (
          <span className="flex flex-wrap gap-1.5 py-1">
            {row.original.streams.map((s) => (
              <StreamChip key={s.name} s={s} />
            ))}
          </span>
        ),
        meta: { className: "w-full hidden md:table-cell whitespace-normal" },
      },
      {
        id: "sync",
        header: "Sync",
        cell: ({ row }) =>
          row.original.sync ? (
            <Link
              to="/workspaces/$workspace/pipelines/$pipeline/syncs/$sync"
              params={{ workspace, pipeline, sync: lastSegment(row.original.sync) }}
              className="font-mono text-[0.8125rem] hover:underline"
            >
              {lastSegment(row.original.sync).slice(-8)}
            </Link>
          ) : (
            <span className="text-muted-foreground">—</span>
          ),
      },
    ],
    [workspace, pipeline],
  );
  const table = useDataTable({ data: all, columns, paged: false });

  if (cps.error) return <ErrorNote title="Couldn't list checkpoints" message={errorMessage(cps.error)} />;
  if (cps.data && all.length === 0) {
    return (
      <Empty title="No checkpoints in the last 30 days">
        Each epoch a sync commits adds a checkpoint. The control plane keeps 30 days of them.
      </Empty>
    );
  }
  return (
    <div className="grid gap-3">
      <p className="text-muted-foreground">
        Each epoch a sync committed, newest first. A forced barrier was placed without a resume point. A stream read again after a
        restore from one shows the delivery level it got in that epoch, below the planned one, since its rows may repeat rows an
        earlier epoch committed. A complete stream finished its read in that epoch.
      </p>
      <DataTable table={table} loading={cps.isPending} />
      {cps.hasNextPage && (
        <Button variant="outline" size="sm" className="w-fit" disabled={cps.isFetchingNextPage} onClick={() => cps.fetchNextPage()}>
          {cps.isFetchingNextPage ? "Loading…" : "Load older checkpoints"}
        </Button>
      )}
    </div>
  );
}
