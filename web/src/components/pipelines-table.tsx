import { useMemo } from "react";
import { useQuery } from "@connectrpc/connect-query";
import { useNavigate } from "@tanstack/react-router";
import type { ColumnDef } from "@tanstack/react-table";
import { DataTable } from "@/components/data-table/data-table";
import { SortHeader } from "@/components/data-table/sort-header";
import { useDataTable } from "@/components/data-table/use-data-table";
import { ErrorNote } from "@/components/empty";
import { Freshness } from "@/components/pipeline-fields";
import { ResourceLink } from "@/components/resource-link";
import { StatusBadge } from "@/components/status";
import { PipelineService, type Pipeline } from "@/gen/openetl/api/v1/pipeline_pb";
import { errorMessage } from "@/lib/api";
import { lastSegment } from "@/lib/names";
import { pipelineStatus } from "@/lib/status";

/** The workspace's pipelines that match, with their status and freshness, and a column of the caller's after the status. */
export function PipelinesTable({
  workspace,
  match,
  column,
  empty,
}: {
  workspace: string;
  match: (p: Pipeline) => boolean;
  column?: ColumnDef<Pipeline>;
  empty: string;
}) {
  const navigate = useNavigate();
  const pipelines = useQuery(PipelineService.method.listPipelines, { parent: `workspaces/${workspace}`, pageSize: 500 });
  const rows = useMemo(() => (pipelines.data?.pipelines ?? []).filter(match), [pipelines.data, match]);
  const columns = useMemo<ColumnDef<Pipeline>[]>(
    () => [
      {
        id: "name",
        accessorFn: (p) => lastSegment(p.name),
        header: ({ column }) => <SortHeader column={column} title="Pipeline" />,
        cell: ({ row }) => <span className="font-medium"><ResourceLink name={row.original.name} /></span>,
      },
      {
        id: "status",
        header: "Status",
        cell: ({ row }) => <StatusBadge status={pipelineStatus(row.original)} />,
      },
      ...(column ? [column] : []),
      {
        id: "freshness",
        header: "Freshness",
        cell: ({ row }) => <Freshness pipeline={row.original} />,
        meta: { className: "hidden sm:table-cell" },
      },
    ],
    [column],
  );
  const table = useDataTable({ data: rows, columns, sort: [{ id: "name", desc: false }], paged: false });
  if (pipelines.error) return <ErrorNote title="Couldn't list pipelines" message={errorMessage(pipelines.error)} />;
  return (
    <DataTable
      table={table}
      loading={pipelines.isPending}
      empty={empty}
      onRowClick={(r) => navigate({ to: "/workspaces/$workspace/pipelines/$pipeline", params: { workspace, pipeline: lastSegment(r.original.name) }, search: {} })}
    />
  );
}
