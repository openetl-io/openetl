import { useMemo } from "react";
import { useQuery } from "@connectrpc/connect-query";
import { Link, getRouteApi } from "@tanstack/react-router";
import type { ColumnDef } from "@tanstack/react-table";
import { Workflow } from "lucide-react";
import { ConnectorLogo, ConnectorRef } from "@/components/connector";
import { FacetedFilter } from "@/components/data-table/faceted-filter";
import { ListPage } from "@/components/data-table/list-page";
import { SortHeader } from "@/components/data-table/sort-header";
import { Toolbar } from "@/components/data-table/toolbar";
import { useDataTable } from "@/components/data-table/use-data-table";
import { EmptyState } from "@/components/empty-state";
import { NewPipelineButton } from "@/components/new-pipeline";
import { FailedAttempts, Freshness, IncidentCount, Schedule } from "@/components/pipeline-fields";
import { StatusBadge, StatusIcon } from "@/components/status";
import { useConnectors } from "@/lib/connectors";
import { PipelineService, type Pipeline } from "@/gen/openetl/api/v1/pipeline_pb";
import { compact } from "@/lib/format";
import { useListFilter } from "@/lib/list-filter";
import { lastSegment } from "@/lib/names";
import { lastUpdate } from "@/lib/pipeline";
import { pipelineStatus, rows, statusLabel, type Status } from "@/lib/status";

const route = getRouteApi("/_app/workspaces/$workspace/pipelines");

/** Most urgent first; the default sort. */
const statusOrder: Status[] = ["failed", "degraded", "running", "stopped", "healthy", "idle", "paused"];

const facets = {
  status: (p: Pipeline) => pipelineStatus(p),
  source: (p: Pipeline) => p.spec?.source?.ref ?? "",
  destination: (p: Pipeline) => p.spec?.destination?.ref ?? "",
};

export function PipelinesPage() {
  const { workspace } = route.useParams();
  const search = route.useSearch();
  const navigate = route.useNavigate();
  const pipelines = useQuery(PipelineService.method.listPipelines, { parent: `workspaces/${workspace}`, pageSize: 500 }, { refetchInterval: 10_000 });
  const connectors = useConnectors(workspace);

  const all = useMemo(() => pipelines.data?.pipelines ?? [], [pipelines.data]);
  const list = useListFilter({ rows: all, search, facets, text: (p) => [lastSegment(p.name), facets.source(p), facets.destination(p)] });
  const setSearch = (s: Partial<typeof search>) => navigate({ search: (prev) => ({ ...prev, ...s }), replace: true });

  const columns = useMemo<ColumnDef<Pipeline>[]>(
    () => [
      {
        id: "name",
        accessorFn: (p) => lastSegment(p.name),
        header: ({ column }) => <SortHeader column={column} title="Pipeline" />,
        cell: ({ row }) => {
          const p = row.original;
          return (
            <div className="grid min-w-0">
              <Link
                to="/workspaces/$workspace/pipelines/$pipeline"
                params={{ workspace, pipeline: lastSegment(p.name) }}
                search={{}}
                className="truncate font-medium hover:underline"
              >
                {lastSegment(p.name)}
              </Link>
              <span className="truncate text-[0.8125rem] text-muted-foreground md:hidden">
                {p.spec?.source?.ref} → {p.spec?.destination?.ref}
              </span>
            </div>
          );
        },
      },
      {
        id: "status",
        accessorFn: (p) => statusOrder.indexOf(pipelineStatus(p)),
        header: ({ column }) => <SortHeader column={column} title="Status" />,
        cell: ({ row }) => (
          <span className="flex items-center gap-2">
            <StatusBadge status={pipelineStatus(row.original)} />
            <FailedAttempts pipeline={row.original} className="hidden sm:inline-flex" />
            <IncidentCount count={row.original.status?.openIncidents ?? 0} className="hidden sm:inline-flex" />
          </span>
        ),
      },
      {
        id: "source",
        accessorFn: facets.source,
        header: ({ column }) => <SortHeader column={column} title="Source" />,
        cell: ({ getValue }) => <ConnectorRef name={getValue<string>()} {...connectors.source(getValue<string>())} />,
        meta: { className: "hidden md:table-cell max-w-56" },
      },
      {
        id: "destination",
        accessorFn: facets.destination,
        header: ({ column }) => <SortHeader column={column} title="Destination" />,
        cell: ({ getValue }) => <ConnectorRef name={getValue<string>()} {...connectors.destination(getValue<string>())} />,
        meta: { className: "hidden md:table-cell max-w-56" },
      },
      {
        id: "schedule",
        header: "Schedule",
        cell: ({ row }) => <Schedule pipeline={row.original} className="text-muted-foreground" />,
        meta: { className: "hidden lg:table-cell" },
      },
      {
        id: "freshness",
        accessorFn: (p) => lastUpdate(p)?.getTime() ?? 0,
        header: ({ column }) => <SortHeader column={column} title="Freshness" />,
        cell: ({ row }) => <Freshness pipeline={row.original} />,
        meta: { className: "hidden sm:table-cell" },
      },
      {
        id: "rows",
        accessorFn: (p) => (p.status?.lastSync ? Number(rows(p.status.lastSync).all) : -1),
        header: ({ column }) => <SortHeader column={column} title="Last sync" />,
        cell: ({ row }) => {
          const last = row.original.status?.lastSync;
          return <span className="tabular-nums">{last ? `${compact(rows(last).all)} rows` : "—"}</span>;
        },
        meta: { className: "hidden xl:table-cell", align: "right" },
      },
    ],
    [workspace, connectors],
  );
  const table = useDataTable({ data: list.visible, columns, sort: [{ id: "status", desc: false }], resetKey: list.key });

  return (
    <ListPage
      title="Pipelines"
      count={pipelines.data ? all.length : undefined}
      actions={<NewPipelineButton />}
      table={table}
      loading={pipelines.isPending}
      error={pipelines.error}
      noun={["pipeline", "pipelines"]}
      onRowClick={(r) => navigate({ to: "/workspaces/$workspace/pipelines/$pipeline", params: { workspace, pipeline: lastSegment(r.original.name) }, search: {} })}
      onReset={() => setSearch({ q: undefined, status: undefined, source: undefined, destination: undefined })}
      empty={
        <EmptyState
          icon={Workflow}
          title={`No pipelines in ${workspace} yet`}
          description="A pipeline is a YAML document that the CLI applies. It shows here once it exists."
          command="openetl apply -f pipeline.yaml"
        />
      }
      toolbar={
        <Toolbar
          search={search.q ?? ""}
          onSearch={(q) => setSearch({ q: q || undefined })}
          placeholder="Search pipelines"
          filtered={list.filtered}
          onReset={() => setSearch({ q: undefined, status: undefined, source: undefined, destination: undefined })}
        >
          <FacetedFilter
            title="Status"
            options={list.values("status", statusOrder).map((o) => ({
              ...o,
              label: statusLabel[o.value as Status],
              icon: <StatusIcon status={o.value as Status} className="animate-none" />,
            }))}
            selected={search.status ?? []}
            onChange={(v) => setSearch({ status: v.length ? v : undefined })}
          />
          <FacetedFilter
            title="Source"
            options={list.values("source").map((o) => ({ ...o, label: o.value, icon: <ConnectorLogo {...connectors.source(o.value)} /> }))}
            selected={search.source ?? []}
            onChange={(v) => setSearch({ source: v.length ? v : undefined })}
          />
          <FacetedFilter
            title="Destination"
            options={list.values("destination").map((o) => ({ ...o, label: o.value, icon: <ConnectorLogo {...connectors.destination(o.value)} /> }))}
            selected={search.destination ?? []}
            onChange={(v) => setSearch({ destination: v.length ? v : undefined })}
          />
        </Toolbar>
      }
    />
  );
}
