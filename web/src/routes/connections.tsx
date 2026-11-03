import { useMemo } from "react";
import { useQuery } from "@connectrpc/connect-query";
import { Link, getRouteApi, useNavigate } from "@tanstack/react-router";
import type { ColumnDef } from "@tanstack/react-table";
import type { Timestamp } from "@bufbuild/protobuf/wkt";
import { Database, Warehouse } from "lucide-react";
import { ConnectorLogo } from "@/components/connector";
import { FacetedFilter } from "@/components/data-table/faceted-filter";
import { ListPage } from "@/components/data-table/list-page";
import { SortHeader } from "@/components/data-table/sort-header";
import { Toolbar } from "@/components/data-table/toolbar";
import { useDataTable } from "@/components/data-table/use-data-table";
import { EmptyState } from "@/components/empty-state";
import { RelativeTime } from "@/components/relative-time";
import { type ConnectorVersion, DestinationService, SourceService } from "@/gen/openetl/api/v1/connection_pb";
import { PipelineService } from "@/gen/openetl/api/v1/pipeline_pb";
import { dateOf } from "@/lib/format";
import { connectorName } from "@/lib/connectors";
import { useListFilter } from "@/lib/list-filter";
import { lastSegment } from "@/lib/names";

export type Kind = "source" | "destination";

type Row = { id: string; connector: string; version?: ConnectorVersion; updateTime?: Timestamp; usedBy: number };

const routes = {
  source: getRouteApi("/_app/workspaces/$workspace/sources"),
  destination: getRouteApi("/_app/workspaces/$workspace/destinations"),
};

const facets = { connector: (r: Row) => r.connector };

function ConnectionsList({ kind }: { kind: Kind }) {
  const route = routes[kind];
  const { workspace } = route.useParams();
  const search = route.useSearch();
  const navigate = useNavigate();
  const parent = `workspaces/${workspace}`;
  const sources = useQuery(SourceService.method.listSources, { parent, pageSize: 500 }, { enabled: kind === "source" });
  const destinations = useQuery(DestinationService.method.listDestinations, { parent, pageSize: 500 }, { enabled: kind === "destination" });
  const pipelines = useQuery(PipelineService.method.listPipelines, { parent, pageSize: 500 });
  const q = kind === "source" ? sources : destinations;

  const all = useMemo<Row[]>(() => {
    const uses = new Map<string, number>();
    for (const p of pipelines.data?.pipelines ?? []) {
      const ref = kind === "source" ? p.spec?.source?.ref : p.spec?.destination?.ref;
      if (ref) uses.set(ref, (uses.get(ref) ?? 0) + 1);
    }
    const items = kind === "source" ? (sources.data?.sources ?? []) : (destinations.data?.destinations ?? []);
    return items.map((x) => ({
      id: lastSegment(x.name),
      connector: connectorName(x.spec?.connector),
      version: x.connector,
      updateTime: x.updateTime,
      usedBy: uses.get(lastSegment(x.name)) ?? 0,
    }));
  }, [kind, sources.data, destinations.data, pipelines.data]);

  const list = useListFilter({ rows: all, search, facets, text: (r) => [r.id, r.connector] });
  const setSearch = (s: Partial<typeof search>) =>
    navigate({ to: ".", search: (prev: Record<string, unknown>) => ({ ...prev, ...s }), replace: true });
  const reset = () => setSearch({ q: undefined, connector: undefined });
  const open = (id: string) =>
    kind === "source"
      ? navigate({ to: "/workspaces/$workspace/sources/$source", params: { workspace, source: id } })
      : navigate({ to: "/workspaces/$workspace/destinations/$destination", params: { workspace, destination: id } });

  const columns = useMemo<ColumnDef<Row>[]>(
    () => [
      {
        id: "name",
        accessorFn: (r) => r.id,
        header: ({ column }) => <SortHeader column={column} title="Name" />,
        cell: ({ row }) => (
          <span className="inline-flex min-w-0 items-center gap-2">
            <ConnectorLogo connector={row.original.connector} version={row.original.version} />
            {kind === "source" ? (
              <Link to="/workspaces/$workspace/sources/$source" params={{ workspace, source: row.original.id }} className="truncate font-medium hover:underline">
                {row.original.id}
              </Link>
            ) : (
              <Link to="/workspaces/$workspace/destinations/$destination" params={{ workspace, destination: row.original.id }} className="truncate font-medium hover:underline">
                {row.original.id}
              </Link>
            )}
          </span>
        ),
      },
      {
        id: "connector",
        accessorFn: (r) => r.connector,
        header: ({ column }) => <SortHeader column={column} title="Connector" />,
        cell: ({ row }) => row.original.connector,
      },
      {
        id: "version",
        accessorFn: (r) => r.version?.version ?? "",
        header: "Version",
        cell: ({ row }) =>
          row.original.version?.version ? (
            <span className="font-mono text-[0.8125rem]">{row.original.version.version}</span>
          ) : (
            <span className="text-muted-foreground">Built in</span>
          ),
        meta: { className: "hidden sm:table-cell" },
      },
      {
        id: "usedBy",
        accessorFn: (r) => r.usedBy,
        header: ({ column }) => <SortHeader column={column} title="Pipelines" />,
        cell: ({ row }) => <span className="tabular-nums">{row.original.usedBy || <span className="text-muted-foreground">None</span>}</span>,
        meta: { className: "hidden md:table-cell", align: "right" },
      },
      {
        id: "updated",
        accessorFn: (r) => dateOf(r.updateTime)?.getTime() ?? 0,
        header: ({ column }) => <SortHeader column={column} title="Updated" />,
        cell: ({ row }) => {
          const t = dateOf(row.original.updateTime);
          return t ? <RelativeTime date={t} /> : null;
        },
        meta: { className: "hidden lg:table-cell" },
      },
    ],
    [kind, workspace],
  );
  const table = useDataTable({ data: list.visible, columns, sort: [{ id: "name", desc: false }], resetKey: list.key });
  const [one, many] = kind === "source" ? ["source", "sources"] : ["destination", "destinations"];

  return (
    <ListPage
      title={kind === "source" ? "Sources" : "Destinations"}
      count={q.data ? all.length : undefined}
      table={table}
      loading={q.isPending}
      error={q.error}
      noun={[one!, many!]}
      onRowClick={(r) => open(r.original.id)}
      onReset={reset}
      empty={
        <EmptyState
          icon={kind === "source" ? Database : Warehouse}
          title={`No ${many} in ${workspace} yet`}
          description={`A ${one} is a YAML document that the CLI applies, and pipelines name it.`}
          command={`openetl apply -f ${one}.yaml`}
        />
      }
      toolbar={
        <Toolbar search={search.q ?? ""} onSearch={(v) => setSearch({ q: v || undefined })} placeholder={`Search ${many}`} filtered={list.filtered} onReset={reset}>
          <FacetedFilter
            title="Connector"
            options={list.values("connector").map((o) => ({ ...o, label: o.value, icon: <ConnectorLogo connector={o.value} /> }))}
            selected={search.connector ?? []}
            onChange={(v) => setSearch({ connector: v.length ? v : undefined })}
          />
        </Toolbar>
      }
    />
  );
}

export const SourcesPage = () => <ConnectionsList kind="source" />;
export const DestinationsPage = () => <ConnectionsList kind="destination" />;
