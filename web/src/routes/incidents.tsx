import { useMemo } from "react";
import { useQuery } from "@connectrpc/connect-query";
import { Link, getRouteApi } from "@tanstack/react-router";
import type { ColumnDef } from "@tanstack/react-table";
import { Siren } from "lucide-react";
import { FacetedFilter } from "@/components/data-table/faceted-filter";
import { ListPage } from "@/components/data-table/list-page";
import { SortHeader } from "@/components/data-table/sort-header";
import { Toolbar } from "@/components/data-table/toolbar";
import { useDataTable } from "@/components/data-table/use-data-table";
import { EmptyState } from "@/components/empty-state";
import { IncidentStateBadge, Severity, SeverityIcon, incidentIds, kindLabel, severityLabel, stateLabel } from "@/components/incident-fields";
import { RelativeTime } from "@/components/relative-time";
import { IncidentService, IncidentSeverity, type Incident } from "@/gen/openetl/api/v1/incident_pb";
import { dateOf } from "@/lib/format";
import { useListFilter } from "@/lib/list-filter";

const route = getRouteApi("/_app/workspaces/$workspace/incidents");

const facets = {
  state: (i: Incident) => stateLabel(i.state),
  severity: (i: Incident) => severityLabel(i.severity),
  pipeline: (i: Incident) => incidentIds(i).pipeline,
  kind: (i: Incident) => kindLabel(i.class),
};

export function IncidentsPage() {
  const { workspace } = route.useParams();
  const search = route.useSearch();
  const navigate = route.useNavigate();
  const incidents = useQuery(IncidentService.method.listIncidents, { parent: `workspaces/${workspace}/pipelines/-`, pageSize: 500 }, { refetchInterval: 15_000 });

  const all = useMemo(() => incidents.data?.incidents ?? [], [incidents.data]);
  const list = useListFilter({ rows: all, search, facets, text: (i) => [i.summary, i.code, i.stream, incidentIds(i).pipeline] });
  const setSearch = (s: Partial<typeof search>) => navigate({ search: (prev) => ({ ...prev, ...s }), replace: true });
  const reset = () => setSearch({ q: undefined, state: undefined, severity: undefined, pipeline: undefined, kind: undefined });

  const columns = useMemo<ColumnDef<Incident>[]>(
    () => [
      {
        id: "summary",
        accessorFn: (i) => i.summary,
        header: ({ column }) => <SortHeader column={column} title="Incident" />,
        cell: ({ row }) => {
          const i = row.original;
          return (
            <div className="grid max-w-[36rem] min-w-0">
              <Link to="/workspaces/$workspace/incidents/$pipeline/$incident" params={{ workspace, ...incidentIds(i) }} className="truncate font-medium hover:underline">
                {i.summary}
              </Link>
              <span className="truncate text-[0.8125rem] text-muted-foreground">
                {incidentIds(i).pipeline}
                {i.stream && ` · ${i.stream}`}
              </span>
            </div>
          );
        },
      },
      {
        id: "severity",
        accessorFn: (i) => -i.severity,
        header: ({ column }) => <SortHeader column={column} title="Severity" />,
        cell: ({ row }) => <Severity severity={row.original.severity} />,
      },
      {
        id: "state",
        accessorFn: (i) => i.state,
        header: ({ column }) => <SortHeader column={column} title="State" />,
        cell: ({ row }) => <IncidentStateBadge state={row.original.state} />,
        meta: { className: "hidden sm:table-cell" },
      },
      {
        id: "kind",
        accessorFn: (i) => kindLabel(i.class),
        header: ({ column }) => <SortHeader column={column} title="Kind" />,
        cell: ({ row }) => (
          <span className="grid">
            <span>{kindLabel(row.original.class)}</span>
            {row.original.code && <span className="font-mono text-xs text-muted-foreground">{row.original.code}</span>}
          </span>
        ),
        meta: { className: "hidden lg:table-cell" },
      },
      {
        id: "occurrences",
        accessorFn: (i) => i.occurrences,
        header: ({ column }) => <SortHeader column={column} title="Seen" />,
        cell: ({ row }) => <span className="tabular-nums">{row.original.occurrences === 1 ? "Once" : `${row.original.occurrences} times`}</span>,
        meta: { className: "hidden xl:table-cell", align: "right" },
      },
      {
        id: "last",
        accessorFn: (i) => dateOf(i.lastSeenTime)?.getTime() ?? 0,
        header: ({ column }) => <SortHeader column={column} title="Last seen" />,
        cell: ({ row }) => {
          const t = dateOf(row.original.lastSeenTime);
          return t ? <RelativeTime date={t} /> : null;
        },
        meta: { className: "hidden md:table-cell" },
      },
    ],
    [workspace],
  );
  const table = useDataTable({
    data: list.visible,
    columns,
    sort: [
      { id: "state", desc: false },
      { id: "last", desc: true },
    ],
    resetKey: list.key,
  });

  return (
    <ListPage
      title="Incidents"
      count={incidents.data ? all.length : undefined}
      table={table}
      loading={incidents.isPending}
      error={incidents.error}
      noun={["incident", "incidents"]}
      onRowClick={(r) => navigate({ to: "/workspaces/$workspace/incidents/$pipeline/$incident", params: { workspace, ...incidentIds(r.original) } })}
      onReset={reset}
      empty={<EmptyState icon={Siren} title="No incidents" description="When a pipeline fails in a way heal recognizes, an incident opens here, with what heal did about it." />}
      toolbar={
        <Toolbar search={search.q ?? ""} onSearch={(q) => setSearch({ q: q || undefined })} placeholder="Search incidents" filtered={list.filtered} onReset={reset}>
          <FacetedFilter
            title="State"
            options={list.values("state", ["Open", "Resolved"]).map((o) => ({ ...o, label: o.value }))}
            selected={search.state ?? []}
            onChange={(v) => setSearch({ state: v.length ? v : undefined })}
          />
          <FacetedFilter
            title="Severity"
            options={list.values("severity", ["Critical", "Warning"]).map((o) => ({
              ...o,
              label: o.value,
              icon: <SeverityIcon severity={o.value === "Critical" ? IncidentSeverity.CRITICAL : IncidentSeverity.WARNING} />,
            }))}
            selected={search.severity ?? []}
            onChange={(v) => setSearch({ severity: v.length ? v : undefined })}
          />
          <FacetedFilter
            title="Pipeline"
            options={list.values("pipeline").map((o) => ({ ...o, label: o.value }))}
            selected={search.pipeline ?? []}
            onChange={(v) => setSearch({ pipeline: v.length ? v : undefined })}
          />
          <FacetedFilter
            title="Kind"
            options={list.values("kind").map((o) => ({ ...o, label: o.value }))}
            selected={search.kind ?? []}
            onChange={(v) => setSearch({ kind: v.length ? v : undefined })}
          />
        </Toolbar>
      }
    />
  );
}
