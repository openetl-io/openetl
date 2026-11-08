import { useMemo } from "react";
import { useQuery } from "@connectrpc/connect-query";
import { Link, getRouteApi } from "@tanstack/react-router";
import type { Timestamp } from "@bufbuild/protobuf/wkt";
import type { ColumnDef } from "@tanstack/react-table";
import { Server } from "lucide-react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { DataTable } from "@/components/data-table/data-table";
import { FacetedFilter } from "@/components/data-table/faceted-filter";
import { Pagination } from "@/components/data-table/pagination";
import { SortHeader } from "@/components/data-table/sort-header";
import { Toolbar } from "@/components/data-table/toolbar";
import { useDataTable } from "@/components/data-table/use-data-table";
import { EmptyState } from "@/components/empty-state";
import { PageHeader } from "@/components/page-header";
import { QueryError } from "@/components/query-error";
import { RelativeTime } from "@/components/relative-time";
import { UsedBy } from "@/components/resource-link";
import { FlinkClusterService } from "@/gen/openetl/api/v1/flink_pb";
import { SparkClusterService } from "@/gen/openetl/api/v1/spark_pb";
import { flinkTarget, sparkTarget, type ClusterKind, type ClusterTarget } from "@/lib/clusters";
import { dateOf } from "@/lib/format";
import { useListFilter } from "@/lib/list-filter";
import { lastSegment, shortDigest } from "@/lib/names";

const route = getRouteApi("/_app/workspaces/$workspace/clusters");

/** A cluster of either kind, as the list shows it. */
type Row = { id: string; target: ClusterTarget; version: string; usedBy: string[]; updateTime?: Timestamp };

const facets = { target: (r: Row) => r.target.label };

const empty = {
  spark: {
    title: "No Spark clusters",
    description: "A Spark cluster runs a pipeline's Spark transform on each epoch of a sync, or its Spark steps after one. Apply one, then name it in a pipeline's transform or postSync.",
    command: "openetl apply -f spark-cluster.yaml",
  },
  flink: {
    title: "No Flink clusters",
    description: "A Flink cluster runs a continuous pipeline's Flink job, between its source and destination. Apply one, then name it in the pipeline's transform.",
    command: "openetl apply -f flink-cluster.yaml",
  },
};

function ClusterList({ kind, rows, loading, error }: { kind: ClusterKind; rows: Row[]; loading: boolean; error: unknown }) {
  const { workspace } = route.useParams();
  const search = route.useSearch();
  const navigate = route.useNavigate();
  const list = useListFilter({ rows, search, facets, text: (r) => [r.id, r.target.where, ...r.usedBy.map(lastSegment)] });
  const setSearch = (s: Partial<typeof search>) => navigate({ search: (prev) => ({ ...prev, ...s }), replace: true });
  const reset = () => setSearch({ q: undefined, target: undefined });
  const kindLabel = kind === "spark" ? "Spark" : "Flink";

  const columns = useMemo<ColumnDef<Row>[]>(
    () => [
      {
        id: "name",
        accessorFn: (r) => r.id,
        header: ({ column }) => <SortHeader column={column} title="Name" />,
        cell: ({ row }) => (
          <div className="grid min-w-0">
            <Link
              to={kind === "spark" ? "/workspaces/$workspace/clusters/spark/$cluster" : "/workspaces/$workspace/clusters/flink/$cluster"}
              params={{ workspace, cluster: row.original.id }}
              className="truncate font-medium hover:underline"
            >
              {row.original.id}
            </Link>
            <span className="truncate font-mono text-[0.8125rem] text-muted-foreground md:hidden">{row.original.target.where}</span>
          </div>
        ),
      },
      {
        id: "target",
        accessorFn: (r) => r.target.label,
        header: ({ column }) => <SortHeader column={column} title="Runs on" />,
        meta: { className: "hidden sm:table-cell" },
      },
      {
        id: "where",
        header: kind === "spark" ? "Master or cluster" : "Where",
        cell: ({ row }) => (
          <span className="block max-w-80 truncate font-mono text-[0.8125rem] text-muted-foreground" title={row.original.target.where}>
            {row.original.target.where}
          </span>
        ),
        meta: { className: "hidden md:table-cell" },
      },
      {
        id: "usedBy",
        accessorFn: (r) => r.usedBy.length,
        header: ({ column }) => <SortHeader column={column} title="Used by" />,
        cell: ({ row }) => <UsedBy names={row.original.usedBy} />,
      },
      {
        id: "version",
        header: "Version",
        cell: ({ row }) => (
          <span className="font-mono text-[0.8125rem] text-muted-foreground" title={row.original.version}>
            {shortDigest(row.original.version)}
          </span>
        ),
        meta: { className: "hidden xl:table-cell" },
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

  if (error) return <QueryError title={`Couldn't list ${kindLabel} clusters`} error={error} />;
  if (!loading && rows.length === 0) return <EmptyState icon={Server} {...empty[kind]} title={`${empty[kind].title} in ${workspace} yet`} />;
  return (
    <div className="grid gap-4">
      <Toolbar search={search.q ?? ""} onSearch={(q) => setSearch({ q: q || undefined })} placeholder={`Search ${kindLabel} clusters`} filtered={list.filtered} onReset={reset}>
        <FacetedFilter
          title="Runs on"
          options={list.values("target").map((o) => ({ ...o, label: o.value }))}
          selected={search.target ?? []}
          onChange={(v) => setSearch({ target: v.length ? v : undefined })}
        />
      </Toolbar>
      <DataTable
        table={table}
        loading={loading}
        empty={`No ${kindLabel} clusters match.`}
        onRowClick={(r) =>
          navigate({
            to: kind === "spark" ? "/workspaces/$workspace/clusters/spark/$cluster" : "/workspaces/$workspace/clusters/flink/$cluster",
            params: { workspace, cluster: r.original.id },
          })
        }
      />
      <Pagination table={table} noun={[`${kindLabel} cluster`, `${kindLabel} clusters`]} />
    </div>
  );
}

export function ClustersPage() {
  const { workspace } = route.useParams();
  const { tab } = route.useSearch();
  const navigate = route.useNavigate();
  const parent = `workspaces/${workspace}`;
  const spark = useQuery(SparkClusterService.method.listSparkClusters, { parent, pageSize: 500 });
  const flink = useQuery(FlinkClusterService.method.listFlinkClusters, { parent, pageSize: 500 });
  const sparkRows = useMemo<Row[]>(
    () => (spark.data?.sparkClusters ?? []).map((c) => ({ id: lastSegment(c.name), target: sparkTarget(c.spec), version: c.version, usedBy: c.usedBy, updateTime: c.updateTime })),
    [spark.data],
  );
  const flinkRows = useMemo<Row[]>(
    () => (flink.data?.flinkClusters ?? []).map((c) => ({ id: lastSegment(c.name), target: flinkTarget(c.spec), version: c.version, usedBy: c.usedBy, updateTime: c.updateTime })),
    [flink.data],
  );
  const count = spark.data && flink.data ? sparkRows.length + flinkRows.length : undefined;
  const tabCount = (n: number | undefined) => n !== undefined && <span className="text-muted-foreground tabular">{n}</span>;

  return (
    <div className="grid gap-4">
      <PageHeader title="Clusters" count={count} />
      <Tabs value={tab ?? "spark"} onValueChange={(t) => navigate({ search: { tab: t === "flink" ? "flink" : undefined } })}>
        <TabsList variant="line">
          <TabsTrigger value="spark">Spark {tabCount(spark.data && sparkRows.length)}</TabsTrigger>
          <TabsTrigger value="flink">Flink {tabCount(flink.data && flinkRows.length)}</TabsTrigger>
        </TabsList>
        <TabsContent value="spark" className="pt-4">
          <ClusterList kind="spark" rows={sparkRows} loading={spark.isPending} error={spark.error} />
        </TabsContent>
        <TabsContent value="flink" className="pt-4">
          <ClusterList kind="flink" rows={flinkRows} loading={flink.isPending} error={flink.error} />
        </TabsContent>
      </Tabs>
    </div>
  );
}
