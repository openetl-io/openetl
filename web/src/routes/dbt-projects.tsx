import { useMemo } from "react";
import { useQuery } from "@connectrpc/connect-query";
import { getRouteApi } from "@tanstack/react-router";
import type { ColumnDef } from "@tanstack/react-table";
import { Boxes } from "lucide-react";
import { ListPage } from "@/components/data-table/list-page";
import { SortHeader } from "@/components/data-table/sort-header";
import { Toolbar } from "@/components/data-table/toolbar";
import { useDataTable } from "@/components/data-table/use-data-table";
import { EmptyState } from "@/components/empty-state";
import { RelativeTime } from "@/components/relative-time";
import { UsedBy } from "@/components/resource-link";
import { DbtProjectService, type DbtProject } from "@/gen/openetl/api/v1/dbt_pb";
import { dateOf } from "@/lib/format";
import { useListFilter } from "@/lib/list-filter";
import { lastSegment, shortDigest } from "@/lib/names";

const route = getRouteApi("/_app/workspaces/$workspace/dbt-projects");

export function DbtProjectsPage() {
  const { workspace } = route.useParams();
  const search = route.useSearch();
  const navigate = route.useNavigate();
  const projects = useQuery(DbtProjectService.method.listDbtProjects, { parent: `workspaces/${workspace}`, pageSize: 500 });
  const all = useMemo(() => projects.data?.dbtProjects ?? [], [projects.data]);
  const list = useListFilter({ rows: all, search, facets: {}, text: (p) => [lastSegment(p.name), ...p.usedBy.map(lastSegment)] });

  const columns = useMemo<ColumnDef<DbtProject>[]>(
    () => [
      {
        id: "name",
        accessorFn: (p) => lastSegment(p.name),
        header: ({ column }) => <SortHeader column={column} title="Name" />,
        cell: ({ getValue }) => <span className="font-medium">{getValue<string>()}</span>,
      },
      {
        id: "digest",
        header: "Bundle",
        cell: ({ row }) => (
          <span className="font-mono text-[0.8125rem] text-muted-foreground" title={row.original.digest}>
            {shortDigest(row.original.digest)}
          </span>
        ),
        meta: { className: "hidden md:table-cell" },
      },
      {
        id: "usedBy",
        accessorFn: (p) => p.usedBy.length,
        header: ({ column }) => <SortHeader column={column} title="Run after" />,
        cell: ({ row }) => <UsedBy names={row.original.usedBy} />,
      },
      {
        id: "updated",
        accessorFn: (p) => dateOf(p.updateTime)?.getTime() ?? 0,
        header: ({ column }) => <SortHeader column={column} title="Updated" />,
        cell: ({ row }) => {
          const t = dateOf(row.original.updateTime);
          return t ? <RelativeTime date={t} /> : null;
        },
        meta: { className: "hidden sm:table-cell" },
      },
    ],
    [],
  );
  const table = useDataTable({ data: list.visible, columns, sort: [{ id: "name", desc: false }], resetKey: list.key });

  return (
    <ListPage
      title="dbt projects"
      count={projects.data ? all.length : undefined}
      table={table}
      loading={projects.isPending}
      error={projects.error}
      noun={["dbt project", "dbt projects"]}
      onReset={() => navigate({ search: {}, replace: true })}
      empty={
        <EmptyState
          icon={Boxes}
          title={`No dbt projects in ${workspace} yet`}
          description="A dbt project runs after a pipeline's sync commits, against its destination. Apply one, then name it in a pipeline's postSync."
          command="openetl apply -f dbt-project.yaml"
        />
      }
      toolbar={
        <Toolbar
          search={search.q ?? ""}
          onSearch={(q) => navigate({ search: { q: q || undefined }, replace: true })}
          placeholder="Search dbt projects"
          filtered={list.filtered}
          onReset={() => navigate({ search: {}, replace: true })}
        />
      }
    />
  );
}
