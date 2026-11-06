import { useMemo } from "react";
import { useQuery } from "@connectrpc/connect-query";
import { getRouteApi } from "@tanstack/react-router";
import type { ColumnDef } from "@tanstack/react-table";
import { KeyRound } from "lucide-react";
import { ListPage } from "@/components/data-table/list-page";
import { SortHeader } from "@/components/data-table/sort-header";
import { Toolbar } from "@/components/data-table/toolbar";
import { useDataTable } from "@/components/data-table/use-data-table";
import { EmptyState } from "@/components/empty-state";
import { RelativeTime } from "@/components/relative-time";
import { UsedBy } from "@/components/resource-link";
import { SecretService, type Secret } from "@/gen/openetl/api/v1/secret_pb";
import { dateOf } from "@/lib/format";
import { useListFilter } from "@/lib/list-filter";
import { lastSegment } from "@/lib/names";

const route = getRouteApi("/_app/workspaces/$workspace/secrets");

export function SecretsPage() {
  const { workspace } = route.useParams();
  const search = route.useSearch();
  const navigate = route.useNavigate();
  const secrets = useQuery(SecretService.method.listSecrets, { parent: `workspaces/${workspace}`, pageSize: 500 });
  const all = useMemo(() => secrets.data?.secrets ?? [], [secrets.data]);
  const list = useListFilter({ rows: all, search, facets: {}, text: (s) => [lastSegment(s.name), s.ref, ...s.usedBy.map(lastSegment)] });

  const columns = useMemo<ColumnDef<Secret>[]>(
    () => [
      {
        id: "name",
        accessorFn: (s) => lastSegment(s.name),
        header: ({ column }) => <SortHeader column={column} title="Name" />,
        cell: ({ getValue }) => <span className="font-medium">{getValue<string>()}</span>,
      },
      {
        id: "value",
        header: "Value",
        cell: ({ row }) =>
          row.original.ref ? (
            <span className="font-mono text-[0.8125rem]">{row.original.ref}</span>
          ) : (
            <span className="text-muted-foreground">Sealed here</span>
          ),
        meta: { className: "hidden lg:table-cell" },
      },
      {
        id: "usedBy",
        accessorFn: (s) => s.usedBy.length,
        header: ({ column }) => <SortHeader column={column} title="Used by" />,
        cell: ({ row }) => <UsedBy names={row.original.usedBy} />,
      },
      {
        id: "updated",
        accessorFn: (s) => dateOf(s.updateTime)?.getTime() ?? 0,
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
      title="Secrets"
      count={secrets.data ? all.length : undefined}
      table={table}
      loading={secrets.isPending}
      error={secrets.error}
      noun={["secret", "secrets"]}
      onReset={() => navigate({ search: {}, replace: true })}
      empty={
        <EmptyState
          icon={KeyRound}
          title={`No secrets in ${workspace} yet`}
          description="Configs name secrets by reference, never by value, and no page shows a value."
          command="openetl secret set --from-file FILE NAME"
        />
      }
      toolbar={
        <Toolbar
          search={search.q ?? ""}
          onSearch={(q) => navigate({ search: { q: q || undefined }, replace: true })}
          placeholder="Search secrets"
          filtered={list.filtered}
          onReset={() => navigate({ search: {}, replace: true })}
        />
      }
    />
  );
}
