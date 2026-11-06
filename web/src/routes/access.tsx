import { useMemo } from "react";
import { useQuery } from "@connectrpc/connect-query";
import { Link, getRouteApi } from "@tanstack/react-router";
import type { ColumnDef } from "@tanstack/react-table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { PrincipalStatus, bindingLabel, roleLabel, topRole, typeLabel } from "@/components/access-fields";
import { DataTable } from "@/components/data-table/data-table";
import { FacetedFilter } from "@/components/data-table/faceted-filter";
import { Pagination } from "@/components/data-table/pagination";
import { SortHeader } from "@/components/data-table/sort-header";
import { Toolbar } from "@/components/data-table/toolbar";
import { useDataTable } from "@/components/data-table/use-data-table";
import { Section } from "@/components/detail-header";
import { QueryError } from "@/components/query-error";
import { PageHeader } from "@/components/page-header";
import { RelativeTime } from "@/components/relative-time";
import { AccessService, Role, type Principal, type RunnerToken } from "@/gen/openetl/api/v1/access_pb";
import { dateOf } from "@/lib/format";
import { useListFilter } from "@/lib/list-filter";
import { lastSegment } from "@/lib/names";

const route = getRouteApi("/_app/workspaces/$workspace/access");

const facets = {
  type: (p: Principal) => typeLabel(p.type),
  role: (p: Principal) => roleLabel(topRole(p)),
};
const roleOrder = [Role.OWNER, Role.ADMIN, Role.EDITOR, Role.OPERATOR, Role.VIEWER].map(roleLabel);

function When({ t, never = "—" }: { t?: Date; never?: string }) {
  return t ? <RelativeTime date={t} /> : <span className="text-muted-foreground">{never}</span>;
}

function Principals() {
  const { workspace } = route.useParams();
  const search = route.useSearch();
  const navigate = route.useNavigate();
  const principals = useQuery(AccessService.method.listPrincipals, { pageSize: 500 });
  const all = useMemo(() => principals.data?.principals ?? [], [principals.data]);
  const list = useListFilter({ rows: all, search, facets, text: (p) => [p.displayName, p.email, lastSegment(p.name)] });
  const setSearch = (s: Partial<typeof search>) => navigate({ search: (prev) => ({ ...prev, ...s }), replace: true });
  const reset = () => setSearch({ q: undefined, type: undefined, role: undefined });

  const columns = useMemo<ColumnDef<Principal>[]>(
    () => [
      {
        id: "name",
        accessorFn: (p) => p.displayName || lastSegment(p.name),
        header: ({ column }) => <SortHeader column={column} title="Name" />,
        cell: ({ row, getValue }) => (
          <div className="grid min-w-0">
            <Link to="/workspaces/$workspace/access/$principal" params={{ workspace, principal: lastSegment(row.original.name) }} className="truncate font-medium hover:underline">
              {getValue<string>()}
            </Link>
            <span className="truncate text-[0.8125rem] text-muted-foreground">{row.original.email || lastSegment(row.original.name)}</span>
          </div>
        ),
      },
      {
        id: "type",
        accessorFn: (p) => typeLabel(p.type),
        header: ({ column }) => <SortHeader column={column} title="Type" />,
        meta: { className: "hidden sm:table-cell" },
      },
      {
        id: "role",
        accessorFn: topRole,
        header: ({ column }) => <SortHeader column={column} title="Role" />,
        cell: ({ row }) => row.original.roleBindings.map(bindingLabel).join(", ") || <span className="text-muted-foreground">None</span>,
      },
      {
        id: "status",
        accessorFn: (p) => p.disabled,
        header: "Status",
        cell: ({ row }) => <PrincipalStatus disabled={row.original.disabled} />,
        meta: { className: "hidden md:table-cell" },
      },
      {
        id: "created",
        accessorFn: (p) => dateOf(p.createTime)?.getTime() ?? 0,
        header: ({ column }) => <SortHeader column={column} title="Added" />,
        cell: ({ row }) => <When t={dateOf(row.original.createTime)} />,
        meta: { className: "hidden lg:table-cell" },
      },
    ],
    [workspace],
  );
  const table = useDataTable({ data: list.visible, columns, sort: [{ id: "role", desc: true }], resetKey: list.key });

  return (
    <div className="grid gap-4">
      <Toolbar search={search.q ?? ""} onSearch={(q) => setSearch({ q: q || undefined })} placeholder="Search principals" filtered={list.filtered} onReset={reset}>
        <FacetedFilter
          title="Type"
          options={list.values("type").map((o) => ({ ...o, label: o.value }))}
          selected={search.type ?? []}
          onChange={(v) => setSearch({ type: v.length ? v : undefined })}
        />
        <FacetedFilter
          title="Role"
          options={list.values("role", roleOrder).map((o) => ({ ...o, label: o.value }))}
          selected={search.role ?? []}
          onChange={(v) => setSearch({ role: v.length ? v : undefined })}
        />
      </Toolbar>
      {principals.error ? (
        <QueryError title="Couldn't list principals" error={principals.error} />
      ) : (
        <>
          <DataTable
            table={table}
            loading={principals.isPending}
            empty="No principals match."
            onRowClick={(r) => navigate({ to: "/workspaces/$workspace/access/$principal", params: { workspace, principal: lastSegment(r.original.name) } })}
          />
          <Pagination table={table} noun={["principal", "principals"]} />
        </>
      )}
    </div>
  );
}

function PlaneTokens({ plane }: { plane: string }) {
  const tokens = useQuery(AccessService.method.listRunnerTokens, { parent: plane });
  const rows = useMemo(() => tokens.data?.tokens ?? [], [tokens.data]);
  const columns = useMemo<ColumnDef<RunnerToken>[]>(
    () => [
      {
        id: "prefix",
        accessorFn: (t) => t.prefix,
        header: "Token",
        cell: ({ getValue }) => <span className="font-mono text-[0.8125rem]">{getValue<string>()}…</span>,
      },
      {
        id: "created",
        accessorFn: (t) => dateOf(t.createTime)?.getTime() ?? 0,
        header: ({ column }) => <SortHeader column={column} title="Created" />,
        cell: ({ row }) => <When t={dateOf(row.original.createTime)} />,
      },
      {
        id: "used",
        accessorFn: (t) => dateOf(t.lastUsedTime)?.getTime() ?? 0,
        header: ({ column }) => <SortHeader column={column} title="Last used" />,
        cell: ({ row }) => <When t={dateOf(row.original.lastUsedTime)} never="Never" />,
      },
    ],
    [],
  );
  const table = useDataTable({ data: rows, columns, sort: [{ id: "created", desc: true }], paged: false });
  return (
    <Section title={`Data plane ${lastSegment(plane)}`}>
      {tokens.error ? (
        <QueryError title="Couldn't list runner tokens" error={tokens.error} />
      ) : (
        <DataTable table={table} loading={tokens.isPending} empty={`No runner tokens. Runners join with one: openetl dataplane token create ${lastSegment(plane)}`} />
      )}
    </Section>
  );
}

function RunnerTokens() {
  const planes = useQuery(AccessService.method.listDataPlanes, {});
  if (planes.error) return <QueryError title="Couldn't list data planes" error={planes.error} />;
  return <div className="grid gap-8">{planes.data?.dataPlanes.map((p) => <PlaneTokens key={p.name} plane={p.name} />)}</div>;
}

export function AccessPage() {
  const { tab } = route.useSearch();
  const navigate = route.useNavigate();
  return (
    <div className="grid gap-4">
      <PageHeader title="Access" />
      <Tabs value={tab ?? "principals"} onValueChange={(t) => navigate({ search: { tab: t === "tokens" ? "tokens" : undefined } })}>
        <TabsList variant="line">
          <TabsTrigger value="principals">Principals</TabsTrigger>
          <TabsTrigger value="tokens">Runner tokens</TabsTrigger>
        </TabsList>
        <TabsContent value="principals" className="pt-4">
          <Principals />
        </TabsContent>
        <TabsContent value="tokens" className="pt-4">
          <RunnerTokens />
        </TabsContent>
      </Tabs>
    </div>
  );
}
