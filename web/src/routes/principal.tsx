import { useMemo } from "react";
import { Code } from "@connectrpc/connect";
import { useQuery } from "@connectrpc/connect-query";
import { getRouteApi } from "@tanstack/react-router";
import type { ColumnDef } from "@tanstack/react-table";
import { Users } from "lucide-react";
import { PrincipalStatus, bindingLabel, typeLabel } from "@/components/access-fields";
import { CommandLine } from "@/components/command-line";
import { DataTable } from "@/components/data-table/data-table";
import { SortHeader } from "@/components/data-table/sort-header";
import { useDataTable } from "@/components/data-table/use-data-table";
import { DescriptionItem, DetailHeader, Section } from "@/components/detail-header";
import { ErrorNote } from "@/components/empty";
import { EmptyState } from "@/components/empty-state";
import { RelativeTime } from "@/components/relative-time";
import { AccessService, type Key } from "@/gen/openetl/api/v1/access_pb";
import { errorCode, errorMessage } from "@/lib/api";
import { dateOf } from "@/lib/format";

const route = getRouteApi("/_app/workspaces/$workspace/access/$principal");

function When({ t, never = "—" }: { t?: Date; never?: string }) {
  return t ? <RelativeTime date={t} /> : <span className="text-muted-foreground">{never}</span>;
}

function Keys({ principal }: { principal: string }) {
  const keys = useQuery(AccessService.method.listKeys, { parent: `principals/${principal}` });
  const rows = useMemo(() => keys.data?.keys ?? [], [keys.data]);
  const columns = useMemo<ColumnDef<Key>[]>(
    () => [
      {
        id: "prefix",
        accessorFn: (k) => k.prefix,
        header: "Key",
        cell: ({ getValue }) => <span className="font-mono text-[0.8125rem]">{getValue<string>()}…</span>,
      },
      {
        id: "created",
        accessorFn: (k) => dateOf(k.createTime)?.getTime() ?? 0,
        header: ({ column }) => <SortHeader column={column} title="Created" />,
        cell: ({ row }) => <When t={dateOf(row.original.createTime)} />,
      },
      {
        id: "used",
        accessorFn: (k) => dateOf(k.lastUsedTime)?.getTime() ?? 0,
        header: ({ column }) => <SortHeader column={column} title="Last used" />,
        cell: ({ row }) => <When t={dateOf(row.original.lastUsedTime)} never="Never" />,
      },
      {
        id: "expires",
        accessorFn: (k) => dateOf(k.expireTime)?.getTime() ?? Infinity,
        header: ({ column }) => <SortHeader column={column} title="Expires" />,
        cell: ({ row }) => <When t={dateOf(row.original.expireTime)} never="Never" />,
        meta: { className: "hidden sm:table-cell" },
      },
    ],
    [],
  );
  const table = useDataTable({ data: rows, columns, sort: [{ id: "created", desc: true }], paged: false });
  if (keys.error) return <ErrorNote title="Couldn't list API keys" message={errorMessage(keys.error)} />;
  return <DataTable table={table} loading={keys.isPending} empty="No API keys." />;
}

export function PrincipalPage() {
  const { principal } = route.useParams();
  const q = useQuery(AccessService.method.getPrincipal, { name: `principals/${principal}` });

  if (q.error) {
    return errorCode(q.error) === Code.NotFound ? (
      <EmptyState icon={Users} title={`There's no principal ${principal}`} />
    ) : (
      <ErrorNote title="Couldn't load the principal" message={errorMessage(q.error)} />
    );
  }
  const p = q.data?.principal;
  const created = dateOf(p?.createTime);

  return (
    <div className="grid gap-8">
      <DetailHeader title={p?.displayName || principal} status={p && <PrincipalStatus disabled={p.disabled} />}>
        {p && (
          <>
            <DescriptionItem label="ID">
              <span className="font-mono text-[0.8125rem]">{principal}</span>
            </DescriptionItem>
            <DescriptionItem label="Type">{typeLabel(p.type)}</DescriptionItem>
            {p.email && <DescriptionItem label="Email">{p.email}</DescriptionItem>}
            <DescriptionItem label="Roles">{p.roleBindings.map(bindingLabel).join(", ") || "None"}</DescriptionItem>
            {created && (
              <DescriptionItem label="Added">
                <RelativeTime date={created} />
              </DescriptionItem>
            )}
          </>
        )}
      </DetailHeader>
      <Section title="API keys">
        <div className="grid gap-3">
          <Keys principal={principal} />
          <CommandLine command={`openetl key create ${principal}`} className="max-w-md" />
        </div>
      </Section>
    </div>
  );
}
