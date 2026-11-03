import { useMemo } from "react";
import { useQuery } from "@connectrpc/connect-query";
import { Link, getRouteApi } from "@tanstack/react-router";
import type { ColumnDef } from "@tanstack/react-table";
import { Webhook as WebhookIcon } from "lucide-react";
import { FacetedFilter } from "@/components/data-table/faceted-filter";
import { ListPage } from "@/components/data-table/list-page";
import { SortHeader } from "@/components/data-table/sort-header";
import { Toolbar } from "@/components/data-table/toolbar";
import { useDataTable } from "@/components/data-table/use-data-table";
import { EmptyState } from "@/components/empty-state";
import { RelativeTime } from "@/components/relative-time";
import { Events, WebhookStateBadge, webhookState } from "@/components/webhook-fields";
import { WebhookService, type Webhook } from "@/gen/openetl/api/v1/webhook_pb";
import { dateOf } from "@/lib/format";
import { useListFilter } from "@/lib/list-filter";
import { lastSegment } from "@/lib/names";

const route = getRouteApi("/_app/workspaces/$workspace/webhooks");

const facets = { state: webhookState };

export function WebhooksPage() {
  const { workspace } = route.useParams();
  const search = route.useSearch();
  const navigate = route.useNavigate();
  const webhooks = useQuery(WebhookService.method.listWebhooks, { parent: `workspaces/${workspace}`, pageSize: 500 }, { refetchInterval: 30_000 });
  const all = useMemo(() => webhooks.data?.webhooks ?? [], [webhooks.data]);
  const list = useListFilter({ rows: all, search, facets, text: (w) => [lastSegment(w.name), w.spec?.url ?? "", ...(w.spec?.events ?? [])] });
  const reset = () => navigate({ search: {}, replace: true });

  const columns = useMemo<ColumnDef<Webhook>[]>(
    () => [
      {
        id: "name",
        accessorFn: (w) => lastSegment(w.name),
        header: ({ column }) => <SortHeader column={column} title="Name" />,
        cell: ({ getValue }) => (
          <Link to="/workspaces/$workspace/webhooks/$webhook" params={{ workspace, webhook: getValue<string>() }} className="font-medium hover:underline">
            {getValue<string>()}
          </Link>
        ),
      },
      {
        id: "state",
        accessorFn: webhookState,
        header: ({ column }) => <SortHeader column={column} title="State" />,
        cell: ({ row }) => <WebhookStateBadge webhook={row.original} />,
      },
      {
        id: "url",
        header: "URL",
        cell: ({ row }) => <span className="block max-w-80 truncate font-mono text-[0.8125rem] text-muted-foreground">{row.original.spec?.url}</span>,
        meta: { className: "hidden lg:table-cell" },
      },
      {
        id: "events",
        header: "Events",
        cell: ({ row }) => <Events events={row.original.spec?.events ?? []} />,
        meta: { className: "hidden md:table-cell" },
      },
      {
        id: "updated",
        accessorFn: (w) => dateOf(w.updateTime)?.getTime() ?? 0,
        header: ({ column }) => <SortHeader column={column} title="Updated" />,
        cell: ({ row }) => {
          const t = dateOf(row.original.updateTime);
          return t ? <RelativeTime date={t} /> : null;
        },
        meta: { className: "hidden sm:table-cell" },
      },
    ],
    [workspace],
  );
  const table = useDataTable({ data: list.visible, columns, sort: [{ id: "name", desc: false }], resetKey: list.key });

  return (
    <ListPage
      title="Webhooks"
      count={webhooks.data ? all.length : undefined}
      table={table}
      loading={webhooks.isPending}
      error={webhooks.error}
      noun={["webhook", "webhooks"]}
      onRowClick={(r) => navigate({ to: "/workspaces/$workspace/webhooks/$webhook", params: { workspace, webhook: lastSegment(r.original.name) } })}
      onReset={reset}
      empty={
        <EmptyState
          icon={WebhookIcon}
          title={`No webhooks in ${workspace} yet`}
          description="A webhook sends platform events, such as failed syncs and incidents, to a URL, signed with Standard Webhooks."
          command="openetl apply -f webhook.yaml"
        />
      }
      toolbar={
        <Toolbar search={search.q ?? ""} onSearch={(q) => navigate({ search: (s) => ({ ...s, q: q || undefined }), replace: true })} placeholder="Search webhooks" filtered={list.filtered} onReset={reset}>
          <FacetedFilter
            title="State"
            options={list.values("state", ["Failing", "Enabled", "Disabled"]).map((o) => ({ ...o, label: o.value }))}
            selected={search.state ?? []}
            onChange={(v) => navigate({ search: (s) => ({ ...s, state: v.length ? v : undefined }), replace: true })}
          />
        </Toolbar>
      }
    />
  );
}
