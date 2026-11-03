import { useMemo } from "react";
import { Code } from "@connectrpc/connect";
import { useQuery } from "@connectrpc/connect-query";
import { getRouteApi } from "@tanstack/react-router";
import type { ColumnDef } from "@tanstack/react-table";
import { Webhook as WebhookIcon } from "lucide-react";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { DataTable } from "@/components/data-table/data-table";
import { Pagination } from "@/components/data-table/pagination";
import { SortHeader } from "@/components/data-table/sort-header";
import { useDataTable } from "@/components/data-table/use-data-table";
import { DescriptionItem, DetailHeader, Section } from "@/components/detail-header";
import { ErrorNote } from "@/components/empty";
import { EmptyState } from "@/components/empty-state";
import { RelativeTime } from "@/components/relative-time";
import { UsedBy } from "@/components/resource-link";
import { DeliveryStateBadge, Events, WebhookStateBadge } from "@/components/webhook-fields";
import { WebhookService, type WebhookDelivery } from "@/gen/openetl/api/v1/webhook_pb";
import { errorCode, errorMessage } from "@/lib/api";
import { dateOf, duration, durationMs } from "@/lib/format";

const route = getRouteApi("/_app/workspaces/$workspace/webhooks/$webhook");

function Deliveries({ name }: { name: string }) {
  const deliveries = useQuery(WebhookService.method.listWebhookDeliveries, { parent: name, pageSize: 500 }, { refetchInterval: 15_000 });
  const rows = useMemo(() => deliveries.data?.deliveries ?? [], [deliveries.data]);
  const columns = useMemo<ColumnDef<WebhookDelivery>[]>(
    () => [
      {
        id: "state",
        accessorFn: (d) => d.state,
        header: ({ column }) => <SortHeader column={column} title="State" />,
        cell: ({ row }) => <DeliveryStateBadge state={row.original.state} />,
      },
      {
        id: "event",
        accessorFn: (d) => d.eventType,
        header: ({ column }) => <SortHeader column={column} title="Event" />,
        cell: ({ row }) => (
          <span className="grid">
            <span className="font-mono text-[0.8125rem]">{row.original.eventType}</span>
            <span className="font-mono text-xs text-muted-foreground">{row.original.eventId}</span>
          </span>
        ),
      },
      {
        id: "response",
        header: "Response",
        cell: ({ row }) => {
          const d = row.original;
          const text = d.responseStatus ? String(d.responseStatus) : "—";
          if (!d.error) return <span className="tabular-nums">{text}</span>;
          return (
            <Tooltip>
              <TooltipTrigger asChild>
                <span className="text-failed tabular-nums">{text}</span>
              </TooltipTrigger>
              <TooltipContent className="max-w-md font-mono whitespace-pre-wrap">{d.error}</TooltipContent>
            </Tooltip>
          );
        },
        meta: { className: "hidden sm:table-cell" },
      },
      {
        id: "attempts",
        accessorFn: (d) => d.attempts,
        header: ({ column }) => <SortHeader column={column} title="Attempts" />,
        cell: ({ row }) => <span className="tabular-nums">{row.original.attempts}</span>,
        meta: { className: "hidden md:table-cell", align: "right" },
      },
      {
        id: "latency",
        header: "Latency",
        cell: ({ row }) => <span className="tabular-nums">{row.original.latency ? duration(durationMs(row.original.latency)) : "—"}</span>,
        meta: { className: "hidden lg:table-cell", align: "right" },
      },
      {
        id: "last",
        accessorFn: (d) => dateOf(d.lastAttemptTime)?.getTime() ?? 0,
        header: ({ column }) => <SortHeader column={column} title="Last attempt" />,
        cell: ({ row }) => {
          const t = dateOf(row.original.lastAttemptTime);
          const next = dateOf(row.original.nextAttemptTime);
          return (
            <span className="grid">
              {t ? <RelativeTime date={t} /> : "—"}
              {next && (
                <span className="text-xs text-muted-foreground">
                  Next <RelativeTime date={next} />
                </span>
              )}
            </span>
          );
        },
      },
    ],
    [],
  );
  const table = useDataTable({ data: rows, columns, sort: [{ id: "last", desc: true }] });
  if (deliveries.error) return <ErrorNote title="Couldn't list deliveries" message={errorMessage(deliveries.error)} />;
  return (
    <div className="grid gap-3">
      <DataTable table={table} loading={deliveries.isPending} empty="No events have matched this webhook yet." />
      <Pagination table={table} noun={["delivery", "deliveries"]} />
    </div>
  );
}

export function WebhookPage() {
  const { workspace, webhook } = route.useParams();
  const name = `workspaces/${workspace}/webhooks/${webhook}`;
  const q = useQuery(WebhookService.method.getWebhook, { name }, { refetchInterval: 30_000 });

  if (q.error) {
    return errorCode(q.error) === Code.NotFound ? (
      <EmptyState icon={WebhookIcon} title={`There's no webhook ${webhook} in ${workspace}`} />
    ) : (
      <ErrorNote title="Couldn't load the webhook" message={errorMessage(q.error)} />
    );
  }
  const w = q.data?.webhook;
  const failing = dateOf(w?.failingSince);
  const updated = dateOf(w?.updateTime);

  return (
    <div className="grid gap-8">
      <DetailHeader title={webhook} status={w && <WebhookStateBadge webhook={w} />}>
        {w && (
          <>
            <DescriptionItem label="URL">
              <span className="font-mono text-[0.8125rem]">{w.spec?.url}</span>
            </DescriptionItem>
            <DescriptionItem label="Events">
              <Events events={w.spec?.events ?? []} max={6} />
            </DescriptionItem>
            <DescriptionItem label="Pipelines">
              {w.spec?.pipelines.length ? <UsedBy names={w.spec.pipelines} /> : "All, and the workspace's own events"}
            </DescriptionItem>
            <DescriptionItem label="Signed with">{w.spec?.secrets.join(", ") || "—"}</DescriptionItem>
            {failing && (
              <DescriptionItem label="Failing since">
                <span className="text-degraded">
                  <RelativeTime date={failing} />
                </span>
              </DescriptionItem>
            )}
            {w.disabledReason && <DescriptionItem label="Disabled because">{w.disabledReason}</DescriptionItem>}
            {updated && (
              <DescriptionItem label="Updated">
                <RelativeTime date={updated} />
              </DescriptionItem>
            )}
          </>
        )}
      </DetailHeader>
      <Section title="Deliveries">
        <Deliveries name={name} />
      </Section>
    </div>
  );
}
