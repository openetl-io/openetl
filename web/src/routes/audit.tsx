import { useMemo, useState } from "react";
import { useQuery } from "@connectrpc/connect-query";
import { getRouteApi } from "@tanstack/react-router";
import type { ColumnDef } from "@tanstack/react-table";
import { ScrollText } from "lucide-react";
import { stringify } from "yaml";
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from "@/components/ui/sheet";
import { CodeBlock } from "@/components/code-block";
import { FacetedFilter } from "@/components/data-table/faceted-filter";
import { ListPage } from "@/components/data-table/list-page";
import { SortHeader } from "@/components/data-table/sort-header";
import { Toolbar } from "@/components/data-table/toolbar";
import { useDataTable } from "@/components/data-table/use-data-table";
import { DescriptionItem, DescriptionList } from "@/components/detail-header";
import { EmptyState } from "@/components/empty-state";
import { RelativeTime } from "@/components/relative-time";
import { ResourceLink } from "@/components/resource-link";
import { AuditService, type AuditEntry } from "@/gen/openetl/api/v1/audit_pb";
import { absolute, dateOf } from "@/lib/format";
import { useListFilter } from "@/lib/list-filter";
import { lastSegment } from "@/lib/names";

const route = getRouteApi("/_app/workspaces/$workspace/audit");

/** "PipelineService/SyncPipeline" from "openetl.api.v1.PipelineService/SyncPipeline". */
const call = (e: AuditEntry) => {
  const [service = "", method = ""] = e.method.split("/");
  return { service: service.split(".").pop() ?? "", method };
};
const result = (e: AuditEntry) => (e.code && e.code !== "OK" ? e.code : "OK");

const facets = {
  actor: (e: AuditEntry) => lastSegment(e.actor),
  method: (e: AuditEntry) => call(e).method,
  result,
};

function Result({ entry }: { entry: AuditEntry }) {
  const r = result(entry);
  return r === "OK" ? <span className="text-muted-foreground">OK</span> : <span className="font-mono text-[0.8125rem] text-failed">{r}</span>;
}

function EntrySheet({ entry, onClose }: { entry?: AuditEntry; onClose: () => void }) {
  const t = dateOf(entry?.time);
  const end = dateOf(entry?.endTime);
  return (
    <Sheet open={!!entry} onOpenChange={(o) => !o && onClose()}>
      <SheetContent className="w-full overflow-y-auto sm:max-w-lg">
        {entry && (
          <>
            <SheetHeader>
              <SheetTitle>{call(entry).method}</SheetTitle>
              <SheetDescription>
                {lastSegment(entry.actor)}
                {t && `, ${absolute(t)}`}
              </SheetDescription>
            </SheetHeader>
            <div className="grid gap-6 px-4 pb-6">
              <DescriptionList className="grid-cols-1 sm:grid-cols-2 lg:grid">
                <DescriptionItem label="Result"><Result entry={entry} /></DescriptionItem>
                {t && end && <DescriptionItem label="Took">{Math.max(0, end.getTime() - t.getTime())} ms</DescriptionItem>}
                <DescriptionItem label="Actor"><span className="font-mono text-[0.8125rem]">{entry.actor}</span></DescriptionItem>
                <DescriptionItem label="Credential"><span className="font-mono text-[0.8125rem]">{entry.credential || "—"}</span></DescriptionItem>
                <DescriptionItem label="From"><span className="font-mono text-[0.8125rem]">{entry.peer || "—"}</span></DescriptionItem>
                <DescriptionItem label="Client">{entry.userAgent || "—"}</DescriptionItem>
              </DescriptionList>
              <div className="grid gap-1">
                <span className="text-[0.8125rem] text-muted-foreground">Method</span>
                <span className="font-mono text-[0.8125rem] break-all">{entry.method}</span>
              </div>
              {entry.resources.length > 0 && (
                <div className="grid gap-1">
                  <span className="text-[0.8125rem] text-muted-foreground">Resources</span>
                  <ul className="grid gap-0.5">
                    {entry.resources.map((r) => (
                      <li key={r} className="font-mono text-[0.8125rem]">
                        <ResourceLink name={r} label={r} />
                      </li>
                    ))}
                  </ul>
                </div>
              )}
              {entry.error && <CodeBlock>{entry.error}</CodeBlock>}
              {entry.details && Object.keys(entry.details).length > 0 && <CodeBlock>{stringify(entry.details)}</CodeBlock>}
            </div>
          </>
        )}
      </SheetContent>
    </Sheet>
  );
}

export function AuditPage() {
  const search = route.useSearch();
  const navigate = route.useNavigate();
  const entries = useQuery(AuditService.method.listAuditEntries, { pageSize: 500 }, { refetchInterval: 30_000 });
  const all = useMemo(() => entries.data?.entries ?? [], [entries.data]);
  const list = useListFilter({ rows: all, search, facets, text: (e) => [e.actor, e.method, ...e.resources] });
  const setSearch = (s: Partial<typeof search>) => navigate({ search: (prev) => ({ ...prev, ...s }), replace: true });
  const reset = () => setSearch({ q: undefined, actor: undefined, method: undefined, result: undefined });
  const [open, setOpen] = useState<AuditEntry>();

  const columns = useMemo<ColumnDef<AuditEntry>[]>(
    () => [
      {
        id: "time",
        accessorFn: (e) => dateOf(e.time)?.getTime() ?? 0,
        header: ({ column }) => <SortHeader column={column} title="Time" />,
        cell: ({ row }) => {
          const t = dateOf(row.original.time);
          return t ? <RelativeTime date={t} /> : null;
        },
      },
      {
        id: "actor",
        accessorFn: (e) => lastSegment(e.actor),
        header: ({ column }) => <SortHeader column={column} title="Actor" />,
      },
      {
        id: "method",
        accessorFn: (e) => call(e).method,
        header: ({ column }) => <SortHeader column={column} title="Action" />,
        cell: ({ row }) => (
          <span className="grid">
            <span className="font-medium">{call(row.original).method}</span>
            <span className="text-xs text-muted-foreground">{call(row.original).service}</span>
          </span>
        ),
      },
      {
        id: "resources",
        header: "Resource",
        cell: ({ row }) => {
          const [first, ...rest] = row.original.resources;
          if (!first) return <span className="text-muted-foreground">—</span>;
          return (
            <span className="inline-flex max-w-80 min-w-0 gap-1">
              <span className="truncate"><ResourceLink name={first} label={first.split("/").slice(-2).join("/")} /></span>
              {rest.length > 0 && <span className="text-muted-foreground">+{rest.length}</span>}
            </span>
          );
        },
        meta: { className: "hidden md:table-cell" },
      },
      {
        id: "result",
        accessorFn: result,
        header: ({ column }) => <SortHeader column={column} title="Result" />,
        cell: ({ row }) => <Result entry={row.original} />,
        meta: { className: "hidden sm:table-cell" },
      },
    ],
    [],
  );
  const table = useDataTable({ data: list.visible, columns, sort: [{ id: "time", desc: true }], resetKey: list.key });

  return (
    <>
      <ListPage
        title="Audit log"
        count={entries.data ? all.length : undefined}
        table={table}
        loading={entries.isPending}
        error={entries.error}
        noun={["entry", "entries"]}
        onRowClick={(r) => setOpen(r.original)}
        onReset={reset}
        empty={<EmptyState icon={ScrollText} title="Nothing in the audit log yet" description="Every change, and every read of a secret, lands here with who made it. Entries are kept for 90 days." />}
        toolbar={
          <Toolbar search={search.q ?? ""} onSearch={(q) => setSearch({ q: q || undefined })} placeholder="Search the audit log" filtered={list.filtered} onReset={reset}>
            <FacetedFilter
              title="Actor"
              options={list.values("actor").map((o) => ({ ...o, label: o.value }))}
              selected={search.actor ?? []}
              onChange={(v) => setSearch({ actor: v.length ? v : undefined })}
            />
            <FacetedFilter
              title="Action"
              options={list.values("method").map((o) => ({ ...o, label: o.value }))}
              selected={search.method ?? []}
              onChange={(v) => setSearch({ method: v.length ? v : undefined })}
            />
            <FacetedFilter
              title="Result"
              options={list.values("result").map((o) => ({ ...o, label: o.value }))}
              selected={search.result ?? []}
              onChange={(v) => setSearch({ result: v.length ? v : undefined })}
            />
          </Toolbar>
        }
      />
      <EntrySheet entry={open} onClose={() => setOpen(undefined)} />
    </>
  );
}
