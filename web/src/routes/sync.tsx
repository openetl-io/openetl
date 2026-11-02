import { useEffect, useMemo, useRef, useState } from "react";
import { useQuery } from "@connectrpc/connect-query";
import { createClient } from "@connectrpc/connect";
import { Link, getRouteApi } from "@tanstack/react-router";
import { useVirtualizer } from "@tanstack/react-virtual";
import { ArrowDownToLine, ArrowLeft, Search } from "lucide-react";
import type { JsonObject } from "@bufbuild/protobuf";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { ErrorNote } from "@/components/empty";
import { RelativeTime } from "@/components/relative-time";
import { StatusBadge } from "@/components/status";
import { SyncService, SyncState, type LogEntry } from "@/gen/openetl/api/v1/sync_pb";
import { errorMessage, transport } from "@/lib/api";
import { clock, compact, dateOf, duration, full } from "@/lib/format";
import { shortDigest, syncName } from "@/lib/names";
import { rows, syncStatus } from "@/lib/status";
import { cn } from "@/lib/utils";

const route = getRouteApi("/_app/workspaces/$workspace/pipelines/$pipeline/syncs/$sync");
const client = createClient(SyncService, transport);

type Level = "all" | "warn" | "error";
const levelRank: Record<string, number> = { DEBUG: 0, INFO: 1, WARN: 2, ERROR: 3 };
const levelTone: Record<string, string> = {
  DEBUG: "text-muted-foreground",
  INFO: "text-foreground",
  WARN: "text-degraded",
  ERROR: "text-failed",
};

/** Reads the log a page at a time, and keeps reading while the sync runs. */
function useSyncLog(name: string) {
  const [entries, setEntries] = useState<LogEntry[]>([]);
  const [error, setError] = useState<string>();
  const [done, setDone] = useState(false);
  const [attempt, setAttempt] = useState(0);
  useEffect(() => {
    let stop = false;
    let token = "";
    setEntries([]);
    setError(undefined);
    setDone(false);
    (async () => {
      while (!stop) {
        try {
          const res = await client.readSyncLog({ name, pageToken: token });
          if (stop) return;
          if (res.entries.length) setEntries((e) => [...e, ...res.entries]);
          if (!res.nextPageToken) {
            setDone(true);
            return;
          }
          token = res.nextPageToken;
          if (!res.entries.length) await new Promise((r) => setTimeout(r, 2_000));
        } catch (err) {
          if (!stop) setError(errorMessage(err));
          return;
        }
      }
    })();
    return () => {
      stop = true;
    };
  }, [name, attempt]);
  return { entries, error, done, retry: () => setAttempt((n) => n + 1) };
}

function attrsText(a?: JsonObject) {
  if (!a) return "";
  return Object.entries(a)
    .filter(([k]) => k !== "component")
    .map(([k, v]) => `${k}=${typeof v === "string" ? v : JSON.stringify(v)}`)
    .join(" ");
}

const taskLabel = (e: LogEntry) =>
  e.postSyncStep ? `Post-sync step ${e.postSyncStep}, attempt ${e.attempt}` : `Attempt ${e.attempt}`;

function LogView({ entries, live }: { entries: LogEntry[]; live: boolean }) {
  const [level, setLevel] = useState<Level>("all");
  const [q, setQ] = useState("");
  const [follow, setFollow] = useState(true);
  const scroller = useRef<HTMLDivElement>(null);
  const shown = useMemo(() => {
    const min = level === "error" ? 3 : level === "warn" ? 2 : 0;
    const needle = q.toLowerCase();
    return entries.filter(
      (e) =>
        (levelRank[e.level] ?? 1) >= min &&
        (!needle || e.message.toLowerCase().includes(needle) || attrsText(e.attrs).toLowerCase().includes(needle)),
    );
  }, [entries, level, q]);
  const v = useVirtualizer({ count: shown.length, getScrollElement: () => scroller.current, estimateSize: () => 24, overscan: 20 });

  useEffect(() => {
    if (follow && live && shown.length) v.scrollToIndex(shown.length - 1, { align: "end" });
  }, [shown.length, follow, live, v]);

  return (
    <div className="grid gap-2">
      <div className="flex flex-wrap items-center gap-2">
        <div role="tablist" aria-label="Levels" className="flex rounded-lg border bg-card p-0.5">
          {(
            [
              ["all", "All"],
              ["warn", "Warnings and errors"],
              ["error", "Errors"],
            ] as const
          ).map(([id, label]) => (
            <button
              key={id}
              role="tab"
              aria-selected={level === id}
              onClick={() => setLevel(id)}
              className={cn(
                "rounded-md px-2.5 py-1 text-muted-foreground hover:text-foreground focus-visible:outline-2 focus-visible:outline-ring",
                level === id && "bg-secondary text-foreground",
              )}
            >
              {label}
            </button>
          ))}
        </div>
        <div className="relative">
          <Search className="pointer-events-none absolute top-1/2 left-2.5 size-4 -translate-y-1/2 text-muted-foreground" aria-hidden />
          <Input type="search" placeholder="Find in the log" aria-label="Find in the log" value={q} onChange={(e) => setQ(e.target.value)} className="h-8 w-56 pl-8" />
        </div>
        <span className="text-[0.8125rem] text-muted-foreground tabular">
          {shown.length === entries.length ? `${full(entries.length)} lines` : `${full(shown.length)} of ${full(entries.length)} lines`}
        </span>
        {live && (
          <Button variant={follow ? "secondary" : "outline"} size="sm" className="ml-auto" onClick={() => setFollow((f) => !f)} aria-pressed={follow}>
            <ArrowDownToLine />
            Follow
          </Button>
        )}
      </div>
      <div
        ref={scroller}
        onWheel={(e) => e.deltaY < 0 && setFollow(false)}
        className="max-h-[min(65vh,720px)] min-h-24 overflow-auto rounded-lg border bg-card font-mono text-[0.8125rem]"
        role="log"
        aria-live={live ? "polite" : "off"}
      >
        <div style={{ height: v.getTotalSize(), position: "relative" }}>
          {v.getVirtualItems().map((item) => {
            const e = shown[item.index]!;
            const prev = shown[item.index - 1];
            const header = !prev || prev.attempt !== e.attempt || prev.postSyncStep !== e.postSyncStep;
            const component = typeof e.attrs?.component === "string" ? e.attrs.component : "";
            const t = dateOf(e.time);
            return (
              <div
                key={item.key}
                data-index={item.index}
                ref={v.measureElement}
                className="absolute inset-x-0"
                style={{ transform: `translateY(${item.start}px)` }}
              >
                {header && (
                  <div className="sticky border-y bg-muted/60 px-4 py-1 font-sans text-xs text-muted-foreground first:border-t-0">
                    {taskLabel(e)}
                  </div>
                )}
                <div className="grid grid-cols-[7.5rem_3.5rem_6rem_1fr] gap-3 px-4 py-0.5 hover:bg-accent/50">
                  <span className="text-muted-foreground tabular">{t ? clock(t) : ""}</span>
                  <span className={levelTone[e.level] ?? ""}>{e.level || "—"}</span>
                  <span className="truncate text-muted-foreground">{component}</span>
                  <span className="min-w-0 break-words whitespace-pre-wrap">
                    {e.message}
                    {attrsText(e.attrs) && <span className="text-muted-foreground"> {attrsText(e.attrs)}</span>}
                  </span>
                </div>
              </div>
            );
          })}
        </div>
        {shown.length === 0 && (
          <p className="px-4 py-6 font-sans text-muted-foreground">
            {entries.length ? "No lines match." : live ? "Waiting for the first lines…" : "This sync's log is empty."}
          </p>
        )}
      </div>
    </div>
  );
}

export function SyncPage() {
  const { workspace, pipeline, sync } = route.useParams();
  const name = syncName(workspace, pipeline, sync);
  const s = useQuery(SyncService.method.getSync, { name }, { refetchInterval: (q) => (q.state.data?.sync?.state === SyncState.RUNNING ? 2_000 : false) });
  const log = useSyncLog(name);
  const it = s.data?.sync;
  const live = it?.state === SyncState.RUNNING;
  const start = dateOf(it?.startTime);
  const end = dateOf(it?.endTime);
  const r = it ? rows(it) : undefined;

  return (
    <div className="grid gap-6">
      <Link
        to="/workspaces/$workspace/pipelines/$pipeline"
        params={{ workspace, pipeline }}
        search={{ tab: "syncs", stream: undefined, version: undefined }}
        className="inline-flex w-fit items-center gap-1 text-muted-foreground hover:text-foreground"
      >
        <ArrowLeft className="size-4" aria-hidden />
        {pipeline}
      </Link>
      {s.error ? (
        <ErrorNote title="Couldn't load the sync" message={errorMessage(s.error)} />
      ) : !it ? (
        <Skeleton className="h-24" />
      ) : (
        <div className="grid gap-4">
          <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
            <h1 className="text-2xl font-semibold tracking-tight">
              Sync <span className="font-mono text-xl">{sync.slice(-8)}</span>
            </h1>
            <StatusBadge status={syncStatus(it)} />
          </div>
          <dl className="flex flex-wrap gap-x-8 gap-y-3">
            <div className="grid gap-0.5">
              <dt className="text-[0.8125rem] text-muted-foreground">Started</dt>
              <dd>{start && <RelativeTime date={start} />}</dd>
            </div>
            <div className="grid gap-0.5">
              <dt className="text-[0.8125rem] text-muted-foreground">{end ? "Took" : "Running for"}</dt>
              <dd className="tabular">{start && duration((end?.getTime() ?? Date.now()) - start.getTime())}</dd>
            </div>
            <div className="grid gap-0.5">
              <dt className="text-[0.8125rem] text-muted-foreground">Rows</dt>
              <dd className="tabular">
                {r && full(r.all)}
                {r && r.quarantined > 0n && <span className="text-degraded">, {full(r.quarantined)} quarantined</span>}
              </dd>
            </div>
            <div className="grid gap-0.5">
              <dt className="text-[0.8125rem] text-muted-foreground">Version</dt>
              <dd>
                <Link
                  to="/workspaces/$workspace/pipelines/$pipeline"
                  params={{ workspace, pipeline }}
                  search={{ tab: "versions", version: it.version }}
                  className="font-mono text-[0.8125rem] hover:text-signal"
                >
                  {shortDigest(it.version)}
                </Link>
              </dd>
            </div>
            <div className="grid gap-0.5">
              <dt className="text-[0.8125rem] text-muted-foreground">Data plane</dt>
              <dd>{it.dataPlane}</dd>
            </div>
          </dl>
          {it.streams.length > 0 && (
            <ul aria-label="Rows by stream" className="flex flex-wrap gap-1.5">
              {it.streams.map((st) => (
                <li key={st.name} className="inline-flex items-center gap-1.5 rounded-full border bg-card px-2.5 py-0.5 text-[0.8125rem]">
                  {st.name}
                  <span className="font-mono text-muted-foreground tabular">{compact(st.rows)}</span>
                </li>
              ))}
            </ul>
          )}
          {it.error && <ErrorNote title="Why it failed" message={it.error} />}
        </div>
      )}
      <section className="grid gap-2" aria-labelledby="log-h">
        <h2 id="log-h" className="text-lg font-semibold tracking-tight">
          Log
        </h2>
        {log.error ? (
          <div className="grid gap-2">
            <ErrorNote title="Couldn't read the log" message={log.error} />
            <Button variant="outline" size="sm" className="w-fit" onClick={log.retry}>
              Try again
            </Button>
          </div>
        ) : (
          <LogView entries={log.entries} live={live || !log.done} />
        )}
      </section>
    </div>
  );
}
