import { createContext, useContext, useMemo, useState, type ReactNode } from "react";
import { useInfiniteQuery, useMutation } from "@connectrpc/connect-query";
import { Link } from "@tanstack/react-router";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from "@/components/ui/sheet";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { CodeBlock } from "@/components/code-block";
import { CommandLine } from "@/components/command-line";
import { Empty } from "@/components/empty";
import { QueryError } from "@/components/query-error";
import { RelativeTime } from "@/components/relative-time";
import { PipelineService, type QuarantineFile } from "@/gen/openetl/api/v1/pipeline_pb";
import { useCan } from "@/lib/caller";
import { dateOf } from "@/lib/format";
import { lastSegment, pipelineName } from "@/lib/names";
import { anyNotHeld, notHeldHint, parseLine, rowsLabel, showCommand } from "@/lib/quarantine";
import { cn } from "@/lib/utils";

/** Which of the stream's files a count came from: a sync's, or a checkpoint's. */
export type QuarantineFocus = { sync: string } | { generation: bigint; epoch: bigint };

type Shown = { stream?: string; focus?: QuarantineFocus };

const QuarantineContext = createContext<((s: Shown) => void) | undefined>(undefined);

/**
 * A pipeline's quarantine view, which each QuarantineCount below it opens: one sheet for the page, so a click in it
 * doesn't reach a table row.
 */
export function QuarantineView({ workspace, pipeline, children }: { workspace: string; pipeline: string; children: ReactNode }) {
  const [shown, setShown] = useState<Shown>();
  const [open, setOpen] = useState(false);
  const show = useMemo(
    () => (s: Shown) => {
      setShown(s);
      setOpen(true);
    },
    [],
  );
  return (
    <QuarantineContext.Provider value={show}>
      {children}
      <Sheet open={open} onOpenChange={setOpen}>
        <SheetContent className="w-full overflow-y-auto sm:max-w-2xl">
          {shown && <QuarantineFiles workspace={workspace} pipeline={pipeline} {...shown} />}
        </SheetContent>
      </Sheet>
    </QuarantineContext.Provider>
  );
}

/** A count of quarantined rows that opens the stream's files; a whole sync's, of several streams, opens them all. */
export function QuarantineCount({ stream, focus, className, children }: Shown & { className?: string; children: ReactNode }) {
  const show = useContext(QuarantineContext);
  if (!show) return <span className={cn("text-degraded tabular-nums", className)}>{children}</span>;
  return (
    <button
      type="button"
      className={cn("cursor-pointer text-degraded tabular-nums underline-offset-4 hover:underline", className)}
      title={stream ? `The files of ${stream}'s quarantined rows, and why each was quarantined` : "The files of the quarantined rows, and why"}
      onClick={() => show({ stream, focus })}
    >
      {children}
    </button>
  );
}

const focused = (f: QuarantineFile, focus?: QuarantineFocus) =>
  !!focus && ("sync" in focus ? f.sync === focus.sync : f.generation === focus.generation && f.epoch === focus.epoch);

function QuarantineFiles({ workspace, pipeline, stream, focus }: { workspace: string; pipeline: string } & Shown) {
  const parent = pipelineName(workspace, pipeline);
  const q = useInfiniteQuery(
    PipelineService.method.listQuarantine,
    { parent, stream: stream ?? "", pageSize: 20, pageToken: "" },
    { pageParamKey: "pageToken", getNextPageParam: (r) => r.nextPageToken || undefined },
  );
  const files = useMemo(() => q.data?.pages.flatMap((p) => p.files) ?? [], [q.data]);
  const read = useCan(workspace)("pipelines.quarantine");
  const label = focus && ("sync" in focus ? "This sync" : "This checkpoint");
  return (
    <>
      <SheetHeader>
        <SheetTitle className="break-all">{stream ?? "Quarantined rows"}</SheetTitle>
        <SheetDescription>Quarantine files, newest first, with why their rows were quarantined.</SheetDescription>
      </SheetHeader>
      <div className="grid grid-cols-1 gap-4 px-4 pb-6">
        <p>
          These rows weren't written to the destination. Held values are different: their rows were written, and the values come back
          when the hold is released.
        </p>
        {anyNotHeld(files) && (
          <p>
            The stream holds values that don't fit, so these are rows whose values it couldn't hold. Each cause's{" "}
            <span className="font-mono">not held</span> says why.
          </p>
        )}
        {q.error && <QueryError title="Couldn't list the quarantine files" error={q.error} />}
        {q.data && files.length === 0 && (
          <Empty title="No quarantine files listed">
            The files of committed epochs are listed until the pipeline's quarantine.retention passes.
          </Empty>
        )}
        {files.map((f) => (
          <FileItem
            key={f.path}
            workspace={workspace}
            pipeline={pipeline}
            file={f}
            showStream={!stream}
            focused={focused(f, focus) ? label : undefined}
            read={read}
          />
        ))}
        {q.hasNextPage && (
          <Button variant="outline" size="sm" className="w-fit" disabled={q.isFetchingNextPage} onClick={() => q.fetchNextPage()}>
            {q.isFetchingNextPage ? "Loading…" : "Load older files"}
          </Button>
        )}
      </div>
    </>
  );
}

function FileItem({
  workspace,
  pipeline,
  file: f,
  showStream,
  focused,
  read,
}: {
  workspace: string;
  pipeline: string;
  file: QuarantineFile;
  showStream: boolean;
  /** Set for a file of the sync or checkpoint the count came from: what to call it. */
  focused?: string;
  read: boolean;
}) {
  const committed = dateOf(f.commitTime);
  return (
    <article className="grid min-w-0 grid-cols-1 gap-3 rounded-lg border bg-card p-4" aria-label={f.path}>
      <div className="flex flex-wrap items-center gap-x-3 gap-y-1.5 text-[0.8125rem]">
        {showStream && <span className="font-medium">{f.stream}</span>}
        {f.output && (
          <Badge variant="outline" className="font-normal">
            Output
          </Badge>
        )}
        <Badge variant="outline" className="font-normal tabular-nums">
          {rowsLabel(f.rows)}
        </Badge>
        {focused && <Badge variant="secondary">{focused}</Badge>}
        <span className="text-muted-foreground sm:ml-auto">
          {f.sync && (
            <>
              Sync{" "}
              <Link
                to="/workspaces/$workspace/pipelines/$pipeline/syncs/$sync"
                params={{ workspace, pipeline, sync: lastSegment(f.sync) }}
                className="font-mono text-foreground hover:underline"
              >
                {lastSegment(f.sync).slice(-8)}
              </Link>
              ,{" "}
            </>
          )}
          epoch {String(f.generation)}/{String(f.epoch)}
          {committed && (
            <>
              , <RelativeTime date={committed} />
            </>
          )}
        </span>
      </div>
      <ul className="grid gap-1" aria-label="Rows by cause">
        {f.causes.map((c) => (
          <li key={`${c.code}/${c.column}/${c.notHeld}`}>
            {rowsLabel(c.rows)}: <code className="font-mono text-[0.8125rem]">{c.code}</code>
            {c.column && (
              <>
                {" "}on <code className="font-mono text-[0.8125rem]">{c.column}</code>
              </>
            )}
            {c.notHeld && (
              <>
                , not held:{" "}
                <Tooltip>
                  <TooltipTrigger
                    className="cursor-help font-mono text-[0.8125rem] underline decoration-muted-foreground/60 decoration-dotted underline-offset-4"
                  >
                    {c.notHeld}
                  </TooltipTrigger>
                  <TooltipContent className="max-w-xs">{notHeldHint[c.notHeld] ?? c.notHeld}</TooltipContent>
                </Tooltip>
              </>
            )}
          </li>
        ))}
      </ul>
      <code className="font-mono text-[0.8125rem] break-all text-muted-foreground" title={f.url}>
        {f.path}
      </code>
      {read && <Records workspace={workspace} pipeline={pipeline} path={f.path} />}
    </article>
  );
}

/** The file's first records, read from the data plane when asked: each read is in the audit log, so nothing reads again by itself. */
function Records({ workspace, pipeline, path }: { workspace: string; pipeline: string; path: string }) {
  const r = useMutation(PipelineService.method.readQuarantineFile);
  if (!r.data && !r.error) {
    return (
      <Button
        variant="outline"
        size="sm"
        className="w-fit"
        disabled={r.isPending}
        onClick={() => r.mutate({ parent: pipelineName(workspace, pipeline), path, limit: 20 })}
      >
        {r.isPending ? "Reading…" : "Show records"}
      </Button>
    );
  }
  if (r.error) return <QueryError title="Couldn't read the file" error={r.error} />;
  const lines = r.data?.lines ?? [];
  return (
    <div className="grid min-w-0 grid-cols-1 gap-3">
      {lines.length === 0 && <p className="text-muted-foreground">The file is empty.</p>}
      {lines.map((l, i) => {
        const line = parseLine(l);
        return (
          <div key={i} className="grid min-w-0 gap-1">
            {line.cause && (
              <p className="text-[0.8125rem]">
                <span className="font-mono">{line.cause}</span>
                {line.reason && <span className="text-muted-foreground">: {line.reason}</span>}
              </p>
            )}
            <CodeBlock>{line.record}</CodeBlock>
          </div>
        );
      })}
      {r.data?.more && (
        <div className="grid min-w-0 gap-1.5">
          <p className="text-[0.8125rem] text-muted-foreground">The file has more lines. This reads up to 100 of them:</p>
          <CommandLine command={showCommand(workspace, pipeline, path)} />
        </div>
      )}
    </div>
  );
}
