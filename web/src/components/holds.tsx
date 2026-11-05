import { useQuery } from "@connectrpc/connect-query";
import { CircleDashed, CircleX, Clock, LoaderCircle, TriangleAlert, type LucideIcon } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { CodeBlock } from "@/components/code-block";
import { CommandLine } from "@/components/command-line";
import { ErrorNote } from "@/components/empty";
import { RelativeTime } from "@/components/relative-time";
import { PipelineService, type Hold } from "@/gen/openetl/api/v1/pipeline_pb";
import { errorMessage } from "@/lib/api";
import { dateOf } from "@/lib/format";
import { heldWhat, holdCommand, holdState, holdStateHint, holdStateLabel, refusalNote, type HoldState } from "@/lib/holds";
import { lastSegment } from "@/lib/names";
import { cn } from "@/lib/utils";

const stateIcon: Record<HoldState, { icon: LucideIcon; className: string }> = {
  held: { icon: TriangleAlert, className: "text-degraded" },
  pending: { icon: Clock, className: "text-muted-foreground" },
  releasing: { icon: LoaderCircle, className: "animate-spin text-running [animation-duration:1.6s]" },
  refused: { icon: CircleX, className: "text-failed" },
  discarded: { icon: CircleDashed, className: "text-muted-foreground" },
};

function HoldStateBadge({ state }: { state: HoldState }) {
  const { icon: Icon, className } = stateIcon[state];
  return (
    <Badge variant="outline" className="gap-1.5 font-normal" title={holdStateHint[state]}>
      <Icon className={cn("size-3.5", className)} aria-hidden />
      {holdStateLabel[state]}
    </Badge>
  );
}

/** The pipeline's holds, each with why and how to clear it; nothing when it holds nothing. */
export function Holds({ workspace, pipeline, live }: { workspace: string; pipeline: string; live: boolean }) {
  const q = useQuery(PipelineService.method.listHolds, { parent: pipeline }, { refetchInterval: live ? 3_000 : 15_000 });
  const holds = q.data?.holds ?? [];
  if (!q.error && holds.length === 0) return null;
  return (
    <section id="holds" className="grid scroll-mt-4 gap-3" aria-labelledby="holds-h">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h2 id="holds-h" className="text-base font-semibold">
          Holds
        </h2>
        <span className="text-[0.8125rem] text-muted-foreground">Held back while the rest of each row is delivered</span>
      </div>
      {q.error && <ErrorNote title="Couldn't list the holds" message={errorMessage(q.error)} />}
      {holds.map((h) => (
        <HoldItem key={`${h.stream}/${h.column}/${h.whole}`} workspace={workspace} pipeline={lastSegment(pipeline)} hold={h} />
      ))}
    </section>
  );
}

export function HoldItem({ workspace, pipeline, hold: h }: { workspace: string; pipeline: string; hold: Hold }) {
  const state = holdState(h);
  const started = dateOf(h.startTime);
  const refusal = refusalNote(h);
  const ways = h.waysOut.flatMap((w) => {
    const command = holdCommand(w.action, workspace, pipeline, h);
    return command ? [{ ...w, command }] : [];
  });
  return (
    <article className="grid gap-3 rounded-lg border bg-card p-4" aria-label={`${h.stream}.${h.column}`}>
      <div className="flex flex-wrap items-center gap-x-3 gap-y-1.5">
        <span className="font-mono text-[0.8125rem]">
          <span className="text-muted-foreground">{h.stream}.</span>
          <span className="font-medium">{h.column}</span>
        </span>
        <Badge variant="outline" className="font-normal tabular-nums">
          {heldWhat(h)}
        </Badge>
        <HoldStateBadge state={state} />
        <span className="text-[0.8125rem] text-muted-foreground sm:ml-auto">
          Since epoch {String(h.sinceGeneration)}/{String(h.sinceEpoch)}
          {started && (
            <>
              , <RelativeTime date={started} />
            </>
          )}
        </span>
      </div>
      {h.reason && <p>{h.reason}</p>}
      {refusal && (
        <p className="flex gap-2 text-degraded">
          <TriangleAlert className="mt-0.5 size-4 shrink-0" aria-hidden />
          <span>{refusal}</span>
        </p>
      )}
      {ways.length > 0 && (
        <div className="grid gap-2">
          <h3 className="text-[0.8125rem] font-medium text-muted-foreground">To clear it</h3>
          <ul className="grid gap-3">
            {ways.map((w, i) => (
              <li key={w.action} className="grid gap-1.5">
                {i > 0 && <span className="text-xs text-muted-foreground uppercase">or</span>}
                {w.first && <p>{w.first}</p>}
                {w.statement && <CodeBlock>{w.statement}</CodeBlock>}
                <CommandLine command={w.command} className="max-w-2xl" />
                {w.effect && <p className="text-[0.8125rem] text-muted-foreground">{w.effect}</p>}
              </li>
            ))}
          </ul>
        </div>
      )}
    </article>
  );
}
