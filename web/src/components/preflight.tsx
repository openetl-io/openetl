import { useMutation } from "@connectrpc/connect-query";
import { CircleCheck, CircleMinus, CircleX, ListChecks, LoaderCircle, type LucideIcon } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle, SheetTrigger } from "@/components/ui/sheet";
import { Skeleton } from "@/components/ui/skeleton";
import { CheckItemList, FailureNote } from "@/components/check-result";
import { ErrorNote } from "@/components/empty";
import { PipelineService, type PreflightPart, type PreflightResult } from "@/gen/openetl/api/v1/pipeline_pb";
import { errorMessage } from "@/lib/api";
import { checkCounts, itemFailed, partFailed, preflightFailed } from "@/lib/checks";
import { cn } from "@/lib/utils";

const stageLabel: Record<string, string> = { check: "Connection", preflight: "Pipeline's streams", plan: "Plan" };

function partTitle(p: PreflightPart): string {
  if (p.role === "sync") return "Sync plan";
  const name = p.resource.slice(p.resource.lastIndexOf("/") + 1);
  return `${p.role === "source" ? "Source" : "Destination"} ${name}: ${stageLabel[p.stage]?.toLowerCase() ?? p.stage}`;
}

function PartMark({ part }: { part: PreflightPart }) {
  const [Icon, className, label]: [LucideIcon, string, string] = partFailed(part)
    ? [CircleX, "text-failed", "Failed"]
    : part.skipped
      ? [CircleMinus, "text-muted-foreground", "Skipped"]
      : [CircleCheck, "text-healthy", "Passed"];
  return (
    <>
      <Icon className={cn("size-4 shrink-0", className)} aria-hidden />
      <span className="sr-only">{label}: </span>
    </>
  );
}

function Part({ part }: { part: PreflightPart }) {
  const counts = checkCounts(part.items);
  const unexplained = partFailed(part) && !part.items.some(itemFailed);
  const what = part.skipped
    ? `Skipped: ${part.skipped}.`
    : part.items.length > 0
      ? `${counts.charAt(0).toUpperCase()}${counts.slice(1)}.`
      : unexplained
        ? ""
        : part.stage === "preflight"
          ? "Passed. The connector has no checks of its own for a pipeline."
          : "Passed.";
  return (
    <section className="grid gap-2">
      <h3 className="flex items-center gap-2 text-sm font-medium">
        <PartMark part={part} />
        {partTitle(part)}
      </h3>
      {unexplained && <FailureNote title="Failed" code={part.code} message={part.message} />}
      {what && <p className="text-sm break-words text-muted-foreground">{what}</p>}
      {part.items.length > 0 && <CheckItemList items={part.items} />}
    </section>
  );
}

/** A pipeline's preflight: each part, in the order it ran. */
export function PreflightView({ result }: { result: PreflightResult }) {
  return (
    <div className="grid gap-6">
      {result.parts.map((p, i) => (
        <Part key={`${i}:${p.role}/${p.stage}`} part={p} />
      ))}
    </div>
  );
}

/** Preflight, for the pipeline's actions: runs it, and shows what it found in a sheet. */
export function PreflightButton({ name }: { name: string }) {
  const preflight = useMutation(PipelineService.method.preflightPipeline);
  const result = preflight.data?.result;
  return (
    <Sheet onOpenChange={(open) => open && !preflight.isPending && preflight.mutate({ name })}>
      <SheetTrigger asChild>
        <Button size="sm" variant="outline">
          <ListChecks aria-hidden />
          Preflight
        </Button>
      </SheetTrigger>
      <SheetContent className="w-full overflow-y-auto sm:max-w-xl">
        <SheetHeader>
          <SheetTitle>Preflight</SheetTitle>
          <SheetDescription>
            Checks the source's and the destination's connections, plans the sync, then asks each connector whether the streams this pipeline
            reads and writes would work. Nothing is read or written.
          </SheetDescription>
        </SheetHeader>
        <div className="grid gap-4 px-4 pb-6">
          {preflight.isPending ? (
            <div className="grid gap-3" aria-busy>
              <p className="inline-flex items-center gap-2 text-sm text-muted-foreground">
                <LoaderCircle className="size-4 animate-spin" aria-hidden />
                Running on a runner of the pipeline's data plane…
              </p>
              <Skeleton className="h-24 w-full" />
              <Skeleton className="h-24 w-full" />
            </div>
          ) : preflight.error ? (
            <ErrorNote title="Couldn't run the preflight" message={errorMessage(preflight.error)} />
          ) : (
            result && (
              <>
                <p className={cn("text-sm font-medium", preflightFailed(result) ? "text-failed" : "text-healthy")}>
                  {preflightFailed(result) ? "The pipeline wouldn't run as it is." : "The pipeline would run."}
                </p>
                <PreflightView result={result} />
                <Button variant="outline" size="sm" className="justify-self-start" onClick={() => preflight.mutate({ name })}>
                  Run again
                </Button>
              </>
            )
          )}
        </div>
      </SheetContent>
    </Sheet>
  );
}
