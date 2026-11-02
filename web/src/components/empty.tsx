import type { ReactNode } from "react";
import { errorSummary } from "@/lib/format";
import { cn } from "@/lib/utils";

/** What to do next, and in P1 that's usually a command to run. */
export function Empty({
  title,
  children,
  command,
  className,
}: {
  title: string;
  children?: ReactNode;
  command?: string;
  className?: string;
}) {
  return (
    <div className={cn("grid justify-items-start gap-2 rounded-lg border border-dashed p-6", className)}>
      <p className="font-medium">{title}</p>
      {children && <div className="max-w-prose text-muted-foreground">{children}</div>}
      {command && (
        <pre className="mt-1 max-w-full overflow-x-auto rounded-md bg-muted px-3 py-2 font-mono text-[0.8125rem]">
          <span className="text-muted-foreground select-none">$ </span>
          {command}
        </pre>
      )}
    </div>
  );
}

/** An error's cause up front, and the whole message on request when it says more. */
export function ErrorNote({ title, message, className }: { title: string; message: string; className?: string }) {
  const summary = errorSummary(message);
  return (
    <div role="alert" className={cn("grid gap-1 rounded-lg border border-failed/40 bg-failed/5 p-4", className)}>
      <p className="font-medium text-failed">{title}</p>
      <p className="font-mono text-[0.8125rem] break-words whitespace-pre-wrap">{summary}</p>
      {summary !== message && (
        <details className="text-[0.8125rem]">
          <summary className="w-fit cursor-pointer text-muted-foreground hover:text-foreground">The whole message</summary>
          <p className="mt-1 font-mono break-words whitespace-pre-wrap text-muted-foreground">{message}</p>
        </details>
      )}
    </div>
  );
}
