import type { ReactNode } from "react";
import { CircleX } from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Empty as EmptyBox, EmptyContent, EmptyDescription, EmptyHeader, EmptyTitle } from "@/components/ui/empty";
import { errorSummary } from "@/lib/format";
import { CommandLine } from "./command-line";

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
    <EmptyBox className={className ?? "border border-dashed"}>
      <EmptyHeader>
        <EmptyTitle>{title}</EmptyTitle>
        {children && <EmptyDescription>{children}</EmptyDescription>}
      </EmptyHeader>
      {command && (
        <EmptyContent>
          <CommandLine command={command} className="w-full max-w-sm" />
        </EmptyContent>
      )}
    </EmptyBox>
  );
}

/** An error's cause up front, and the whole message on request when it says more. */
export function ErrorNote({ title, message, className }: { title: string; message: string; className?: string }) {
  const summary = errorSummary(message);
  return (
    <Alert variant="destructive" className={className}>
      <CircleX />
      <AlertTitle>{title}</AlertTitle>
      <AlertDescription>
        <p className="font-mono text-[0.8125rem] wrap-anywhere whitespace-pre-wrap">{summary}</p>
        {summary !== message && (
          <details className="text-[0.8125rem]">
            <summary className="w-fit cursor-pointer text-muted-foreground hover:text-foreground">The whole message</summary>
            <p className="mt-1 font-mono wrap-anywhere whitespace-pre-wrap text-muted-foreground">{message}</p>
          </details>
        )}
      </AlertDescription>
    </Alert>
  );
}
