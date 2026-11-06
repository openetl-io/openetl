import { cn } from "@/lib/utils";

/** A message or value to read as written, such as an error. */
export function CodeBlock({ children, className }: { children: string; className?: string }) {
  return (
    <pre className={cn("rounded-md border bg-muted px-3 py-2 font-mono text-[0.8125rem] wrap-break-word whitespace-pre-wrap", className)}>
      {children}
    </pre>
  );
}
