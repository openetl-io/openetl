import { Check, Copy } from "lucide-react";
import { useState } from "react";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

/** A CLI command to run, with a button that copies it. */
export function CommandLine({ command, className }: { command: string; className?: string }) {
  const [copied, setCopied] = useState(false);
  const copy = () =>
    void navigator.clipboard.writeText(command).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  return (
    <div className={cn("flex items-center gap-2 rounded-md border bg-muted py-1 pr-1 pl-3 font-mono text-[0.8125rem]", className)}>
      <span className="text-muted-foreground select-none">$</span>
      <code className="min-w-0 flex-1 overflow-x-auto whitespace-nowrap">{command}</code>
      <Button variant="ghost" size="icon-sm" onClick={copy} aria-label={copied ? "Copied" : "Copy the command"}>
        {copied ? <Check /> : <Copy />}
      </Button>
    </div>
  );
}
