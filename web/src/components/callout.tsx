import type { ReactNode } from "react";
import { Info, TriangleAlert } from "lucide-react";

const tones = {
  problem: "flex gap-2 text-failed",
  warning: "flex gap-2 text-degraded",
  note: "flex gap-2 text-muted-foreground",
};

/** A line of a plan: a problem, a warning or a note. */
export function Callout({ tone, children }: { tone: keyof typeof tones; children: ReactNode }) {
  const Icon = tone === "note" ? Info : TriangleAlert;
  return (
    <p className={tones[tone]}>
      <Icon className="mt-0.5 size-4 shrink-0" aria-hidden />
      <span className="min-w-0">{children}</span>
    </p>
  );
}
