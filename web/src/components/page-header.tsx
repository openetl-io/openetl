import type { ReactNode } from "react";

/** A page's title, how many things it lists, and its actions. */
export function PageHeader({ title, count, actions }: { title: string; count?: number; actions?: ReactNode }) {
  return (
    <div className="flex flex-wrap items-center justify-between gap-x-4 gap-y-2">
      <h1 className="flex items-baseline gap-2 text-2xl font-semibold tracking-tight">
        {title}
        {count !== undefined && <span className="text-base font-normal text-muted-foreground tabular-nums">{count}</span>}
      </h1>
      {actions && <div className="flex items-center gap-2">{actions}</div>}
    </div>
  );
}
