import type { ReactNode } from "react";
import { Skeleton } from "@/components/ui/skeleton";
import { cn } from "@/lib/utils";

/** A resource page's title, its status, its actions, and its key facts below. */
export function DetailHeader({
  title,
  status,
  actions,
  children,
}: {
  title: string;
  status?: ReactNode;
  actions?: ReactNode;
  /** The facts, as DescriptionItems; a skeleton while unset. */
  children?: ReactNode;
}) {
  return (
    <div className="grid gap-4">
      <div className="flex flex-wrap items-center justify-between gap-x-4 gap-y-2">
        <div className="flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1">
          <h1 className="text-2xl font-semibold tracking-tight break-all">{title}</h1>
          {status}
        </div>
        {actions && <div className="flex items-center gap-2">{actions}</div>}
      </div>
      {children ? <DescriptionList>{children}</DescriptionList> : <Skeleton className="h-10 w-full max-w-3xl" />}
    </div>
  );
}

export function DescriptionList({ children, className }: { children: ReactNode; className?: string }) {
  return <dl className={cn("grid grid-cols-2 gap-x-8 gap-y-3 sm:grid-cols-3 lg:flex lg:flex-wrap", className)}>{children}</dl>;
}

export function DescriptionItem({ label, children, className }: { label: string; children: ReactNode; className?: string }) {
  return (
    <div className={cn("grid min-w-0 content-start gap-0.5", className)}>
      <dt className="text-[0.8125rem] text-muted-foreground">{label}</dt>
      <dd className="min-w-0 truncate">{children}</dd>
    </div>
  );
}

/** A titled part of a resource page. */
export function Section({ title, actions, children }: { title: string; actions?: ReactNode; children: ReactNode }) {
  return (
    <section className="grid gap-3">
      <div className="flex items-center justify-between gap-4">
        <h2 className="text-base font-semibold">{title}</h2>
        {actions}
      </div>
      {children}
    </section>
  );
}
