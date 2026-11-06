import type { ReactNode } from "react";
import type { LucideIcon } from "lucide-react";
import { Empty, EmptyContent, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "@/components/ui/empty";
import { cn } from "@/lib/utils";
import { CommandLine } from "./command-line";

/** Nothing here yet, and in the alpha that's usually a CLI command away. */
export function EmptyState({
  icon: Icon,
  title,
  description,
  command,
  className,
}: {
  icon: LucideIcon;
  title: string;
  description?: ReactNode;
  command?: string;
  className?: string;
}) {
  return (
    <Empty className={cn("border border-dashed", className)}>
      <EmptyHeader>
        <EmptyMedia variant="icon">
          <Icon />
        </EmptyMedia>
        <EmptyTitle>{title}</EmptyTitle>
        {description && <EmptyDescription>{description}</EmptyDescription>}
      </EmptyHeader>
      {command && (
        <EmptyContent>
          <CommandLine command={command} className="w-full max-w-sm" />
        </EmptyContent>
      )}
    </Empty>
  );
}
