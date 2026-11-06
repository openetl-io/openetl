import type { Column } from "@tanstack/react-table";
import { ArrowDown, ArrowUp, ChevronsUpDown } from "lucide-react";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

/** A column header that sorts by its column. */
export function SortHeader<T>({ column, title }: { column: Column<T>; title: string }) {
  const sorted = column.getIsSorted();
  const Icon = sorted === "asc" ? ArrowUp : sorted === "desc" ? ArrowDown : ChevronsUpDown;
  return (
    <Button
      variant="ghost"
      size="sm"
      onClick={column.getToggleSortingHandler()}
      className={cn("-ml-2 h-7 gap-1 px-2 font-medium text-inherit", sorted && "text-foreground")}
    >
      {title}
      <Icon className={cn("size-3.5", !sorted && "opacity-40")} aria-hidden />
    </Button>
  );
}
