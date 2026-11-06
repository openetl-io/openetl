import type { ReactNode } from "react";
import { flexRender, type Row, type RowData, type Table as TableModel } from "@tanstack/react-table";
import { Skeleton } from "@/components/ui/skeleton";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { cn } from "@/lib/utils";

declare module "@tanstack/react-table" {
  interface ColumnMeta<TData extends RowData, TValue> {
    /** Added to the column's header and cells, such as to hide it at narrow widths. */
    className?: string;
    align?: "right";
  }
}

export function DataTable<T>({
  table,
  loading,
  empty,
  onRowClick,
  rowClassName,
}: {
  table: TableModel<T>;
  loading?: boolean;
  /** Shown in place of rows when there are none. */
  empty?: ReactNode;
  onRowClick?: (row: Row<T>) => void;
  rowClassName?: (row: Row<T>) => string | undefined;
}) {
  const columns = table.getVisibleLeafColumns();
  const rows = table.getRowModel().rows;
  return (
    // contain-inline-size: a wide table scrolls inside its frame instead of widening the page.
    <div className="overflow-hidden rounded-lg border bg-card contain-inline-size">
      <Table>
        <TableHeader className="bg-muted/60">
          {table.getHeaderGroups().map((group) => (
            <TableRow key={group.id} className="hover:bg-transparent">
              {group.headers.map((h) => {
                const sorted = h.column.getIsSorted();
                const meta = h.column.columnDef.meta;
                return (
                  <TableHead
                    key={h.id}
                    aria-sort={sorted === "asc" ? "ascending" : sorted === "desc" ? "descending" : undefined}
                    className={cn("h-9 px-3 text-[0.8125rem] text-muted-foreground", meta?.align === "right" && "text-right", meta?.className)}
                  >
                    {h.isPlaceholder ? null : flexRender(h.column.columnDef.header, h.getContext())}
                  </TableHead>
                );
              })}
            </TableRow>
          ))}
        </TableHeader>
        <TableBody>
          {loading ? (
            Array.from({ length: 5 }, (_, i) => (
              <TableRow key={i} className="hover:bg-transparent">
                {columns.map((c) => (
                  <TableCell key={c.id} className={cn("h-row px-3", c.columnDef.meta?.className)}>
                    <Skeleton className="h-4 w-3/4 max-w-40" />
                  </TableCell>
                ))}
              </TableRow>
            ))
          ) : rows.length ? (
            rows.map((r) => (
              <TableRow
                key={r.id}
                onClick={
                  onRowClick &&
                  ((e) => {
                    // Links and buttons in a row do their own thing, and selecting text isn't a click.
                    if ((e.target as HTMLElement).closest("a, button") || window.getSelection()?.toString()) return;
                    onRowClick(r);
                  })
                }
                className={cn(onRowClick && "cursor-pointer", rowClassName?.(r))}
              >
                {r.getVisibleCells().map((c) => {
                  const meta = c.column.columnDef.meta;
                  return (
                    <TableCell key={c.id} className={cn("h-row px-3", meta?.align === "right" && "text-right", meta?.className)}>
                      {flexRender(c.column.columnDef.cell, c.getContext())}
                    </TableCell>
                  );
                })}
              </TableRow>
            ))
          ) : (
            <TableRow className="hover:bg-transparent">
              <TableCell colSpan={columns.length} className="h-28 text-center whitespace-normal text-muted-foreground">
                {empty}
              </TableCell>
            </TableRow>
          )}
        </TableBody>
      </Table>
    </div>
  );
}
