import { useEffect, useState } from "react";
import {
  getCoreRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  useReactTable,
  type ColumnDef,
  type SortingState,
} from "@tanstack/react-table";

/**
 * A sortable table, paged unless `paged` is false (for tables that show every
 * row, without a Pagination below). It goes back to the first page when
 * `resetKey` changes, not when the data refreshes.
 */
export function useDataTable<T>({
  data,
  columns,
  sort,
  resetKey,
  paged = true,
}: {
  data: T[];
  columns: ColumnDef<T>[];
  sort?: SortingState;
  resetKey?: string;
  paged?: boolean;
}) {
  const [sorting, setSorting] = useState<SortingState>(sort ?? []);
  const table = useReactTable({
    data,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getPaginationRowModel: paged ? getPaginationRowModel() : undefined,
    autoResetPageIndex: false,
    initialState: { pagination: { pageSize: 25 } },
  });
  useEffect(() => table.setPageIndex(0), [table, resetKey]);
  return table;
}
