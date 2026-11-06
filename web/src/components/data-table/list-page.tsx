import type { ReactNode } from "react";
import type { Row, Table } from "@tanstack/react-table";
import { Button } from "@/components/ui/button";
import { PageHeader } from "@/components/page-header";
import { QueryError } from "@/components/query-error";
import { DataTable } from "./data-table";
import { Pagination } from "./pagination";

/** A page that lists one kind of resource: its header, toolbar, table and pages, and its loading, error and empty states. */
export function ListPage<T>({
  title,
  count,
  actions,
  toolbar,
  table,
  loading,
  error,
  noun,
  empty,
  onRowClick,
  onReset,
  footer,
}: {
  title: string;
  /** Before filtering; unset while loading. */
  count?: number;
  actions?: ReactNode;
  toolbar?: ReactNode;
  table: Table<T>;
  loading: boolean;
  error: unknown;
  noun: [one: string, many: string];
  /** Shown instead of the table when there's nothing to list. */
  empty: ReactNode;
  onRowClick?: (row: Row<T>) => void;
  /** Clears the filters, offered when they hide every row. */
  onReset?: () => void;
  /** Below the pages, such as a button that loads older rows. */
  footer?: ReactNode;
}) {
  if (count === 0) {
    return (
      <div className="grid gap-6">
        <PageHeader title={title} actions={actions} />
        {empty}
      </div>
    );
  }
  return (
    <div className="grid gap-4">
      <PageHeader title={title} count={count} actions={actions} />
      {toolbar}
      {error ? (
        <QueryError title={`Couldn't list ${noun[1]}`} error={error} />
      ) : (
        <>
          <DataTable
            table={table}
            loading={loading}
            onRowClick={onRowClick}
            empty={
              <span className="inline-flex items-center gap-2">
                No {noun[1]} match.
                {onReset && (
                  <Button variant="link" size="sm" className="h-auto p-0" onClick={onReset}>
                    Reset the filters
                  </Button>
                )}
              </span>
            }
          />
          <Pagination table={table} noun={noun} />
          {footer}
        </>
      )}
    </div>
  );
}
