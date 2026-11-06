import type { ReactNode } from "react";
import { Search, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { InputGroup, InputGroupAddon, InputGroupInput } from "@/components/ui/input-group";

/** Above a table: a search box, its facet filters, and a reset once anything filters. */
export function Toolbar({
  search,
  onSearch,
  placeholder,
  filtered,
  onReset,
  children,
}: {
  search: string;
  onSearch: (q: string) => void;
  placeholder: string;
  filtered: boolean;
  onReset: () => void;
  /** The facet filters. */
  children?: ReactNode;
}) {
  return (
    <div className="flex flex-wrap items-center gap-2">
      <InputGroup className="h-8 w-full sm:w-64">
        <InputGroupAddon>
          <Search aria-hidden />
        </InputGroupAddon>
        <InputGroupInput type="search" value={search} onChange={(e) => onSearch(e.target.value)} placeholder={placeholder} aria-label={placeholder} />
      </InputGroup>
      {children}
      {filtered && (
        <Button variant="ghost" size="sm" onClick={onReset}>
          Reset
          <X aria-hidden />
        </Button>
      )}
    </div>
  );
}
