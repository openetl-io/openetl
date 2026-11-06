import type { ReactNode } from "react";
import { PlusCircle } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Command, CommandEmpty, CommandGroup, CommandInput, CommandItem, CommandList, CommandSeparator } from "@/components/ui/command";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Separator } from "@/components/ui/separator";

export type FacetOption = { value: string; label: string; icon?: ReactNode; count: number };

/** A toolbar button that filters by one facet's values, with how many rows have each. */
export function FacetedFilter({
  title,
  options,
  selected,
  onChange,
}: {
  title: string;
  options: FacetOption[];
  selected: string[];
  onChange: (values: string[]) => void;
}) {
  const chosen = new Set(selected);
  const toggle = (v: string) => {
    const next = new Set(chosen);
    if (!next.delete(v)) next.add(v);
    onChange(options.map((o) => o.value).filter((o) => next.has(o)));
  };
  return (
    <Popover>
      <PopoverTrigger asChild>
        <Button variant="outline" size="sm" className="border-dashed">
          <PlusCircle aria-hidden />
          {title}
          {chosen.size > 0 && (
            <>
              <Separator orientation="vertical" className="mx-0.5 data-[orientation=vertical]:h-4" />
              {chosen.size > 2 ? (
                <Badge variant="secondary" className="rounded-sm px-1 font-normal">
                  {chosen.size} selected
                </Badge>
              ) : (
                options
                  .filter((o) => chosen.has(o.value))
                  .map((o) => (
                    <Badge key={o.value} variant="secondary" className="rounded-sm px-1 font-normal">
                      {o.label}
                    </Badge>
                  ))
              )}
            </>
          )}
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-56 p-0" align="start">
        <Command>
          {options.length > 7 && <CommandInput placeholder={title} />}
          <CommandList>
            <CommandEmpty>Nothing matches.</CommandEmpty>
            <CommandGroup>
              {options.map((o) => (
                <CommandItem key={o.value} value={o.label} onSelect={() => toggle(o.value)}>
                  <Checkbox checked={chosen.has(o.value)} tabIndex={-1} aria-hidden className="pointer-events-none" />
                  {o.icon}
                  <span className="truncate">{o.label}</span>
                  <span className="ml-auto font-mono text-xs text-muted-foreground tabular-nums">{o.count}</span>
                </CommandItem>
              ))}
            </CommandGroup>
            {chosen.size > 0 && (
              <>
                <CommandSeparator />
                <CommandGroup>
                  <CommandItem onSelect={() => onChange([])} className="justify-center">
                    Clear
                  </CommandItem>
                </CommandGroup>
              </>
            )}
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  );
}
