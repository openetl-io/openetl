import { useEffect, useState } from "react";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { absolute, relative } from "@/lib/format";
import { cn } from "@/lib/utils";

function useNow(every = 30_000) {
  const [now, setNow] = useState(Date.now);
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), every);
    return () => clearInterval(t);
  }, [every]);
  return now;
}

export function RelativeTime({ date, className }: { date: Date; className?: string }) {
  const now = useNow();
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <time dateTime={date.toISOString()} className={cn("whitespace-nowrap tabular", className)}>
          {relative(date, now)}
        </time>
      </TooltipTrigger>
      <TooltipContent>{absolute(date)}</TooltipContent>
    </Tooltip>
  );
}
