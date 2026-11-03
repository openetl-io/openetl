import { useState } from "react";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import type { ConnectorVersion } from "@/gen/openetl/api/v1/connection_pb";
import { cn } from "@/lib/utils";

/**
 * A connector's mark on a neutral chip, so it reads on either theme: its logo
 * from its metadata, or its name's first letter when it has none (built-in
 * connectors) or the image doesn't load.
 */
export function ConnectorLogo({ version, connector, className }: { version?: ConnectorVersion; connector?: string; className?: string }) {
  const [failed, setFailed] = useState<string>();
  const logo = version?.logo && failed !== version.logo ? version.logo : undefined;
  const initial = (connector ?? version?.name.split("/")[1]?.replace(/^(source|destination)-/, "") ?? "").charAt(0).toUpperCase();
  if (!logo && !initial) return null;
  return (
    <span
      aria-hidden
      className={cn(
        "inline-grid size-5 shrink-0 place-items-center rounded-sm bg-white align-[-0.3125em] ring-1 ring-black/10 dark:ring-white/10",
        className,
      )}
    >
      {logo ? (
        <img
          src={logo}
          alt=""
          loading="lazy"
          referrerPolicy="no-referrer"
          onError={() => setFailed(logo)}
          className="size-3.5 object-contain"
        />
      ) : (
        <span className="text-[0.6875rem] leading-none font-semibold text-zinc-600">{initial}</span>
      )}
    </span>
  );
}

/** "postgres 0.1.0" for a source's or destination's connector: its name without a pin, and the version it runs. */
export function connectorLabel(ref?: string, version?: ConnectorVersion): string {
  const name = (ref ?? "").split("@")[0] ?? "";
  return version?.version ? `${name} ${version.version}` : name;
}

/** A source or destination: its connector's mark and its name, with the connector on hover. */
export function ConnectorRef({ name, connector, version }: { name: string; connector?: string; version?: ConnectorVersion }) {
  const label = connectorLabel(connector, version);
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className="inline-flex max-w-full min-w-0 items-center gap-2">
          <ConnectorLogo version={version} connector={connector} />
          <span className="truncate">{name}</span>
        </span>
      </TooltipTrigger>
      {label && <TooltipContent>{label}</TooltipContent>}
    </Tooltip>
  );
}
