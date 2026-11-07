import { useState } from "react";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import type { ConnectorVersion } from "@/gen/openetl/api/v1/connection_pb";
import { cn } from "@/lib/utils";

/**
 * A connector's mark on a neutral chip, so it reads on either theme: its logo
 * (the URL given, or its version's), or its name's first letter when it has
 * none (built-in connectors) or the image doesn't load.
 */
export function ConnectorLogo({
  version,
  connector,
  logo: url,
  size = "sm",
  className,
}: {
  version?: ConnectorVersion;
  /** Its name, such as postgres or acme/files, for the initial. */
  connector?: string;
  /** A logo's URL, in place of its version's. */
  logo?: string;
  size?: "sm" | "lg";
  className?: string;
}) {
  const [failed, setFailed] = useState<string>();
  const src = url || version?.logo;
  const logo = src && failed !== src ? src : undefined;
  const name = connector ?? version?.name.split("/")[2] ?? "";
  const initial = (name.split("/").pop() ?? "").replace(/^(source|destination)-/, "").charAt(0).toUpperCase();
  if (!logo && !initial) return null;
  const lg = size === "lg";
  return (
    <span
      aria-hidden
      className={cn(
        "inline-grid shrink-0 place-items-center bg-white ring-1 ring-black/10 dark:ring-white/10",
        lg ? "size-10 rounded-md" : "size-5 rounded-sm align-[-0.3125em]",
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
          className={cn("object-contain", lg ? "size-7" : "size-3.5")}
        />
      ) : (
        <span className={cn("leading-none font-semibold text-zinc-600", lg ? "text-base" : "text-[0.6875rem]")}>{initial}</span>
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
