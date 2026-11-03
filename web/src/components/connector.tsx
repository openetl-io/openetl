import { useState } from "react";

import type { ConnectorVersion } from "@/gen/openetl/api/v1/connection_pb";
import { cn } from "@/lib/utils";

/** A connector version's logo, from its metadata. Nothing when it has none or the image doesn't load. */
export function ConnectorLogo({ version, className }: { version?: ConnectorVersion; className?: string }) {
  const [failed, setFailed] = useState<string>();
  if (!version?.logo || failed === version.logo) return null;
  return (
    <img
      src={version.logo}
      alt=""
      loading="lazy"
      referrerPolicy="no-referrer"
      onError={() => setFailed(version.logo)}
      className={cn("inline-block size-4 shrink-0 rounded-sm object-contain align-[-0.1875em]", className)}
    />
  );
}

/** "postgres 0.1.0" for a source's or destination's connector: its name without a pin, and the version it runs. */
export function connectorLabel(ref?: string, version?: ConnectorVersion): string {
  const name = (ref ?? "").split("@")[0] ?? "";
  return version?.version ? `${name} ${version.version}` : name;
}

function connectorTitle(version?: ConnectorVersion): string | undefined {
  if (!version) return undefined;
  return [version.author && `By ${version.author}`, version.releaseTag && `released as ${version.releaseTag}`].filter(Boolean).join(", ") || undefined;
}

/** The connector's label, linked to its release when its metadata names one. */
export function ConnectorName({ connector, version }: { connector?: string; version?: ConnectorVersion }) {
  const label = connectorLabel(connector, version);
  if (!version?.releaseUrl) return <span title={connectorTitle(version)}>{label}</span>;
  return (
    <a
      href={version.releaseUrl}
      target="_blank"
      rel="noreferrer noopener"
      title={connectorTitle(version)}
      className="underline decoration-border underline-offset-2 hover:decoration-foreground"
    >
      {label}
    </a>
  );
}
