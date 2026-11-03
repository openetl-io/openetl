import { useMemo } from "react";
import { useQuery } from "@connectrpc/connect-query";
import { type ConnectorVersion, DestinationService, SourceService } from "@/gen/openetl/api/v1/connection_pb";
import { lastSegment } from "./names";

/** "postgres" from "postgres@1.2.0". */
export const connectorName = (ref?: string) => (ref ?? "").split("@")[0] ?? "";

export type ConnectorInfo = { connector?: string; version?: ConnectorVersion };

/** The connector each of the workspace's sources and destinations runs, by ID. */
export function useConnectors(workspace: string) {
  const parent = `workspaces/${workspace}`;
  const sources = useQuery(SourceService.method.listSources, { parent, pageSize: 500 });
  const destinations = useQuery(DestinationService.method.listDestinations, { parent, pageSize: 500 });
  return useMemo(() => {
    const s = new Map<string, ConnectorInfo>();
    const d = new Map<string, ConnectorInfo>();
    for (const x of sources.data?.sources ?? []) s.set(lastSegment(x.name), { connector: x.spec?.connector, version: x.connector });
    for (const x of destinations.data?.destinations ?? []) d.set(lastSegment(x.name), { connector: x.spec?.connector, version: x.connector });
    return { source: (id: string): ConnectorInfo => s.get(id) ?? {}, destination: (id: string): ConnectorInfo => d.get(id) ?? {} };
  }, [sources.data, destinations.data]);
}
