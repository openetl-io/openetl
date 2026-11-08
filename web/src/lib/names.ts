// Resource names, as the API spells them.

export const pipelineName = (workspace: string, pipeline: string) =>
  `workspaces/${workspace}/pipelines/${pipeline}`;

export const syncName = (workspace: string, pipeline: string, sync: string) =>
  `${pipelineName(workspace, pipeline)}/syncs/${sync}`;

export const sourceName = (workspace: string, source: string) => `workspaces/${workspace}/sources/${source}`;

export const destinationName = (workspace: string, destination: string) =>
  `workspaces/${workspace}/destinations/${destination}`;

export const clusterName = (workspace: string, kind: "spark" | "flink", cluster: string) =>
  `workspaces/${workspace}/${kind}Clusters/${cluster}`;

/** The last segment of a resource name. */
export const lastSegment = (name: string) => name.slice(name.lastIndexOf("/") + 1);

/** A digest or version cut to something a person can compare at a glance. */
export function shortDigest(d: string, n = 12) {
  const v = d.startsWith("sha256:") ? d.slice(7) : d;
  return v.length > n ? v.slice(0, n) : v;
}

/** A stream's key in the schemas tab's URL: a transform's output's is o/{name}. */
export function schemaKey(s: { stream: string; output: boolean }) {
  return s.output ? `o/${s.stream}` : s.stream;
}
