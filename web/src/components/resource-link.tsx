import { Link } from "@tanstack/react-router";

const linkClass = "hover:underline";

/** A link to a resource by its API name, workspaces/{workspace}/{collection}/{id}; plain text for kinds without a page. */
export function ResourceLink({ name, label }: { name: string; label?: string }) {
  const [, workspace = "", collection, id = ""] = name.split("/");
  const text = label ?? id;
  switch (collection) {
    case "pipelines":
      return (
        <Link to="/workspaces/$workspace/pipelines/$pipeline" params={{ workspace, pipeline: id }} search={{}} className={linkClass}>
          {text}
        </Link>
      );
    case "sources":
      return (
        <Link to="/workspaces/$workspace/sources/$source" params={{ workspace, source: id }} className={linkClass}>
          {text}
        </Link>
      );
    case "destinations":
      return (
        <Link to="/workspaces/$workspace/destinations/$destination" params={{ workspace, destination: id }} className={linkClass}>
          {text}
        </Link>
      );
    case "webhooks":
      return (
        <Link to="/workspaces/$workspace/webhooks/$webhook" params={{ workspace, webhook: id }} className={linkClass}>
          {text}
        </Link>
      );
    default:
      return <span>{text}</span>;
  }
}

/** What uses a resource: the first few as links, then how many more. */
export function UsedBy({ names, max = 3 }: { names: string[]; max?: number }) {
  if (names.length === 0) return <span className="text-muted-foreground">Nothing</span>;
  return (
    <span className="inline-flex flex-wrap gap-x-1">
      {names.slice(0, max).map((n, i) => (
        <span key={n}>
          <ResourceLink name={n} />
          {i < Math.min(names.length, max) - 1 && ","}
        </span>
      ))}
      {names.length > max && <span className="text-muted-foreground">and {names.length - max} more</span>}
    </span>
  );
}
