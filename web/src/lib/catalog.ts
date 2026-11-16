import type { CatalogPackage, CatalogRole, ConfigField, ConnectorSource } from "@/gen/openetl/api/v1/connector_pb";
import { lastSegment } from "./names";

// The connector catalog: the packages connected sources list, as their
// publishers describe them, until the org applies one, and the packages the
// org applied that no source lists.

export const roleLabel = (role: string) => (role === "source" ? "Source" : role === "destination" ? "Destination" : role);

/** Official packages first, then by title. */
export function sortPackages(pkgs: CatalogPackage[]) {
  return [...pkgs].sort(
    (a, b) =>
      Number(b.official) - Number(a.official) ||
      a.title.localeCompare(b.title, undefined, { sensitivity: "base" }) ||
      a.package.localeCompare(b.package) ||
      a.source.localeCompare(b.source),
  );
}

/** What the search box matches in a package. */
export const packageText = (p: CatalogPackage) => [p.title, p.package, p.description, p.author, ...p.tags];

/** "official" for connectorSources/official. */
export const sourceId = (name: string) => lastSegment(name);

/** What the console calls a source: its name, but for the one OpenETL connects, whose name says less than its Official badge. */
export const sourceLabel = (name: string) => {
  const id = sourceId(name);
  return id === "official" ? "OpenETL" : id;
};

/** A manual package's value in the Source filter, which no source's name can be: they're lowercase, without spaces. */
export const manualLabel = "Manually installed";

/** The value of the Source filter a package falls under. */
export const packageSource = (p: CatalogPackage) => (p.manual ? manualLabel : sourceLabel(p.source));

/** The command that applies the package's listed version. */
export const applyCommand = (p: CatalogPackage) => `openetl connector apply ${p.reference}`;

/** Whether the org has applied a version at least as late as the listed one, an earlier one, or none. */
export function appliedState(p: CatalogPackage): "current" | "older" | "none" {
  if (!p.appliedVersion) return "none";
  return p.updateAvailable ? "older" : "current";
}

/** The sources whose last listing failed, which keep their earlier packages meanwhile. */
export const failingSources = (sources: ConnectorSource[]) => sources.filter((s) => s.error);

/** A config field's row in its section: its depth under the field above it there, and its label, its path if its parent is in another section. */
export type ConfigRow = { field: ConfigField; depth: number; label: string };

export type ConfigSection = { id: string; title: string; description: string; rows: ConfigRow[] };

/**
 * A role's config fields by section, in the schema's order with an object's advanced fields last: first those in no group the role
 * declares, then each group's. A nested field is in its parent's group unless it has its own.
 */
export function configSections(role: CatalogRole): ConfigSection[] {
  const known = new Set(role.groups.map((g) => g.id));
  const rows = new Map<string, ConfigRow[]>();
  const walk = (fields: ConfigField[], parent?: { group: string; section: string; depth: number }) => {
    for (const field of [...fields.filter((f) => !f.advanced), ...fields.filter((f) => f.advanced)]) {
      const group = field.group || parent?.group || "";
      const section = known.has(group) ? group : "";
      const under = parent?.section === section;
      const depth = under ? parent.depth + 1 : 0;
      const label = parent && !under ? field.path || field.name : field.name;
      rows.set(section, [...(rows.get(section) ?? []), { field, depth, label }]);
      walk(field.fields, { group, section, depth });
    }
  };
  walk(role.config);
  return [{ id: "", title: "", description: "" }, ...role.groups]
    .map((g) => ({ id: g.id, title: g.title, description: g.description, rows: rows.get(g.id) ?? [] }))
    .filter((s) => s.rows.length > 0);
}
