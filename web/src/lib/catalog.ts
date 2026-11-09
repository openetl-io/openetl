import type { CatalogPackage, ConnectorSource } from "@/gen/openetl/api/v1/connector_pb";
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

/** A manual package's value in the Source filter, which no source's name can be: they're lowercase, without spaces. */
export const manualLabel = "Manually installed";

/** The value of the Source filter a package falls under. */
export const packageSource = (p: CatalogPackage) => (p.manual ? manualLabel : sourceId(p.source));

/** The command that applies the package's listed version. */
export const applyCommand = (p: CatalogPackage) => `openetl connector apply ${p.reference}`;

/** Whether the org runs the listed version, an older one, or none. */
export function appliedState(p: CatalogPackage): "current" | "older" | "none" {
  if (!p.appliedVersion) return "none";
  return p.appliedVersion === p.version ? "current" : "older";
}

/** The sources whose last listing failed, which keep their earlier packages meanwhile. */
export const failingSources = (sources: ConnectorSource[]) => sources.filter((s) => s.error);
