import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import { CatalogPackageSchema, ConnectorSourceSchema } from "@/gen/openetl/api/v1/connector_pb";
import { appliedState, applyCommand, failingSources, manualLabel, packageSource, packageText, sortPackages } from "../catalog";

const pkg = (p: Partial<{ title: string; package: string; source: string; official: boolean; version: string; appliedVersion: string; tags: string[] }>) =>
  create(CatalogPackageSchema, { title: p.package, version: "1.0.0", source: "connectorSources/official", ...p });

describe("catalog", () => {
  it("puts official packages first, then sorts by title", () => {
    const sorted = sortPackages([
      pkg({ package: "acme/zendesk", title: "Zendesk" }),
      pkg({ package: "openetl/postgres", title: "PostgreSQL", official: true }),
      pkg({ package: "acme/airtable", title: "airtable" }),
      pkg({ package: "openetl/kafka", title: "Apache Kafka", official: true }),
    ]);
    expect(sorted.map((p) => p.package)).toEqual(["openetl/kafka", "openetl/postgres", "acme/airtable", "acme/zendesk"]);
  });

  it("tells the listed version from an older applied one", () => {
    expect(appliedState(pkg({}))).toBe("none");
    expect(appliedState(pkg({ appliedVersion: "1.0.0" }))).toBe("current");
    expect(appliedState(pkg({ appliedVersion: "0.9.0" }))).toBe("older");
  });

  it("files a manual package under its own value of the Source filter", () => {
    expect(packageSource(pkg({ source: "connectorSources/acme" }))).toBe("acme");
    expect(packageSource(create(CatalogPackageSchema, { manual: true }))).toBe(manualLabel);
  });

  it("applies by the listed reference", () => {
    const p = create(CatalogPackageSchema, { reference: "oci://ghcr.io/openetl-io/openetl-connectors/postgres:1.0.0" });
    expect(applyCommand(p)).toBe("openetl connector apply oci://ghcr.io/openetl-io/openetl-connectors/postgres:1.0.0");
  });

  it("searches titles, names and tags", () => {
    expect(packageText(pkg({ package: "acme/files", title: "Files", tags: ["storage"] }))).toEqual(["Files", "acme/files", "", "", "storage"]);
  });

  it("finds the sources that failed to list", () => {
    const s = [create(ConnectorSourceSchema, { name: "connectorSources/a" }), create(ConnectorSourceSchema, { name: "connectorSources/b", error: "401" })];
    expect(failingSources(s).map((x) => x.name)).toEqual(["connectorSources/b"]);
  });
});
