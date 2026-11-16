import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import {
  CatalogPackageSchema,
  type CatalogRole,
  CatalogRoleSchema,
  type ConfigField,
  ConfigFieldSchema,
  ConfigGroupSchema,
  ConnectorSourceSchema,
} from "@/gen/openetl/api/v1/connector_pb";
import { appliedState, applyCommand, configSections, failingSources, manualLabel, packageSource, packageText, sortPackages, sourceLabel } from "../catalog";

const pkg = (p: Partial<{ title: string; package: string; source: string; official: boolean; version: string; appliedVersion: string; updateAvailable: boolean; tags: string[] }>) =>
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
    expect(appliedState(pkg({ appliedVersion: "0.9.0", updateAvailable: true }))).toBe("older");
    expect(appliedState(pkg({ appliedVersion: "1.1.0" }))).toBe("current");
  });

  it("files a manual package under its own value of the Source filter", () => {
    expect(packageSource(pkg({ source: "connectorSources/acme" }))).toBe("acme");
    expect(packageSource(create(CatalogPackageSchema, { manual: true }))).toBe(manualLabel);
  });

  it("calls the official source OpenETL, and others by their names", () => {
    expect(sourceLabel("connectorSources/official")).toBe("OpenETL");
    expect(sourceLabel("connectorSources/acme")).toBe("acme");
    expect(packageSource(pkg({}))).toBe("OpenETL");
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

describe("configSections", () => {
  const field = (path: string, o: Partial<{ group: string; advanced: boolean; fields: ConfigField[] }> = {}) =>
    create(ConfigFieldSchema, { name: path.slice(path.lastIndexOf(".") + 1), path, ...o });
  const role = (config: ConfigField[]) =>
    create(CatalogRoleSchema, {
      config,
      groups: [create(ConfigGroupSchema, { id: "connection", title: "Connection" }), create(ConfigGroupSchema, { id: "reading", title: "Reading" })],
    });
  const shown = (r: CatalogRole) => configSections(r).map((s) => [s.id, s.rows.map((x) => `${x.depth}:${x.label}`)]);

  it("puts a nested field in its own group, by its path, and the rest in their parent's", () => {
    const consumer = field("consumer", {
      group: "connection",
      fields: [field("consumer.name"), field("consumer.interval", { group: "reading", fields: [field("consumer.interval.jitter")] })],
    });
    expect(shown(role([field("topic", { group: "connection" }), consumer]))).toEqual([
      ["connection", ["0:topic", "0:consumer", "1:name"]],
      ["reading", ["0:consumer.interval", "1:jitter"]],
    ]);
  });

  it("puts the fields in no group the role declares first, and an object's advanced fields last", () => {
    const tls = field("tls", { fields: [field("tls.ca", { advanced: true }), field("tls.mode")] });
    expect(shown(role([field("region", { group: "nowhere" }), tls, field("stream", { group: "reading" })]))).toEqual([
      ["", ["0:region", "0:tls", "1:mode", "1:ca"]],
      ["reading", ["0:stream"]],
    ]);
  });

  it("starts a field back in an earlier section at the left", () => {
    const a = field("a", { group: "connection", fields: [field("a.b", { group: "reading", fields: [field("a.b.c", { group: "connection" })] })] });
    expect(shown(role([a]))).toEqual([
      ["connection", ["0:a", "0:a.b.c"]],
      ["reading", ["0:a.b"]],
    ]);
  });
});
