package spec

import (
	"bytes"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
)

const descriptor = `
apiVersion: openetl.io/v1alpha1
kind: Connector
metadata:
  name: postgres
  version: 0.1.0
  author: Example
  logo: https://example.com/postgres.svg
  release: {tag: destination-postgres/v0.1.0, url: https://example.com/releases/destination-postgres/v0.1.0}
spec:
  binaries:
    linux/amd64: {url: https://example.com/destination-postgres_linux_amd64, digest: sha256:0000000000000000000000000000000000000000000000000000000000000000}
    darwin/arm64: {url: https://example.com/destination-postgres_darwin_arm64}
  protocol: v1
  kind: destination
  egress: {tier: raw}
  capabilities:
    class: stage_and_apply
    scopes: [pipeline, stream]
    dedupeAtApply: true
    defaultCommitInterval: 10s
    maxCommitInterval: 7m
    maxIdentifierLength: 63
    writeModes: [append, overwrite, merge]
  config:
    type: object
    required: [dsn]
    properties:
      dsn: {type: string, x-secret: true}
`

func TestParseDescriptor(t *testing.T) {
	doc, err := Parse([]byte(descriptor))
	if err != nil {
		t.Fatal(err)
	}
	if doc.Kind != KindConnector || doc.Name != "openetl/postgres" || doc.Version != "0.1.0" {
		t.Fatalf("doc = %+v", doc)
	}
	wantInfo := Info{Author: "Example", Logo: "https://example.com/postgres.svg", ReleaseTag: "destination-postgres/v0.1.0", ReleaseURL: "https://example.com/releases/destination-postgres/v0.1.0"}
	if !reflect.DeepEqual(doc.Info, wantInfo) {
		t.Errorf("info = %+v", doc.Info)
	}
	if k := doc.Descriptor.Kinds(); len(k) != 1 || k[0] != RoleDestination {
		t.Fatalf("a v1alpha1 descriptor has roles %v", k)
	}
	role := doc.Descriptor.Roles[RoleDestination]
	if u := doc.Descriptor.Unpinned(); len(u) != 1 || u[0] != "destination/darwin/arm64" || role.Binaries["linux/amd64"].Digest == "" {
		t.Errorf("binaries %v, unpinned %v", role.Binaries, u)
	}
	want := &protocolv1.SpecResponse{
		ProtocolVersion: "v1",
		Kind:            protocolv1.ConnectorKind_CONNECTOR_KIND_DESTINATION,
		Egress:          &protocolv1.Egress{Tier: protocolv1.EgressTier_EGRESS_TIER_RAW},
		ConfigSchema:    []byte(`{"properties":{"dsn":{"type":"string","x-secret":true}},"required":["dsn"],"type":"object"}`),
		Destination: &protocolv1.DestinationCapabilities{
			SinkClass:             protocolv1.SinkClass_SINK_CLASS_STAGE_AND_APPLY,
			Scopes:                []protocolv1.CommitScope{protocolv1.CommitScope_COMMIT_SCOPE_PIPELINE, protocolv1.CommitScope_COMMIT_SCOPE_STREAM},
			DedupeAtApply:         true,
			DefaultCommitInterval: durationpb.New(10 * time.Second),
			MaxCommitInterval:     durationpb.New(7 * time.Minute),
			MaxIdentifierLength:   63,
			WriteModes:            []protocolv1.WriteMode{protocolv1.WriteMode_WRITE_MODE_APPEND, protocolv1.WriteMode_WRITE_MODE_OVERWRITE, protocolv1.WriteMode_WRITE_MODE_MERGE},
		},
	}
	if !proto.Equal(role.Spec, want) {
		t.Errorf("spec = %v\nwant %v", role.Spec, want)
	}

	// It's written out as v1alpha2, which reads back the same.
	y, err := MarshalDescriptor(doc.Name, doc.Version, doc.Info, doc.Descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(y), "apiVersion: "+APIVersionConnector+"\n") {
		t.Errorf("written as:\n%s", y)
	}
	info := doc.Info
	info.Repository = "https://github.com/example/postgres"
	y, err = MarshalDescriptor(doc.Name, doc.Version, info, doc.Descriptor)
	if err != nil {
		t.Fatal(err)
	}
	again, err := Parse(y)
	if err != nil {
		t.Fatalf("%v\n%s", err, y)
	}
	if !proto.Equal(again.Descriptor.Roles[RoleDestination].Spec, want) || !reflect.DeepEqual(again.Descriptor.Roles[RoleDestination].Binaries, role.Binaries) || !reflect.DeepEqual(again.Info, info) {
		t.Errorf("round trip gave %v, %+v", again.Descriptor, again.Info)
	}
	a, err := EncodeDescriptor(doc.Name, doc.Version, info, doc.Descriptor)
	if err != nil {
		t.Fatal(err)
	}
	b, err := EncodeDescriptor(again.Name, again.Version, again.Info, again.Descriptor)
	if err != nil || !bytes.Equal(a, b) {
		t.Errorf("encodings differ:\n%s\n%s", a, b)
	}
	if fromJSON, err := Parse(a); err != nil || !proto.Equal(fromJSON.Descriptor.Roles[RoleDestination].Spec, want) {
		t.Errorf("the JSON encoding parses to %v, %v", fromJSON, err)
	}

	if d := SameSpec(want, want); d != "" {
		t.Errorf("a spec differs from itself: %s", d)
	}
	reordered := proto.Clone(want).(*protocolv1.SpecResponse)
	reordered.ConfigSchema = []byte(`{"type": "object", "required": ["dsn"], "properties": {"dsn": {"x-secret": true, "type": "string"}}}`)
	if d := SameSpec(reordered, want); d != "" {
		t.Errorf("reordered keys differ: %s", d)
	}
	raw := proto.Clone(want).(*protocolv1.SpecResponse)
	raw.Egress.Tier = protocolv1.EgressTier_EGRESS_TIER_HOST
	if d := SameSpec(raw, want); !strings.Contains(d, `"tier":"host"`) {
		t.Errorf("another tier: %q", d)
	}
	schema := proto.Clone(want).(*protocolv1.SpecResponse)
	schema.ConfigSchema = []byte(`{"type":"object"}`)
	if d := SameSpec(schema, want); d != "config schema" {
		t.Errorf("another schema: %q", d)
	}
	features := proto.Clone(want).(*protocolv1.SpecResponse)
	features.RequiredFeatures = []string{"staged-v2"}
	if d := SameSpec(features, want); !strings.Contains(d, "staged-v2") {
		t.Errorf("another required feature: %q", d)
	}
}

// pkgDescriptor has every field v1alpha2 names.
const pkgDescriptor = `
apiVersion: openetl.io/v1alpha2
kind: Connector
metadata:
  name: acme/salesforce
  version: 1.2.0
  description: Salesforce objects, incrementally and through the Bulk API
  license: Apache-2.0
  repository: https://github.com/acme/openetl-salesforce
  docs: https://github.com/acme/openetl-salesforce#readme
  logo: https://example.com/salesforce.svg
  author: Acme
  tags: [crm, saas]
  release: {tag: v1.2.0, url: https://github.com/acme/openetl-salesforce/releases/tag/v1.2.0}
spec:
  roles:
    source:
      requires: {protocol: v1, features: [staged-v2], platform: 0.1.0}
      runtime:
        binaries:
          linux/amd64: {url: https://example.com/source_linux_amd64, digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111", attestation: https://example.com/att}
        image:
          ref: ghcr.io/acme/salesforce-source
          digest: "sha256:2222222222222222222222222222222222222222222222222222222222222222"
          platforms: [linux/amd64, linux/arm64]
          command: [/connector, serve]
          attestation: https://example.com/image-att
      resources: {cpu: "1", memory: 512Mi, disk: 1Gi}
      state: {version: 2, reads: 1}
      egress: {tier: proxy, allow: [login.salesforce.com]}
      acknowledges: true
      config: {type: object, properties: {auth: {type: object, properties: {client_secret: {type: string, x-secret: true}}}}}
    destination:
      requires: {protocol: v1}
      runtime:
        image: {ref: ghcr.io/acme/salesforce-destination, digest: "sha256:3333333333333333333333333333333333333333333333333333333333333333", platforms: [linux/amd64]}
      egress: {tier: host}
      setup: true
      capabilities: {class: deliver_and_ack, writeModes: [append, merge]}
      config: {type: object}
`

func TestParsePackage(t *testing.T) {
	doc, err := Parse([]byte(pkgDescriptor))
	if err != nil {
		t.Fatal(err)
	}
	wantInfo := Info{
		Description: "Salesforce objects, incrementally and through the Bulk API", License: "Apache-2.0",
		Repository: "https://github.com/acme/openetl-salesforce", Docs: "https://github.com/acme/openetl-salesforce#readme",
		Logo: "https://example.com/salesforce.svg", Author: "Acme", Tags: []string{"crm", "saas"},
		ReleaseTag: "v1.2.0", ReleaseURL: "https://github.com/acme/openetl-salesforce/releases/tag/v1.2.0",
	}
	if doc.Name != "acme/salesforce" || !reflect.DeepEqual(doc.Info, wantInfo) {
		t.Errorf("name %s, info %+v", doc.Name, doc.Info)
	}
	src, dst := doc.Descriptor.Roles[RoleSource], doc.Descriptor.Roles[RoleDestination]
	if src == nil || dst == nil {
		t.Fatalf("roles %v", doc.Descriptor.Kinds())
	}
	if src.Requires.Platform != "0.1.0" || !slices.Equal(src.Spec.GetRequiredFeatures(), []string{"staged-v2"}) || src.Spec.GetProtocolVersion() != "v1" ||
		src.Resources != (Resources{CPU: "1", Memory: "512Mi", Disk: "1Gi"}) || *src.State != (State{Version: 2, Reads: 1}) ||
		src.Image.Ref != "ghcr.io/acme/salesforce-source" || !slices.Equal(src.Image.Command, []string{"/connector", "serve"}) ||
		src.Binaries["linux/amd64"].Attestation != "https://example.com/att" || !src.Spec.GetSource().GetAcknowledges() {
		t.Errorf("source role %+v", src)
	}
	if dst.Binaries != nil || dst.Image == nil || !dst.Spec.GetSetup() || dst.Spec.GetDestination().GetSinkClass() != protocolv1.SinkClass_SINK_CLASS_DELIVER_AND_ACK {
		t.Errorf("destination role %+v", dst)
	}
	a, err := EncodeDescriptor(doc.Name, doc.Version, doc.Info, doc.Descriptor)
	if err != nil {
		t.Fatal(err)
	}
	again, err := Parse(a)
	if err != nil {
		t.Fatalf("%v\n%s", err, a)
	}
	b, err := EncodeDescriptor(again.Name, again.Version, again.Info, again.Descriptor)
	if err != nil || !bytes.Equal(a, b) {
		t.Errorf("the round trip changed it:\n%s\n%s", a, b)
	}
	for _, field := range []string{"attestation", "platforms", "command", "resources", "state", "requires", "features", "platform", "description", "license", "repository", "docs", "tags"} {
		if !strings.Contains(string(a), `"`+field+`"`) {
			t.Errorf("the encoding has no %s: %s", field, a)
		}
	}

	manifest, err := Parse([]byte(strings.Replace(pkgDescriptor[:strings.Index(pkgDescriptor, "    source:")], "acme/salesforce", "acme/api", 1) +
		"    source:\n      runtime:\n        manifest: {config: {type: object}, http: {base_url: https://example.com}, streams: []}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if m := manifest.Descriptor.Roles[RoleSource]; m.Spec != nil || !strings.Contains(string(m.Manifest), "base_url") {
		t.Errorf("a manifest role: %+v", m)
	}
}

func TestParsePackageErrors(t *testing.T) {
	only := func(role string) string { return pkgDescriptor[:strings.Index(pkgDescriptor, "    source:")] + role }
	for name, doc := range map[string]string{
		"no repository":     strings.Replace(pkgDescriptor, "  repository: https://github.com/acme/openetl-salesforce\n", "", 1),
		"no roles":          pkgDescriptor[:strings.Index(pkgDescriptor, "  roles:")] + "  roles: {}\n",
		"another role":      strings.Replace(pkgDescriptor, "    destination:", "    sink:", 1),
		"no runtime":        only("    source:\n      requires: {protocol: v1}\n      runtime: {}\n      egress: {tier: host}\n      config: {}\n"),
		"image tag":         strings.Replace(pkgDescriptor, "ref: ghcr.io/acme/salesforce-source", "ref: ghcr.io/acme/salesforce-source:1.2.0", 1),
		"image digest":      strings.Replace(pkgDescriptor, `digest: "sha256:2222`, `digest: "sha256:22`, 1),
		"image platforms":   strings.Replace(pkgDescriptor, "platforms: [linux/amd64, linux/arm64]", "platforms: []", 1),
		"cpu":               strings.Replace(pkgDescriptor, `cpu: "1"`, `cpu: one`, 1),
		"memory":            strings.Replace(pkgDescriptor, "memory: 512Mi", "memory: 512MiB", 1),
		"state reads":       strings.Replace(pkgDescriptor, "state: {version: 2, reads: 1}", "state: {version: 2, reads: 3}", 1),
		"feature":           strings.Replace(pkgDescriptor, "features: [staged-v2]", "features: [Staged]", 1),
		"platform":          strings.Replace(pkgDescriptor, "platform: 0.1.0", "platform: latest", 1),
		"protocol":          strings.Replace(pkgDescriptor, "requires: {protocol: v1}", "requires: {protocol: v2}", 1),
		"no protocol":       strings.Replace(pkgDescriptor, "requires: {protocol: v1}\n", "", 1),
		"tags":              strings.Replace(pkgDescriptor, "tags: [crm, saas]", "tags: [CRM]", 1),
		"license":           strings.Replace(pkgDescriptor, "license: Apache-2.0", "license: 'Apache 2.0, or MIT; \"see\" LICENSE'", 1),
		"docs":              strings.Replace(pkgDescriptor, "docs: https://github", "docs: http://github", 1),
		"manifest dest":     strings.Replace(pkgDescriptor, "      runtime:\n        image: {ref: ghcr.io/acme/salesforce-destination", "      runtime:\n        manifest: {a: 1}\n        image: {ref: ghcr.io/acme/salesforce-destination", 1),
		"manifest and spec": only("    source:\n      runtime:\n        manifest: {a: 1}\n      egress: {tier: host}\n"),
		"unknown field":     pkgDescriptor + "      heal: true\n",
		"v1alpha2 source":   "apiVersion: openetl.io/v1alpha2\nkind: Source\nmetadata: {name: s}\nspec: {connector: faker}\n",
		"bad owner":         strings.Replace(pkgDescriptor, "name: acme/salesforce", "name: Acme/salesforce", 1),
		"two slashes":       strings.Replace(pkgDescriptor, "name: acme/salesforce", "name: acme/x/salesforce", 1),
	} {
		if _, err := Parse([]byte(doc)); err == nil {
			t.Errorf("%s: parsed", name)
		}
	}
}

// A role whose config schema is empty keeps it through encoding.
func TestEmptyConfigSchema(t *testing.T) {
	d := &Descriptor{Roles: map[string]*Role{RoleSource: {Binaries: map[string]Binary{"linux/amd64": {URL: "https://example.com/x"}}, Spec: &protocolv1.SpecResponse{
		ProtocolVersion: "v1", Kind: protocolv1.ConnectorKind_CONNECTOR_KIND_SOURCE, Egress: &protocolv1.Egress{Tier: protocolv1.EgressTier_EGRESS_TIER_HOST}, Source: &protocolv1.SourceCapabilities{},
	}}}}
	for _, encode := range []func(string, string, Info, *Descriptor) ([]byte, error){EncodeDescriptor, MarshalDescriptor} {
		b, err := encode("x", "1.0.0", Info{Repository: "https://example.com/x"}, d)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Parse(b); err != nil {
			t.Errorf("%v\n%s", err, b)
		}
	}
	doc, err := Parse([]byte("apiVersion: openetl.io/v1alpha1\nkind: Connector\nmetadata: {name: x, version: 0.1.0}\nspec:\n  binaries: {linux/amd64: {url: 'https://x/y'}}\n  protocol: v1\n  kind: source\n  egress: {tier: raw}\n  config: {}\n"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := EncodeVersion(doc)
	if err != nil {
		t.Fatal(err)
	}
	if again, err := Parse(b); err != nil || again.APIVersion != APIVersion {
		t.Errorf("a v1alpha1 descriptor encoded: %v\n%s", err, b)
	}
}

func TestPackageNames(t *testing.T) {
	if FullName("postgres") != "openetl/postgres" || FullName("acme/x") != "acme/x" {
		t.Error("full names")
	}
	if o, n := SplitName("postgres"); o != "openetl" || n != "postgres" {
		t.Errorf("split %s %s", o, n)
	}
	if !ValidPackageName("acme/x") || !ValidPackageName("x") || ValidPackageName("acme/") || ValidPackageName("/x") || ValidPackageName("a/b/c") {
		t.Error("valid names")
	}
}

func TestParseDeclarativeSource(t *testing.T) {
	doc, err := Parse([]byte("apiVersion: openetl.io/v1alpha1\nkind: DeclarativeSource\nmetadata: {version: 1.0.0, name: acme}\nspec: {b: 1, a: [x]}\n"))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"apiVersion":"openetl.io/v1alpha1","kind":"DeclarativeSource","metadata":{"name":"acme","version":"1.0.0"},"spec":{"a":["x"],"b":1}}`
	if doc.Name != "openetl/acme" || doc.Version != "1.0.0" || string(doc.Manifest) != want {
		t.Errorf("doc = %+v, manifest %s", doc, doc.Manifest)
	}
	doc, err = Parse([]byte("apiVersion: openetl.io/v1alpha1\nkind: DeclarativeSource\nmetadata: {version: 1.0.0, name: acme, author: Acme, logo: 'https://acme.example/logo.png'}\nspec: {}\n"))
	if err != nil || !reflect.DeepEqual(doc.Info, Info{Author: "Acme", Logo: "https://acme.example/logo.png"}) {
		t.Errorf("a manifest's info: %+v, %v", doc, err)
	}
}

func TestParseConnectorErrors(t *testing.T) {
	for name, doc := range map[string]string{
		"no version":         strings.Replace(descriptor, "  version: 0.1.0\n", "", 1),
		"bad version":        strings.Replace(descriptor, "  version: 0.1.0\n", "  version: v0.1\n", 1),
		"workspace":          strings.Replace(descriptor, "  version: 0.1.0\n", "  version: 0.1.0\n  workspace: default\n", 1),
		"no binaries":        descriptor[:strings.Index(descriptor, "  binaries:")] + descriptor[strings.Index(descriptor, "  protocol:"):],
		"platform":           strings.Replace(descriptor, "darwin/arm64:", "darwin-arm64:", 1),
		"binary url":         strings.Replace(descriptor, "https://example.com/destination-postgres_darwin_arm64", "file:///tmp/x", 1),
		"binary digest":      strings.Replace(descriptor, "sha256:0000", "md5:0000", 1),
		"logo over http":     strings.Replace(descriptor, "logo: https://", "logo: http://", 1),
		"release url":        strings.Replace(descriptor, "url: https://example.com/releases", "url: ftp://example.com/releases", 1),
		"release tag":        strings.Replace(descriptor, "tag: destination-postgres/v0.1.0", "tag: 'a tag'", 1),
		"author on sources":  "apiVersion: openetl.io/v1alpha1\nkind: Source\nmetadata: {name: s, author: me}\nspec: {connector: faker}\n",
		"protocol":           strings.Replace(descriptor, "protocol: v1", "protocol: v2", 1),
		"kind":               strings.Replace(descriptor, "kind: destination", "kind: sink", 1),
		"tier":               strings.Replace(descriptor, "{tier: raw}", "{tier: any}", 1),
		"no capabilities":    descriptor[:strings.Index(descriptor, "  capabilities:")] + descriptor[strings.Index(descriptor, "  config:"):],
		"source with caps":   strings.Replace(descriptor, "kind: destination", "kind: source", 1),
		"acknowledges":       strings.Replace(descriptor, "egress: {tier: raw}", "egress: {tier: raw}\n  acknowledges: true", 1),
		"no config":          descriptor[:strings.Index(descriptor, "  config:")],
		"unknown field":      descriptor + "  heal: true\n",
		"version on sources": "apiVersion: openetl.io/v1alpha1\nkind: Source\nmetadata: {name: s, version: 1.0.0}\nspec: {connector: faker}\n",
		"manifest version":   "apiVersion: openetl.io/v1alpha1\nkind: DeclarativeSource\nmetadata: {name: acme}\nspec: {}\n",
	} {
		if _, err := Parse([]byte(doc)); err == nil {
			t.Errorf("%s: parsed", name)
		}
	}
}

func TestCompareVersions(t *testing.T) {
	if CompareVersions("0.10.0", "0.9.1") <= 0 || CompareVersions("1.0.0-rc.1", "1.0.0") >= 0 || CompareVersions("1.0.0", "1.0.0") != 0 {
		t.Error("semver order")
	}
	if !Prerelease("1.0.0-rc.1") || Prerelease("1.0.0") {
		t.Error("prerelease")
	}
}

func TestDescriptorNumbers(t *testing.T) {
	spec := &protocolv1.SpecResponse{
		ProtocolVersion: "v1", Kind: protocolv1.ConnectorKind_CONNECTOR_KIND_SOURCE,
		Egress:       &protocolv1.Egress{Tier: protocolv1.EgressTier_EGRESS_TIER_HOST},
		ConfigSchema: []byte(`{"properties": {"n": {"default": 4194304}, "f": {"default": 0.5}, "e": {"default": 4.194304e6}}}`),
		Source:       &protocolv1.SourceCapabilities{},
	}
	d := &Descriptor{Roles: map[string]*Role{RoleSource: {Binaries: map[string]Binary{"linux/amd64": {URL: "https://example.com/y"}}, Spec: spec}}}
	y, err := MarshalDescriptor("x", "1.0.0", Info{Repository: "https://example.com/x"}, d)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(y), "default: 4194304\n") || !strings.Contains(string(y), "default: 0.5\n") {
		t.Errorf("numbers:\n%s", y)
	}
	doc, err := Parse(y)
	if err != nil {
		t.Fatal(err)
	}
	if diff := SameSpec(spec, doc.Descriptor.Roles[RoleSource].Spec); diff != "" {
		t.Errorf("the connector's spec and its descriptor's differ: %s", diff)
	}
}
