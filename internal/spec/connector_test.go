package spec

import (
	"bytes"
	"reflect"
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
	if doc.Kind != KindConnector || doc.Name != "postgres" || doc.Version != "0.1.0" {
		t.Fatalf("doc = %+v", doc)
	}
	wantInfo := Info{Author: "Example", Logo: "https://example.com/postgres.svg", ReleaseTag: "destination-postgres/v0.1.0", ReleaseURL: "https://example.com/releases/destination-postgres/v0.1.0"}
	if doc.Info != wantInfo {
		t.Errorf("info = %+v", doc.Info)
	}
	if u := doc.Descriptor.Unpinned(); len(u) != 1 || u[0] != "darwin/arm64" || doc.Descriptor.Binaries["linux/amd64"].Digest == "" {
		t.Errorf("binaries %v, unpinned %v", doc.Descriptor.Binaries, u)
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
	if !proto.Equal(doc.Descriptor.Spec, want) {
		t.Errorf("spec = %v\nwant %v", doc.Descriptor.Spec, want)
	}

	// Writing it out and reading it back changes nothing.
	y, err := MarshalDescriptor(doc.Name, doc.Version, doc.Info, doc.Descriptor)
	if err != nil {
		t.Fatal(err)
	}
	again, err := Parse(y)
	if err != nil {
		t.Fatalf("%v\n%s", err, y)
	}
	if !proto.Equal(again.Descriptor.Spec, want) || !reflect.DeepEqual(again.Descriptor.Binaries, doc.Descriptor.Binaries) || again.Info != wantInfo {
		t.Errorf("round trip gave %v, %+v", again.Descriptor, again.Info)
	}
	a, err := EncodeDescriptor(doc.Name, doc.Version, doc.Info, doc.Descriptor)
	if err != nil {
		t.Fatal(err)
	}
	b, err := EncodeDescriptor(again.Name, again.Version, again.Info, again.Descriptor)
	if err != nil || !bytes.Equal(a, b) {
		t.Errorf("encodings differ:\n%s\n%s", a, b)
	}
	if fromJSON, err := Parse(a); err != nil || !proto.Equal(fromJSON.Descriptor.Spec, want) {
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
}

func TestParseDeclarativeSource(t *testing.T) {
	doc, err := Parse([]byte("apiVersion: openetl.io/v1alpha1\nkind: DeclarativeSource\nmetadata: {version: 1.0.0, name: acme}\nspec: {b: 1, a: [x]}\n"))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"apiVersion":"openetl.io/v1alpha1","kind":"DeclarativeSource","metadata":{"name":"acme","version":"1.0.0"},"spec":{"a":["x"],"b":1}}`
	if doc.Name != "acme" || doc.Version != "1.0.0" || string(doc.Manifest) != want {
		t.Errorf("doc = %+v, manifest %s", doc, doc.Manifest)
	}
	doc, err = Parse([]byte("apiVersion: openetl.io/v1alpha1\nkind: DeclarativeSource\nmetadata: {version: 1.0.0, name: acme, author: Acme, logo: 'https://acme.example/logo.png'}\nspec: {}\n"))
	if err != nil || doc.Info != (Info{Author: "Acme", Logo: "https://acme.example/logo.png"}) {
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
	d := &Descriptor{Binaries: map[string]Binary{"linux/amd64": {URL: "https://example.com/y"}}, Spec: &protocolv1.SpecResponse{
		ProtocolVersion: "v1", Kind: protocolv1.ConnectorKind_CONNECTOR_KIND_SOURCE,
		Egress:       &protocolv1.Egress{Tier: protocolv1.EgressTier_EGRESS_TIER_HOST},
		ConfigSchema: []byte(`{"properties": {"n": {"default": 4194304}, "f": {"default": 0.5}, "e": {"default": 4.194304e6}}}`),
		Source:       &protocolv1.SourceCapabilities{},
	}}
	y, err := MarshalDescriptor("x", "1.0.0", Info{}, d)
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
	if diff := SameSpec(d.Spec, doc.Descriptor.Spec); diff != "" {
		t.Errorf("the connector's spec and its descriptor's differ: %s", diff)
	}
}
