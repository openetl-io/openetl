package spec

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"go.yaml.in/yaml/v3"
	"golang.org/x/mod/semver"
	"google.golang.org/protobuf/types/known/durationpb"

	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
)

var semverPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$`)

// CompareVersions compares two semantic versions, as semver orders them.
func CompareVersions(a, b string) int { return semver.Compare("v"+a, "v"+b) }

// Prerelease reports whether a semantic version is a prerelease.
func Prerelease(v string) bool { return semver.Prerelease("v"+v) != "" }

// Descriptor is an external connector's version: its image, and the spec its
// executable returns, which the runner checks.
type Descriptor struct {
	// A tag, or repository@sha256:… once applied.
	Image string
	Spec  *protocolv1.SpecResponse
}

// descriptorYAML is a Connector document's spec. Enums are lowercase.
type descriptorYAML struct {
	Image    string `yaml:"image" json:"image"`
	Protocol string `yaml:"protocol" json:"protocol"`
	Kind     string `yaml:"kind" json:"kind"`
	Egress   struct {
		Tier  string   `yaml:"tier" json:"tier"`
		Allow []string `yaml:"allow,omitempty" json:"allow,omitempty"`
	} `yaml:"egress" json:"egress"`
	Setup        bool              `yaml:"setup,omitempty" json:"setup,omitempty"`
	Acknowledges bool              `yaml:"acknowledges,omitempty" json:"acknowledges,omitempty"`
	Capabilities *capabilitiesYAML `yaml:"capabilities,omitempty" json:"capabilities,omitempty"`
	// A JSON Schema.
	Config map[string]any `yaml:"config" json:"config"`
}

type capabilitiesYAML struct {
	Class                 string   `yaml:"class" json:"class"`
	Scopes                []string `yaml:"scopes,omitempty" json:"scopes,omitempty"`
	DedupeAtApply         bool     `yaml:"dedupeAtApply,omitempty" json:"dedupeAtApply,omitempty"`
	DefaultCommitInterval string   `yaml:"defaultCommitInterval,omitempty" json:"defaultCommitInterval,omitempty"`
	MinCommitInterval     string   `yaml:"minCommitInterval,omitempty" json:"minCommitInterval,omitempty"`
	MaxCommitInterval     string   `yaml:"maxCommitInterval,omitempty" json:"maxCommitInterval,omitempty"`
	NanosecondTimestamps  bool     `yaml:"nanosecondTimestamps,omitempty" json:"nanosecondTimestamps,omitempty"`
	MaxIdentifierLength   uint32   `yaml:"maxIdentifierLength,omitempty" json:"maxIdentifierLength,omitempty"`
	// Empty: append only.
	WriteModes []string `yaml:"writeModes,omitempty" json:"writeModes,omitempty"`
}

func parseDescriptor(b []byte) (*Descriptor, error) {
	var y descriptorYAML
	if err := strict(b, &y); err != nil {
		return nil, err
	}
	if y.Image == "" {
		return nil, errors.New("image is missing")
	}
	if y.Protocol != "v1" {
		return nil, fmt.Errorf("protocol %q isn't v1", y.Protocol)
	}
	s := &protocolv1.SpecResponse{ProtocolVersion: y.Protocol, Setup: y.Setup, Egress: &protocolv1.Egress{Allow: y.Egress.Allow}}
	var err error
	if s.Kind, err = enum(protocolv1.ConnectorKind_value, "CONNECTOR_KIND_", "kind", y.Kind, protocolv1.ConnectorKind_CONNECTOR_KIND_UNSPECIFIED); err != nil {
		return nil, err
	}
	if s.Egress.Tier, err = enum(protocolv1.EgressTier_value, "EGRESS_TIER_", "egress.tier", y.Egress.Tier, protocolv1.EgressTier_EGRESS_TIER_UNSPECIFIED); err != nil {
		return nil, err
	}
	switch {
	case s.Kind == protocolv1.ConnectorKind_CONNECTOR_KIND_UNSPECIFIED:
		return nil, errors.New("kind must be source or destination")
	case s.Egress.Tier == protocolv1.EgressTier_EGRESS_TIER_UNSPECIFIED:
		return nil, errors.New("egress.tier must be host, proxy or raw")
	case y.Config == nil:
		return nil, errors.New("config, the config's JSON Schema, is missing")
	case y.Capabilities != nil && s.Kind != protocolv1.ConnectorKind_CONNECTOR_KIND_DESTINATION:
		return nil, errors.New("capabilities are a destination's")
	case y.Acknowledges && s.Kind != protocolv1.ConnectorKind_CONNECTOR_KIND_SOURCE:
		return nil, errors.New("acknowledges is a source's")
	}
	if s.ConfigSchema, err = json.Marshal(y.Config); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	if s.Kind == protocolv1.ConnectorKind_CONNECTOR_KIND_SOURCE {
		s.Source = &protocolv1.SourceCapabilities{Acknowledges: y.Acknowledges}
	} else if s.Destination, err = parseCapabilities(y.Capabilities); err != nil {
		return nil, fmt.Errorf("capabilities: %w", err)
	}
	return &Descriptor{Image: y.Image, Spec: s}, nil
}

func parseCapabilities(y *capabilitiesYAML) (*protocolv1.DestinationCapabilities, error) {
	if y == nil {
		return nil, errors.New("a destination's are required")
	}
	c := &protocolv1.DestinationCapabilities{
		DedupeAtApply: y.DedupeAtApply, NanosecondTimestamps: y.NanosecondTimestamps, MaxIdentifierLength: y.MaxIdentifierLength,
	}
	var err error
	if c.SinkClass, err = enum(protocolv1.SinkClass_value, "SINK_CLASS_", "class", y.Class, protocolv1.SinkClass_SINK_CLASS_UNSPECIFIED); err != nil {
		return nil, err
	}
	if c.SinkClass == protocolv1.SinkClass_SINK_CLASS_UNSPECIFIED {
		return nil, errors.New("class is missing")
	}
	for _, v := range y.Scopes {
		s, err := enum(protocolv1.CommitScope_value, "COMMIT_SCOPE_", "scopes", v, protocolv1.CommitScope_COMMIT_SCOPE_UNSPECIFIED)
		if err != nil {
			return nil, err
		}
		c.Scopes = append(c.Scopes, s)
	}
	for _, v := range y.WriteModes {
		m, err := enum(protocolv1.WriteMode_value, "WRITE_MODE_", "writeModes", v, protocolv1.WriteMode_WRITE_MODE_UNSPECIFIED)
		if err != nil {
			return nil, err
		}
		c.WriteModes = append(c.WriteModes, m)
	}
	for field, d := range map[string]struct {
		v   string
		dst **durationpb.Duration
	}{
		"defaultCommitInterval": {y.DefaultCommitInterval, &c.DefaultCommitInterval},
		"minCommitInterval":     {y.MinCommitInterval, &c.MinCommitInterval},
		"maxCommitInterval":     {y.MaxCommitInterval, &c.MaxCommitInterval},
	} {
		if d.v == "" {
			continue
		}
		if *d.dst, err = duration(field, d.v); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// lower is the YAML spelling of an enum value.
func lower(name, prefix string) string { return strings.ToLower(strings.TrimPrefix(name, prefix)) }

type descriptorDoc struct {
	APIVersion string         `yaml:"apiVersion" json:"apiVersion"`
	Kind       string         `yaml:"kind" json:"kind"`
	Metadata   versionedMeta  `yaml:"metadata" json:"metadata"`
	Spec       descriptorYAML `yaml:"spec" json:"spec"`
}

type versionedMeta struct {
	Name    string `yaml:"name" json:"name"`
	Version string `yaml:"version" json:"version"`
}

func toYAML(d *Descriptor) (descriptorYAML, error) {
	s := d.Spec
	y := descriptorYAML{
		Image: d.Image, Protocol: s.GetProtocolVersion(), Kind: lower(s.GetKind().String(), "CONNECTOR_KIND_"),
		Setup: s.GetSetup(), Acknowledges: s.GetSource().GetAcknowledges(),
	}
	y.Egress.Tier = lower(s.GetEgress().GetTier().String(), "EGRESS_TIER_")
	y.Egress.Allow = s.GetEgress().GetAllow()
	// No schema accepts any config.
	y.Config = map[string]any{}
	if len(s.GetConfigSchema()) > 0 {
		v, err := decodeJSON(s.GetConfigSchema())
		if err != nil {
			return y, fmt.Errorf("the config schema: %w", err)
		}
		obj, ok := v.(map[string]any)
		if !ok {
			return y, errors.New("the config schema isn't a JSON object")
		}
		y.Config = obj
	}
	if c := s.GetDestination(); s.GetKind() == protocolv1.ConnectorKind_CONNECTOR_KIND_DESTINATION {
		y.Capabilities = &capabilitiesYAML{
			Class: lower(c.GetSinkClass().String(), "SINK_CLASS_"), DedupeAtApply: c.GetDedupeAtApply(),
			NanosecondTimestamps: c.GetNanosecondTimestamps(), MaxIdentifierLength: c.GetMaxIdentifierLength(),
		}
		for _, sc := range c.GetScopes() {
			y.Capabilities.Scopes = append(y.Capabilities.Scopes, lower(sc.String(), "COMMIT_SCOPE_"))
		}
		for _, m := range c.GetWriteModes() {
			y.Capabilities.WriteModes = append(y.Capabilities.WriteModes, lower(m.String(), "WRITE_MODE_"))
		}
		if c.GetDefaultCommitInterval() != nil {
			y.Capabilities.DefaultCommitInterval = c.GetDefaultCommitInterval().AsDuration().String()
		}
		if c.GetMinCommitInterval() != nil {
			y.Capabilities.MinCommitInterval = c.GetMinCommitInterval().AsDuration().String()
		}
		if c.GetMaxCommitInterval() != nil {
			y.Capabilities.MaxCommitInterval = c.GetMaxCommitInterval().AsDuration().String()
		}
	}
	return y, nil
}

func descriptorDocument(name, version string, d *Descriptor) (descriptorDoc, error) {
	y, err := toYAML(d)
	return descriptorDoc{APIVersion: APIVersion, Kind: KindConnector, Metadata: versionedMeta{name, version}, Spec: y}, err
}

// EncodeDescriptor encodes a Connector document as canonical JSON: the same
// descriptor always gives the same bytes.
func EncodeDescriptor(name, version string, d *Descriptor) ([]byte, error) {
	doc, err := descriptorDocument(name, version, d)
	if err != nil {
		return nil, err
	}
	return json.Marshal(doc)
}

// MarshalDescriptor writes a Connector document as YAML.
func MarshalDescriptor(name, version string, d *Descriptor) ([]byte, error) {
	doc, err := descriptorDocument(name, version, d)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(doc); err != nil {
		return nil, err
	}
	return buf.Bytes(), enc.Close()
}

// canonicalJSON re-encodes a YAML or JSON document as JSON with sorted keys.
func canonicalJSON(content []byte) ([]byte, error) {
	var v any
	if err := yaml.Unmarshal(content, &v); err != nil {
		return nil, err
	}
	return json.Marshal(v)
}

// decodeJSON decodes JSON with whole numbers as int64s, so YAML writes them
// as integers.
func decodeJSON(b []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	var walk func(any) any
	walk = func(v any) any {
		switch t := v.(type) {
		case json.Number:
			if n, err := t.Int64(); err == nil {
				return n
			}
			f, _ := t.Float64()
			return f
		case map[string]any:
			for k, e := range t {
				t[k] = walk(e)
			}
		case []any:
			for i, e := range t {
				t[i] = walk(e)
			}
		}
		return v
	}
	return walk(v), nil
}

// SameSpec reports how got, a connector's spec, differs from want, its
// descriptor's, or "" if it doesn't. Config schemas compare as JSON values.
func SameSpec(got, want *protocolv1.SpecResponse) string {
	g, gerr := toYAML(&Descriptor{Spec: got})
	w, werr := toYAML(&Descriptor{Spec: want})
	if gerr != nil || werr != nil {
		return "config schema"
	}
	var gc, wc any
	gb, _ := json.Marshal(g.Config)
	wb, _ := json.Marshal(w.Config)
	if json.Unmarshal(gb, &gc) != nil || json.Unmarshal(wb, &wc) != nil || !reflect.DeepEqual(gc, wc) {
		return "config schema"
	}
	g.Config, w.Config = nil, nil
	gb, _ = json.Marshal(g)
	wb, _ = json.Marshal(w)
	if !bytes.Equal(gb, wb) {
		return fmt.Sprintf("%s, not %s", gb, wb)
	}
	return ""
}
