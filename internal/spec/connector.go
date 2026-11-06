package spec

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"reflect"
	"regexp"
	"slices"
	"strings"

	"go.yaml.in/yaml/v3"
	"golang.org/x/mod/semver"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/openetl-io/openetl/internal/platform/resources"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

//go:embed connector.schema.json
var descriptorSchema []byte

// DescriptorSchema is a v1alpha2 descriptor's JSON Schema. Parse checks
// more than it says, such as a state's reads being at most its version.
func DescriptorSchema() []byte { return bytes.Clone(descriptorSchema) }

// APIVersionConnector is a Connector descriptor's: a package with roles.
// A v1alpha1 descriptor reads as a package with one role.
const APIVersionConnector = "openetl.io/v1alpha2"

// A package's roles.
const (
	RoleSource      = "source"
	RoleDestination = "destination"
)

// OfficialOwner owns the official packages and the built-ins. A bare name
// is one of its.
const OfficialOwner = "openetl"

var semverPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$`)

// ValidVersion reports whether v is a semantic version, such as 1.2.0.
func ValidVersion(v string) bool { return semverPattern.MatchString(v) }

// CompareVersions compares two semantic versions, as semver orders them.
func CompareVersions(a, b string) int { return semver.Compare("v"+a, "v"+b) }

// Prerelease reports whether a semantic version is a prerelease.
func Prerelease(v string) bool { return semver.Prerelease("v"+v) != "" }

// FullName is a package name with its owner: a bare name is the official
// owner's.
func FullName(name string) string {
	if strings.Contains(name, "/") {
		return name
	}
	return OfficialOwner + "/" + name
}

// SplitName splits a package name into its owner and name.
func SplitName(name string) (owner, short string) {
	owner, short, _ = strings.Cut(FullName(name), "/")
	return owner, short
}

// ValidPackageName reports whether name is owner/name or name, each part
// lowercase letters, digits and hyphens, at most 63 characters.
func ValidPackageName(name string) bool {
	owner, short, ok := strings.Cut(name, "/")
	if !ok {
		return ValidName(name)
	}
	return ValidName(owner) && ValidName(short)
}

// Descriptor is a connector package's version: its roles, and how each
// runs.
type Descriptor struct {
	Roles map[string]*Role
}

// Role is what a package provides as a source or a destination.
type Role struct {
	Requires Requires
	// By platform, as os/arch.
	Binaries map[string]Binary
	Image    *Image
	// A DeclarativeSource's spec, as canonical JSON: a source's whole
	// runtime.
	Manifest  []byte
	Resources Resources
	// Nil: the role says nothing about its state's versions.
	State *State
	// What the role's executable returns from Spec; nil for a manifest. Its
	// protocol and required features are Requires'.
	Spec *protocolv1.SpecResponse
}

// Requires is what a role needs from the platform.
type Requires struct {
	// "v1"; empty for a manifest.
	Protocol string
	Features []string
	// The oldest release it runs on; empty for any.
	Platform string
}

// Binary is one platform's executable, downloaded over HTTP.
type Binary struct {
	URL string
	// sha256:{hex}. The API fills in a missing one when the version is
	// applied.
	Digest string
	// An https URL of the binary's provenance attestation.
	Attestation string
}

// Image is an OCI image index that serves the launch contract.
type Image struct {
	// A repository, without a tag or digest.
	Ref string
	// The index's: sha256:{hex}.
	Digest    string
	Platforms []string
	// The connector's argv, where it isn't the image's entrypoint.
	Command     []string
	Attestation string
}

// Resources are a container launcher's requests and limits, as Kubernetes
// quantities.
type Resources struct {
	CPU, Memory string
	// The size of TMPDIR.
	Disk string
	// The job size it wants, xs to xl, which auto starts from (ADR-0027 §3).
	Hint string
}

// State is the version of the checkpoint state a role writes, and the
// oldest it can resume from.
type State struct {
	Version, Reads uint32
}

// Unpinned returns the binaries without a digest, as role/os/arch.
func (d *Descriptor) Unpinned() []string {
	var out []string
	for role, r := range d.Roles {
		for p, b := range r.Binaries {
			if b.Digest == "" {
				out = append(out, role+"/"+p)
			}
		}
	}
	slices.Sort(out)
	return out
}

// Kinds are the descriptor's roles, sorted.
func (d *Descriptor) Kinds() []string { return slices.Sorted(maps.Keys(d.Roles)) }

var (
	platformPattern = regexp.MustCompile(`^[a-z0-9]+/[a-z0-9]+$`)
	digestPattern   = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	featurePattern  = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{0,62}$`)
	imagePattern    = regexp.MustCompile(`^[a-z0-9]+([._-][a-z0-9]+)*(\.[a-z0-9-]+)*(:[0-9]+)?(/[a-z0-9]+([._-][a-z0-9]+)*)+$`)
	cpuPattern      = regexp.MustCompile(`^([0-9]+m|[0-9]+(\.[0-9]{1,3})?)$`)
	bytesPattern    = regexp.MustCompile(`^[0-9]+(Ki|Mi|Gi|Ti|k|M|G|T)?$`)
)

// ValidPlatform reports whether p is os/arch.
func ValidPlatform(p string) bool { return platformPattern.MatchString(p) }

// ValidImageRef reports whether ref is an image repository, without a tag or
// digest.
func ValidImageRef(ref string) bool { return imagePattern.MatchString(ref) }

type binaryYAML struct {
	URL         string `yaml:"url" json:"url"`
	Digest      string `yaml:"digest,omitempty" json:"digest,omitempty"`
	Attestation string `yaml:"attestation,omitempty" json:"attestation,omitempty"`
}

type imageYAML struct {
	Ref         string   `yaml:"ref" json:"ref"`
	Digest      string   `yaml:"digest" json:"digest"`
	Platforms   []string `yaml:"platforms" json:"platforms"`
	Command     []string `yaml:"command,omitempty" json:"command,omitempty"`
	Attestation string   `yaml:"attestation,omitempty" json:"attestation,omitempty"`
}

type egressYAML struct {
	Tier  string   `yaml:"tier" json:"tier"`
	Allow []string `yaml:"allow,omitempty" json:"allow,omitempty"`
}

// specYAML is what a role's Spec returns. Enums are lowercase.
type specYAML struct {
	Egress       *egressYAML       `yaml:"egress,omitempty" json:"egress,omitempty"`
	Setup        bool              `yaml:"setup,omitempty" json:"setup,omitempty"`
	Acknowledges bool              `yaml:"acknowledges,omitempty" json:"acknowledges,omitempty"`
	Capabilities *capabilitiesYAML `yaml:"capabilities,omitempty" json:"capabilities,omitempty"`
	// A JSON Schema.
	Config schemaObject `yaml:"config,omitempty" json:"config,omitzero"`
}

// schemaObject is a JSON Schema. An empty one, which accepts any config, is
// written out; only a missing one is left out.
type schemaObject map[string]any

func (s schemaObject) IsZero() bool { return s == nil }

type requiresYAML struct {
	Protocol string   `yaml:"protocol,omitempty" json:"protocol,omitempty"`
	Features []string `yaml:"features,omitempty" json:"features,omitempty"`
	Platform string   `yaml:"platform,omitempty" json:"platform,omitempty"`
}

type runtimeYAML struct {
	Binaries map[string]binaryYAML `yaml:"binaries,omitempty" json:"binaries,omitempty"`
	Image    *imageYAML            `yaml:"image,omitempty" json:"image,omitempty"`
	Manifest map[string]any        `yaml:"manifest,omitempty" json:"manifest,omitempty"`
}

type resourcesYAML struct {
	CPU    string `yaml:"cpu,omitempty" json:"cpu,omitempty"`
	Memory string `yaml:"memory,omitempty" json:"memory,omitempty"`
	Disk   string `yaml:"disk,omitempty" json:"disk,omitempty"`
	Hint   string `yaml:"hint,omitempty" json:"hint,omitempty"`
}

type stateYAML struct {
	Version uint32 `yaml:"version" json:"version"`
	Reads   uint32 `yaml:"reads" json:"reads"`
}

type roleYAML struct {
	Requires  *requiresYAML  `yaml:"requires,omitempty" json:"requires,omitempty"`
	Runtime   runtimeYAML    `yaml:"runtime" json:"runtime"`
	Resources *resourcesYAML `yaml:"resources,omitempty" json:"resources,omitempty"`
	State     *stateYAML     `yaml:"state,omitempty" json:"state,omitempty"`
	specYAML  `yaml:",inline"`
}

type descriptorYAML struct {
	Roles map[string]*roleYAML `yaml:"roles" json:"roles"`
}

// v1alpha1DescriptorYAML is a v1alpha1 Connector's spec: one role.
type v1alpha1DescriptorYAML struct {
	Binaries map[string]struct {
		URL    string `yaml:"url"`
		Digest string `yaml:"digest"`
	} `yaml:"binaries"`
	Protocol string `yaml:"protocol"`
	Kind     string `yaml:"kind"`
	specYAML `yaml:",inline"`
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
	WriteModes []string   `yaml:"writeModes,omitempty" json:"writeModes,omitempty"`
	Holds      *holdsYAML `yaml:"holds,omitempty" json:"holds,omitempty"`
}

type holdsYAML struct {
	ByKey   bool `yaml:"byKey,omitempty" json:"byKey,omitempty"`
	ByID    bool `yaml:"byId,omitempty" json:"byId,omitempty"`
	Last    bool `yaml:"last,omitempty" json:"last,omitempty"`
	Guarded bool `yaml:"guarded,omitempty" json:"guarded,omitempty"`
}

func parseDescriptor(b []byte) (*Descriptor, error) {
	var y descriptorYAML
	if err := strict(b, &y); err != nil {
		return nil, err
	}
	if len(y.Roles) == 0 {
		return nil, errors.New("roles: a package has a source role, a destination role or both")
	}
	d := &Descriptor{Roles: map[string]*Role{}}
	for name, ry := range y.Roles {
		if name != RoleSource && name != RoleDestination {
			return nil, fmt.Errorf("roles: %q isn't source or destination", name)
		}
		if ry == nil {
			return nil, fmt.Errorf("roles.%s is empty", name)
		}
		r, err := parseRole(name, ry)
		if err != nil {
			return nil, fmt.Errorf("roles.%s: %w", name, err)
		}
		d.Roles[name] = r
	}
	return d, nil
}

// parseV1alpha1Descriptor reads a v1alpha1 descriptor as a package with its
// one role.
func parseV1alpha1Descriptor(b []byte) (*Descriptor, error) {
	var y v1alpha1DescriptorYAML
	if err := strict(b, &y); err != nil {
		return nil, err
	}
	if y.Kind != RoleSource && y.Kind != RoleDestination {
		return nil, errors.New("kind must be source or destination")
	}
	if len(y.Binaries) == 0 {
		return nil, errors.New("binaries, the executables by platform, are missing")
	}
	ry := &roleYAML{Requires: &requiresYAML{Protocol: y.Protocol}, Runtime: runtimeYAML{Binaries: map[string]binaryYAML{}}, specYAML: y.specYAML}
	for p, b := range y.Binaries {
		ry.Runtime.Binaries[p] = binaryYAML{URL: b.URL, Digest: b.Digest}
	}
	r, err := parseRole(y.Kind, ry)
	if err != nil {
		return nil, err
	}
	return &Descriptor{Roles: map[string]*Role{y.Kind: r}}, nil
}

func parseRole(role string, y *roleYAML) (*Role, error) {
	r := &Role{}
	if y.Requires != nil {
		r.Requires = Requires{Protocol: y.Requires.Protocol, Features: y.Requires.Features, Platform: y.Requires.Platform}
	}
	for _, f := range r.Requires.Features {
		if !featurePattern.MatchString(f) {
			return nil, fmt.Errorf("requires.features: %q isn't a feature name", f)
		}
	}
	if p := r.Requires.Platform; p != "" && !ValidVersion(p) {
		return nil, fmt.Errorf("requires.platform %q isn't a release, such as 0.1.0", p)
	}
	rt := y.Runtime
	if len(rt.Binaries) == 0 && rt.Image == nil && rt.Manifest == nil {
		return nil, errors.New("runtime needs binaries, an image or a manifest")
	}
	if rt.Binaries != nil {
		r.Binaries = map[string]Binary{}
	}
	for p, b := range rt.Binaries {
		switch {
		case !platformPattern.MatchString(p):
			return nil, fmt.Errorf("runtime.binaries: %q isn't a platform, such as linux/amd64", p)
		case !httpURL(b.URL, true):
			return nil, fmt.Errorf("runtime.binaries.%s.url %q must be an http or https URL", p, b.URL)
		case b.Digest != "" && !digestPattern.MatchString(b.Digest):
			return nil, fmt.Errorf("runtime.binaries.%s.digest %q isn't sha256:{hex}", p, b.Digest)
		case b.Attestation != "" && !httpURL(b.Attestation, false):
			return nil, fmt.Errorf("runtime.binaries.%s.attestation %q must be an https URL", p, b.Attestation)
		}
		r.Binaries[p] = Binary(b)
	}
	if im := rt.Image; im != nil {
		switch {
		case !imagePattern.MatchString(im.Ref):
			return nil, fmt.Errorf("runtime.image.ref %q must be an image repository, such as ghcr.io/acme/source, without a tag or digest", im.Ref)
		case !digestPattern.MatchString(im.Digest):
			return nil, fmt.Errorf("runtime.image.digest %q must be its index's sha256:{hex}", im.Digest)
		case len(im.Platforms) == 0:
			return nil, errors.New("runtime.image.platforms, the platforms the index holds, are missing")
		case im.Attestation != "" && !httpURL(im.Attestation, false):
			return nil, fmt.Errorf("runtime.image.attestation %q must be an https URL", im.Attestation)
		case slices.Contains(im.Command, ""):
			return nil, errors.New("runtime.image.command has an empty argument")
		}
		for _, p := range im.Platforms {
			if !platformPattern.MatchString(p) {
				return nil, fmt.Errorf("runtime.image.platforms: %q isn't a platform, such as linux/amd64", p)
			}
		}
		r.Image = &Image{Ref: im.Ref, Digest: im.Digest, Platforms: im.Platforms, Command: im.Command, Attestation: im.Attestation}
	}
	if y.Resources != nil {
		r.Resources = Resources{CPU: y.Resources.CPU, Memory: y.Resources.Memory, Disk: y.Resources.Disk, Hint: y.Resources.Hint}
		if err := r.Resources.check(); err != nil {
			return nil, err
		}
	}
	if s := y.State; s != nil {
		r.State = &State{Version: s.Version, Reads: s.Reads}
		if err := r.State.check(); err != nil {
			return nil, err
		}
	}
	if rt.Manifest != nil {
		switch {
		case role != RoleSource:
			return nil, errors.New("runtime.manifest: only a source runs from a manifest")
		case len(rt.Binaries) > 0 || rt.Image != nil:
			return nil, errors.New("runtime.manifest is the role's whole runtime: it runs in the platform, so it has no binaries or image")
		case !reflect.ValueOf(y.specYAML).IsZero():
			return nil, errors.New("a manifest's config, egress and capabilities are in the manifest")
		case r.Requires.Protocol != "":
			return nil, errors.New("requires.protocol: a manifest speaks no protocol")
		}
		var err error
		if r.Manifest, err = json.Marshal(rt.Manifest); err != nil {
			return nil, fmt.Errorf("runtime.manifest: %w", err)
		}
		return r, nil
	}
	if r.Requires.Protocol != "v1" {
		return nil, fmt.Errorf("requires.protocol %q isn't v1", r.Requires.Protocol)
	}
	var err error
	if r.Spec, err = parseSpec(role, &y.specYAML); err != nil {
		return nil, err
	}
	r.Spec.ProtocolVersion, r.Spec.RequiredFeatures = r.Requires.Protocol, r.Requires.Features
	return r, nil
}

func (r Resources) check() error {
	switch {
	case r.CPU != "" && !cpuPattern.MatchString(r.CPU):
		return fmt.Errorf("resources.cpu %q isn't a CPU quantity, such as 1 or 500m", r.CPU)
	case r.Memory != "" && !bytesPattern.MatchString(r.Memory):
		return fmt.Errorf("resources.memory %q isn't a size, such as 512Mi", r.Memory)
	case r.Disk != "" && !bytesPattern.MatchString(r.Disk):
		return fmt.Errorf("resources.disk %q isn't a size, such as 1Gi", r.Disk)
	}
	return CheckHint(r.Hint)
}

// CheckHint checks a connector's resources.hint.
func CheckHint(hint string) error {
	if _, ok := resources.Preset(hint); hint != "" && !ok {
		return fmt.Errorf("resources.hint %q isn't a job size: xs, s, m, l or xl", hint)
	}
	return nil
}

func (s State) check() error {
	if s.Version == 0 || s.Reads == 0 || s.Reads > s.Version {
		return fmt.Errorf("state: version %d and reads %d must be at least 1, with reads at most version", s.Version, s.Reads)
	}
	return nil
}

func httpURL(s string, plain bool) bool {
	u, err := url.Parse(s)
	return err == nil && u.Host != "" && (u.Scheme == "https" || plain && u.Scheme == "http") && len(s) <= 2048
}

func parseSpec(role string, y *specYAML) (*protocolv1.SpecResponse, error) {
	s := &protocolv1.SpecResponse{Setup: y.Setup, Egress: &protocolv1.Egress{}}
	s.Kind = protocolv1.ConnectorKind_CONNECTOR_KIND_SOURCE
	if role == RoleDestination {
		s.Kind = protocolv1.ConnectorKind_CONNECTOR_KIND_DESTINATION
	}
	if y.Egress == nil {
		return nil, errors.New("egress.tier must be host, proxy or raw")
	}
	s.Egress.Allow = y.Egress.Allow
	var err error
	if s.Egress.Tier, err = enum(protocolv1.EgressTier_value, "EGRESS_TIER_", "egress.tier", y.Egress.Tier, protocolv1.EgressTier_EGRESS_TIER_UNSPECIFIED); err != nil {
		return nil, err
	}
	switch {
	case s.Egress.Tier == protocolv1.EgressTier_EGRESS_TIER_UNSPECIFIED:
		return nil, errors.New("egress.tier must be host, proxy or raw")
	case y.Config == nil:
		return nil, errors.New("config, the config's JSON Schema, is missing")
	case y.Capabilities != nil && role != RoleDestination:
		return nil, errors.New("capabilities are a destination's")
	case y.Acknowledges && role != RoleSource:
		return nil, errors.New("acknowledges is a source's")
	}
	if s.ConfigSchema, err = json.Marshal(y.Config); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	if role == RoleSource {
		s.Source = &protocolv1.SourceCapabilities{Acknowledges: y.Acknowledges}
	} else if s.Destination, err = parseCapabilities(y.Capabilities); err != nil {
		return nil, fmt.Errorf("capabilities: %w", err)
	}
	return s, nil
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
	if h := y.Holds; h != nil {
		c.Holds = &protocolv1.HoldCapabilities{ByKey: h.ByKey, ById: h.ByID, Last: h.Last, Guarded: h.Guarded}
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
	Name        string       `yaml:"name" json:"name"`
	Version     string       `yaml:"version" json:"version"`
	Description string       `yaml:"description,omitempty" json:"description,omitempty"`
	License     string       `yaml:"license,omitempty" json:"license,omitempty"`
	Repository  string       `yaml:"repository,omitempty" json:"repository,omitempty"`
	Docs        string       `yaml:"docs,omitempty" json:"docs,omitempty"`
	Logo        string       `yaml:"logo,omitempty" json:"logo,omitempty"`
	Author      string       `yaml:"author,omitempty" json:"author,omitempty"`
	Tags        []string     `yaml:"tags,omitempty" json:"tags,omitempty"`
	Release     *releaseYAML `yaml:"release,omitempty" json:"release,omitempty"`
}

func specToYAML(s *protocolv1.SpecResponse) (specYAML, error) {
	y := specYAML{Setup: s.GetSetup(), Acknowledges: s.GetSource().GetAcknowledges()}
	y.Egress = &egressYAML{Tier: lower(s.GetEgress().GetTier().String(), "EGRESS_TIER_"), Allow: s.GetEgress().GetAllow()}
	// No schema accepts any config.
	y.Config = schemaObject{}
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
		if h := c.GetHolds(); h != nil {
			y.Capabilities.Holds = &holdsYAML{ByKey: h.GetByKey(), ByID: h.GetById(), Last: h.GetLast(), Guarded: h.GetGuarded()}
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

func roleToYAML(r *Role) (*roleYAML, error) {
	y := &roleYAML{}
	req := r.Requires
	if r.Spec != nil {
		req.Protocol, req.Features = r.Spec.GetProtocolVersion(), r.Spec.GetRequiredFeatures()
	}
	if req.Protocol != "" || len(req.Features) > 0 || req.Platform != "" {
		y.Requires = &requiresYAML{Protocol: req.Protocol, Features: req.Features, Platform: req.Platform}
	}
	if r.Binaries != nil {
		y.Runtime.Binaries = map[string]binaryYAML{}
		for p, b := range r.Binaries {
			y.Runtime.Binaries[p] = binaryYAML(b)
		}
	}
	if im := r.Image; im != nil {
		y.Runtime.Image = &imageYAML{Ref: im.Ref, Digest: im.Digest, Platforms: im.Platforms, Command: im.Command, Attestation: im.Attestation}
	}
	if r.Manifest != nil {
		v, err := decodeJSON(r.Manifest)
		if err != nil {
			return nil, fmt.Errorf("the manifest: %w", err)
		}
		m, ok := v.(map[string]any)
		if !ok {
			return nil, errors.New("the manifest isn't a JSON object")
		}
		y.Runtime.Manifest = m
	}
	if r.Resources != (Resources{}) {
		y.Resources = &resourcesYAML{CPU: r.Resources.CPU, Memory: r.Resources.Memory, Disk: r.Resources.Disk, Hint: r.Resources.Hint}
	}
	if r.State != nil {
		y.State = &stateYAML{Version: r.State.Version, Reads: r.State.Reads}
	}
	if r.Spec != nil {
		var err error
		if y.specYAML, err = specToYAML(r.Spec); err != nil {
			return nil, err
		}
	}
	return y, nil
}

func descriptorDocument(name, version string, info Info, d *Descriptor) (descriptorDoc, error) {
	meta := versionedMeta{
		Name: FullName(name), Version: version, Description: info.Description, License: info.License,
		Repository: info.Repository, Docs: info.Docs, Logo: info.Logo, Author: info.Author, Tags: info.Tags,
	}
	if info.ReleaseTag != "" || info.ReleaseURL != "" {
		meta.Release = &releaseYAML{Tag: info.ReleaseTag, URL: info.ReleaseURL}
	}
	doc := descriptorDoc{APIVersion: APIVersionConnector, Kind: KindConnector, Metadata: meta, Spec: descriptorYAML{Roles: map[string]*roleYAML{}}}
	for role, r := range d.Roles {
		y, err := roleToYAML(r)
		if err != nil {
			return doc, fmt.Errorf("roles.%s: %w", role, err)
		}
		doc.Spec.Roles[role] = y
	}
	return doc, nil
}

// EncodeDescriptor encodes a Connector document as canonical JSON, in
// v1alpha2: the same descriptor always gives the same bytes.
func EncodeDescriptor(name, version string, info Info, d *Descriptor) ([]byte, error) {
	doc, err := descriptorDocument(name, version, info, d)
	if err != nil {
		return nil, err
	}
	return json.Marshal(doc)
}

// EncodeVersion encodes a Connector document as canonical JSON in its own
// apiVersion, so a v1alpha1 descriptor stays one.
func EncodeVersion(doc *Document) ([]byte, error) {
	if doc.APIVersion != APIVersion {
		return EncodeDescriptor(doc.Name, doc.Version, doc.Info, doc.Descriptor)
	}
	if len(doc.Descriptor.Roles) != 1 {
		return nil, errors.New("a v1alpha1 descriptor has one role")
	}
	role := doc.Descriptor.Kinds()[0]
	r := doc.Descriptor.Roles[role]
	y, err := roleToYAML(r)
	if err != nil {
		return nil, err
	}
	type v1alpha1 struct {
		Binaries map[string]binaryYAML `json:"binaries"`
		Protocol string                `json:"protocol"`
		Kind     string                `json:"kind"`
		specYAML
	}
	name := doc.Name
	if owner, short := SplitName(name); owner == OfficialOwner {
		name = short
	}
	meta := versionedMeta{Name: name, Version: doc.Version, Author: doc.Info.Author, Logo: doc.Info.Logo}
	if doc.Info.ReleaseTag != "" || doc.Info.ReleaseURL != "" {
		meta.Release = &releaseYAML{Tag: doc.Info.ReleaseTag, URL: doc.Info.ReleaseURL}
	}
	return json.Marshal(struct {
		APIVersion string        `json:"apiVersion"`
		Kind       string        `json:"kind"`
		Metadata   versionedMeta `json:"metadata"`
		Spec       v1alpha1      `json:"spec"`
	}{APIVersion, KindConnector, meta, v1alpha1{Binaries: y.Runtime.Binaries, Protocol: r.Requires.Protocol, Kind: role, specYAML: y.specYAML}})
}

// MarshalDescriptor writes a Connector document as YAML, in v1alpha2.
func MarshalDescriptor(name, version string, info Info, d *Descriptor) ([]byte, error) {
	doc, err := descriptorDocument(name, version, info, d)
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
	if got.GetKind() != want.GetKind() {
		return fmt.Sprintf("kind %s, not %s", lower(got.GetKind().String(), "CONNECTOR_KIND_"), lower(want.GetKind().String(), "CONNECTOR_KIND_"))
	}
	g, gerr := roleToYAML(&Role{Spec: got})
	w, werr := roleToYAML(&Role{Spec: want})
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

// RoleSpec is a connector's spec as a descriptor's role carries it, with
// the role, for YAML or JSON.
type RoleSpec struct {
	Role     string        `yaml:"role" json:"role"`
	Requires *requiresYAML `yaml:"requires,omitempty" json:"requires,omitempty"`
	specYAML `yaml:",inline"`
}

// RoleSpecOf is s as its descriptor would carry it.
func RoleSpecOf(s *protocolv1.SpecResponse) (*RoleSpec, error) {
	y, err := roleToYAML(&Role{Spec: s})
	if err != nil {
		return nil, err
	}
	return &RoleSpec{Role: RoleOf(s), Requires: y.Requires, specYAML: y.specYAML}, nil
}

// KindOf is a role's connector kind.
func KindOf(role string) protocolv1.ConnectorKind {
	switch role {
	case RoleSource:
		return protocolv1.ConnectorKind_CONNECTOR_KIND_SOURCE
	case RoleDestination:
		return protocolv1.ConnectorKind_CONNECTOR_KIND_DESTINATION
	}
	return protocolv1.ConnectorKind_CONNECTOR_KIND_UNSPECIFIED
}

// RoleOf is a spec's role: source or destination.
func RoleOf(s *protocolv1.SpecResponse) string {
	if s.GetKind() == protocolv1.ConnectorKind_CONNECTOR_KIND_DESTINATION {
		return RoleDestination
	}
	return RoleSource
}
