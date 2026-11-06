// Package connectorhost hosts connectors: the native connectors compiled
// into openetld, the engine's Parquet sink, declarative sources, and
// external connectors, each instance with its own host services.
package connectorhost

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/openetl-io/openetl/connectors/native/faker"
	"github.com/openetl-io/openetl/connectors/native/stdout"
	"github.com/openetl-io/openetl/internal/connectorhost/external"
	"github.com/openetl-io/openetl/internal/connectorhost/hostservices"
	"github.com/openetl-io/openetl/internal/declarative"
	"github.com/openetl-io/openetl/internal/enginesink"
	"github.com/openetl-io/openetl/internal/netguard"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/platform/resources"
	"github.com/openetl-io/openetl/internal/spec"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// Constructors only parse config; they must not do I/O, so the API can use
// them to validate.
var (
	sources = map[string]func([]byte) (source.Source, error){
		"faker": func(c []byte) (source.Source, error) { return faker.New(c) },
	}
	destinations = map[string]func([]byte) (destination.Destination, error){
		"parquet": func(c []byte) (destination.Destination, error) { return enginesink.NewParquet(c) },
		"stdout":  func(c []byte) (destination.Destination, error) { return stdout.New(c) },
	}
)

// Env is what the runner gives the connectors it hosts. The zero Env is
// enough to validate a config.
type Env struct {
	// Which addresses connectors may reach besides public ones.
	Egress netguard.Policy
	// Declarative sources, by full name, such as openetl/acme. Native
	// connectors' names come first.
	Manifests map[string]*declarative.Connector
	// External connectors with descriptors, by role and full name, such as
	// source/openetl/postgres. Native connectors' names come first.
	Externals map[string]External
	// A directory of external connectors' executables: ROLE-NAME for
	// openetl/NAME, and OWNER/ROLE-NAME for the rest. Names it shares with
	// the others come last.
	Connectors string
	// Nil: slog's default.
	Logger *slog.Logger
	// Counts what the external connectors it starts use. Nil: nothing does.
	Meter *resources.Meter
}

// NewSource binds a source connector to its config, a JSON object. It does
// no I/O, so the API can use it to validate; Start starts an external one.
func NewSource(connector string, config []byte, env Env) (source.Source, error) {
	if !Builtin("source", connector) {
		full := spec.FullName(connector)
		if m := env.Manifests[full]; m != nil {
			return declarativeSource(m, config, env)
		}
		if x, ok := env.Externals["source/"+full]; ok {
			if err := external.ValidateConfig(connector, x.Spec.GetConfigSchema(), config); err != nil {
				return nil, err
			}
			return external.NewSource(x.Executable, config, env.external(x.Spec)), nil
		}
		if exe := env.executable("source", connector); exe != "" {
			return external.NewSource(exe, config, env.external(nil)), nil
		}
	}
	f, err := lookup(sources, "source", connector, env)
	if err != nil {
		return nil, err
	}
	s, err := f(config)
	return s, errs.Wrap(errs.ConfigInvalid, err, "")
}

// SecretProperties lists the config properties a connector's schema marks
// x-secret, looking connectors up as NewSource and NewDestination do. known
// is false for one only the connectors directory has, whose schema the API
// doesn't know.
func SecretProperties(kind, connector string, env Env) (props []string, known bool, err error) {
	if Builtin(kind, connector) {
		return nil, true, nil
	}
	full := spec.FullName(connector)
	if m := env.Manifests[full]; kind == "source" && m != nil {
		return m.SecretProperties(), true, nil
	}
	if x, ok := env.Externals[kind+"/"+full]; ok {
		props, err := external.SecretProperties(x.Spec.GetConfigSchema())
		return props, true, err
	}
	return nil, false, nil
}

// executable is the external connector's path, or empty if there is none.
func (e Env) executable(kind, connector string) string {
	if e.Connectors == "" || !spec.ValidPackageName(connector) {
		return ""
	}
	p := filepath.Join(e.Connectors, ExecutablePath(kind, connector))
	if fi, err := os.Stat(p); err != nil || !fi.Mode().IsRegular() || fi.Mode()&0o111 == 0 {
		return ""
	}
	return p
}

// ExecutablePath is where a connectors directory keeps a package's role:
// ROLE-NAME for openetl/NAME, and OWNER/ROLE-NAME for the rest.
func ExecutablePath(role, name string) string {
	owner, short := spec.SplitName(name)
	if owner == spec.OfficialOwner {
		return role + "-" + short
	}
	return filepath.Join(owner, role+"-"+short)
}

func (e Env) external(spec *protocolv1.SpecResponse) external.Options {
	return external.Options{Egress: e.Egress, Logger: e.Logger, Spec: spec, Meter: e.Meter}
}

// externals lists the external connectors of a kind, by name.
func (e Env) externals(kind string) []string {
	if e.Connectors == "" {
		return nil
	}
	ents, err := os.ReadDir(e.Connectors)
	if err != nil {
		return nil
	}
	var names []string
	for _, ent := range ents {
		if name, ok := strings.CutPrefix(ent.Name(), kind+"-"); ok && e.executable(kind, name) != "" {
			names = append(names, name)
			continue
		}
		if !ent.IsDir() || !spec.ValidName(ent.Name()) || ent.Name() == spec.OfficialOwner {
			continue
		}
		owned, err := os.ReadDir(filepath.Join(e.Connectors, ent.Name()))
		if err != nil {
			continue
		}
		for _, o := range owned {
			if name, ok := strings.CutPrefix(o.Name(), kind+"-"); ok && e.executable(kind, ent.Name()+"/"+name) != "" {
				names = append(names, ent.Name()+"/"+name)
			}
		}
	}
	return names
}

// Start starts an external connector's process, so its spec is known. It
// does nothing for the others.
func Start(ctx context.Context, connector any) error {
	if s, ok := connector.(interface{ Start(context.Context) error }); ok {
		return s.Start(ctx)
	}
	return nil
}

// Close stops an external connector's process. It does nothing for the
// others.
func Close(connector any) {
	if c, ok := connector.(io.Closer); ok {
		_ = c.Close()
	}
}

// declarativeSource gives the instance host services of its own, which
// resolve only its secrets.
func declarativeSource(m *declarative.Connector, config []byte, env Env) (source.Source, error) {
	b, err := m.Bind(config)
	if err != nil {
		return nil, err
	}
	log := env.Logger
	if log == nil {
		log = slog.Default()
	}
	h := hostservices.New(hostservices.Options{
		Policy:     env.Egress,
		Secrets:    hostservices.StaticSecrets(b.Secrets),
		RateLimits: b.RateLimits,
		Logger:     log.With("connector", m.Name(), "connector_version", m.Version()),
	})
	return b.Source(h), nil
}

func NewDestination(connector string, config []byte, env Env) (destination.Destination, error) {
	if !Builtin("destination", connector) {
		if x, ok := env.Externals["destination/"+spec.FullName(connector)]; ok {
			if err := external.ValidateConfig(connector, x.Spec.GetConfigSchema(), config); err != nil {
				return nil, err
			}
			return external.NewDestination(x.Executable, config, env.external(x.Spec)), nil
		}
		if exe := env.executable("destination", connector); exe != "" {
			return external.NewDestination(exe, config, env.external(nil)), nil
		}
	}
	f, err := lookup(destinations, "destination", connector, env)
	if err != nil {
		return nil, err
	}
	d, err := f(config)
	return d, errs.Wrap(errs.ConfigInvalid, err, "")
}

func lookup[F any](m map[string]F, kind, connector string, env Env) (F, error) {
	var zero F
	if strings.Contains(connector, "@") {
		return zero, errs.New(errs.ConfigInvalid, "%s connector %q: resolve its version first", kind, connector)
	}
	f, ok := m[builtinName(connector)]
	if !ok {
		names := make([]string, 0, len(m)+len(env.Manifests))
		for n := range m {
			names = append(names, n)
		}
		if kind == "source" {
			for n := range env.Manifests {
				names = append(names, shortName(n))
			}
		}
		for k := range env.Externals {
			if n, ok := strings.CutPrefix(k, kind+"/"); ok {
				names = append(names, shortName(n))
			}
		}
		names = append(names, env.externals(kind)...)
		slices.Sort(names)
		names = slices.Compact(names)
		return zero, errs.New(errs.ConfigInvalid, "unknown %s connector %q; known: %s", kind, connector, strings.Join(names, ", "))
	}
	return f, nil
}

// Builtin reports whether a connector of a kind, "source" or "destination",
// is compiled into openetld: openetl/NAME, or NAME.
func Builtin(kind, connector string) bool {
	name := builtinName(connector)
	if kind == "source" {
		_, ok := sources[name]
		return ok
	}
	_, ok := destinations[name]
	return ok
}

// builtinName is a built-in's key for openetl/NAME or NAME, and "" for
// another owner's.
func builtinName(connector string) string {
	if owner, short := spec.SplitName(connector); owner == spec.OfficialOwner {
		return short
	}
	return ""
}

// shortName is a full name as a user writes it: bare for openetl's.
func shortName(name string) string {
	if owner, short := spec.SplitName(name); owner == spec.OfficialOwner {
		return short
	}
	return name
}

// SplitRef splits a connector reference, such as postgres, postgres@1.2.0
// or postgres@sha256:…, into its name and version or digest.
func SplitRef(connector string) (name, ref string) {
	name, ref, _ = strings.Cut(connector, "@")
	return name, ref
}

// ConfigValue is the value at a dotted path in a config, such as
// auth.client_secret.
func ConfigValue(cfg *structpb.Struct, path string) (*structpb.Value, bool) {
	parent, key, ok := configParent(cfg, path)
	if !ok {
		return nil, false
	}
	v, ok := parent.GetFields()[key]
	return v, ok
}

// SetConfigValue sets the value at a dotted path that's already set, and
// reports whether it was.
func SetConfigValue(cfg *structpb.Struct, path string, v *structpb.Value) bool {
	parent, key, ok := configParent(cfg, path)
	if _, set := parent.GetFields()[key]; !ok || !set {
		return false
	}
	parent.Fields[key] = v
	return true
}

func configParent(cfg *structpb.Struct, path string) (*structpb.Struct, string, bool) {
	parts := strings.Split(path, ".")
	for _, p := range parts[:len(parts)-1] {
		next := cfg.GetFields()[p].GetStructValue()
		if next == nil {
			return nil, "", false
		}
		cfg = next
	}
	return cfg, parts[len(parts)-1], true
}
