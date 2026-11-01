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

	"github.com/unsuitable001/openetl/connectors/native/faker"
	"github.com/unsuitable001/openetl/connectors/native/stdout"
	"github.com/unsuitable001/openetl/internal/connectorhost/external"
	"github.com/unsuitable001/openetl/internal/connectorhost/hostservices"
	"github.com/unsuitable001/openetl/internal/declarative"
	"github.com/unsuitable001/openetl/internal/enginesink"
	"github.com/unsuitable001/openetl/internal/netguard"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
	"github.com/unsuitable001/openetl/sdk/go/source"
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
	// Declarative sources, by name. Native connectors' names come first.
	Manifests map[string]*declarative.Connector
	// External connectors with descriptors, by kind and name, such as
	// source/postgres. Native connectors' names come first.
	Externals map[string]External
	// A directory of external connectors' executables, named source-{name}
	// and destination-{name}. Names it shares with the others come last.
	Connectors string
	// Nil: slog's default.
	Logger *slog.Logger
}

// NewSource binds a source connector to its config, a JSON object. It does
// no I/O, so the API can use it to validate; Start starts an external one.
func NewSource(connector string, config []byte, env Env) (source.Source, error) {
	if _, native := sources[connector]; !native {
		if m := env.Manifests[connector]; m != nil {
			return declarativeSource(m, config, env)
		}
		if x, ok := env.Externals["source/"+connector]; ok {
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

// executable is the external connector's path, or empty if there is none.
func (e Env) executable(kind, connector string) string {
	if e.Connectors == "" || strings.ContainsAny(connector, `/\@`) || connector == "" {
		return ""
	}
	p := filepath.Join(e.Connectors, kind+"-"+connector)
	if fi, err := os.Stat(p); err != nil || !fi.Mode().IsRegular() || fi.Mode()&0o111 == 0 {
		return ""
	}
	return p
}

func (e Env) external(spec *protocolv1.SpecResponse) external.Options {
	return external.Options{Egress: e.Egress, Logger: e.Logger, Spec: spec}
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
	if _, native := destinations[connector]; !native {
		if x, ok := env.Externals["destination/"+connector]; ok {
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
	f, ok := m[connector]
	if !ok {
		names := make([]string, 0, len(m)+len(env.Manifests))
		for n := range m {
			names = append(names, n)
		}
		if kind == "source" {
			for n := range env.Manifests {
				names = append(names, n)
			}
		}
		for k := range env.Externals {
			if n, ok := strings.CutPrefix(k, kind+"/"); ok {
				names = append(names, n)
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
// is compiled into openetld.
func Builtin(kind, connector string) bool {
	if kind == "source" {
		_, ok := sources[connector]
		return ok
	}
	_, ok := destinations[connector]
	return ok
}

// SplitRef splits a connector reference, such as postgres, postgres@1.2.0
// or postgres@sha256:…, into its name and version or digest.
func SplitRef(connector string) (name, ref string) {
	name, ref, _ = strings.Cut(connector, "@")
	return name, ref
}
