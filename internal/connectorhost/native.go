// Package connectorhost hosts connectors: the native connectors compiled
// into openetld, the engine's Parquet sink, and declarative sources, each
// instance with its own host services.
package connectorhost

import (
	"log/slog"
	"slices"
	"strings"

	"github.com/unsuitable001/openetl/connectors/native/faker"
	"github.com/unsuitable001/openetl/connectors/native/postgres"
	"github.com/unsuitable001/openetl/connectors/native/stdout"
	"github.com/unsuitable001/openetl/internal/connectorhost/hostservices"
	"github.com/unsuitable001/openetl/internal/declarative"
	"github.com/unsuitable001/openetl/internal/enginesink"
	"github.com/unsuitable001/openetl/internal/netguard"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

// Constructors only parse config; they must not do I/O, so the API can use
// them to validate.
var (
	sources = map[string]func([]byte) (source.Source, error){
		"faker":    func(c []byte) (source.Source, error) { return faker.New(c) },
		"postgres": func(c []byte) (source.Source, error) { return postgres.NewSource(c) },
	}
	destinations = map[string]func([]byte) (destination.Destination, error){
		"parquet":  func(c []byte) (destination.Destination, error) { return enginesink.NewParquet(c) },
		"postgres": func(c []byte) (destination.Destination, error) { return postgres.NewDestination(c) },
		"stdout":   func(c []byte) (destination.Destination, error) { return stdout.New(c) },
	}
)

// Env is what the runner gives the connectors it hosts. The zero Env is
// enough to validate a config.
type Env struct {
	// Which addresses connectors may reach besides public ones.
	Egress netguard.Policy
	// Declarative sources, by name. Native connectors' names come first.
	Manifests map[string]*declarative.Connector
	// Nil: slog's default.
	Logger *slog.Logger
}

// NewSource binds a source connector to its config, a JSON object.
func NewSource(connector string, config []byte, env Env) (source.Source, error) {
	if _, native := sources[connector]; !native {
		if m := env.Manifests[connector]; m != nil {
			return declarativeSource(m, config, env)
		}
	}
	f, err := lookup(sources, "source", connector, env.Manifests)
	if err != nil {
		return nil, err
	}
	s, err := f(config)
	return s, errs.Wrap(errs.ConfigInvalid, err, "")
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

func NewDestination(connector string, config []byte) (destination.Destination, error) {
	f, err := lookup(destinations, "destination", connector, nil)
	if err != nil {
		return nil, err
	}
	d, err := f(config)
	return d, errs.Wrap(errs.ConfigInvalid, err, "")
}

func lookup[F any](m map[string]F, kind, connector string, manifests map[string]*declarative.Connector) (F, error) {
	var zero F
	if strings.Contains(connector, "@") {
		return zero, errs.New(errs.ConfigInvalid, "%s connector %q: native connectors have no versions yet", kind, connector)
	}
	f, ok := m[connector]
	if !ok {
		names := make([]string, 0, len(m)+len(manifests))
		for n := range m {
			names = append(names, n)
		}
		for n := range manifests {
			names = append(names, n)
		}
		slices.Sort(names)
		return zero, errs.New(errs.ConfigInvalid, "unknown %s connector %q; known: %s", kind, connector, strings.Join(names, ", "))
	}
	return f, nil
}
