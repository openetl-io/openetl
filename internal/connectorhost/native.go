// Package connectorhost hosts connectors. Today that means the native
// connectors compiled into openetld.
package connectorhost

import (
	"slices"
	"strings"

	"github.com/unsuitable001/openetl/connectors/native/faker"
	"github.com/unsuitable001/openetl/connectors/native/postgres"
	"github.com/unsuitable001/openetl/connectors/native/stdout"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

// Constructors only parse config; they must not do I/O, so the API can use
// them to validate.
var (
	sources = map[string]func([]byte) (source.Source, error){
		"faker": func(c []byte) (source.Source, error) { return faker.New(c) },
	}
	destinations = map[string]func([]byte) (destination.Destination, error){
		"postgres": func(c []byte) (destination.Destination, error) { return postgres.NewDestination(c) },
		"stdout":   func(c []byte) (destination.Destination, error) { return stdout.New(c) },
	}
)

// NewSource binds a source connector to its config, a JSON object.
func NewSource(connector string, config []byte) (source.Source, error) {
	f, err := lookup(sources, "source", connector)
	if err != nil {
		return nil, err
	}
	s, err := f(config)
	return s, errs.Wrap(errs.ConfigInvalid, err, "")
}

func NewDestination(connector string, config []byte) (destination.Destination, error) {
	f, err := lookup(destinations, "destination", connector)
	if err != nil {
		return nil, err
	}
	d, err := f(config)
	return d, errs.Wrap(errs.ConfigInvalid, err, "")
}

func lookup[F any](m map[string]F, kind, connector string) (F, error) {
	var zero F
	if strings.Contains(connector, "@") {
		return zero, errs.New(errs.ConfigInvalid, "%s connector %q: native connectors have no versions yet", kind, connector)
	}
	f, ok := m[connector]
	if !ok {
		names := make([]string, 0, len(m))
		for n := range m {
			names = append(names, n)
		}
		slices.Sort(names)
		return zero, errs.New(errs.ConfigInvalid, "unknown %s connector %q; known: %s", kind, connector, strings.Join(names, ", "))
	}
	return f, nil
}
