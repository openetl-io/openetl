// Package sdk is how connector authors write sources: declare the config and
// the streams, say how to fetch, and mark where it is safe to resume. The
// platform decides when to commit.
package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/openetl-io/openetl/sdk/go/host"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// Source declares a source connector with config C.
type Source[C any] struct {
	Name string
	// Optional: verifies that cfg reaches the source.
	Check   func(ctx context.Context, cfg *C) error
	Streams []Stream[C]
}

// Ctx is a read's context and config.
type Ctx[C any] struct {
	context.Context
	Config *C
	// Nil if the source was bound without host services.
	Host host.Host
}

// HTTP sends req through host services: retries, rate limits and auth.
func (c *Ctx[C]) HTTP(req *host.Request) (*host.Response, error) {
	if c.Host == nil {
		return nil, WithCode(protocolv1.ErrorCode_ERROR_CODE_INTERNAL, errors.New("HTTP: the source has no host services"))
	}
	return c.Host.HTTP(c, req)
}

// New binds s to config without host services.
func (s *Source[C]) New(config []byte) (source.Source, error) { return s.Bind(config, nil) }

// Bind binds s to config, a JSON object, and to its host services. Unknown
// fields are an error, and C's Validate method runs if it has one. Bind
// does no I/O.
func (s *Source[C]) Bind(config []byte, h host.Host) (source.Source, error) {
	seen := make(map[string]bool, len(s.Streams))
	for _, st := range s.Streams {
		if st.name == "" || seen[st.name] {
			return nil, fmt.Errorf("%s: stream name %q is empty or repeated", s.Name, st.name)
		}
		seen[st.name] = true
	}
	b := &bound[C]{src: s, host: h}
	if len(bytes.TrimSpace(config)) > 0 {
		dec := json.NewDecoder(bytes.NewReader(config))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&b.cfg); err != nil {
			return nil, fmt.Errorf("%s: config: %w", s.Name, err)
		}
	}
	if v, ok := any(&b.cfg).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return nil, fmt.Errorf("%s: config: %w", s.Name, err)
		}
	}
	return b, nil
}

type bound[C any] struct {
	src  *Source[C]
	cfg  C
	host host.Host
}

var (
	_ source.Source  = (*bound[struct{}])(nil)
	_ source.Checker = (*bound[struct{}])(nil)
)

func (b *bound[C]) Check(ctx context.Context) error {
	if b.src.Check == nil {
		return nil
	}
	return b.src.Check(ctx, &b.cfg)
}

func (b *bound[C]) Discover(context.Context) (source.Catalog, error) {
	var c source.Catalog
	for _, st := range b.src.Streams {
		cs := source.CatalogStream{Name: st.name, PrimaryKey: st.primaryKey, CursorField: st.cursor, Replay: source.Superset}
		switch st.kind {
		case full:
			cs.SyncModes = []source.SyncMode{source.FullRefresh}
		case incremental:
			cs.SyncModes = []source.SyncMode{source.Incremental}
		}
		c.Streams = append(c.Streams, cs)
	}
	return c, nil
}

// Read reads the requested streams one after another.
func (b *bound[C]) Read(ctx context.Context, req source.ReadRequest, emit source.Emitter) error {
	for _, cs := range req.Streams {
		i := b.stream(cs.Name)
		if i < 0 {
			return fmt.Errorf("%s: unknown stream %q", b.src.Name, cs.Name)
		}
		if err := b.read(ctx, &b.src.Streams[i], cs.State, emit); err != nil {
			return fmt.Errorf("%s: %s: %w", b.src.Name, cs.Name, err)
		}
	}
	return nil
}

func (b *bound[C]) stream(name string) int {
	for i, st := range b.src.Streams {
		if st.name == name {
			return i
		}
	}
	return -1
}

func (b *bound[C]) read(ctx context.Context, st *Stream[C], state []byte, emit source.Emitter) error {
	var s streamState
	if len(state) > 0 && st.kind != resumable {
		if err := json.Unmarshal(state, &s); err != nil {
			return fmt.Errorf("state: %w", err)
		}
	}
	c := &Ctx[C]{Context: ctx, Config: &b.cfg, Host: b.host}
	out := newOut(ctx, st, emit, s, state)
	err := st.run(c, state, out)
	if errors.Is(err, ErrPageTokenExpired) && out.expired {
		// Start over from the floor, without the page token.
		s.Since, s.Max, s.Page = nil, nil, ""
		out = newOut(ctx, st, emit, s, state)
		err = st.run(c, state, out)
	}
	if err != nil {
		return err
	}
	return out.complete()
}
