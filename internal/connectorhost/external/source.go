package external

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/sdk/go/arrowipc"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

// instance is one bound connector, whose process starts on first use.
type instance struct {
	exe    string
	kind   protocolv1.ConnectorKind
	config []byte
	opts   Options

	mu sync.Mutex
	p  *process
}

// Start starts the connector's process, if it isn't running.
func (c *instance) Start(ctx context.Context) error {
	_, err := c.proc(ctx)
	return err
}

func (c *instance) proc(ctx context.Context) (*process, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.p != nil {
		return c.p, nil
	}
	p, err := start(ctx, c.exe, c.kind, c.config, c.opts)
	if err != nil {
		return nil, err
	}
	c.p = p
	return p, nil
}

// Close stops the process.
func (c *instance) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.p != nil {
		c.p.close()
		c.p = nil
	}
	return nil
}

func (c *instance) check(ctx context.Context) error {
	p, err := c.proc(ctx)
	if err != nil {
		return err
	}
	resp, err := protocolv1.NewConnectorServiceClient(p.conn).Check(ctx, &protocolv1.CheckRequest{Config: p.config})
	if err != nil {
		return p.failure(errs.FromStatus(err))
	}
	return errs.FromProto(resp.GetError())
}

// Source is an external source. It acknowledges upstream only if its spec
// says it does.
type Source struct{ instance }

var (
	_ source.Source       = (*Source)(nil)
	_ source.Checker      = (*Source)(nil)
	_ source.Acknowledger = (*Source)(nil)
	_ source.Teardowner   = (*Source)(nil)
)

// NewSource binds the source at exe to config, a JSON object. It does no
// I/O.
func NewSource(exe string, config []byte, opts Options) *Source {
	return &Source{instance{exe: exe, kind: protocolv1.ConnectorKind_CONNECTOR_KIND_SOURCE, config: config, opts: opts}}
}

func (s *Source) Check(ctx context.Context) error { return s.check(ctx) }

func (s *Source) Teardown(ctx context.Context, pipeline string) error {
	p, err := s.proc(ctx)
	if err != nil {
		return err
	}
	if _, err := protocolv1.NewConnectorServiceClient(p.conn).Teardown(ctx, &protocolv1.TeardownRequest{Config: p.config, Pipeline: pipeline}); err != nil {
		return p.failure(errs.FromStatus(err))
	}
	return nil
}

func (s *Source) Discover(ctx context.Context) (source.Catalog, error) {
	var cat source.Catalog
	p, err := s.proc(ctx)
	if err != nil {
		return cat, err
	}
	resp, err := protocolv1.NewSourceServiceClient(p.conn).Discover(ctx, &protocolv1.DiscoverRequest{Config: p.config})
	if err != nil {
		return cat, p.failure(errs.FromStatus(err))
	}
	for _, cs := range resp.GetCatalog().GetStreams() {
		schema, err := arrowipc.DecodeSchema(cs.GetArrowSchema())
		if err != nil {
			return cat, errs.Wrap(errs.Internal, err, "connector: discover: the schema of "+cs.GetName())
		}
		st := source.CatalogStream{
			Name:        cs.GetName(),
			PrimaryKey:  cs.GetPrimaryKey(),
			CursorField: cs.GetDefaultCursor(),
			Replay:      source.Replay(cs.GetReplay()),
			Schema:      schema,
		}
		for _, m := range cs.GetSyncModes() {
			st.SyncModes = append(st.SyncModes, source.SyncMode(m))
		}
		cat.Streams = append(cat.Streams, st)
	}
	return cat, nil
}

func (s *Source) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	p, err := s.proc(ctx)
	if err != nil {
		return err
	}
	preq := &protocolv1.ReadRequest{Config: p.config, Pipeline: req.Pipeline, Continuous: req.Continuous}
	for _, cs := range req.Streams {
		preq.Streams = append(preq.Streams, &protocolv1.ConfiguredStream{Name: cs.Name, SyncMode: protocolv1.SyncMode(cs.SyncMode), State: cs.State}) //nolint:gosec // small
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := protocolv1.NewSourceServiceClient(p.conn).Read(ctx, preq)
	if err != nil {
		return p.failure(errs.FromStatus(err))
	}
	for {
		m, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return p.failure(errs.FromStatus(err))
		}
		if err := emit(m, out); err != nil {
			return err
		}
	}
}

func emit(m *protocolv1.ReadResponse, out source.Emitter) error {
	switch x := m.GetMsg().(type) {
	case *protocolv1.ReadResponse_Batch:
		b := x.Batch
		switch r := b.GetRecords().(type) {
		case *protocolv1.RecordBatch_Rows:
			rows := make([]json.RawMessage, len(r.Rows.GetRows()))
			for i, row := range r.Rows.GetRows() {
				rows[i] = row
			}
			return out.Rows(b.GetStream(), rows)
		case *protocolv1.RecordBatch_Arrow:
			if fw, ok := out.(source.Forwarder); ok {
				enc, err := arrowipc.Inspect(r.Arrow)
				if err != nil {
					return errs.Wrap(errs.Internal, err, "connector: a batch for "+b.GetStream())
				}
				return fw.Forward(b.GetStream(), enc)
			}
			rec, err := arrowipc.DecodeBatch(r.Arrow, memory.DefaultAllocator)
			if err != nil {
				return errs.Wrap(errs.Internal, err, "connector: a batch for "+b.GetStream())
			}
			if err := out.Records(b.GetStream(), rec); err != nil {
				rec.Release()
				return err
			}
			return nil
		}
		return errs.New(errs.Internal, "connector: an empty batch for %s", b.GetStream())
	case *protocolv1.ReadResponse_ResumePoint:
		return out.ResumePoint(x.ResumePoint.GetStream(), x.ResumePoint.GetState())
	case *protocolv1.ReadResponse_Complete:
		return out.Complete(x.Complete.GetStream(), x.Complete.Next)
	default:
		return errs.New(errs.Internal, "connector: unexpected %T", x)
	}
}

func (s *Source) Committed(ctx context.Context, states map[string][]byte) error {
	p, err := s.proc(ctx)
	if err != nil {
		return err
	}
	if !p.spec.GetSource().GetAcknowledges() {
		return nil
	}
	if _, err := protocolv1.NewSourceServiceClient(p.conn).Committed(ctx, &protocolv1.CommittedRequest{States: states, Config: p.config}); err != nil {
		return fmt.Errorf("connector: committed: %w", p.failure(errs.FromStatus(err)))
	}
	return nil
}
