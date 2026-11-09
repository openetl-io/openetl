package external

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/platform/ints"
	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
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
	_ source.Setupper     = (*Source)(nil)
	_ source.Teardowner   = (*Source)(nil)
)

// NewSource binds the source at exe to config, a JSON object. It does no
// I/O.
func NewSource(exe string, config []byte, opts Options) *Source {
	return &Source{instance{exe: exe, kind: protocolv1.ConnectorKind_CONNECTOR_KIND_SOURCE, config: config, opts: opts}}
}

func (s *Source) Check(ctx context.Context) error { return s.check(ctx) }

// Setup and Teardown call the connector only when its spec sets setup, as
// the protocol says.
func (c *instance) Setup(ctx context.Context, pipeline string) error {
	return c.prepare(ctx, pipeline, false)
}

func (c *instance) Teardown(ctx context.Context, pipeline string) error {
	return c.prepare(ctx, pipeline, true)
}

func (c *instance) prepare(ctx context.Context, pipeline string, teardown bool) error {
	p, err := c.proc(ctx)
	if err != nil || !p.spec.GetSetup() {
		return err
	}
	cc := protocolv1.NewConnectorServiceClient(p.conn)
	if teardown {
		_, err = cc.Teardown(ctx, &protocolv1.TeardownRequest{Config: p.config, Pipeline: pipeline})
	} else {
		_, err = cc.Setup(ctx, &protocolv1.SetupRequest{Config: p.config, Pipeline: pipeline})
	}
	if err != nil {
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
			SplitsByKey: cs.GetSplitsByKey(),
		}
		st.Schema, st.Seq = source.SplitDeclared(schema)
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
	if req.Lanes > 0 && req.Lanes <= math.MaxUint32 {
		preq.Lanes = uint32(req.Lanes)
	}
	for _, cs := range req.Streams {
		preq.Streams = append(preq.Streams, configuredStream(cs))
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

func configuredStream(cs source.ConfiguredStream) *protocolv1.ConfiguredStream {
	return &protocolv1.ConfiguredStream{Name: cs.Name, SyncMode: ints.Enum[protocolv1.SyncMode](cs.SyncMode), Cursor: cs.Cursor, State: cs.State, Splits: splitsProto(cs.Splits), Columns: cs.Columns}
}

func splitsProto(ss []source.Split) []*protocolv1.Split {
	if len(ss) == 0 {
		return nil
	}
	out := make([]*protocolv1.Split, len(ss))
	for i, s := range ss {
		out[i] = &protocolv1.Split{Id: s.ID, State: s.State}
	}
	return out
}

func splitsOf(ps []*protocolv1.Split) []source.Split {
	out := make([]source.Split, len(ps))
	for i, p := range ps {
		out[i] = source.Split{ID: p.GetId(), State: p.State}
	}
	return out
}

func emit(m *protocolv1.ReadResponse, out source.Emitter) error {
	switch x := m.GetMsg().(type) {
	case *protocolv1.ReadResponse_Batch:
		b := x.Batch
		sp := out.Split(b.GetStream(), b.GetSplit())
		switch r := b.GetRecords().(type) {
		case *protocolv1.RecordBatch_Rows:
			rows := make([]json.RawMessage, len(r.Rows.GetRows()))
			for i, row := range r.Rows.GetRows() {
				rows[i] = row
			}
			return sp.Rows(rows)
		case *protocolv1.RecordBatch_Arrow:
			if fw, ok := sp.(source.SplitForwarder); ok {
				enc, err := arrowipc.Inspect(r.Arrow)
				if err != nil {
					return errs.Wrap(errs.Internal, err, "connector: a batch for "+b.GetStream())
				}
				return fw.Forward(enc)
			}
			rec, err := arrowipc.DecodeBatch(r.Arrow, memory.DefaultAllocator)
			if err != nil {
				return errs.Wrap(errs.Internal, err, "connector: a batch for "+b.GetStream())
			}
			if err := sp.Records(rec); err != nil {
				rec.Release()
				return err
			}
			return nil
		}
		return errs.New(errs.Internal, "connector: an empty batch for %s", b.GetStream())
	case *protocolv1.ReadResponse_ResumePoint:
		r := x.ResumePoint
		return out.Split(r.GetStream(), r.GetSplit()).ResumePoint(r.GetState(), splitsOf(r.GetAdded())...)
	case *protocolv1.ReadResponse_Complete:
		c := x.Complete
		return out.Split(c.GetStream(), c.GetSplit()).Complete(c.Next, splitsOf(c.GetAdded())...)
	case *protocolv1.ReadResponse_Progress:
		if pr, ok := out.(source.Progresser); ok {
			return pr.Progress(progress(x.Progress))
		}
		return nil
	case nil:
		return nil // a message this runner doesn't know, from a newer connector
	default:
		return errs.New(errs.Internal, "connector: unexpected %T", x)
	}
}

func progress(m *protocolv1.Progress) source.Progress {
	p := source.Progress{Stream: m.GetStream(), LagBytes: m.LagBytes, LagRecords: m.LagRecords, RetentionLeftBytes: m.RetentionLeftBytes}
	if m.Lag != nil {
		d := m.GetLag().AsDuration()
		p.Lag = &d
	}
	if m.RetentionLeft != nil {
		d := m.GetRetentionLeft().AsDuration()
		p.RetentionLeft = &d
	}
	return p
}

func (s *Source) Committed(ctx context.Context, states map[string][]byte, splits map[string][]source.Split) error {
	p, err := s.proc(ctx)
	if err != nil {
		return err
	}
	if !p.spec.GetSource().GetAcknowledges() {
		return nil
	}
	req := &protocolv1.CommittedRequest{States: states, Config: p.config}
	for name, ss := range splits {
		if req.Splits == nil {
			req.Splits = map[string]*protocolv1.Splits{}
		}
		req.Splits[name] = &protocolv1.Splits{Splits: splitsProto(ss)}
	}
	if _, err := protocolv1.NewSourceServiceClient(p.conn).Committed(ctx, req); err != nil {
		return fmt.Errorf("connector: committed: %w", p.failure(errs.FromStatus(err)))
	}
	return nil
}

// Splits asks a connector that splits streams for a stream's splits. One
// that doesn't reads it whole, from where its last read ended.
func (s *Source) Splits(ctx context.Context, stream source.ConfiguredStream, ended []source.Split) ([]source.Split, error) {
	p, err := s.proc(ctx)
	if err != nil {
		return nil, err
	}
	if !p.spec.GetSource().GetSplits() {
		return source.Whole(ended)
	}
	resp, err := protocolv1.NewSourceServiceClient(p.conn).Splits(ctx, &protocolv1.SplitsRequest{Config: p.config, Stream: configuredStream(stream), Ended: splitsProto(ended)})
	if err != nil {
		return nil, fmt.Errorf("connector: splits of %s: %w", stream.Name, p.failure(errs.FromStatus(err)))
	}
	return splitsOf(resp.GetSplits()), nil
}

// Resplits says the connector splits a running split's remainder.
func (s *Source) Resplits(ctx context.Context) bool {
	p, err := s.proc(ctx)
	return err == nil && p.spec.GetSource().GetResplit()
}

// Resplit asks a connector that splits a running split's remainder to; one
// that doesn't, doesn't.
func (s *Source) Resplit(ctx context.Context, stream, split string) (bool, error) {
	p, err := s.proc(ctx)
	if err != nil {
		return false, err
	}
	if !p.spec.GetSource().GetResplit() {
		return false, nil
	}
	resp, err := protocolv1.NewSourceServiceClient(p.conn).Resplit(ctx, &protocolv1.ResplitRequest{Config: p.config, Stream: stream, Split: split})
	if err != nil {
		return false, fmt.Errorf("connector: resplit %s of %s: %w", split, stream, p.failure(errs.FromStatus(err)))
	}
	return resp.GetAccepted(), nil
}

// Retention asks a connector that reports retention how far a checkpoint's
// positions are behind its source; one that doesn't reports none.
func (s *Source) Retention(ctx context.Context, pipeline string, states map[string][]byte, splits map[string][]source.Split) ([]source.Progress, error) {
	p, err := s.proc(ctx)
	if err != nil {
		return nil, err
	}
	if !p.spec.GetSource().GetRetention() {
		return nil, nil
	}
	req := &protocolv1.RetentionRequest{Config: p.config, Pipeline: pipeline, States: states}
	for name, ss := range splits {
		if req.Splits == nil {
			req.Splits = map[string]*protocolv1.Splits{}
		}
		req.Splits[name] = &protocolv1.Splits{Splits: splitsProto(ss)}
	}
	resp, err := protocolv1.NewSourceServiceClient(p.conn).Retention(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("connector: retention: %w", p.failure(errs.FromStatus(err)))
	}
	out := make([]source.Progress, len(resp.GetProgress()))
	for i, m := range resp.GetProgress() {
		out[i] = progress(m)
	}
	return out, nil
}
