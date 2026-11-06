package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

type sourceServer struct {
	protocolv1.UnimplementedSourceServiceServer
	*base

	mu sync.Mutex
	// The running read's source, which Committed acknowledges on and
	// Resplit splits on.
	reading   source.Acknowledger
	splitting source.Resplitter
}

func (s *sourceServer) Discover(ctx context.Context, req *protocolv1.DiscoverRequest) (*protocolv1.DiscoverResponse, error) {
	src, err := s.source(ctx, req.GetConfig())
	if err != nil {
		return nil, status(err)
	}
	cat, err := src.Discover(ctx)
	if err != nil {
		return nil, status(err)
	}
	pc := &protocolv1.Catalog{}
	for _, cs := range cat.Streams {
		declared := cs.Schema
		if cs.Seq != nil {
			var fields []arrow.Field
			if declared != nil {
				fields = declared.Fields()
			}
			declared = arrow.NewSchema(append(fields, source.OpField(), source.SeqField(cs.Seq)), nil)
		}
		schema, err := arrowipc.EncodeSchema(declared)
		if err != nil {
			return nil, status(err)
		}
		pc.Streams = append(pc.Streams, &protocolv1.CatalogStream{
			Name:          cs.Name,
			ArrowSchema:   schema,
			PrimaryKey:    cs.PrimaryKey,
			DefaultCursor: cs.CursorField,
			Replay:        protocolv1.Replay(cs.Replay), //nolint:gosec // small
			SplitsByKey:   cs.SplitsByKey,
		})
		for _, m := range cs.SyncModes {
			pc.Streams[len(pc.Streams)-1].SyncModes = append(pc.Streams[len(pc.Streams)-1].SyncModes, protocolv1.SyncMode(m)) //nolint:gosec // small
		}
	}
	return &protocolv1.DiscoverResponse{Catalog: pc}, nil
}

func (s *sourceServer) Read(req *protocolv1.ReadRequest, stream grpc.ServerStreamingServer[protocolv1.ReadResponse]) error {
	ctx := stream.Context()
	src, err := s.source(ctx, req.GetConfig())
	if err != nil {
		return status(err)
	}
	if ack, ok := src.(source.Acknowledger); ok && s.c.Acknowledges {
		s.mu.Lock()
		s.reading = ack
		s.mu.Unlock()
		defer func() {
			s.mu.Lock()
			s.reading = nil
			s.mu.Unlock()
		}()
	}
	if rs, ok := src.(source.Resplitter); ok && s.c.Resplit {
		s.mu.Lock()
		s.splitting = rs
		s.mu.Unlock()
		defer func() {
			s.mu.Lock()
			s.splitting = nil
			s.mu.Unlock()
		}()
	}
	r := source.ReadRequest{Pipeline: req.GetPipeline(), Continuous: req.GetContinuous()}
	for _, cs := range req.GetStreams() {
		r.Streams = append(r.Streams, configuredStream(cs))
	}
	return status(src.Read(ctx, r, &emitter{stream: stream}))
}

func configuredStream(cs *protocolv1.ConfiguredStream) source.ConfiguredStream {
	return source.ConfiguredStream{Name: cs.GetName(), SyncMode: source.SyncMode(cs.GetSyncMode()), Cursor: cs.GetCursor(), State: cs.State, Splits: splitsOf(cs.GetSplits())}
}

func splitsOf(ps []*protocolv1.Split) []source.Split {
	if len(ps) == 0 {
		return nil
	}
	out := make([]source.Split, len(ps))
	for i, p := range ps {
		out[i] = source.Split{ID: p.GetId(), State: p.State}
	}
	return out
}

func splitsProto(ss []source.Split) []*protocolv1.Split {
	out := make([]*protocolv1.Split, len(ss))
	for i, s := range ss {
		out[i] = &protocolv1.Split{Id: s.ID, State: s.State}
	}
	return out
}

func (s *sourceServer) Splits(ctx context.Context, req *protocolv1.SplitsRequest) (*protocolv1.SplitsResponse, error) {
	src, err := s.source(ctx, req.GetConfig())
	if err != nil {
		return nil, status(err)
	}
	sp, ok := src.(source.Splitter)
	if !ok {
		return nil, status(WithCode(protocolv1.ErrorCode_ERROR_CODE_INTERNAL, errors.New("the source doesn't split streams")))
	}
	splits, err := sp.Splits(ctx, configuredStream(req.GetStream()), splitsOf(req.GetEnded()))
	if err != nil {
		return nil, status(err)
	}
	return &protocolv1.SplitsResponse{Splits: splitsProto(splits)}, nil
}

func (s *sourceServer) Resplit(ctx context.Context, req *protocolv1.ResplitRequest) (*protocolv1.ResplitResponse, error) {
	s.mu.Lock()
	rs, ok := s.splitting, s.splitting != nil
	s.mu.Unlock()
	if !ok {
		return nil, status(WithCode(protocolv1.ErrorCode_ERROR_CODE_INTERNAL, errors.New("no read that splits again is running")))
	}
	accepted, err := rs.Resplit(ctx, req.GetStream(), req.GetSplit())
	if err != nil {
		return nil, status(err)
	}
	return &protocolv1.ResplitResponse{Accepted: accepted}, nil
}

func (s *sourceServer) Committed(ctx context.Context, req *protocolv1.CommittedRequest) (*protocolv1.CommittedResponse, error) {
	s.mu.Lock()
	ack := s.reading
	s.mu.Unlock()
	if ack == nil {
		src, err := s.source(ctx, req.GetConfig())
		if err != nil {
			return nil, status(err)
		}
		var ok bool
		if ack, ok = src.(source.Acknowledger); !ok {
			return nil, status(WithCode(protocolv1.ErrorCode_ERROR_CODE_INTERNAL, errNoAcknowledger))
		}
	}
	var splits map[string][]source.Split
	for name, ss := range req.GetSplits() {
		if splits == nil {
			splits = map[string][]source.Split{}
		}
		splits[name] = splitsOf(ss.GetSplits())
	}
	if err := ack.Committed(ctx, req.GetStates(), splits); err != nil {
		return nil, status(err)
	}
	return &protocolv1.CommittedResponse{}, nil
}

var errNoAcknowledger = errors.New("the source doesn't acknowledge")

// emitter sends what a source reads. Its methods may be called from several
// goroutines.
type emitter struct {
	mu     sync.Mutex
	stream grpc.ServerStreamingServer[protocolv1.ReadResponse]
}

var _ source.Progresser = (*emitter)(nil)

func (e *emitter) send(m *protocolv1.ReadResponse) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stream.Send(m)
}

func (e *emitter) Rows(stream string, rows []json.RawMessage) error {
	return e.Split(stream, source.WholeStream).Rows(rows)
}

func (e *emitter) Records(stream string, batch arrow.RecordBatch) error {
	return e.Split(stream, source.WholeStream).Records(batch)
}

func (e *emitter) ResumePoint(stream string, state []byte) error {
	return e.Split(stream, source.WholeStream).ResumePoint(state)
}

func (e *emitter) Complete(stream string, next []byte) error {
	return e.Split(stream, source.WholeStream).Complete(next)
}

func (e *emitter) Split(stream, id string) source.SplitEmitter {
	return splitEmitter{e: e, stream: stream, id: id}
}

type splitEmitter struct {
	e          *emitter
	stream, id string
}

func (s splitEmitter) Rows(rows []json.RawMessage) error {
	raw := make([][]byte, len(rows))
	for i, r := range rows {
		raw[i] = r
	}
	return s.e.send(&protocolv1.ReadResponse{Msg: &protocolv1.ReadResponse_Batch{Batch: &protocolv1.RecordBatch{
		Stream:  s.stream,
		Split:   s.id,
		Records: &protocolv1.RecordBatch_Rows{Rows: &protocolv1.JSONRows{Rows: raw}},
	}}})
}

// Records releases the batch once it's sent.
func (s splitEmitter) Records(batch arrow.RecordBatch) error {
	b, err := arrowipc.EncodeBatch(batch)
	if err != nil {
		return err
	}
	if err := s.e.send(&protocolv1.ReadResponse{Msg: &protocolv1.ReadResponse_Batch{Batch: &protocolv1.RecordBatch{
		Stream:  s.stream,
		Split:   s.id,
		Records: &protocolv1.RecordBatch_Arrow{Arrow: b},
	}}}); err != nil {
		return err
	}
	batch.Release()
	return nil
}

func (s splitEmitter) ResumePoint(state []byte, added ...source.Split) error {
	return s.e.send(&protocolv1.ReadResponse{Msg: &protocolv1.ReadResponse_ResumePoint{ResumePoint: &protocolv1.ResumePoint{
		Stream: s.stream, State: state, Split: s.id, Added: splitsProto(added),
	}}})
}

func (s splitEmitter) Complete(next []byte, added ...source.Split) error {
	return s.e.send(&protocolv1.ReadResponse{Msg: &protocolv1.ReadResponse_Complete{Complete: &protocolv1.StreamComplete{
		Stream: s.stream, Next: next, Split: s.id, Added: splitsProto(added),
	}}})
}

func (e *emitter) Progress(p source.Progress) error {
	m := &protocolv1.Progress{Stream: p.Stream, LagBytes: p.LagBytes, LagRecords: p.LagRecords, RetentionLeftBytes: p.RetentionLeftBytes}
	if p.Lag != nil {
		m.Lag = durationpb.New(*p.Lag)
	}
	if p.RetentionLeft != nil {
		m.RetentionLeft = durationpb.New(*p.RetentionLeft)
	}
	return e.send(&protocolv1.ReadResponse{Msg: &protocolv1.ReadResponse_Progress{Progress: m}})
}
