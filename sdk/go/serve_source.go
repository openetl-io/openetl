package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/internal/ints"
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
			Replay:        ints.Enum[protocolv1.Replay](cs.Replay),
			SplitsByKey:   cs.SplitsByKey,
		})
		for _, m := range cs.SyncModes {
			pc.Streams[len(pc.Streams)-1].SyncModes = append(pc.Streams[len(pc.Streams)-1].SyncModes, ints.Enum[protocolv1.SyncMode](m))
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
	r := source.ReadRequest{Pipeline: req.GetPipeline(), Continuous: req.GetContinuous(), Lanes: int(req.GetLanes())}
	for _, cs := range req.GetStreams() {
		r.Streams = append(r.Streams, configuredStream(cs))
	}
	out := newEmitter(stream, r.Lanes)
	err = src.Read(ctx, r, out)
	if sent := out.finish(); err == nil {
		err = sent
	}
	return status(err)
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
		out[i] = &protocolv1.Split{Id: s.ID, State: bytes.Clone(s.State)}
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

func (s *sourceServer) Retention(ctx context.Context, req *protocolv1.RetentionRequest) (*protocolv1.RetentionResponse, error) {
	src, err := s.source(ctx, req.GetConfig())
	if err != nil {
		return nil, status(err)
	}
	rr, ok := src.(source.RetentionReporter)
	if !ok {
		return nil, status(WithCode(protocolv1.ErrorCode_ERROR_CODE_INTERNAL, errors.New("the source doesn't report retention")))
	}
	var splits map[string][]source.Split
	for name, ss := range req.GetSplits() {
		if splits == nil {
			splits = map[string][]source.Split{}
		}
		splits[name] = splitsOf(ss.GetSplits())
	}
	ps, err := rr.Retention(ctx, req.GetPipeline(), req.GetStates(), splits)
	if err != nil {
		return nil, status(err)
	}
	resp := &protocolv1.RetentionResponse{}
	for _, p := range ps {
		resp.Progress = append(resp.Progress, progressProto(p))
	}
	return resp, nil
}

// emitter sends what a source reads, in the order it's emitted, encoding and
// sending behind a short queue so the goroutines reading aren't held up by
// the crossing. Its methods may be called from several goroutines. What they
// accept is sent after they return, so they keep nothing of the caller's, and
// a failure to send comes back from a later call or from finish.
type emitter struct {
	stream grpc.ServerStreamingServer[protocolv1.ReadResponse]
	// A nil entry is the end of the read.
	queue chan *outgoing
	// Closed once err holds the first failure; nothing is sent after it.
	failed chan struct{}
	err    error
	// Closed once nothing more will be sent.
	done chan struct{}
}

// outgoing is a queued message. A batch's has ready, closed once msg or err
// is set by its encoding.
type outgoing struct {
	msg   *protocolv1.ReadResponse
	err   error
	ready chan struct{}
}

var _ source.Progresser = (*emitter)(nil)

var errReadOver = errors.New("the read has ended")

// newEmitter queues one message per lane and one more, on top of the one
// being sent. Each queued batch is memory the source holds beyond its own.
func newEmitter(stream grpc.ServerStreamingServer[protocolv1.ReadResponse], lanes int) *emitter {
	e := &emitter{
		stream: stream,
		queue:  make(chan *outgoing, max(lanes, 1)+1),
		failed: make(chan struct{}),
		done:   make(chan struct{}),
	}
	go e.run()
	return e
}

func (e *emitter) run() {
	defer close(e.done)
	for o := range e.queue {
		if o == nil {
			return
		}
		if o.ready != nil {
			<-o.ready
		}
		if e.err != nil {
			continue
		}
		err := o.err
		if err == nil {
			err = e.stream.Send(o.msg)
		}
		if err != nil {
			e.err = err
			close(e.failed)
		}
	}
}

// finish waits until everything accepted has been sent, or sending has
// failed, and returns the failure. Nothing may be emitted after it.
func (e *emitter) finish() error {
	e.queue <- nil
	<-e.done
	return e.err
}

// put queues o, blocking under backpressure, and fails once the read is
// cancelled or sending has failed.
func (e *emitter) put(o *outgoing) error {
	ctx := e.stream.Context()
	select {
	case <-e.failed:
		return e.err
	case <-ctx.Done():
		return ctx.Err()
	case <-e.done:
		return errReadOver
	default:
	}
	select {
	case e.queue <- o:
		return nil
	case <-e.failed:
		return e.err
	case <-ctx.Done():
		return ctx.Err()
	case <-e.done:
		return errReadOver
	}
}

func (e *emitter) send(m *protocolv1.ReadResponse) error {
	return e.put(&outgoing{msg: m})
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
	return s.e.send(&protocolv1.ReadResponse{Msg: &protocolv1.ReadResponse_Batch{Batch: &protocolv1.RecordBatch{
		Stream:  s.stream,
		Split:   s.id,
		Records: &protocolv1.RecordBatch_Rows{Rows: &protocolv1.JSONRows{Rows: cloneRows(rows)}},
	}}})
}

// cloneRows copies rows into one allocation.
func cloneRows(rows []json.RawMessage) [][]byte {
	n := 0
	for _, r := range rows {
		n += len(r)
	}
	buf := make([]byte, 0, n)
	out := make([][]byte, len(rows))
	for i, r := range rows {
		start := len(buf)
		buf = append(buf, r...)
		out[i] = buf[start:len(buf):len(buf)]
	}
	return out
}

// Records takes the batch unless it returns an error.
func (s splitEmitter) Records(batch arrow.RecordBatch) error {
	o := &outgoing{ready: make(chan struct{})}
	if err := s.e.put(o); err != nil {
		return err
	}
	// As many of these run at once as the queue holds, so lanes' batches are
	// encoded in parallel and leave in the order they arrived.
	go func() {
		defer close(o.ready)
		defer batch.Release()
		b, err := arrowipc.EncodeBatch(batch)
		if err != nil {
			o.err = err
			return
		}
		o.msg = &protocolv1.ReadResponse{Msg: &protocolv1.ReadResponse_Batch{Batch: &protocolv1.RecordBatch{
			Stream:  s.stream,
			Split:   s.id,
			Records: &protocolv1.RecordBatch_Arrow{Arrow: b},
		}}}
	}()
	return nil
}

func (s splitEmitter) ResumePoint(state []byte, added ...source.Split) error {
	return s.e.send(&protocolv1.ReadResponse{Msg: &protocolv1.ReadResponse_ResumePoint{ResumePoint: &protocolv1.ResumePoint{
		Stream: s.stream, State: bytes.Clone(state), Split: s.id, Added: splitsProto(added),
	}}})
}

func (s splitEmitter) Complete(next []byte, added ...source.Split) error {
	return s.e.send(&protocolv1.ReadResponse{Msg: &protocolv1.ReadResponse_Complete{Complete: &protocolv1.StreamComplete{
		Stream: s.stream, Next: bytes.Clone(next), Split: s.id, Added: splitsProto(added),
	}}})
}

func (e *emitter) Progress(p source.Progress) error {
	return e.send(&protocolv1.ReadResponse{Msg: &protocolv1.ReadResponse_Progress{Progress: progressProto(p)}})
}

func progressProto(p source.Progress) *protocolv1.Progress {
	m := &protocolv1.Progress{Stream: p.Stream, LagBytes: p.LagBytes, LagRecords: p.LagRecords, RetentionLeftBytes: p.RetentionLeftBytes}
	if p.Lag != nil {
		m.Lag = durationpb.New(*p.Lag)
	}
	if p.RetentionLeft != nil {
		m.RetentionLeft = durationpb.New(*p.RetentionLeft)
	}
	return m
}
