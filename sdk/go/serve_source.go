package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"google.golang.org/grpc"

	"github.com/unsuitable001/openetl/sdk/go/arrowipc"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

type sourceServer struct {
	protocolv1.UnimplementedSourceServiceServer
	*base

	mu sync.Mutex
	// The running read's source, which Committed acknowledges on.
	reading source.Acknowledger
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
		schema, err := arrowipc.EncodeSchema(cs.Schema)
		if err != nil {
			return nil, status(err)
		}
		pc.Streams = append(pc.Streams, &protocolv1.CatalogStream{
			Name:          cs.Name,
			ArrowSchema:   schema,
			PrimaryKey:    cs.PrimaryKey,
			DefaultCursor: cs.CursorField,
			Replay:        protocolv1.Replay(cs.Replay), //nolint:gosec // small
		})
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
	var r source.ReadRequest
	for _, cs := range req.GetStreams() {
		r.Streams = append(r.Streams, source.ConfiguredStream{Name: cs.GetName(), State: cs.State})
	}
	return status(src.Read(ctx, r, &emitter{stream: stream}))
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
	if err := ack.Committed(ctx, req.GetStates()); err != nil {
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

var _ source.Emitter = (*emitter)(nil)

func (e *emitter) send(m *protocolv1.ReadResponse) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stream.Send(m)
}

func (e *emitter) Rows(stream string, rows []json.RawMessage) error {
	raw := make([][]byte, len(rows))
	for i, r := range rows {
		raw[i] = r
	}
	return e.send(&protocolv1.ReadResponse{Msg: &protocolv1.ReadResponse_Batch{Batch: &protocolv1.RecordBatch{
		Stream:  stream,
		Records: &protocolv1.RecordBatch_Rows{Rows: &protocolv1.JSONRows{Rows: raw}},
	}}})
}

// Records releases the batch once it's sent.
func (e *emitter) Records(stream string, batch arrow.RecordBatch) error {
	b, err := arrowipc.EncodeBatch(batch)
	if err != nil {
		return err
	}
	if err := e.send(&protocolv1.ReadResponse{Msg: &protocolv1.ReadResponse_Batch{Batch: &protocolv1.RecordBatch{
		Stream:  stream,
		Records: &protocolv1.RecordBatch_Arrow{Arrow: b},
	}}}); err != nil {
		return err
	}
	batch.Release()
	return nil
}

func (e *emitter) ResumePoint(stream string, state []byte) error {
	return e.send(&protocolv1.ReadResponse{Msg: &protocolv1.ReadResponse_ResumePoint{ResumePoint: &protocolv1.ResumePoint{Stream: stream, State: state}}})
}

func (e *emitter) Complete(stream string, next []byte) error {
	return e.send(&protocolv1.ReadResponse{Msg: &protocolv1.ReadResponse_Complete{Complete: &protocolv1.StreamComplete{Stream: stream, Next: next}}})
}
