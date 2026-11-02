package sdk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/unsuitable001/openetl/sdk/go/arrowipc"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
)

type destinationServer struct {
	protocolv1.UnimplementedDestinationServiceServer
	*base
}

// Write runs one writer. Recover, batches, reads' starts and ends, and
// pre-commits run in order on this goroutine; schemas and commits on
// another, so a commit runs alongside later batches.
func (d *destinationServer) Write(stream grpc.BidiStreamingServer[protocolv1.WriteRequest, protocolv1.WriteResponse]) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	open := first.GetOpen()
	if open == nil {
		return status(errors.New("write: the first message must be open"))
	}
	dst, err := d.destination(ctx, open.GetConfig())
	if err != nil {
		return status(err)
	}
	cfg := destination.OpenConfig{
		Pipeline:   open.GetPipeline(),
		Generation: open.GetGeneration(),
		Scope:      destination.Scope(open.GetScope()),
		Namespace:  open.GetNamespace(),
	}
	for _, s := range open.GetStreams() {
		cfg.Streams = append(cfg.Streams, destination.Stream{
			Name: s.GetName(), Mode: destination.WriteMode(s.GetMode()), PrimaryKey: s.GetPrimaryKey(),
			Dedupe: destination.Dedupe(s.GetDedupe()), Deletes: destination.DeleteMode(s.GetDeletes()),
		})
	}
	w, err := dst.Open(ctx, cfg)
	if err != nil {
		return status(err)
	}
	sa, applies := w.(destination.SchemaApplier)
	rt, _ := w.(destination.ReadTracker)

	var sendMu sync.Mutex
	send := func(m *protocolv1.WriteResponse) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.Send(m)
	}
	fail := send(&protocolv1.WriteResponse{Msg: &protocolv1.WriteResponse_Opened{Opened: &protocolv1.WriteOpened{AppliesSchema: applies}}})

	lane := make(chan *protocolv1.WriteRequest, 1)
	laneErr := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Go(func() {
		for m := range lane {
			var resp *protocolv1.WriteResponse
			switch x := m.GetMsg().(type) {
			case *protocolv1.WriteRequest_Commit:
				e := epoch(x.Commit.GetEpoch())
				c, err := w.Commit(ctx, e, x.Commit.GetStaged())
				resp = &protocolv1.WriteResponse{Msg: &protocolv1.WriteResponse_Committed{Committed: &protocolv1.WriteCommitted{Epoch: x.Commit.GetEpoch(), Error: protoError(err), CollapsedRows: c.Collapsed}}}
			case *protocolv1.WriteRequest_ApplySchema:
				err := applySchema(ctx, sa, x.ApplySchema)
				resp = &protocolv1.WriteResponse{Msg: &protocolv1.WriteResponse_SchemaApplied{SchemaApplied: &protocolv1.WriteSchemaApplied{Error: protoError(err)}}}
			}
			if err := send(resp); err != nil {
				laneErr <- err
				cancel()
				return
			}
		}
	})

	gen := open.GetGeneration()
	for fail == nil {
		var m *protocolv1.WriteRequest
		if m, fail = stream.Recv(); fail != nil {
			if errors.Is(fail, io.EOF) {
				fail = nil
			}
			break
		}
		switch x := m.GetMsg().(type) {
		case *protocolv1.WriteRequest_Recover:
			var r destination.Recovery
			if r, fail = w.Recover(ctx, gen, epoch(x.Recover.GetEpoch()), x.Recover.GetStaged()); fail == nil {
				fail = send(&protocolv1.WriteResponse{Msg: &protocolv1.WriteResponse_Recovered{Recovered: &protocolv1.WriteRecovered{Recovery: protocolv1.Recovery(r)}}}) //nolint:gosec // small
			}
		case *protocolv1.WriteRequest_Batch:
			fail = write(ctx, w, x.Batch)
		case *protocolv1.WriteRequest_StartReads:
			if rt != nil {
				starts := make(map[string]time.Time, len(x.StartReads.GetStarts()))
				for name, t := range x.StartReads.GetStarts() {
					starts[name] = t.AsTime()
				}
				fail = rt.StartReads(ctx, starts)
			}
		case *protocolv1.WriteRequest_EndRead:
			if rt != nil {
				fail = rt.EndRead(ctx, epoch(x.EndRead.GetEpoch()), x.EndRead.GetStream())
			}
		case *protocolv1.WriteRequest_PreCommit:
			var st destination.Staged
			if st, fail = w.PreCommit(ctx, epoch(x.PreCommit.GetEpoch())); fail == nil {
				fail = send(&protocolv1.WriteResponse{Msg: &protocolv1.WriteResponse_Staged{Staged: &protocolv1.WriteStaged{Epoch: x.PreCommit.GetEpoch(), Staged: st}}})
			}
		case *protocolv1.WriteRequest_Commit, *protocolv1.WriteRequest_ApplySchema:
			if _, ok := x.(*protocolv1.WriteRequest_ApplySchema); ok && !applies {
				fail = errors.New("write: schemas sent to a destination without one")
				break
			}
			select {
			case lane <- m:
			case <-ctx.Done():
				fail = ctx.Err()
			}
		default:
			fail = fmt.Errorf("write: unexpected %T", x)
		}
	}
	close(lane)
	wg.Wait()
	cerr := w.Close(context.WithoutCancel(ctx))
	select {
	case err := <-laneErr:
		fail = cmpErr(fail, err)
	default:
	}
	return status(cmpErr(fail, cerr))
}

func cmpErr(a, b error) error {
	if a != nil {
		return a
	}
	return b
}

func write(ctx context.Context, w destination.Writer, b *protocolv1.WriteBatch) error {
	rec, err := arrowipc.DecodeBatch(b.GetArrow(), memory.DefaultAllocator)
	if err != nil {
		return fmt.Errorf("write: batch for %s: %w", b.GetStream(), err)
	}
	defer rec.Release()
	return w.Write(ctx, epoch(b.GetEpoch()), b.GetStream(), rec)
}

func applySchema(ctx context.Context, sa destination.SchemaApplier, m *protocolv1.WriteApplySchema) error {
	streams := make([]destination.StreamSchema, 0, len(m.GetStreams()))
	for _, s := range m.GetStreams() {
		schema, err := arrowipc.DecodeSchema(s.GetArrowSchema())
		if err != nil || schema == nil {
			return fmt.Errorf("schema for %s: %w", s.GetName(), cmpErr(err, errors.New("empty")))
		}
		streams = append(streams, destination.StreamSchema{Name: s.GetName(), Schema: schema})
	}
	return sa.ApplySchema(ctx, streams)
}

func epoch(e *protocolv1.Epoch) destination.Epoch {
	return destination.Epoch{Generation: e.GetGeneration(), N: e.GetN()}
}

func capabilitiesToProto(c destination.Capabilities) *protocolv1.DestinationCapabilities {
	p := &protocolv1.DestinationCapabilities{
		SinkClass:            protocolv1.SinkClass(c.Class), //nolint:gosec // small
		DedupeAtApply:        c.DedupeAtApply,
		NanosecondTimestamps: c.NanosecondTimestamps,
		MaxIdentifierLength:  uint32(c.MaxIdentifierLength), //nolint:gosec // small
	}
	for _, s := range c.Scopes {
		p.Scopes = append(p.Scopes, protocolv1.CommitScope(s)) //nolint:gosec // small
	}
	for _, m := range c.WriteModes {
		p.WriteModes = append(p.WriteModes, protocolv1.WriteMode(m)) //nolint:gosec // small
	}
	if c.DefaultCommitInterval > 0 {
		p.DefaultCommitInterval = durationpb.New(c.DefaultCommitInterval)
	}
	if c.MinCommitInterval > 0 {
		p.MinCommitInterval = durationpb.New(c.MinCommitInterval)
	}
	return p
}
