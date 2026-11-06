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

	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
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
		// Unset is zero.
		CommitInterval: open.GetCommitInterval().AsDuration(),
		Lane:           int(open.GetLane()),
		Lanes:          max(int(open.GetLanes()), 1),
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
	sw, _ := w.(destination.StagedWriter)
	pw, _ := w.(destination.Patcher)

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
				c, err := w.Commit(ctx, e, staged(x.Commit.GetStaged()))
				resp = &protocolv1.WriteResponse{Msg: &protocolv1.WriteResponse_Committed{Committed: &protocolv1.WriteCommitted{Epoch: x.Commit.GetEpoch(), Error: protoError(err), CollapsedRows: c.Collapsed}}}
			case *protocolv1.WriteRequest_ApplySchema:
				applied, err := applySchema(ctx, sa, x.ApplySchema)
				done := schemaAppliedToProto(applied)
				done.Error = protoError(err)
				resp = &protocolv1.WriteResponse{Msg: &protocolv1.WriteResponse_SchemaApplied{SchemaApplied: done}}
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
			if r, fail = w.Recover(ctx, gen, epoch(x.Recover.GetEpoch()), staged(x.Recover.GetStaged())); fail == nil {
				fail = send(&protocolv1.WriteResponse{Msg: &protocolv1.WriteResponse_Recovered{Recovered: &protocolv1.WriteRecovered{Recovery: protocolv1.Recovery(r)}}}) //nolint:gosec // small
			}
		case *protocolv1.WriteRequest_Batch:
			fail = write(ctx, w, pw, x.Batch)
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
		case *protocolv1.WriteRequest_StagedFiles:
			if sw == nil || x.StagedFiles.GetPatch() {
				fail = errors.New("write: staged files sent to a destination that doesn't take them")
				break
			}
			fail = sw.WriteStaged(ctx, epoch(x.StagedFiles.GetEpoch()), x.StagedFiles.GetStream(), stagedFiles(x.StagedFiles.GetFiles()))
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

func write(ctx context.Context, w destination.Writer, pw destination.Patcher, b *protocolv1.WriteBatch) error {
	if b.GetPatch() && pw == nil {
		return fmt.Errorf("write: patches for %s sent to a destination that doesn't take them", b.GetStream())
	}
	rec, err := arrowipc.DecodeBatch(b.GetArrow(), memory.DefaultAllocator)
	if err != nil {
		return fmt.Errorf("write: batch for %s: %w", b.GetStream(), err)
	}
	defer rec.Release()
	if b.GetPatch() {
		return pw.Patch(ctx, epoch(b.GetEpoch()), b.GetStream(), rec)
	}
	return w.Write(ctx, epoch(b.GetEpoch()), b.GetStream(), rec)
}

func applySchema(ctx context.Context, sa destination.SchemaApplier, m *protocolv1.WriteApplySchema) (destination.SchemaApplied, error) {
	streams := make([]destination.StreamSchema, 0, len(m.GetStreams()))
	for _, s := range m.GetStreams() {
		schema, err := arrowipc.DecodeSchema(s.GetArrowSchema())
		if err != nil || schema == nil {
			return destination.SchemaApplied{}, fmt.Errorf("schema for %s: %w", s.GetName(), cmpErr(err, errors.New("empty")))
		}
		streams = append(streams, destination.StreamSchema{Name: s.GetName(), Schema: schema})
	}
	return sa.ApplySchema(ctx, streams)
}

func schemaAppliedToProto(a destination.SchemaApplied) *protocolv1.WriteSchemaApplied {
	p := &protocolv1.WriteSchemaApplied{}
	for _, r := range a.Refused {
		p.Refused = append(p.Refused, &protocolv1.RefusedColumn{Stream: r.Stream, Column: r.Column, Reason: r.Reason, FromType: r.FromType, ToType: r.ToType, Fix: r.Fix})
	}
	if len(a.Limits) > 0 {
		p.Limits = make(map[string]*protocolv1.TableLimits, len(a.Limits))
		for name, l := range a.Limits {
			p.Limits[name] = &protocolv1.TableLimits{
				NotNull: l.NotNull, Unholdable: l.Unholdable, Fixed: l.Fixed,
				Columns: uint32(l.Columns), MaxColumns: uint32(l.MaxColumns), //nolint:gosec // small
			}
		}
	}
	return p
}

func stagedFiles(fs []*protocolv1.StagedFile) []destination.StagedFile {
	out := make([]destination.StagedFile, len(fs))
	for i, f := range fs {
		out[i] = destination.StagedFile{Path: f.GetPath(), Rows: f.GetRows(), Bytes: f.GetBytes()}
	}
	return out
}

func epoch(e *protocolv1.Epoch) destination.Epoch {
	return destination.Epoch{Generation: e.GetGeneration(), N: e.GetN()}
}

func staged(b [][]byte) []destination.Staged {
	out := make([]destination.Staged, len(b))
	for i, s := range b {
		out[i] = s
	}
	return out
}

func capabilitiesToProto(c destination.Capabilities) *protocolv1.DestinationCapabilities {
	p := &protocolv1.DestinationCapabilities{
		SinkClass:            protocolv1.SinkClass(c.Class), //nolint:gosec // small
		DedupeAtApply:        c.DedupeAtApply,
		NanosecondTimestamps: c.NanosecondTimestamps,
		MaxIdentifierLength:  uint32(c.MaxIdentifierLength),   //nolint:gosec // small
		Delivery:             protocolv1.Delivery(c.Delivery), //nolint:gosec // small
		MaxWriterLanes:       uint32(c.WriterLanes),           //nolint:gosec // small
	}
	if h := c.Holds; h != (destination.Holds{}) {
		p.Holds = &protocolv1.HoldCapabilities{ByKey: h.ByKey, ById: h.ByID, Last: h.Last, Guarded: h.Guarded}
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
	if c.MaxCommitInterval > 0 {
		p.MaxCommitInterval = durationpb.New(c.MaxCommitInterval)
	}
	return p
}
