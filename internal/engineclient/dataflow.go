package engineclient

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	enginev1 "github.com/unsuitable001/openetl/gen/openetl/engine/v1"
	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/runner"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

// Dataflow runs each attempt's dataflow in its own engine process.
type Dataflow struct {
	Options Options
	// started sees each engine once it is ready.
	started func(*process)
}

var _ runner.Dataflow = (*Dataflow)(nil)

const (
	inEdge  = "in"
	outEdge = "out"
	// How long a failed edge waits for the engine to explain the failure.
	failureGrace = 5 * time.Second
)

// eventsResult is how the engine's Events stream ended. Reported errors came
// from the engine with a taxonomy code.
type eventsResult struct {
	err      error
	reported bool
}

func (d *Dataflow) Run(ctx context.Context, plan runner.Plan, in <-chan runner.Frame, out chan<- runner.Frame) error {
	defer close(out)
	log := d.Options.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	alloc := d.Options.Allocator
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	opts := d.Options
	opts.Logger = log.With("pipeline", plan.Pipeline, "owner", plan.Owner)
	p, err := start(ctx, opts)
	if err != nil {
		return err
	}
	defer p.close()
	if d.started != nil {
		d.started(p)
	}

	req, names, index, err := startRequest(plan)
	if err != nil {
		return err
	}
	req.Passthrough = d.Options.Passthrough
	if _, err := p.client.StartJob(ctx, req); err != nil {
		return p.explain(ctx, err)
	}
	evCtx, evCancel := context.WithCancel(ctx)
	defer evCancel()
	events, err := p.client.Events(evCtx, &enginev1.EventsRequest{})
	if err != nil {
		return p.explain(ctx, err)
	}
	engineSink := plan.Sink != runner.SinkPlan{}
	onReady := func(r *enginev1.EpochReady) error {
		ready := &runner.Ready{Staged: r.GetStaged(), Schemas: r.GetSchemas()}
		for _, st := range r.GetFiles() {
			if ready.Files == nil {
				ready.Files = map[string][]destination.StagedFile{}
			}
			for _, f := range st.GetFiles() {
				ready.Files[st.GetStream()] = append(ready.Files[st.GetStream()], destination.StagedFile{Path: f.GetPath(), Rows: f.GetRows(), Bytes: f.GetBytes()})
			}
		}
		for _, s := range r.GetSnapshots() {
			ready.Snapshots = append(ready.Snapshots, checkpoint.Snapshot{Operator: s.GetOperator(), URL: s.GetUrl()})
		}
		for _, q := range r.GetQuarantine() {
			ready.Quarantine = append(ready.Quarantine, checkpoint.QuarantineFile{Stream: q.GetStream(), URL: q.GetUrl(), Rows: q.GetRows()})
		}
		f := runner.Frame{Kind: runner.FrameReady, Epoch: r.GetEpoch().GetN(), Ready: ready}
		select {
		case out <- f:
			return nil
		case <-evCtx.Done():
			return evCtx.Err()
		}
	}
	// ev is set before evDone closes.
	var ev eventsResult
	evDone := make(chan struct{})
	counters := newJobCounters(plan.Org, plan.Pipeline)
	go func() {
		ev = consumeEvents(events, p.log, onReady, counters)
		close(evDone)
	}()
	defer func() {
		evCancel()
		<-evDone
	}()

	toEngine, err := p.dialEdge(ctx, inEdge)
	if err != nil {
		return err
	}
	defer toEngine.Close()
	var fromEngine *net.UnixConn
	if !engineSink {
		if fromEngine, err = p.dialEdge(ctx, outEdge); err != nil {
			return err
		}
		defer fromEngine.Close()
	}

	g, gctx := errgroup.WithContext(ctx)
	// Unblocks reads and writes once the group is done.
	defer context.AfterFunc(gctx, func() {
		_ = toEngine.Close()
		if fromEngine != nil {
			_ = fromEngine.Close()
		}
	})()
	g.Go(func() error { return writeEdge(gctx, toEngine, plan.Generation, names, index, alloc, in) })
	if fromEngine != nil {
		g.Go(func() error { return readEdge(gctx, fromEngine, plan.Generation, names, alloc, out) })
	}
	g.Go(func() error {
		select {
		case <-evDone:
			return ev.err
		case <-gctx.Done():
			return nil
		}
	})
	dataErr := g.Wait()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if dataErr == nil {
		// Events ends once the engine has finished the job.
		<-evDone
		return ev.err
	}
	// Prefer the engine's account of the failure, then how it exited.
	t := time.NewTimer(failureGrace)
	defer t.Stop()
	select {
	case <-evDone:
		if ev.reported {
			return ev.err
		}
	case <-t.C:
	}
	if xerr := p.waitExit(failureGrace); xerr != nil {
		return xerr
	}
	return dataErr
}

// explain turns a failed control call into the engine's error, or its exit.
func (p *process) explain(ctx context.Context, err error) error {
	if e := fromStatus(err); e != nil {
		return e
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if xerr := p.waitExit(failureGrace); xerr != nil {
		return xerr
	}
	return errs.Wrap(errs.Internal, err, "engineclient: control call")
}

func startRequest(plan runner.Plan) (*enginev1.StartJobRequest, []string, map[string]uint32, error) {
	names := make([]string, len(plan.Streams))
	index := make(map[string]uint32, len(plan.Streams))
	req := &enginev1.StartJobRequest{
		Pipeline:   plan.Pipeline,
		Job:        plan.Owner,
		Generation: plan.Generation,
		FirstEpoch: plan.FirstEpoch,
		Sink:       &enginev1.SinkPlan{Sink: &enginev1.SinkPlan_Runner{Runner: &enginev1.RunnerSink{Edge: outEdge}}},
		Naming:     &enginev1.Naming{KeepSourceNames: plan.Naming.KeepSource, MaxLength: uint32(max(plan.Naming.MaxLength, 0))}, //nolint:gosec // identifier limits are small

		QuarantineUrl:      plan.QuarantineURL,
		SchemaPolicy:       schemaPolicies[plan.Policy],
		QuarantineInfinity: plan.QuarantineInfinity,
	}
	for i, s := range plan.Streams {
		names[i] = s.Name
		index[s.Name] = uint32(i)
		declared, err := encodeSchema(s.Declared())
		if err != nil {
			return nil, nil, nil, fmt.Errorf("engineclient: %s declared schema: %w", s.Name, err)
		}
		req.Streams = append(req.Streams, &enginev1.StreamPlan{
			Name:                 s.Name,
			PrimaryKey:           s.PrimaryKey,
			ArrowSchema:          declared,
			CursorField:          s.CursorField,
			ReadStartMicros:      plan.ReadStarts[s.Name],
			RestoredSchema:       plan.Schemas[s.Name],
			NanosecondTimestamps: s.NanosecondTimestamps,
			SoftDeletes:          s.Mode == destination.Merge && s.Deletes != destination.HardDelete,
			Changelog:            s.Read == source.CDC,
		})
	}
	for _, s := range plan.Snapshots {
		req.RestoredSnapshots = append(req.RestoredSnapshots, &enginev1.StateSnapshot{Operator: s.Operator, Url: s.URL})
	}
	req.Edges = []*enginev1.Edge{
		{Id: inEdge, Direction: enginev1.EdgeDirection_EDGE_DIRECTION_TO_ENGINE, Streams: names},
	}
	switch {
	case plan.Sink.ParquetURL != "":
		req.Sink = &enginev1.SinkPlan{Sink: &enginev1.SinkPlan_Parquet{Parquet: &enginev1.ParquetSink{Url: plan.Sink.ParquetURL}}}
	case plan.Sink.StagedURL != "":
		req.Sink = &enginev1.SinkPlan{Sink: &enginev1.SinkPlan_Staged{Staged: &enginev1.StagedSink{Url: plan.Sink.StagedURL, Dir: plan.Sink.StagedDir}}}
	case plan.Sink.Discard:
		req.Sink = &enginev1.SinkPlan{Sink: &enginev1.SinkPlan_Discard{Discard: &enginev1.DiscardSink{}}}
	default:
		req.Edges = append(req.Edges, &enginev1.Edge{Id: outEdge, Direction: enginev1.EdgeDirection_EDGE_DIRECTION_FROM_ENGINE, Streams: names})
	}
	return req, names, index, nil
}

var schemaPolicies = map[runner.SchemaPolicy]enginev1.SchemaPolicy{
	runner.AllowAll:     enginev1.SchemaPolicy_SCHEMA_POLICY_ALLOW_ALL,
	runner.AllowColumns: enginev1.SchemaPolicy_SCHEMA_POLICY_ALLOW_COLUMNS,
	runner.Block:        enginev1.SchemaPolicy_SCHEMA_POLICY_BLOCK,
	runner.IgnoreNew:    enginev1.SchemaPolicy_SCHEMA_POLICY_IGNORE_NEW,
}

// encodeSchema returns an Arrow IPC stream holding only s; nil for nil.
func encodeSchema(s *arrow.Schema) ([]byte, error) {
	if s == nil {
		return nil, nil
	}
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(s))
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func consumeEvents(stream grpc.ServerStreamingClient[enginev1.EventsResponse], log *slog.Logger, onReady func(*enginev1.EpochReady) error, counters *jobCounters) eventsResult {
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return eventsResult{}
		}
		if err != nil {
			if e := fromStatus(err); e != nil {
				return eventsResult{err: e, reported: true}
			}
			return eventsResult{err: fmt.Errorf("engineclient: events: %w", err)}
		}
		switch e := resp.GetEvent().(type) {
		case *enginev1.EventsResponse_EpochReady:
			log.Debug("engine epoch ready", "epoch", e.EpochReady.GetEpoch().GetN())
			if err := onReady(e.EpochReady); err != nil {
				return eventsResult{err: err}
			}
		case *enginev1.EventsResponse_Metrics:
			counters.record(stream.Context(), e.Metrics)
		}
	}
}

// fromStatus returns the taxonomy error the engine put in a status, or nil.
func fromStatus(err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return nil
	}
	for _, d := range st.Details() {
		if pe, ok := d.(*protocolv1.Error); ok {
			return fmt.Errorf("engine: %w", errs.FromProto(pe))
		}
	}
	return nil
}

func writeEdge(ctx context.Context, conn *net.UnixConn, gen uint64, names []string, index map[string]uint32, alloc memory.Allocator, in <-chan runner.Frame) error {
	fw := newFrameWriter(conn)
	var buf bytes.Buffer
	var err error
	for {
		var f runner.Frame
		var ok bool
		select {
		case f, ok = <-in:
		default:
			if err := fw.flush(); err != nil {
				return fmt.Errorf("engineclient: write edge %s: %w", inEdge, err)
			}
			select {
			case f, ok = <-in:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if !ok {
			break
		}
		i, known := index[f.Stream]
		var kind enginev1.FrameKind
		var payload []byte
		if known {
			kind, payload, err = encode(f, gen, alloc, &buf)
		}
		if f.Batch != nil {
			f.Batch.Release()
		}
		if !known {
			return fmt.Errorf("engineclient: frame for unknown stream %q", f.Stream)
		}
		if err != nil {
			return err
		}
		if err := fw.write(kind, i, payload); err != nil {
			return fmt.Errorf("engineclient: write edge %s: %w", inEdge, err)
		}
		edgeBytes.Add(ctx, int64(len(payload)), toEngine)
	}
	for _, name := range names {
		if err := fw.write(enginev1.FrameKind_FRAME_KIND_END_OF_STREAM, index[name], nil); err != nil {
			return fmt.Errorf("engineclient: write edge %s: %w", inEdge, err)
		}
	}
	if err := fw.flush(); err != nil {
		return fmt.Errorf("engineclient: write edge %s: %w", inEdge, err)
	}
	return conn.CloseWrite()
}

func encode(f runner.Frame, gen uint64, alloc memory.Allocator, buf *bytes.Buffer) (enginev1.FrameKind, []byte, error) {
	switch f.Kind {
	case runner.FrameRows:
		rows := make([][]byte, len(f.Rows))
		for i, r := range f.Rows {
			rows[i] = r
		}
		b, err := proto.Marshal(&enginev1.Rows{Rows: rows})
		return enginev1.FrameKind_FRAME_KIND_ROWS, b, err
	case runner.FrameBatch:
		if f.IPC != nil {
			return enginev1.FrameKind_FRAME_KIND_BATCH, f.IPC, nil
		}
		buf.Reset()
		w := ipc.NewWriter(buf, ipc.WithSchema(f.Batch.Schema()), ipc.WithAllocator(alloc))
		if err := w.Write(f.Batch); err != nil {
			return 0, nil, fmt.Errorf("engineclient: encode %s batch: %w", f.Stream, err)
		}
		if err := w.Close(); err != nil {
			return 0, nil, fmt.Errorf("engineclient: encode %s batch: %w", f.Stream, err)
		}
		return enginev1.FrameKind_FRAME_KIND_BATCH, buf.Bytes(), nil
	case runner.FrameBarrier:
		b, err := proto.Marshal(&enginev1.Barrier{Epoch: &protocolv1.Epoch{Generation: gen, N: f.Epoch}})
		return enginev1.FrameKind_FRAME_KIND_BARRIER, b, err
	default:
		return 0, nil, fmt.Errorf("engineclient: unexpected frame kind %d", f.Kind)
	}
}

func readEdge(ctx context.Context, conn *net.UnixConn, gen uint64, names []string, alloc memory.Allocator, out chan<- runner.Frame) error {
	fr := newFrameReader(conn)
	ended := make([]bool, len(names))
	open := len(names)
	for {
		kind, stream, payload, err := fr.next()
		if errors.Is(err, io.EOF) && open == 0 {
			return nil
		}
		if errors.Is(err, io.EOF) {
			return errors.New("engineclient: the engine closed its edge before every stream ended")
		}
		if err != nil {
			return fmt.Errorf("engineclient: read edge %s: %w", outEdge, err)
		}
		if int(stream) >= len(names) || ended[stream] {
			return fmt.Errorf("engineclient: unexpected %v frame for stream %d", kind, stream)
		}
		edgeBytes.Add(ctx, int64(len(payload)), fromEngine)
		f := runner.Frame{Stream: names[stream]}
		switch kind {
		case enginev1.FrameKind_FRAME_KIND_BATCH:
			if f.Batch, err = decodeBatch(payload, alloc); err != nil {
				return fmt.Errorf("engineclient: decode %s batch: %w", f.Stream, err)
			}
			f.Kind = runner.FrameBatch
		case enginev1.FrameKind_FRAME_KIND_BARRIER:
			var b enginev1.Barrier
			if err := proto.Unmarshal(payload, &b); err != nil {
				return fmt.Errorf("engineclient: decode barrier: %w", err)
			}
			if b.GetEpoch().GetGeneration() != gen {
				return fmt.Errorf("engineclient: barrier from generation %d in generation %d", b.GetEpoch().GetGeneration(), gen)
			}
			f.Kind, f.Epoch = runner.FrameBarrier, b.GetEpoch().GetN()
		case enginev1.FrameKind_FRAME_KIND_SCHEMA_CHANGE:
			// Batches carry their schema; sinks don't apply changes yet.
			continue
		case enginev1.FrameKind_FRAME_KIND_END_OF_STREAM:
			ended[stream] = true
			open--
			continue
		default:
			return fmt.Errorf("engineclient: unexpected %v frame from the engine", kind)
		}
		select {
		case out <- f:
		case <-ctx.Done():
			if f.Batch != nil {
				f.Batch.Release()
			}
			return ctx.Err()
		}
	}
}

func decodeBatch(payload []byte, alloc memory.Allocator) (arrow.RecordBatch, error) {
	r, err := ipc.NewReader(bytes.NewReader(payload), ipc.WithAllocator(alloc))
	if err != nil {
		return nil, err
	}
	defer r.Release()
	if !r.Next() {
		if err := r.Err(); err != nil {
			return nil, err
		}
		return nil, errors.New("no batch in the payload")
	}
	rec := r.RecordBatch()
	rec.Retain()
	if r.Next() {
		rec.Release()
		return nil, errors.New("more than one batch in the payload")
	}
	return rec, nil
}
