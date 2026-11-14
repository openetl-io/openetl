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

	enginev1 "github.com/openetl-io/openetl/gen/openetl/engine/v1"
	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/platform/ints"
	"github.com/openetl-io/openetl/internal/platform/resources"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// Dataflow runs each attempt's dataflow in its own engine process.
type Dataflow struct {
	Options Options
	// started sees each engine once it is ready.
	started func(*process)
}

var _ runner.Dataflow = (*Dataflow)(nil)

const (
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

	req, names, sinkNames, err := startRequest(plan)
	if err != nil {
		return err
	}
	index := make(map[string]uint32, len(names))
	for i, n := range names {
		index[n] = ints.Uint32(i)
	}
	req.Passthrough = d.Options.Passthrough
	if s := d.Options.Size; s != (resources.Size{}) {
		req.Limits = &enginev1.Limits{MemoryBytes: ints.Uint64(s.EngineMemory())}
	}
	if _, err := p.client.StartJob(ctx, req); err != nil {
		return p.explain(ctx, err)
	}
	evCtx, evCancel := context.WithCancel(ctx)
	defer evCancel()
	events, err := p.client.Events(evCtx, &enginev1.EventsRequest{})
	if err != nil {
		return p.explain(ctx, err)
	}
	// Opened before the edges; the job's group relays it, and ends it.
	var schemas enginev1.EngineService_SchemasClient
	schemasCtx, schemasCancel := context.WithCancel(ctx)
	defer schemasCancel()
	if plan.SchemaOwner != nil {
		if schemas, err = p.client.Schemas(schemasCtx); err != nil {
			return p.explain(ctx, err)
		}
	}
	engineSink := plan.Sink != runner.SinkPlan{}
	onReady := func(r *enginev1.EpochReady) error {
		ready, err := readyOf(r, plan)
		if err != nil {
			return err
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
	counters := newJobCounters(plan.Org, plan.Pipeline, p.meter)
	go func() {
		ev = consumeEvents(events, p.log, onReady, counters)
		close(evDone)
	}()
	defer func() {
		evCancel()
		<-evDone
	}()

	var conns []*net.UnixConn
	defer func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}()
	dial := func(edge string) (*net.UnixConn, error) {
		c, err := p.dialEdge(ctx, edge)
		if err == nil {
			conns = append(conns, c)
		}
		return c, err
	}
	toEngine := make([]*net.UnixConn, max(plan.Lanes, 1))
	for l := range toEngine {
		if toEngine[l], err = dial(inEdge(l)); err != nil {
			return err
		}
	}
	var fromEngine []*net.UnixConn
	if !engineSink {
		fromEngine = make([]*net.UnixConn, max(plan.WriterLanes, 1))
		for w := range fromEngine {
			if fromEngine[w], err = dial(outEdge(w)); err != nil {
				return err
			}
		}
	}

	g, gctx := errgroup.WithContext(ctx)
	// Unblocks reads and writes once the group is done.
	defer context.AfterFunc(gctx, func() {
		for _, c := range conns {
			_ = c.Close()
		}
	})()
	lanes := make([]chan runner.Frame, len(toEngine))
	for l, conn := range toEngine {
		lanes[l] = make(chan runner.Frame, 16)
		g.Go(func() error { return writeEdge(gctx, conn, l, plan.Generation, names, index, alloc, lanes[l]) })
	}
	g.Go(func() error { return splitLanes(gctx, in, lanes) })
	for w, conn := range fromEngine {
		g.Go(func() error {
			return readEdge(gctx, conn, w, plan.Generation, sinkNames, alloc, plan.ReportSchemas, out)
		})
	}
	g.Go(func() error {
		select {
		case <-evDone:
			return ev.err
		case <-gctx.Done():
			return nil
		}
	})
	// Set before g.Wait returns: the relay failed, which ends the job, and
	// what the engine says of that end isn't why.
	var relayErr error
	if schemas != nil {
		g.Go(func() error {
			defer schemasCancel()
			relayErr = relaySchemas(gctx, schemas, plan.SchemaOwner, names, evDone)
			return relayErr
		})
	}
	dataErr := g.Wait()
	if relayErr != nil {
		return relayErr
	}
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

// relaySchemas relays the engine's proposals on st to owner, and owner's
// versions to the engine, by the job's stream index. It returns nil once
// ctx ends or the engine's job does (done), whatever the call does then,
// and otherwise why the relay failed, which fails the job. The caller ends
// st once it returns.
func relaySchemas(ctx context.Context, st enginev1.EngineService_SchemasClient, owner runner.SchemaOwner, names []string, done <-chan struct{}) error {
	index := make(map[string]uint32, len(names))
	for i, n := range names {
		index[n] = ints.Uint32(i)
	}
	failed := make(chan error, 2)
	go func() {
		for {
			p, err := st.Recv()
			switch {
			case errors.Is(err, io.EOF):
				// The engine's job ended, and with it its proposals.
				return
			case err != nil:
				failed <- errs.Wrap(errs.Internal, err, "engineclient: the engine's schemas call broke")
				return
			case int(p.GetStream()) >= len(names):
				failed <- errs.New(errs.Internal, "engineclient: the engine proposed a schema of stream %d, of %d", p.GetStream(), len(names))
				return
			}
			if err := owner.Propose(names[p.GetStream()], p.GetBase(), p.GetArrowSchema()); err != nil {
				failed <- fmt.Errorf("engineclient: proposing a schema of %s: %w", names[p.GetStream()], err)
				return
			}
		}
	}()
	go func() {
		for {
			select {
			case u := <-owner.Updates():
				i, ok := index[u.Stream]
				if !ok {
					continue
				}
				if err := st.Send(&enginev1.SchemasRequest{Stream: i, Version: u.Version, ArrowSchema: u.Schema}); err != nil {
					failed <- errs.Wrap(errs.Internal, err, "engineclient: the engine's schemas call broke")
					return
				}
			case <-ctx.Done():
				return
			case <-done:
				return
			}
		}
	}()
	select {
	case err := <-failed:
		// Ending the job, or ctx, ends the call: that isn't why.
		select {
		case <-done:
			return nil
		default:
		}
		if ctx.Err() != nil {
			return nil
		}
		return err
	case <-done:
		return nil
	case <-ctx.Done():
		return nil
	}
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

// startRequest is the plan's StartJobRequest, with the streams on the edges
// into the engine and those on the edges from it, the destination's.
func startRequest(plan runner.Plan) (req *enginev1.StartJobRequest, names, sinkNames []string, err error) {
	names = make([]string, len(plan.Streams))
	req = &enginev1.StartJobRequest{
		Pipeline:   plan.Pipeline,
		Job:        plan.Owner,
		Generation: plan.Generation,
		FirstEpoch: plan.FirstEpoch,
		Sink:       &enginev1.SinkPlan{Sink: &enginev1.SinkPlan_Runner{Runner: &enginev1.RunnerSink{}}},
		Naming:     &enginev1.Naming{KeepSourceNames: plan.Naming.KeepSource, MaxLength: ints.Uint32(plan.Naming.MaxLength)},

		QuarantineUrl:      plan.QuarantineURL,
		SchemaPolicy:       schemaPolicies[plan.Policy],
		QuarantineInfinity: plan.QuarantineInfinity,
		HeldUrl:            plan.HeldURL,
		Lanes:              ints.Uint32(plan.Lanes),
		WriterLanes:        ints.Uint32(plan.WriterLanes),
		KeyGroups:          ints.Uint32(plan.KeyGroups),
		RemoteSchemas:      plan.SchemaOwner != nil,
		FirstLane:          ints.Uint32(plan.FirstLane),
	}
	sql := plan.SQL != nil
	for i, s := range plan.Streams {
		names[i] = s.Name
		declared, err := encodeSchema(s.Declared())
		if err != nil {
			return nil, nil, nil, fmt.Errorf("engineclient: %s declared schema: %w", s.Name, err)
		}
		sp := streamPlan(plan, s, plan.Schemas[s.Name], !sql)
		sp.ArrowSchema, sp.CursorField, sp.ReadStartMicros = declared, s.CursorField, plan.ReadStarts[s.Name]
		req.Streams = append(req.Streams, sp)
	}
	sinkNames = names
	if sql {
		sinkNames = make([]string, len(plan.Outputs))
		for i, o := range plan.Outputs {
			sinkNames[i] = o.Name
			req.Outputs = append(req.Outputs, streamPlan(plan, o, plan.OutputSchemas[o.Name], true))
		}
		if req.Transform, err = transform(plan.SQL); err != nil {
			return nil, nil, nil, err
		}
	}
	for _, s := range plan.Snapshots {
		ss := &enginev1.StateSnapshot{Operator: s.Operator, Url: s.URL}
		if r := s.KeyGroups; r != nil {
			ss.KeyGroups = &enginev1.KeyGroupRange{First: ints.Uint32(r.First), Last: ints.Uint32(r.Last)}
		}
		req.RestoredSnapshots = append(req.RestoredSnapshots, ss)
	}
	for l := range max(plan.Lanes, 1) {
		req.Edges = append(req.Edges, &enginev1.Edge{Id: inEdge(l), Direction: enginev1.EdgeDirection_EDGE_DIRECTION_TO_ENGINE, Streams: names, Lane: uint32(l)})
	}
	switch {
	case plan.Sink.ParquetURL != "":
		req.Sink = &enginev1.SinkPlan{Sink: &enginev1.SinkPlan_Parquet{Parquet: &enginev1.ParquetSink{Url: plan.Sink.ParquetURL}}}
	case plan.Sink.StagedURL != "":
		req.Sink = &enginev1.SinkPlan{Sink: &enginev1.SinkPlan_Staged{Staged: &enginev1.StagedSink{Url: plan.Sink.StagedURL, Dir: plan.Sink.StagedDir}}}
	case plan.Sink.Discard:
		req.Sink = &enginev1.SinkPlan{Sink: &enginev1.SinkPlan_Discard{Discard: &enginev1.DiscardSink{}}}
	default:
		for w := range max(plan.WriterLanes, 1) {
			req.Edges = append(req.Edges, &enginev1.Edge{Id: outEdge(w), Direction: enginev1.EdgeDirection_EDGE_DIRECTION_FROM_ENGINE, Streams: sinkNames, Lane: uint32(w)})
		}
	}
	return req, names, sinkNames, nil
}

// streamPlan is s's StreamPlan, without what only a stream the engine reads
// has: a declared schema, a cursor and a read start. Only the destination's
// streams get the destination's settings.
func streamPlan(plan runner.Plan, s runner.Stream, restored []byte, sink bool) *enginev1.StreamPlan {
	sp := &enginev1.StreamPlan{
		Name:                 s.Name,
		PrimaryKey:           s.PrimaryKey,
		RestoredSchema:       restored,
		NanosecondTimestamps: s.NanosecondTimestamps,
		Changelog:            s.Read == source.CDC,
	}
	if sink {
		sp.SoftDeletes = s.Mode == destination.Merge && s.Deletes != destination.HardDelete
		sp.Holds = holdPlan(plan.Holds[s.Name])
		sp.Table = s.Table
		if plan.KeyGrouped[s.Name] {
			sp.Routing = enginev1.Routing_ROUTING_KEY_GROUP
		}
	}
	return sp
}

func transform(t *runner.SQLTransform) (*enginev1.Transform, error) {
	out := &enginev1.Transform{Sql: t.SQL, Operators: t.Operators}
	for _, e := range t.Expected {
		schema, err := encodeSchema(e.Schema)
		if err != nil {
			return nil, fmt.Errorf("engineclient: expected output %s's schema: %w", e.Name, err)
		}
		out.ExpectedOutputs = append(out.ExpectedOutputs, &enginev1.ExpectedOutput{Name: e.Name, ArrowSchema: schema, PrimaryKey: e.PrimaryKey})
	}
	return out, nil
}

// readyOf is the engine's report of an epoch, as the runner records it.
func readyOf(r *enginev1.EpochReady, plan runner.Plan) (*runner.Ready, error) {
	ready := &runner.Ready{Schemas: r.GetSchemas()}
	if plan.Sink != (runner.SinkPlan{}) {
		ready.Lanes = make([]runner.LaneReady, max(plan.WriterLanes, 1))
		for w, st := range r.GetStaged() {
			if w >= len(ready.Lanes) {
				return nil, errs.New(errs.Internal, "engineclient: the engine staged %d writer lanes of %d", len(r.GetStaged()), len(ready.Lanes))
			}
			ready.Lanes[w].Staged = st
		}
	}
	for _, st := range r.GetFiles() {
		if int(st.GetLane()) >= len(ready.Lanes) {
			return nil, errs.New(errs.Internal, "engineclient: the engine staged %s on writer lane %d of %d", st.GetStream(), st.GetLane(), len(ready.Lanes))
		}
		lane := &ready.Lanes[st.GetLane()]
		if lane.Files == nil {
			lane.Files = map[string][]destination.StagedFile{}
		}
		for _, f := range st.GetFiles() {
			lane.Files[st.GetStream()] = append(lane.Files[st.GetStream()], destination.StagedFile{Path: f.GetPath(), Rows: f.GetRows(), Bytes: f.GetBytes()})
		}
	}
	for _, s := range r.GetSnapshots() {
		snap := checkpoint.Snapshot{Operator: s.GetOperator(), URL: s.GetUrl()}
		if kg := s.GetKeyGroups(); kg != nil {
			snap.KeyGroups = &checkpoint.KeyGroupRange{First: int(kg.GetFirst()), Last: int(kg.GetLast())}
		}
		ready.Snapshots = append(ready.Snapshots, snap)
	}
	for _, q := range r.GetQuarantine() {
		ready.Quarantine = append(ready.Quarantine, checkpoint.QuarantineFile{Stream: q.GetStream(), Output: q.GetOutput(), URL: q.GetUrl(), Rows: q.GetRows()})
	}
	if s := r.GetOutputSchemas(); len(s) > 0 {
		ready.OutputSchemas = s
	}
	ready.Delivered = r.GetDelivered()
	ready.Holds = readyHolds(r.GetHolds())
	return ready, nil
}

var heldModes = map[runner.HeldMode]enginev1.HeldMode{
	runner.HeldNull:  enginev1.HeldMode_HELD_MODE_NULL,
	runner.HeldValue: enginev1.HeldMode_HELD_MODE_VALUE,
	runner.HeldLast:  enginev1.HeldMode_HELD_MODE_LAST,
}

func held(h runner.Held) *enginev1.Held {
	return &enginev1.Held{Mode: heldModes[h.Mode], Value: h.Value}
}

func holdPlan(h *runner.HoldPlan) *enginev1.HoldPlan {
	if h == nil {
		return nil
	}
	p := &enginev1.HoldPlan{
		Cells:   h.Cells,
		Held:    held(h.Held),
		Columns: make(map[string]*enginev1.Held, len(h.Columns)),
		Limits: &protocolv1.TableLimits{
			NotNull: h.Limits.NotNull, Unholdable: h.Limits.Unholdable, Fixed: h.Limits.Fixed,
			Columns: ints.Uint32(h.Limits.Columns), MaxColumns: ints.Uint32(h.Limits.MaxColumns),
		},
	}
	for c, v := range h.Columns {
		p.Columns[c] = held(v)
	}
	for _, g := range h.Groups {
		p.Groups = append(p.Groups, &enginev1.ColumnGroup{Columns: g})
	}
	for _, c := range h.Holding {
		p.Holding = append(p.Holding, columnHold(c))
		if c.Discarded {
			p.Discarded = append(p.Discarded, c.Column)
		}
	}
	if r := h.Replay; r != nil {
		p.Replay = &enginev1.Replay{
			Files:   r.Files,
			From:    &enginev1.ReplayProgress{File: ints.Uint32(r.File), Row: r.Row, StillHeld: r.StillHeld},
			Columns: r.Columns,
		}
		for _, c := range r.Versioned {
			p.Replay.Versioned = append(p.Replay.Versioned, columnHold(c))
		}
	}
	return p
}

var holdKinds = map[checkpoint.HoldKind]enginev1.HoldCause_Kind{
	checkpoint.HoldPolicy:      enginev1.HoldCause_KIND_POLICY,
	checkpoint.HoldRefused:     enginev1.HoldCause_KIND_REFUSED,
	checkpoint.HoldFixed:       enginev1.HoldCause_KIND_FIXED,
	checkpoint.HoldColumnLimit: enginev1.HoldCause_KIND_COLUMN_LIMIT,
	checkpoint.HoldGroup:       enginev1.HoldCause_KIND_GROUP,
}

func columnHold(c checkpoint.ColumnHold) *enginev1.ColumnHold {
	h := &enginev1.ColumnHold{Column: c.Column, Delivered: c.Delivered, Reason: c.Reason}
	if c.Cause != nil {
		h.Cause = &enginev1.HoldCause{Kind: holdKinds[c.Cause.Kind], Policy: c.Cause.Policy, With: c.Cause.With}
	}
	return h
}

// holdCause is the engine's cause; the destination's refusal isn't in it.
func holdCause(c *enginev1.HoldCause) *checkpoint.HoldCause {
	if c == nil {
		return nil
	}
	out := &checkpoint.HoldCause{Policy: c.GetPolicy(), With: c.GetWith(), Source: c.GetSource(), From: c.GetFrom(), To: c.GetTo()}
	for k, v := range holdKinds {
		if v == c.GetKind() {
			out.Kind = k
		}
	}
	return out
}

func readyHolds(m map[string]*enginev1.StreamHolds) map[string]*runner.ReadyHolds {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]*runner.ReadyHolds, len(m))
	for name, h := range m {
		rh := &runner.ReadyHolds{}
		for _, c := range h.GetHolding() {
			rh.Holding = append(rh.Holding, checkpoint.ColumnHold{
				Column: c.GetColumn(), Delivered: c.GetDelivered(), Reason: c.GetReason(), Cause: holdCause(c.GetCause()),
			})
		}
		for col, c := range h.GetCells() {
			if rh.Cells == nil {
				rh.Cells = map[string]checkpoint.CellHold{}
			}
			rh.Cells[col] = checkpoint.CellHold{Cells: c.GetCells(), Reason: c.GetReason()}
		}
		for _, f := range h.GetFiles() {
			rh.Files = append(rh.Files, destination.StagedFile{Path: f.GetPath(), Rows: f.GetRows(), Bytes: f.GetBytes()})
		}
		if p := h.GetReplayed(); p != nil {
			rh.Replayed = &runner.ReplayProgress{File: int(p.GetFile()), Row: p.GetRow(), StillHeld: p.GetStillHeld(), Done: p.GetDone()}
		}
		out[name] = rh
	}
	return out
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

func inEdge(lane int) string { return fmt.Sprintf("in-%d", lane) }

func outEdge(lane int) string { return fmt.Sprintf("out-%d", lane) }

// splitLanes sends each frame to its lane's edge, and closes them all once
// in is drained.
func splitLanes(ctx context.Context, in <-chan runner.Frame, lanes []chan runner.Frame) error {
	defer func() {
		for _, l := range lanes {
			close(l)
		}
	}()
	for {
		var f runner.Frame
		var ok bool
		select {
		case f, ok = <-in:
			if !ok {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
		if f.Lane < 0 || f.Lane >= len(lanes) {
			if f.Batch != nil {
				f.Batch.Release()
			}
			return fmt.Errorf("engineclient: a frame on lane %d of %d", f.Lane, len(lanes))
		}
		select {
		case lanes[f.Lane] <- f:
		case <-ctx.Done():
			if f.Batch != nil {
				f.Batch.Release()
			}
			return ctx.Err()
		}
	}
}

func writeEdge(ctx context.Context, conn *net.UnixConn, lane int, gen uint64, names []string, index map[string]uint32, alloc memory.Allocator, in <-chan runner.Frame) error {
	edge := inEdge(lane)
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
				return fmt.Errorf("engineclient: write edge %s: %w", edge, err)
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
			return fmt.Errorf("engineclient: write edge %s: %w", edge, err)
		}
		edgeBytes.Add(ctx, int64(len(payload)), toEngine)
	}
	for _, name := range names {
		if err := fw.write(enginev1.FrameKind_FRAME_KIND_END_OF_STREAM, index[name], nil); err != nil {
			return fmt.Errorf("engineclient: write edge %s: %w", edge, err)
		}
	}
	if err := fw.flush(); err != nil {
		return fmt.Errorf("engineclient: write edge %s: %w", edge, err)
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

func readEdge(ctx context.Context, conn *net.UnixConn, lane int, gen uint64, names []string, alloc memory.Allocator, schemas bool, out chan<- runner.Frame) error {
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
			return fmt.Errorf("engineclient: read edge %s: %w", outEdge(lane), err)
		}
		if int(stream) >= len(names) || ended[stream] {
			return fmt.Errorf("engineclient: unexpected %v frame for stream %d", kind, stream)
		}
		edgeBytes.Add(ctx, int64(len(payload)), fromEngine)
		f := runner.Frame{Stream: names[stream], Lane: lane}
		switch kind {
		case enginev1.FrameKind_FRAME_KIND_BATCH:
			if f.Batch, err = decodeBatch(payload, alloc); err != nil {
				return fmt.Errorf("engineclient: decode %s batch: %w", f.Stream, err)
			}
			f.Kind = runner.FrameBatch
		case enginev1.FrameKind_FRAME_KIND_PATCH:
			if f.Batch, err = decodeBatch(payload, alloc); err != nil {
				return fmt.Errorf("engineclient: decode %s patches: %w", f.Stream, err)
			}
			f.Kind = runner.FramePatch
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
			if !schemas {
				// Batches carry their schema; sinks don't apply changes yet.
				continue
			}
			var sc enginev1.SchemaChange
			if err := proto.Unmarshal(payload, &sc); err != nil {
				return fmt.Errorf("engineclient: decode a schema change: %w", err)
			}
			f.Kind, f.Schema = runner.FrameSchema, sc.GetArrowSchema()
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
