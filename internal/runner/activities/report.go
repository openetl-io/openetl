package activities

import (
	"context"
	"log/slog"
	"maps"
	"slices"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
	"github.com/unsuitable001/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/runner"
	"github.com/unsuitable001/openetl/internal/streamschema"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

// reporter sends a pipeline's schemas and checkpoints to the control plane,
// in commit order and off the commit path. A report it can't send is logged
// and dropped: the next attempt reports every schema again, and the
// checkpoint mirror misses that checkpoint.
type reporter struct {
	api      dataplanev1connect.DataPlaneServiceClient
	pipeline string
	runID    string
	log      *slog.Logger
	ctx      context.Context
	cancel   context.CancelFunc
	queue    chan report
	done     chan struct{}
	digests  map[string]string // by Arrow schema; owned by run()
	// What the read reports of its progress; nil if nothing.
	progress *runner.Progress
	// Each stream's held cells by column as last reported, for the cells an
	// epoch held; owned by checkpoint's caller.
	cells map[string]map[string]uint64
}

type report struct {
	what  string
	epoch uint64
	send  func(ctx context.Context) error
}

func newReporter(ctx context.Context, api dataplanev1connect.DataPlaneServiceClient, pipeline, run string, log *slog.Logger) *reporter {
	r := &reporter{
		api:      api,
		pipeline: pipeline,
		runID:    run,
		log:      log,
		queue:    make(chan report, 64),
		done:     make(chan struct{}),
		digests:  make(map[string]string),
	}
	r.ctx, r.cancel = context.WithCancel(context.WithoutCancel(ctx))
	go r.run()
	return r
}

// schemas queues a schema history report without blocking.
func (r *reporter) schemas(e destination.Epoch, schemas, outputs map[string][]byte) {
	req := &dataplanev1.ReportSchemasRequest{PipelineId: r.pipeline, Generation: e.Generation, Epoch: e.N, Schemas: schemas, OutputSchemas: outputs}
	r.enqueue(report{what: "schema history", epoch: e.N, send: func(ctx context.Context) error {
		_, err := r.api.ReportSchemas(ctx, connect.NewRequest(req))
		return err
	}})
}

// checkpoint queues a checkpoint mirror report without blocking. rec must
// not change.
func (r *reporter) checkpoint(rec *checkpoint.Record, collapsed map[string]uint64) {
	var progress []source.Progress
	if r.progress != nil {
		progress = r.progress.Latest()
	}
	held := r.heldCells(rec)
	r.enqueue(report{what: "checkpoint mirror", epoch: rec.Epoch, send: func(ctx context.Context) error {
		_, err := r.api.ReportCheckpoint(ctx, connect.NewRequest(r.mirrored(rec, collapsed, held, progress)))
		return err
	}})
}

// heldCells are the cells each stream held in rec's epoch: what its cell
// holds grew by since the last record.
func (r *reporter) heldCells(rec *checkpoint.Record) map[string]uint64 {
	if r.cells == nil {
		r.cells = map[string]map[string]uint64{}
	}
	out := map[string]uint64{}
	for name, st := range rec.Streams {
		now := map[string]uint64{}
		if st.Holds != nil {
			for col, c := range st.Holds.Cells {
				now[col] = c.Cells
				if c.Cells > r.cells[name][col] {
					out[name] += c.Cells - r.cells[name][col]
				}
			}
		}
		r.cells[name] = now
	}
	return out
}

func (r *reporter) mirrored(rec *checkpoint.Record, collapsed, held map[string]uint64, progress []source.Progress) *dataplanev1.ReportCheckpointRequest {
	req := &dataplanev1.ReportCheckpointRequest{
		PipelineId: r.pipeline, RunId: r.runID, Generation: rec.Generation, Epoch: rec.Epoch,
		CreateTime: timestamppb.New(rec.CreatedAt),
	}
	for _, p := range progress {
		m := &protocolv1.Progress{Stream: p.Stream, LagBytes: p.LagBytes, LagRecords: p.LagRecords, RetentionLeftBytes: p.RetentionLeftBytes}
		if p.Lag != nil {
			m.Lag = durationpb.New(*p.Lag)
		}
		if p.RetentionLeft != nil {
			m.RetentionLeft = durationpb.New(*p.RetentionLeft)
		}
		req.Progress = append(req.Progress, m)
	}
	if p := rec.Previous; p != nil {
		req.Previous = &dataplanev1.CheckpointRef{Generation: p.Generation, Epoch: p.Epoch}
	}
	for name, st := range rec.Streams {
		cs := &dataplanev1.CheckpointStream{
			Name: name, Rows: st.Rows, Forced: slices.Contains(rec.Forced, name), Complete: st.Complete,
			SchemaDigest: r.digest(name, st.Schema), CollapsedRows: collapsed[name],
		}
		for _, q := range rec.Quarantine {
			if q.Stream == name {
				cs.QuarantinedRows += q.Rows
			}
		}
		cs.Holds, cs.HeldCells = mirroredHolds(st.Holds), held[name]
		req.Streams = append(req.Streams, cs)
	}
	return req
}

func (r *reporter) digest(stream string, schema []byte) string {
	if len(schema) == 0 {
		return ""
	}
	if d, ok := r.digests[string(schema)]; ok {
		return d
	}
	d, err := streamschema.Describe(schema)
	if err != nil {
		r.log.Warn("checkpoint mirror: unreadable schema", "stream", stream, "err", err)
		return ""
	}
	r.digests[string(schema)] = d.Digest
	return d.Digest
}

func (r *reporter) enqueue(rep report) {
	select {
	case r.queue <- rep:
	default:
		r.log.Warn(rep.what+" report dropped: too many queued", "epoch", rep.epoch)
	}
}

func (r *reporter) run() {
	defer close(r.done)
	for rep := range r.queue {
		ctx, cancel := context.WithTimeout(r.ctx, 10*time.Second)
		err := rep.send(ctx)
		cancel()
		if err != nil {
			r.log.Warn(rep.what+" report failed", "epoch", rep.epoch, "err", err)
		}
	}
}

// close sends what is queued, for up to grace.
func (r *reporter) close(grace time.Duration) {
	close(r.queue)
	t := time.NewTimer(grace)
	defer t.Stop()
	select {
	case <-r.done:
	case <-t.C:
	}
	r.cancel()
}

// mirroredHolds are a stream's open holds. A released column held whole
// stays listed, as releasing, until its values are patched in.
func mirroredHolds(h *checkpoint.Holds) []*dataplanev1.CheckpointHold {
	if h == nil {
		return nil
	}
	ref := func(r checkpoint.RecordRef) *dataplanev1.CheckpointRef {
		return &dataplanev1.CheckpointRef{Generation: r.Generation, Epoch: r.Epoch}
	}
	var replay []string
	if h.Replay != nil {
		replay = h.Replay.Columns
	}
	var out []*dataplanev1.CheckpointHold
	for _, c := range h.Columns {
		out = append(out, &dataplanev1.CheckpointHold{
			Column: c.Column, Whole: true, Reason: c.Reason, Since: ref(c.Since),
			StartTime: timestamppb.New(c.Started), Discarded: c.Discarded,
		})
	}
	for _, col := range slices.Sorted(maps.Keys(h.Cells)) {
		c := h.Cells[col]
		out = append(out, &dataplanev1.CheckpointHold{
			Column: col, Reason: c.Reason, Since: ref(c.Since), StartTime: timestamppb.New(c.Started),
			Cells: c.Cells, Releasing: slices.Contains(replay, col),
		})
	}
	for _, col := range replay {
		if _, ok := h.Cells[col]; !ok {
			out = append(out, &dataplanev1.CheckpointHold{Column: col, Whole: true, Reason: "released", Releasing: true})
		}
	}
	return out
}
