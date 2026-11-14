// Package runner runs a pipeline's jobs: it hosts the connectors, drives the
// dataflow, and coordinates checkpoints.
package runner

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/delivery"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/platform/ints"
	"github.com/openetl-io/openetl/internal/platform/resources"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

type Stream struct {
	Name   string
	Replay source.Replay
	// How the source reads it; 0 if neither the spec nor the catalog says.
	Read        source.SyncMode
	Mode        destination.WriteMode
	PrimaryKey  []string
	CursorField string
	// Declared logical types under source names; nil: inferred.
	Schema *arrow.Schema
	// Its seq control column's type, if the source declares one.
	Seq arrow.DataType
	// Decimal columns that arrive as another type instead, by source name:
	// String or Float64.
	Overrides map[string]arrow.DataType
	// Timestamps keep nanoseconds, which the destination must too.
	NanosecondTimestamps bool
	// Overrides Settings.Require; 0: the pipeline's.
	Require delivery.Level
	// Whether rows with the same content become one; nil: the default
	// delivery.Stream gives.
	Collapse *bool
	// How merge applies deletes; zero: soft.
	Deletes destination.DeleteMode
	// The destination may change a column's type itself when the stream's
	// widens.
	AllowTypeChanges bool
	// One of a transform's outputs that the user's code or a statement
	// writes, whose Replay is exact when every input's is. Its Read is CDC
	// when it's a changelog: merged, from a changelog. A stream a SQL
	// transform passes through isn't.
	Transformed bool
	// Nil: the stream holds nothing back.
	Holds *StreamHolds
	// Where the source splits it, every record of a key is in one split.
	SplitsByKey bool
	// Its name in the destination, exactly; empty: from Name, by the naming.
	Table string
	// One of a SQL transform's outputs: the stream its statement reads,
	// whose settings it takes.
	From string
}

func (s Stream) delivery() delivery.Stream {
	return delivery.Stream{Name: s.Name, Replay: s.Replay, Mode: s.Mode, Keyed: len(s.PrimaryKey) > 0, Require: s.Require, Collapse: s.Collapse, Transformed: s.Transformed,
		From: s.From}
}

// columns names the columns of Schema; nil without one.
func (s Stream) columns() []string {
	if s.Schema == nil {
		return nil
	}
	fields := s.Schema.Fields()
	names := make([]string, len(fields))
	for i, f := range fields {
		names[i] = f.Name
	}
	return names
}

// Declared is Schema with Overrides applied.
func (s Stream) Declared() *arrow.Schema {
	if s.Schema == nil || len(s.Overrides) == 0 {
		return s.Schema
	}
	fields := s.Schema.Fields()
	for i, f := range fields {
		if t, ok := s.Overrides[f.Name]; ok {
			fields[i] = arrow.Field{Name: f.Name, Type: t, Nullable: f.Nullable}
		}
	}
	md := s.Schema.Metadata()
	return arrow.NewSchema(fields, &md)
}

func (s Stream) checkOverrides() error {
	for _, name := range slices.Sorted(maps.Keys(s.Overrides)) {
		if t := s.Overrides[name]; !arrow.TypeEqual(t, arrow.BinaryTypes.String) && !arrow.TypeEqual(t, arrow.PrimitiveTypes.Float64) {
			return fmt.Errorf("stream %q: column %q can't be overridden to %s, only to string or float64", s.Name, name, t)
		}
		var fs []arrow.Field
		if s.Schema != nil {
			fs, _ = s.Schema.FieldsByName(name)
		}
		if len(fs) != 1 || fs[0].Type.ID() != arrow.DECIMAL128 {
			return fmt.Errorf("stream %q: column %q has a type override, but the source doesn't declare it a decimal", s.Name, name)
		}
	}
	return nil
}

type Job struct {
	// Unique per attempt.
	Owner string
	// Identifies the sync. Its attempts don't read again the streams one of
	// them completed. Empty: every attempt reads every stream.
	Sync     string
	Org      string
	Pipeline string
	// The read never ends (a continuous pipeline's).
	Continuous bool
	Log        *checkpoint.Log
	Source     source.Reader
	Streams    []Stream
	// Streams paused on their own: the attempt doesn't read them, and each
	// record it writes carries their state from the record it restored.
	Paused      []Stream
	Destination destination.Destination
	// The destination's schema or dataset; empty for its default.
	Namespace string
	// Tables and columns keep their source names instead of normalized ones.
	KeepSourceNames bool
	SchemaPolicy    SchemaPolicy
	// Where the dataflow keeps records it can't take, as Plan says.
	QuarantineURL string
	// Infinite dates and timestamps are quarantined instead of clamped.
	QuarantineInfinity bool
	Dataflow           Dataflow
	Settings           delivery.Settings
	Logger             *slog.Logger
	// Called on the coordinator's goroutine after each epoch commits. It
	// must not block.
	OnCommit func(epoch uint64)
	// Counts what its source emits. Nil: nothing does.
	Meter *resources.Meter
	// Called after an epoch commits with the schemas that changed since the
	// last call, the source's streams' and a transform's outputs' apart, the
	// attempt's first holding every one. Each is an Arrow IPC stream holding
	// only the schema. It must not block.
	OnSchemas func(e destination.Epoch, schemas, outputs map[string][]byte)
	// Called after an epoch commits, and after OnSchemas, with its record,
	// which it must not modify, and the rows each stream's apply didn't
	// insert because of dedupe. It must not block.
	OnCheckpoint func(rec *checkpoint.Record, collapsed map[string]uint64)
	// Closing it stops the attempt at its next checkpoint with ErrDrained,
	// unless the read has already ended, in which case it finishes.
	Drain <-chan struct{}
	// Records kept along the checkpoint chain, at least 2. Zero: 10.
	KeepRecords int
	// Gets what the source reports of its progress. Nil: dropped.
	Progress *Progress
	// Where the dataflow stages epochs for a destination with
	// destination.DeliveryStaged, which needs one.
	Staging Staging
	// The user's code between the source and the destination; nil for
	// none. With one, Streams are its inputs, and Outputs, which it needs,
	// the destination's streams.
	Transform Transform
	Outputs   []Stream
	// SQL the engine runs; nil for none. With it, Streams are its inputs,
	// and Outputs, which it needs, the destination's streams.
	SQL *SQLTransform
	// The user's Flink job between the source and the destination of a
	// continuous pipeline; nil for none. With one, Outputs are the
	// destination's streams, and Bridge serves the job.
	Flink  Flink
	Bridge Bridge
	// How long the job may run without a reader on every stream and a sink
	// on every output, from its enumerator's hello. Zero: a minute.
	FlinkConnectTimeout time.Duration
	// Where the dataflow keeps held values; empty: nothing is held.
	HeldURL string
	// Holds released or discarded since they started.
	Releases []Release
	// Streams to read again from the start, oldest first. The attempt takes
	// those newer than its restored record's, and its records carry the
	// newest it took.
	Resets []Reset
	// What the attempt does when the record HEAD points to can't be read.
	Rewind Rewind
	// Called once the attempt restarted from the record before HEAD's, which
	// couldn't be read. It must not block.
	OnRewind func(Rewound)
	// The most lanes the attempt runs; 0: one. It runs no more than
	// it has splits to read, and has as many writer lanes, up to what the
	// destination takes.
	Lanes int
	// The pipeline's key groups; 0: checkpoint.KeyGroups. A restore from a
	// record with another count fails.
	KeyGroups int
	// Columns the destination refused in an earlier attempt of this call.
	refused []RefusedColumn
}

func (j *Job) lanes() int { return max(j.Lanes, 1) }

// Reset is a request to read streams again from the start.
type Reset struct {
	// Ordered by creation.
	ID string
	// Empty: every stream.
	Streams []string
}

// Rewind says whether an attempt that can't read the record HEAD points to
// restarts from the one before it, which delivers that record's epoch
// again.
type Rewind struct {
	Policy RewindPolicy
	// A person's approval, for RewindAny.
	Approval string
}

type RewindPolicy int

const (
	// The attempt fails with CHECKPOINT_CORRUPT.
	RewindNever RewindPolicy = iota
	// Only if delivering the epoch again keeps every stream's guarantee:
	// it's merged, deduped at apply, or delivered at least once anyway.
	RewindSafe
	// Whatever it does to the streams' guarantees.
	RewindAny
)

// Rewound is a restart from the record before an unreadable one.
type Rewound struct {
	Unreadable checkpoint.RecordRef
	// Nil: there was none before it, and the read started again.
	Restored *checkpoint.RecordRef
	Reason   string
	Approval string
}

// inTurn says a stream's frames can go to the lanes in turn: it has no key
// to keep together, and its destination puts rows in tables, whose order
// doesn't follow what a stream emits. A log keeps a split's order, as a
// partition's or a shard's matters there.
func (j *Job) inTurn(s Stream) bool {
	return j.lanes() > 1 && len(s.PrimaryKey) == 0 && j.Destination.Capabilities().Class == destination.StageAndApply
}

// canResplit says the source splits a running split's remainder.
func (j *Job) canResplit(ctx context.Context) bool {
	rs, ok := j.Source.(source.Resplitter)
	if !ok {
		return false
	}
	if c, ok := rs.(interface{ Resplits(context.Context) bool }); ok {
		return c.Resplits(ctx)
	}
	return true
}

// settleLanes bounds the attempt's lanes by the splits there are to read,
// unless a stream is read in splits by a source that can make more, and
// records them, with that bound, on the attempt's meter.
func (j *Job) settleLanes(ctx context.Context, splits map[string][]checkpoint.SplitState, done map[string]bool) {
	limit := j.laneLimit(ctx, splits, done)
	if limit > 0 {
		j.Lanes = min(j.lanes(), limit)
	}
	j.Meter.SetLanes(j.lanes(), limit)
}

// laneLimit is the most lanes the attempt could use: its open splits, or 0
// if its source splits running splits. Spark and Flink transforms run one
// lane.
func (j *Job) laneLimit(ctx context.Context, splits map[string][]checkpoint.SplitState, done map[string]bool) int {
	if j.ownCode() {
		return 1
	}
	open, inParts := 0, false
	for _, s := range j.Streams {
		if done[s.Name] {
			continue
		}
		for _, sp := range splits[s.Name] {
			if !sp.Complete {
				open++
				inParts = inParts || sp.ID != checkpoint.WholeStream
			}
		}
	}
	if inParts && j.canResplit(ctx) {
		return 0
	}
	return max(open, 1)
}

func (j *Job) writerLanes() int {
	return min(j.lanes(), j.Destination.Capabilities().Lanes())
}

func (j *Job) keyGroups() int { return cmp.Or(j.KeyGroups, checkpoint.KeyGroups) }

// transformed: the user's code runs between the source and the destination.
func (j *Job) transformed() bool { return j.ownCode() || j.SQL != nil }

// ownCode: the user's Spark or Flink code runs between the source and the
// destination.
func (j *Job) ownCode() bool { return j.Transform != nil || j.Flink != nil }

// sinkStreams are the destination's streams.
func (j *Job) sinkStreams() []Stream {
	if j.transformed() {
		return j.Outputs
	}
	return j.Streams
}

// Staging keeps the epochs staged for a destination with
// destination.DeliveryStaged: the dataflow writes them below URL, and the
// destination reads the files it was handed.
type Staging interface {
	URL() string
	// Hand lets the destination read a stream's files of epoch e.
	Hand(e destination.Epoch, stream string, files []destination.StagedFile) error
	// Flush writes the latest delivery cursor the destination saved for e,
	// if it isn't written yet.
	Flush(ctx context.Context, e destination.Epoch) error
	// Drop deletes what was staged for e, and its cursor.
	Drop(ctx context.Context, e destination.Epoch) error
	// Sweep deletes what's staged except keep's epoch and generation gen's
	// epochs after it, and returns how many objects it deleted.
	Sweep(ctx context.Context, keep destination.Epoch, gen uint64) (int, error)
}

// ErrDrained: the attempt stopped at a checkpoint because Job.Drain closed.
var ErrDrained = errors.New("runner: drained at a checkpoint")

type Result struct {
	Generation uint64
	Recovery   destination.Recovery
	// The epoch the attempt resumed from; 0 from the beginning.
	Restored uint64
	// Epochs committed by this attempt.
	Committed int
	// The last epoch committed, or Restored.
	Last uint64
}

const defaultKeepRecords = 10

// commitAttempts bounds in-process retries of the idempotent commit steps.
// Recovery in the next attempt finishes what still fails.
const commitAttempts = 4

// RunAttempt runs one attempt of a bounded sync: claim, recover HEAD's epoch,
// then read until every stream is complete, committing along the way. If
// the destination refuses columns before an epoch is recorded, it runs
// another attempt at once that holds them; one it refuses again fails it.
func RunAttempt(ctx context.Context, job Job) (Result, error) {
	for {
		res, err := runAttempt(ctx, job)
		// A fenced attempt's refusal stays in its error, but another claim
		// would fence the attempt that took over.
		if errors.Is(err, checkpoint.ErrFenced) {
			return res, err
		}
		var re *RefusedError
		if !errors.As(err, &re) {
			return res, err
		}
		for _, c := range re.Columns {
			if slices.ContainsFunc(job.refused, func(r RefusedColumn) bool { return r.Stream == c.Stream && r.Column == c.Column }) {
				return res, errs.Wrap(errs.SchemaDrift, err, "runner: refused again after holding it")
			}
			if !slices.ContainsFunc(job.Streams, func(s Stream) bool { return s.Name == c.Stream && job.holdsOn(s) }) {
				return res, errs.Wrap(errs.SchemaDrift, err, "runner: the stream doesn't hold columns")
			}
		}
		job.refused = append(job.refused, re.Columns...)
		if job.Logger != nil {
			job.Logger.Info("holding refused columns; starting another attempt", "err", re)
		}
	}
}

func runAttempt(ctx context.Context, job Job) (res Result, err error) {
	if err := job.validate(); err != nil {
		return res, err
	}
	if job.Flink != nil {
		return runFlink(ctx, job)
	}
	log := job.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log = log.With("pipeline", job.Pipeline, "owner", job.Owner)
	if job.Progress != nil {
		live.Store(job.Progress, struct{}{})
		defer live.Delete(job.Progress)
	}

	rctx, span := tracer.Start(ctx, "recover", trace.WithAttributes(attribute.String("openetl.pipeline", job.Pipeline)))
	// Ended before the epochs start; a later End is ignored.
	defer func() { endSpan(span, err) }()
	sess, err := job.Log.Claim(rctx, job.Owner)
	if err != nil {
		return res, err
	}
	// An attempt that fails once another has claimed the pipeline was fenced,
	// whatever it failed on: staging the other's recovery dropped, or a
	// source connection the other took over.
	defer func() {
		if err == nil || errors.Is(err, ErrDrained) || errors.Is(err, checkpoint.ErrFenced) || errors.Is(err, destination.ErrFenced) {
			return
		}
		if ferr := sess.Check(context.WithoutCancel(ctx)); errors.Is(ferr, checkpoint.ErrFenced) {
			err = fmt.Errorf("%w; the attempt failed with: %w", ferr, err)
		}
	}()
	res.Generation = sess.Generation()
	log = log.With("generation", res.Generation)

	head, err := sess.Load(rctx)
	var rewound *Rewound
	if errors.Is(err, checkpoint.ErrUnreadable) {
		head, rewound, err = job.rewind(rctx, sess, err)
	}
	if err != nil {
		return res, err
	}
	if head != nil && head.KeyGroupCount() != job.keyGroups() {
		return res, errs.New(errs.ConfigInvalid, "runner: the pipeline has %d key groups, and its checkpoint %d-g%d has %d", job.keyGroups(), head.Epoch, head.Generation, head.KeyGroupCount())
	}
	// The previous record is needed if the sink reports HEAD's epoch lost.
	// One that can't be read fails the attempt only then.
	var prev *checkpoint.Record
	var prevErr error
	if head != nil && head.Previous != nil {
		if prev, err = job.Log.Record(rctx, *head.Previous); errors.Is(err, checkpoint.ErrUnreadable) {
			prev, prevErr = nil, err
		} else if err != nil {
			return res, err
		}
	}

	// Either record may become the restore point, so dedupe conservatively.
	forced := make(map[string]bool)
	for _, r := range []*checkpoint.Record{head, prev} {
		if r != nil {
			for _, s := range r.Forced {
				forced[s] = true
			}
		}
	}
	if rewound != nil {
		// The unreadable record's epoch was delivered, and is read again.
		for _, s := range job.Streams {
			forced[s.Name] = true
		}
		markForced(head, job.Streams)
		if job.OnRewind != nil {
			job.OnRewind(*rewound)
		}
		log.Warn("restarted from the checkpoint before the one HEAD pointed to, which can't be read", "unreadable", rewound.Unreadable, "reason", rewound.Reason)
	}
	canForce := job.canForce()
	ws := make([]destination.Writer, 0, job.writerLanes())
	defer func() {
		for i, w := range ws {
			if cerr := w.Close(context.WithoutCancel(ctx)); err == nil && cerr != nil {
				err = fmt.Errorf("runner: close destination lane %d: %w", i, cerr)
			}
		}
	}()
	w, err := job.Destination.Open(rctx, job.openConfig(res.Generation, forced, 0))
	if err != nil {
		return res, fmt.Errorf("runner: open destination: %w", err)
	}
	ws = append(ws, w)

	var e destination.Epoch
	var staged []destination.Staged
	applied := map[string]*arrow.Schema{}
	var limits map[string]destination.TableLimits
	// After a rewind, the restored record's epoch is applied, and the
	// destination may have dropped its markers since, so only the fence is
	// raised.
	recovering := head
	if rewound != nil {
		recovering = nil
	}
	if recovering != nil {
		e, staged = destination.Epoch{Generation: head.Generation, N: head.Epoch}, committables(head)
		schemas := make(map[string][]byte, len(head.Streams))
		for name, st := range head.Streams {
			schemas[name] = st.Schema
			if st.Delivered != nil {
				schemas[name] = st.Delivered
			}
		}
		if job.transformed() {
			schemas = outputSchemas(head)
		}
		// A redone epoch needs its schemas in place, and can't change.
		a, err := applySchemas(rctx, w, applied, schemas)
		if err == nil && len(a.Refused) > 0 {
			err = errs.Wrap(errs.SchemaDrift, &RefusedError{Columns: []RefusedColumn{{RefusedColumn: a.Refused[0]}}}, "runner: recovering a recorded epoch")
		}
		if err != nil {
			return res, err
		}
		limits = a.Limits
	}
	if job.staged() && recovering != nil {
		if err := handOver(rctx, job.Staging, w, e, stagedFiles(head.Files)); err != nil {
			return res, err
		}
	}
	if res.Recovery, err = w.Recover(rctx, res.Generation, e, staged); err != nil {
		return res, fmt.Errorf("runner: recover epoch %d: %w", e.N, err)
	}
	if job.staged() && recovering != nil {
		flush(rctx, job.Staging, e, log)
	}
	restored := head
	if res.Recovery == destination.Lost {
		if prevErr != nil {
			return res, errs.Wrap(errs.CheckpointCorrupt, prevErr, fmt.Sprintf("runner: the destination lost epoch %d, and the checkpoint before it can't be read", e.N))
		}
		// Safe because the epoch never became visible, and upstream is
		// acknowledged at most up to the previous record.
		if err := sess.RewindTo(rctx, prev); err != nil {
			return res, err
		}
		restored = prev
	}
	restored, reset, resetStreams := job.reset(restored)
	if len(resetStreams) > 0 {
		log.Info("reading streams again from the start", "streams", resetStreams, "reset", reset)
	}
	if restored != nil {
		res.Restored = restored.Epoch
	}
	log.Debug("attempt recovered", "recovery", res.Recovery.String(), "restored", res.Restored)
	done := job.doneStreams(restored)
	splits, err := job.readSplits(rctx, restored, done)
	if err != nil {
		return res, err
	}
	job.settleLanes(rctx, splits, done)
	// The other lanes open behind the fence lane 0 raised.
	for lane := 1; lane < job.writerLanes(); lane++ {
		lw, err := job.Destination.Open(rctx, job.openConfig(res.Generation, forced, lane))
		if err != nil {
			return res, fmt.Errorf("runner: open destination lane %d: %w", lane, err)
		}
		ws = append(ws, lw)
	}
	prune(rctx, sess, restored, job.keep(), job.Staging, job.HeldURL != "", log)
	holds, holdPlans, err := job.startHolds(rctx, restored, limits)
	if err != nil {
		return res, err
	}
	span.SetAttributes(attribute.Int64("openetl.generation", ints.Int64Clamped(res.Generation)), attribute.String("openetl.recovery", res.Recovery.String()), attribute.Int64("openetl.epoch", ints.Int64Clamped(res.Restored)))
	span.End()

	c := &coordinator{job: &job, sess: sess, w: w, ws: ws, log: log, res: &res, pending: make(map[uint64]*epochState), applied: applied, reset: reset,
		reported: map[string]*arrow.Schema{}, reportedOutputs: map[string]*arrow.Schema{}, measures: map[string]metric.MeasurementOption{},
		holds: holds, holdPlans: holdPlans}
	for _, p := range holdPlans {
		c.replaying = c.replaying || p.Replay != nil
	}
	if restored != nil && len(restored.Files) > 0 {
		c.lastStaged = destination.Epoch{Generation: restored.Generation, N: restored.Epoch}
	}
	if job.transformed() {
		c.outputSchemas = outputSchemas(restored)
	}
	if job.Transform != nil {
		// Stopped even if it didn't start: a submission whose outcome is
		// unknown may be running.
		defer func() {
			if serr := job.Transform.Stop(context.WithoutCancel(ctx)); serr != nil {
				log.Warn("stopping the transform failed", "err", serr)
			}
		}()
		if err := job.Transform.Start(ctx, res.Generation); err != nil {
			return res, fmt.Errorf("runner: start the transform: %w", err)
		}
	}
	return res, c.run(ctx, restored, canForce, splits, done)
}

// rewind restarts from the record before HEAD's, which can't be read (cause),
// if the job lets it, and returns that record. It fails with
// CHECKPOINT_CORRUPT if it doesn't.
func (j *Job) rewind(ctx context.Context, sess *checkpoint.Session, cause error) (*checkpoint.Record, *Rewound, error) {
	h := sess.Head()
	at := *h.Record
	fail := func(format string, args ...any) error {
		return errs.Wrap(errs.CheckpointCorrupt, cause, fmt.Sprintf("runner: the checkpoint the pipeline resumes from, %d-g%d, can't be read; ", at.Epoch, at.Generation)+fmt.Sprintf(format, args...))
	}
	before, known := h.Before()
	if !known {
		return nil, nil, fail("HEAD doesn't name the checkpoint before it, as one written by an older runner doesn't, so it can't restart from that")
	}
	switch j.Rewind.Policy {
	case RewindNever:
		return nil, nil, fail("restarting from the checkpoint before it delivers its epoch again, which the pipeline's heal mode leaves to a person")
	case RewindSafe:
		if lowered := j.replayLowers(); len(lowered) > 0 {
			return nil, nil, fail("restarting from the checkpoint before it would deliver its epoch again, and %s would get rows twice, below the guarantee they require", quoteList(lowered))
		}
	}
	var prev *checkpoint.Record
	if before != nil {
		var err error
		if prev, err = j.Log.Record(ctx, *before); errors.Is(err, checkpoint.ErrUnreadable) {
			return nil, nil, fail("nor can the one before it (%v)", err)
		} else if err != nil {
			return nil, nil, err
		}
	}
	if err := sess.RewindTo(ctx, prev); err != nil {
		return nil, nil, err
	}
	rw := &Rewound{Unreadable: at, Reason: cause.Error(), Approval: j.Rewind.Approval}
	if prev != nil {
		ref := prev.Ref()
		rw.Restored = &ref
	}
	return prev, rw, nil
}

// replayLowers are the streams whose guarantee rows read twice would lower,
// sorted.
func (j *Job) replayLowers() []string {
	var out []string
	for name, ok := range j.canForce() {
		if !ok {
			out = append(out, name)
		}
	}
	slices.Sort(out)
	return out
}

func quoteList(names []string) string {
	q := make([]string, len(names))
	for i, n := range names {
		q[i] = strconv.Quote(n)
	}
	if len(q) == 1 {
		return "stream " + q[0]
	}
	return "streams " + strings.Join(q[:len(q)-1], ", ") + " and " + q[len(q)-1]
}

// markForced marks the open splits of rec's streams that the attempt reads
// as forced: restoring from rec reads rows again that may be applied. A
// paused stream isn't read, so it keeps its marks.
func markForced(rec *checkpoint.Record, streams []Stream) {
	if rec == nil {
		return
	}
	for _, s := range streams {
		name := s.Name
		if _, ok := rec.Streams[name]; !ok {
			continue
		}
		splits := rec.Splits(name)
		for i := range splits {
			splits[i].Forced = splits[i].Forced || !splits[i].Complete
		}
		rec.SetSplits(name, splits)
	}
}

// reset returns rec with the positions of the streams of resets newer than
// its own dropped, as a copy, the newest reset it then carries, and those
// streams. A stream keeps its schema and holds: the destination still has
// them. A paused stream is reset too, so it carries no position and reads
// from the start once it's resumed.
func (j *Job) reset(rec *checkpoint.Record) (*checkpoint.Record, string, []string) {
	var taken string
	if rec != nil {
		taken = rec.Reset
	}
	newest, all := taken, false
	want := map[string]bool{}
	for _, r := range j.Resets {
		if r.ID <= taken {
			continue
		}
		newest = max(newest, r.ID)
		all = all || len(r.Streams) == 0
		for _, s := range r.Streams {
			want[s] = true
		}
	}
	if newest == taken {
		return rec, taken, nil
	}
	var streams []string
	for _, s := range slices.Concat(j.Streams, j.Paused) {
		if all || want[s.Name] {
			streams = append(streams, s.Name)
		}
	}
	if rec == nil {
		return nil, newest, streams
	}
	cp := *rec
	cp.Streams = maps.Clone(rec.Streams)
	cp.Forced, cp.Reread = slices.Clone(rec.Forced), slices.Clone(rec.Reread)
	for _, name := range streams {
		st, ok := cp.Streams[name]
		if !ok {
			continue
		}
		st.State, st.Splits, st.Complete, st.ReadStart = nil, nil, false, 0
		cp.Streams[name] = st
		cp.Forced = slices.DeleteFunc(cp.Forced, func(s string) bool { return s == name })
		cp.Reread = slices.DeleteFunc(cp.Reread, func(s string) bool { return s == name })
	}
	cp.Reset = newest
	return &cp, newest, streams
}

// doneStreams are those a retry within the sync doesn't read again.
func (j *Job) doneStreams(restored *checkpoint.Record) map[string]bool {
	done := map[string]bool{}
	if restored != nil && j.Sync != "" && restored.Sync == j.Sync {
		for name, st := range restored.Streams {
			done[name] = st.Complete
		}
	}
	return done
}

// openConfig is the destination's for writer lane lane of generation gen.
// forced are the streams a record that may be restored was forced on.
func (j *Job) openConfig(gen uint64, forced map[string]bool, lane int) destination.OpenConfig {
	caps := j.Destination.Capabilities()
	cfg := destination.OpenConfig{Pipeline: j.Pipeline, Generation: gen, Scope: j.Settings.Scope, Namespace: j.Namespace, CommitInterval: j.Settings.CommitInterval,
		Lane: lane}
	for _, s := range j.sinkStreams() {
		cfg.Streams = append(cfg.Streams, destination.Stream{
			Name:             s.Name,
			Mode:             s.Mode,
			PrimaryKey:       s.PrimaryKey,
			Dedupe:           delivery.Dedupe(caps, s.delivery(), j.Settings.Require, forced[s.Name] || s.Transformed && len(forced) > 0),
			Deletes:          s.Deletes,
			AllowTypeChanges: s.AllowTypeChanges,
		})
	}
	return cfg
}

// canForce says, by stream, whether a barrier may be forced on it.
func (j *Job) canForce() map[string]bool {
	caps := j.Destination.Capabilities()
	// A barrier forced on any input makes every output superset.
	outputsCanForce := true
	for _, o := range j.Outputs {
		outputsCanForce = outputsCanForce && delivery.CanForce(caps, o.delivery(), j.Settings.Require)
	}
	canForce := make(map[string]bool, len(j.Streams))
	for _, s := range j.Streams {
		canForce[s.Name] = delivery.CanForce(caps, s.delivery(), j.Settings.Require)
		if j.transformed() {
			canForce[s.Name] = outputsCanForce
		}
	}
	return canForce
}

// Lowered is the level of the rows a stream reads again after a restore from
// a forced barrier, and the one planned for it.
type Lowered struct{ Level, Planned delivery.Level }

// Lowered says, by stream, the level of the rows it reads again after a
// restore from a forced barrier, where that's below its planned level.
func (j *Job) Lowered() map[string]Lowered {
	caps, require := j.Destination.Capabilities(), j.Settings.Require
	lowered := func(of []Stream) (Lowered, bool) {
		l, ok := Lowered{delivery.ExactlyOnce, delivery.ExactlyOnce}, false
		for _, s := range of {
			planned := delivery.PlanStream(caps, s.delivery(), require).Level
			if got := delivery.PlanStream(caps, s.delivery().Forced(require), require).Level; got < planned {
				l, ok = Lowered{min(l.Level, got), min(l.Planned, planned)}, true
			}
		}
		return l, ok
	}
	out := map[string]Lowered{}
	for _, s := range j.Streams {
		of := []Stream{s}
		if j.transformed() {
			// An input read again makes every output superset.
			of = j.Outputs
		}
		if l, ok := lowered(of); ok {
			out[s.Name] = l
		}
	}
	return out
}

// outputSchemas are a record's outputs' schemas; nil without a record.
func outputSchemas(r *checkpoint.Record) map[string][]byte {
	if r == nil {
		return nil
	}
	schemas := make(map[string][]byte, len(r.Outputs))
	for name, o := range r.Outputs {
		if o.Schema != nil {
			schemas[name] = o.Schema
		}
	}
	return schemas
}

func (j *Job) keep() int { return cmp.Or(j.KeepRecords, defaultKeepRecords) }

func (j *Job) staged() bool {
	return j.Destination.Capabilities().Delivery == destination.DeliveryStaged
}

// prune is best effort: a later prune deletes what it leaves. Only
// restored's epoch can be recovered again, so it's the one whose staging is
// kept, with whatever this attempt stages after it.
func prune(ctx context.Context, sess *checkpoint.Session, restored *checkpoint.Record, keep int, staging Staging, held bool, log *slog.Logger) {
	if n, err := sess.Prune(ctx, restored, keep); err != nil {
		log.Warn("pruning the checkpoint log failed", "deleted", n, "err", err)
	} else if n > 0 {
		log.Debug("pruned the checkpoint log", "deleted", n)
	}
	if held {
		if n, err := sess.PruneHeld(ctx, restored); err != nil {
			log.Warn("pruning held values failed", "deleted", n, "err", err)
		} else if n > 0 {
			log.Debug("pruned held values", "deleted", n)
		}
	}
	if staging == nil {
		return
	}
	var e destination.Epoch
	if restored != nil {
		e = destination.Epoch{Generation: restored.Generation, N: restored.Epoch}
	}
	if n, err := staging.Sweep(ctx, e, sess.Generation()); err != nil {
		log.Warn("sweeping staged epochs failed", "deleted", n, "err", err)
	} else if n > 0 {
		log.Debug("swept staged epochs", "deleted", n)
	}
}

// handOver lets the destination read an epoch's staged files, and hands them
// to its writer, stream by stream.
func handOver(ctx context.Context, staging Staging, w destination.Writer, e destination.Epoch, files map[string][]destination.StagedFile) error {
	sw, ok := w.(destination.StagedWriter)
	if !ok {
		return errs.New(errs.Internal, "runner: the destination loads staged epochs, and its writer takes no files")
	}
	for _, stream := range slices.Sorted(maps.Keys(files)) {
		if err := staging.Hand(e, stream, files[stream]); err != nil {
			return fmt.Errorf("runner: epoch %d: %w", e.N, err)
		}
		if err := sw.WriteStaged(ctx, e, stream, files[stream]); err != nil {
			return fmt.Errorf("runner: hand over %s in epoch %d: %w", stream, e.N, err)
		}
	}
	return nil
}

// flush is best effort: the cursor is a hint, and a lost one only means the
// next attempt delivers more again.
func flush(ctx context.Context, staging Staging, e destination.Epoch, log *slog.Logger) {
	if err := retry(ctx, func() error { return staging.Flush(ctx, e) }, nil); err != nil {
		log.Warn("writing the delivery cursor failed", "epoch", e.N, "err", err)
	}
}

// readSplits are each stream's splits as the attempt starts: a read it
// continues, or one that completed earlier in the sync (done), keeps its
// restored splits, and a read that starts gets the source's, from where the
// last one's ended.
func (j *Job) readSplits(ctx context.Context, restored *checkpoint.Record, done map[string]bool) (map[string][]checkpoint.SplitState, error) {
	out := make(map[string][]checkpoint.SplitState, len(j.Streams))
	splitter, _ := j.Source.(source.Splitter)
	for _, s := range j.Streams {
		var prev []checkpoint.SplitState
		if restored != nil {
			prev = restored.Splits(s.Name)
		}
		// A continuous read follows every split, so one that a bounded read
		// completed means a new read.
		ongoing := prev != nil && !restored.Streams[s.Name].Complete && !(j.Continuous && slices.ContainsFunc(prev, func(sp checkpoint.SplitState) bool { return sp.Complete }))
		if done[s.Name] || ongoing {
			out[s.Name] = prev
			continue
		}
		ended := make([]source.Split, 0, len(prev))
		for _, sp := range prev {
			ended = append(ended, source.Split{ID: sp.ID, State: sp.State})
		}
		if prev == nil {
			ended = nil
		}
		var splits []source.Split
		var err error
		// A Spark or Flink transform's read isn't split, unless its last
		// read was, which only the source knows how to go on from.
		split := len(ended) > 1 || len(ended) == 1 && ended[0].ID != checkpoint.WholeStream
		if splitter != nil && (!j.ownCode() || split) {
			splits, err = splitter.Splits(ctx, source.ConfiguredStream{Name: s.Name, SyncMode: s.Read, Cursor: s.CursorField}, ended)
		} else {
			splits, err = source.Whole(ended)
		}
		if err != nil {
			return nil, fmt.Errorf("runner: the splits of stream %q: %w", s.Name, err)
		}
		if len(splits) == 0 {
			return nil, errs.New(errs.Internal, "runner: the source gave stream %q no splits", s.Name)
		}
		seen := map[string]bool{}
		for _, sp := range splits {
			if seen[sp.ID] {
				return nil, errs.New(errs.Internal, "runner: the source gave stream %q split %q twice", s.Name, sp.ID)
			}
			seen[sp.ID] = true
			out[s.Name] = append(out[s.Name], checkpoint.SplitState{ID: sp.ID, State: sp.State})
		}
	}
	return out, nil
}

// readRequest reads the splits of every stream not done that haven't
// completed, and streams read whole from their states.
func (j *Job) readRequest(splits map[string][]checkpoint.SplitState, done map[string]bool) source.ReadRequest {
	req := source.ReadRequest{Pipeline: j.Pipeline, Continuous: j.Continuous, Lanes: j.lanes()}
	for _, s := range j.Streams {
		if done[s.Name] {
			continue
		}
		cs := source.ConfiguredStream{Name: s.Name, SyncMode: s.Read, Cursor: s.CursorField, Columns: s.columns()}
		if ss := splits[s.Name]; len(ss) == 1 && ss[0].ID == checkpoint.WholeStream {
			cs.State = ss[0].State
		} else {
			for _, sp := range ss {
				if !sp.Complete {
					cs.Splits = append(cs.Splits, source.Split{ID: sp.ID, State: sp.State})
				}
			}
		}
		req.Streams = append(req.Streams, cs)
	}
	return req
}

// States are a record's streams' states as an Acknowledger takes them.
func States(rec *checkpoint.Record) (map[string][]byte, map[string][]source.Split) {
	splits := make(map[string][]checkpoint.SplitState, len(rec.Streams))
	for name := range rec.Streams {
		splits[name] = rec.Splits(name)
	}
	return acknowledged(splits)
}

// acknowledged are an epoch's splits as an Acknowledger takes them: the
// states of streams read whole, and the splits of those that aren't.
func acknowledged(splits map[string][]checkpoint.SplitState) (map[string][]byte, map[string][]source.Split) {
	states := map[string][]byte{}
	var split map[string][]source.Split
	for name, ss := range splits {
		if len(ss) == 1 && ss[0].ID == checkpoint.WholeStream {
			states[name] = ss[0].State
			continue
		}
		if split == nil {
			split = map[string][]source.Split{}
		}
		for _, sp := range ss {
			split[name] = append(split[name], source.Split{ID: sp.ID, State: sp.State})
		}
	}
	return states, split
}

// committables are a record's, as the destination takes them.
func committables(r *checkpoint.Record) []destination.Staged {
	out := make([]destination.Staged, len(r.Committables))
	for i, c := range r.Committables {
		out[i] = c
	}
	return out
}

func stagedFiles(m map[string][]checkpoint.StagedFile) map[string][]destination.StagedFile {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string][]destination.StagedFile, len(m))
	for stream, fs := range m {
		for _, f := range fs {
			out[stream] = append(out[stream], destination.StagedFile(f))
		}
	}
	return out
}

func recordFiles(m map[string][]destination.StagedFile) map[string][]checkpoint.StagedFile {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string][]checkpoint.StagedFile, len(m))
	for stream, fs := range m {
		for _, f := range fs {
			out[stream] = append(out[stream], checkpoint.StagedFile(f))
		}
	}
	return out
}

func (j *Job) validate() error {
	switch {
	case j.Owner == "":
		return errors.New("runner: job has no owner")
	case j.Log == nil || j.Source == nil || j.Destination == nil || j.Dataflow == nil:
		return errors.New("runner: job is missing its log, source, destination or dataflow")
	case len(j.Streams) == 0:
		return errors.New("runner: job has no streams")
	case j.KeepRecords < 0 || j.KeepRecords == 1:
		return fmt.Errorf("runner: the job keeps %d checkpoint records, not at least 2", j.KeepRecords)
	case j.Settings.Require == 0 || j.Settings.Scope == 0:
		return errors.New("runner: job settings need Require and Scope")
	case j.staged() && j.Staging == nil:
		return errs.New(errs.ConfigInvalid, "runner: the destination loads staged epochs, and this runner has nowhere to stage them")
	}
	switch {
	case j.Transform != nil && len(j.Outputs) == 0:
		return errs.New(errs.ConfigInvalid, "runner: the transform has no outputs")
	case j.Transform != nil && j.Continuous:
		return errs.New(errs.ConfigInvalid, "runner: a transform runs in a scheduled pipeline's syncs")
	case j.Flink != nil && (!j.Continuous || j.Transform != nil || j.Bridge == nil || len(j.Outputs) == 0):
		return errs.New(errs.ConfigInvalid, "runner: a Flink job runs in a continuous pipeline, with a bridge and outputs, and without a Spark transform")
	case j.SQL != nil && len(j.Outputs) == 0:
		return errs.New(errs.ConfigInvalid, "runner: the SQL transform has no outputs")
	case j.SQL != nil && (j.Transform != nil || j.Flink != nil):
		return errs.New(errs.ConfigInvalid, "runner: a pipeline has one transform, and this one has SQL and Spark or Flink")
	case !j.transformed() && len(j.Outputs) > 0:
		return errors.New("runner: outputs without a transform")
	case j.lanes() > 1 && j.ownCode():
		return errs.New(errs.ConfigInvalid, "runner: a Spark or Flink transform runs on one lane")
	case j.KeyGroups < 0 || j.KeyGroups&(j.KeyGroups-1) != 0:
		return errs.New(errs.ConfigInvalid, "runner: %d key groups, not a power of two", j.KeyGroups)
	}
	seen := make(map[string]bool)
	for _, s := range j.Streams {
		if seen[s.Name] {
			return errs.New(errs.ConfigInvalid, "runner: stream %q configured twice", s.Name)
		}
		seen[s.Name] = true
		if err := s.checkOverrides(); err != nil {
			return errs.Wrap(errs.ConfigInvalid, err, "runner")
		}
		switch {
		case s.NanosecondTimestamps && j.Transform != nil:
			return errs.New(errs.ConfigInvalid, "runner: stream %q keeps nanosecond timestamps, and Spark keeps only microseconds", s.Name)
		case j.Flink != nil && s.Replay != source.Exact:
			return errs.New(errs.ConfigInvalid, "runner: stream %q doesn't replay exactly from a resume point, which a Flink job's restores need", s.Name)
		case s.NanosecondTimestamps && !j.Destination.Capabilities().NanosecondTimestamps:
			return errs.New(errs.ConfigInvalid, "runner: stream %q keeps nanosecond timestamps, but the destination keeps only microseconds", s.Name)
		}
	}
	if err := j.checkHolds(); err != nil {
		return errs.Wrap(errs.ConfigInvalid, err, "runner")
	}
	clear(seen)
	for _, o := range j.Outputs {
		if seen[o.Name] || !o.Transformed && o.From == "" {
			return errs.New(errs.ConfigInvalid, "runner: output %q configured twice, or not as an output", o.Name)
		}
		seen[o.Name] = true
	}
	streams := make([]delivery.Stream, 0, len(j.sinkStreams()))
	for _, s := range j.sinkStreams() {
		ds := s.delivery()
		if j.Flink != nil {
			ds.Engine = "flink"
		}
		streams = append(streams, ds)
	}
	return errs.Wrap(errs.ConfigInvalid, delivery.Check(j.Destination.Capabilities(), j.Settings, streams), "runner: plan checks")
}

type eventKind uint8

const (
	evPlaced    eventKind = iota + 1 // every stream crossed barrier epoch; complete lists the streams it ended
	evStaged                         // the sink staged epoch
	evReady                          // the dataflow's part of epoch is durable
	evThreshold                      // CommitMaxBytes was reached
	evReadDone                       // Read returned; active says whether anything emitted after barrier epoch is uncommitted
	evCommitted                      // epoch is visible in the destination
)

type event struct {
	kind  eventKind
	epoch uint64
	lane  int // evStaged's writer lane
	// evPlaced's: each stream's splits at the barrier.
	splits map[string][]checkpoint.SplitState
	rows   map[string]uint64
	staged destination.Staged
	ready  *Ready
	active bool
}

type epochState struct {
	requested time.Time
	// When the epoch began: the barrier before it, or the attempt's start.
	began         time.Time
	placed, ready bool
	// Each writer lane's committable, which lanes have staged, and how many.
	descs    []destination.Staged
	stagedOn []bool
	staged   int
	splits   map[string][]checkpoint.SplitState
	rows     map[string]uint64
	flow     *Ready
	// In a Flink pipeline, the Flink checkpoint the epoch ends at.
	flink *checkpoint.FlinkCheckpoint
}

type commitReq struct {
	n  uint64
	st *epochState
}

// coordinator requests barriers and commits each epoch once it is placed and
// staged. Commits run one at a time, in epoch order, on the committer's
// goroutine, so barriers keep being requested while an epoch commits.
type coordinator struct {
	job  *Job
	sess *checkpoint.Session
	// Lane 0's writer, which applies schemas and commits, and every lane's.
	w   destination.Writer
	ws  []destination.Writer
	log *slog.Logger
	res *Result
	// The newest reset the attempt took, which its records carry.
	reset string

	events      chan event
	commits     chan commitReq
	pump        *sourcePump
	next        uint64 // the next epoch to request
	outstanding bool   // a barrier is being placed
	want        bool   // a commit trigger fired
	readDone    bool
	active      bool // emitted after the last barrier, so a final epoch is needed
	committing  bool
	draining    bool
	drainEpoch  uint64 // the barrier requested after draining began; 0 until then
	pending     map[uint64]*epochState
	committed   uint64                             // the last epoch committed or restored
	ackStates   map[string][]checkpoint.SplitState // owned by the committer: splits of the committed epoch, acked after the next commit
	applied     map[string]*arrow.Schema           // owned by the committer: each sink stream's schema last applied
	reported    map[string]*arrow.Schema           // owned by the committer: each stream's schema last passed to OnSchemas
	// Owned by the committer: each output's schema last passed to OnSchemas.
	reportedOutputs map[string]*arrow.Schema
	// The restored record's outputs' schemas.
	outputSchemas map[string][]byte
	readStarts    map[string]int64
	// The paused streams' states and splits in the restored record, which
	// every record carries.
	carried     map[string]carried
	sincePrune  int    // owned by the committer: commits since it last pruned
	seen        uint64 // what the source had emitted when the last barrier was requested
	lastBarrier time.Time
	lastStaged  destination.Epoch                   // owned by the committer: the last recorded epoch with staged files
	syncStart   *checkpoint.RecordRef               // the record the sync started after; nil if unknown
	syncRows    map[string]uint64                   // owned by the committer: rows by stream in the sync so far; nil without a sync
	measures    map[string]metric.MeasurementOption // owned by the committer: metrics' attributes by stream, "" for the job
	// Called on the committer's goroutine after it pruned the log.
	afterPrune func(ctx context.Context, head *checkpoint.Record)
	// Owned by the committer: each holding stream's holds as last recorded,
	// or as the attempt started them.
	holds     map[string]*checkpoint.Holds
	holdPlans map[string]*HoldPlan
	// A release is being patched in, so barriers go on after the read ends.
	replaying bool
}

func (c *coordinator) run(ctx context.Context, restored *checkpoint.Record, canForce map[string]bool, splits map[string][]checkpoint.SplitState, done map[string]bool) error {
	schemas := map[string][]byte{}
	now := time.Now().UnixMicro()
	c.readStarts = make(map[string]int64, len(c.job.Streams))
	for _, s := range c.job.Streams {
		c.readStarts[s.Name] = now
		if restored == nil {
			continue
		}
		if st := restored.Streams[s.Name]; st.ReadStart != 0 && !st.Complete {
			c.readStarts[s.Name] = st.ReadStart
		}
	}
	if restored != nil {
		c.committed = restored.Epoch
		c.ackStates = map[string][]checkpoint.SplitState{}
		for name, st := range restored.Streams {
			c.ackStates[name] = restored.Splits(name)
			if st.Schema != nil {
				schemas[name] = st.Schema
			}
		}
		for _, s := range c.job.Paused {
			if st, ok := restored.Streams[s.Name]; ok {
				if c.carried == nil {
					c.carried = map[string]carried{}
				}
				// Not read in the epoch, so nothing is read again; a forced
				// position stays forced.
				splits := slices.Clone(restored.Splits(s.Name))
				for i := range splits {
					splits[i].Reread = false
				}
				st.Rows = 0
				c.carried[s.Name] = carried{st, splits}
				// The source isn't reading it, so it isn't told about it.
				delete(c.ackStates, s.Name)
			}
		}
	}
	c.next = c.committed + 1
	c.res.Last = c.committed
	c.lastBarrier = time.Now()
	if c.job.Sync != "" {
		c.syncStart, c.syncRows = syncSoFar(restored, c.job.Sync)
	}

	g, gctx := errgroup.WithContext(ctx)
	toFlow := make(chan Frame, 16)
	fromFlow := make(chan Frame, 16)
	c.events = make(chan event, 64)
	c.commits = make(chan commitReq, 1)
	ends := newReadEnds()
	c.pump = newSourcePump(gctx, c.job, c.committed, splits, canForce, done, ends, toFlow, c.events)
	plan := Plan{
		Org:        c.job.Org,
		Pipeline:   c.job.Pipeline,
		Owner:      c.job.Owner,
		Generation: c.sess.Generation(),
		FirstEpoch: c.next,
		Streams:    c.job.Streams,
		Naming: Naming{
			KeepSource: c.job.KeepSourceNames,
			MaxLength:  c.job.Destination.Capabilities().MaxIdentifierLength,
		},
		Policy:             c.job.SchemaPolicy,
		QuarantineURL:      c.job.QuarantineURL,
		QuarantineInfinity: c.job.QuarantineInfinity,
		ReadStarts:         c.readStarts,
		Schemas:            schemas,
	}
	if c.job.SQL != nil {
		plan.SQL, plan.Outputs, plan.OutputSchemas = c.job.SQL, c.job.Outputs, c.outputSchemas
	}
	if len(c.holdPlans) > 0 {
		plan.HeldURL, plan.Holds = c.job.HeldURL, c.holdPlans
	}
	plan.Lanes, plan.WriterLanes, plan.KeyGroups = c.job.lanes(), c.job.writerLanes(), c.job.keyGroups()
	for _, s := range c.job.sinkStreams() {
		// A key's rows meet on one writer lane. An output of a SQL
		// transform has its stream's splits and read.
		if len(s.PrimaryKey) > 0 && (!s.SplitsByKey || s.Read == source.CDC) && plan.WriterLanes > 1 {
			if plan.KeyGrouped == nil {
				plan.KeyGrouped = map[string]bool{}
			}
			plan.KeyGrouped[s.Name] = true
		}
	}
	if restored != nil {
		plan.Snapshots = restored.Snapshots
	}
	if es, ok := c.job.Destination.(EngineSink); ok {
		plan.Sink = es.EngineSink()
	}
	var staging Staging
	if c.job.staged() {
		staging = c.job.Staging
		plan.Sink = SinkPlan{StagedURL: staging.URL()}
	}
	starts := map[string]time.Time{}
	for _, s := range c.job.Streams {
		if !done[s.Name] {
			starts[s.Name] = time.UnixMicro(c.readStarts[s.Name]).UTC()
		}
	}
	var sinkEnds readEnder = ends
	if c.job.transformed() {
		// An output's read starts with its inputs' first, and ends with
		// their last.
		var first time.Time
		var reading []string
		for _, s := range c.job.Streams {
			if t, ok := starts[s.Name]; ok {
				reading = append(reading, s.Name)
				if first.IsZero() || t.Before(first) {
					first = t
				}
			}
		}
		starts = map[string]time.Time{}
		for _, o := range c.job.Outputs {
			if len(reading) > 0 {
				starts[o.Name] = first
			}
		}
		sinkEnds = lastEnd{ends: ends, inputs: reading}
	}
	lanes := make([]chan Frame, len(c.ws))
	sinks := make([]*sinkPump, len(c.ws))
	for i, w := range c.ws {
		lanes[i] = make(chan Frame, 16)
		sinks[i] = newSinkPump(w, i, c.sess.Generation(), c.next, plan.Sink != SinkPlan{}, staging, c.job.sinkStreams(), starts, sinkEnds, lanes[i], c.events)
	}

	req := c.job.readRequest(splits, done)
	g.Go(func() error {
		if len(req.Streams) > 0 {
			if err := c.job.Source.Read(gctx, req, c.pump); err != nil {
				return fmt.Errorf("runner: read: %w", err)
			}
		}
		return c.pump.readDone()
	})
	g.Go(c.pump.run)
	if c.job.Transform != nil {
		c.runTransform(gctx, g, plan, toFlow, fromFlow)
	} else {
		g.Go(func() error { return c.job.Dataflow.Run(gctx, plan, toFlow, fromFlow) })
	}
	g.Go(func() error { return demux(gctx, fromFlow, lanes) })
	for _, sink := range sinks {
		g.Go(func() error { return sink.run(gctx) })
	}
	g.Go(func() error { return c.committer(gctx) })
	g.Go(func() error {
		defer close(c.commits)
		return c.loop(gctx)
	})
	return g.Wait()
}

func (c *coordinator) loop(ctx context.Context) error {
	var tick <-chan time.Time
	if d := c.job.Settings.CommitInterval; d > 0 {
		t := time.NewTicker(d)
		defer t.Stop()
		tick = t.C
	}
	drain := c.job.Drain
	for {
		// Events first, so decisions don't depend on select's random choice.
		select {
		case ev := <-c.events:
			if err := c.handle(ctx, ev); err != nil {
				return err
			}
			continue
		default:
		}
		if c.finished() {
			c.pump.stop()
			return nil
		}
		if c.drained() {
			c.log.Info("drained", "epoch", c.committed)
			return ErrDrained
		}
		if c.shouldRequest() {
			c.outstanding, c.want = true, false
			c.active = c.active && !c.readDone // after Read, this barrier covers everything
			n := c.next
			c.next++
			if c.draining && c.drainEpoch == 0 {
				c.drainEpoch = n
			}
			now := time.Now()
			c.pending[n] = &epochState{requested: now, began: c.lastBarrier}
			c.lastBarrier = now
			c.seen = c.pump.emitted.Load()
			if err := c.pump.requestBarrier(n); err != nil {
				return err
			}
			continue
		}
		select {
		case ev := <-c.events:
			if err := c.handle(ctx, ev); err != nil {
				return err
			}
		case <-tick:
			// An idle source's epoch would hold nothing, unless it carries
			// patches.
			c.want = c.want || c.pump.emitted.Load() != c.seen || c.replaying
		case <-drain:
			drain = nil
			c.draining, c.want = true, true
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// One epoch staging, one waiting to commit; with a transform, one more in
// it.
const maxInFlight = 2

// How far commit times can raise an epoch's byte threshold.
const maxEpochBytes = 16 << 30

func (c *coordinator) shouldRequest() bool {
	limit := uint64(maxInFlight)
	if c.job.Transform != nil {
		limit++
	}
	if c.outstanding || c.next-1-c.committed >= limit {
		return false
	}
	if c.readDone {
		return c.active || c.replaying
	}
	return c.want && c.drainEpoch == 0
}

func (c *coordinator) finished() bool {
	return c.readDone && !c.active && !c.replaying && c.settled()
}

func (c *coordinator) drained() bool {
	return c.drainEpoch != 0 && !c.readDone && c.settled()
}

// settled: no barrier is being placed and every epoch requested has committed.
func (c *coordinator) settled() bool {
	return !c.outstanding && !c.committing && c.committed == c.next-1
}

func (c *coordinator) handle(ctx context.Context, ev event) error {
	switch ev.kind {
	case evThreshold:
		c.want = true
		return nil
	case evReadDone:
		// A barrier requested after the pump's last one is placed after Read
		// ended, so it covers everything.
		c.readDone, c.active = true, ev.active && ev.epoch == c.next-1
		return nil
	case evPlaced:
		st := c.pending[ev.epoch]
		if st == nil || st.placed {
			return fmt.Errorf("runner: unexpected placement of epoch %d", ev.epoch)
		}
		st.placed, st.splits, st.rows = true, ev.splits, ev.rows
		c.outstanding = false
	case evStaged:
		st := c.pending[ev.epoch]
		if st != nil && st.descs == nil {
			st.descs, st.stagedOn = make([]destination.Staged, len(c.ws)), make([]bool, len(c.ws))
		}
		if st == nil || ev.lane >= len(c.ws) || st.stagedOn[ev.lane] {
			return fmt.Errorf("runner: unexpected staging of epoch %d on lane %d", ev.epoch, ev.lane)
		}
		st.descs[ev.lane], st.stagedOn[ev.lane] = ev.staged, true
		st.staged++
	case evReady:
		st := c.pending[ev.epoch]
		if st == nil || st.ready {
			return fmt.Errorf("runner: unexpected readiness of epoch %d", ev.epoch)
		}
		st.ready, st.flow = true, ev.ready
		if len(c.holdPlans) > 0 {
			c.replaying = false
			for _, h := range ev.ready.Holds {
				c.replaying = c.replaying || h.Replayed != nil && !h.Replayed.Done
			}
		}
	case evCommitted:
		if !c.committing || ev.epoch != c.committed+1 {
			return fmt.Errorf("runner: unexpected commit of epoch %d", ev.epoch)
		}
		c.committing = false
		c.committed = ev.epoch
		c.res.Committed++
		c.res.Last = ev.epoch
		delete(c.pending, ev.epoch)
		if c.job.OnCommit != nil {
			c.job.OnCommit(ev.epoch)
		}
	}
	n := c.committed + 1
	st := c.pending[n]
	if c.committing || st == nil || !st.placed || st.staged < len(c.ws) || !st.ready {
		return nil
	}
	c.committing = true
	select {
	case c.commits <- commitReq{n: n, st: st}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *coordinator) committer(ctx context.Context) error {
	for req := range c.commits {
		if err := c.commit(ctx, req.n, req.st); err != nil {
			return err
		}
	}
	return nil
}

// commit records the checkpoint, makes the epoch visible, reports it to the
// loop, then acknowledges the previous epoch upstream.
func (c *coordinator) commit(ctx context.Context, n uint64, st *epochState) (err error) {
	start := time.Now()
	ctx, span := tracer.Start(ctx, "commit", trace.WithAttributes(attribute.String("openetl.pipeline", c.job.Pipeline), attribute.Int64("openetl.epoch", ints.Int64Clamped(n))))
	defer func() { endSpan(span, err) }()
	// Before the record: a refused column can then be held, with the epoch
	// read again.
	if err := c.applySchemas(ctx, n, st); err != nil {
		return err
	}
	rec, err := c.record(ctx, n, st)
	if err != nil {
		return err
	}
	if err := c.apply(ctx, n, st, rec, start); err != nil {
		return err
	}
	select {
	case c.events <- event{kind: evCommitted, epoch: n}:
	case <-ctx.Done():
		return ctx.Err()
	}
	c.acknowledge(ctx, st, rec)
	return nil
}

// carried is a paused stream as the restored record has it.
type carried struct {
	state  checkpoint.StreamState
	splits []checkpoint.SplitState
}

// record writes epoch n's checkpoint record, and drops what the epoch
// before it staged.
func (c *coordinator) record(ctx context.Context, n uint64, st *epochState) (*checkpoint.Record, error) {
	rec := &checkpoint.Record{
		Epoch:        n,
		Streams:      make(map[string]checkpoint.StreamState, len(st.splits)),
		KeyGroups:    c.job.keyGroups(),
		Committables: make([][]byte, len(st.descs)),
		Files:        recordFiles(st.flow.Files()),
		Snapshots:    st.flow.Snapshots,
		Quarantine:   st.flow.Quarantine,
		Sync:         c.job.Sync,
		Flink:        st.flink,
		Reset:        c.reset,
		CreatedAt:    time.Now().UTC(),
	}
	if c.syncRows != nil {
		for name, rows := range st.rows {
			c.syncRows[name] += rows
		}
		rec.SyncStart, rec.SyncRows = c.syncStart, maps.Clone(c.syncRows)
	}
	at := checkpoint.RecordRef{Epoch: n, Generation: c.sess.Generation()}
	holds := make(map[string]*checkpoint.Holds, len(c.holds))
	for name, h := range c.holds {
		holds[name] = mergeHolds(h, st.flow.Holds[name], at, rec.CreatedAt)
	}
	for _, name := range slices.Sorted(maps.Keys(st.splits)) {
		rec.Streams[name] = checkpoint.StreamState{
			Schema:    st.flow.Schemas[name],
			ReadStart: c.readStarts[name],
			Rows:      st.rows[name],
			Delivered: st.flow.Delivered[name],
			Holds:     holds[name],
		}
		rec.SetSplits(name, st.splits[name])
	}
	for name, cs := range c.carried {
		rec.Streams[name] = cs.state
		rec.SetSplits(name, cs.splits)
	}
	if c.job.transformed() {
		rec.Outputs = make(map[string]checkpoint.OutputState, len(c.job.Outputs))
		for _, o := range c.job.Outputs {
			rec.Outputs[o.Name] = checkpoint.OutputState{Schema: st.flow.OutputSchemas[o.Name], Rows: st.flow.OutputRows[o.Name]}
		}
	}
	for i, d := range st.descs {
		rec.Committables[i] = d
	}
	if err := retry(ctx, func() error { return c.sess.Commit(ctx, rec) }, checkpoint.ErrFenced); err != nil {
		return nil, fmt.Errorf("runner: record checkpoint %d: %w", n, err)
	}
	for name := range c.holds {
		c.holds[name] = holds[name]
	}
	e := destination.Epoch{Generation: c.sess.Generation(), N: n}
	c.dropStaged(ctx, e, len(rec.Files) > 0)
	if c.job.Transform != nil {
		if err := c.job.Transform.Drop(ctx, e); err != nil {
			c.log.Warn("dropping the transform's files of an epoch failed", "epoch", n, "err", err)
		}
	}
	return rec, nil
}

// applySchemas applies epoch n's schemas, as the destination gets them,
// before n is recorded. A refusal is a RefusedError.
func (c *coordinator) applySchemas(ctx context.Context, n uint64, st *epochState) error {
	sinkSchemas := maps.Clone(st.flow.Schemas)
	maps.Copy(sinkSchemas, st.flow.Delivered)
	if c.job.transformed() {
		sinkSchemas = st.flow.OutputSchemas
	}
	before := maps.Clone(c.applied)
	var a destination.SchemaApplied
	err := retry(ctx, func() (err error) {
		a, err = applySchemas(ctx, c.w, c.applied, sinkSchemas)
		return err
	}, nil)
	if err != nil {
		return fmt.Errorf("runner: epoch %d: %w", n, err)
	}
	if len(a.Refused) == 0 {
		return nil
	}
	cols, err := refusedColumns(a, before)
	if err != nil {
		return fmt.Errorf("runner: epoch %d: %w", n, err)
	}
	return &RefusedError{Columns: cols}
}

// apply makes recorded epoch n visible, and reports it. start is when its
// commit began.
func (c *coordinator) apply(ctx context.Context, n uint64, st *epochState, rec *checkpoint.Record, start time.Time) error {
	e := destination.Epoch{Generation: c.sess.Generation(), N: n}
	collapsed := map[string]uint64{}
	err := retry(ctx, func() error {
		done, err := c.w.Commit(ctx, e, st.descs)
		for name, rows := range done.Collapsed {
			collapsed[name] += rows
		}
		return err
	}, destination.ErrFenced)
	if err != nil {
		return fmt.Errorf("runner: commit epoch %d: %w", n, err)
	}
	if len(rec.Files) > 0 {
		flush(ctx, c.job.Staging, e, c.log)
	}
	c.log.Debug("epoch committed", "epoch", n, "forced", rec.Forced, "commit", time.Since(start), "since_requested", time.Since(st.requested), "since_began", time.Since(st.began))
	c.measure(ctx, rec, st.flow, collapsed, time.Since(start), time.Since(st.requested))
	c.growEpochs(time.Since(start), time.Since(st.began))
	if c.job.OnSchemas != nil {
		inputs, err := changedRaw(c.reported, st.flow.Schemas)
		if err != nil {
			return fmt.Errorf("runner: epoch %d: %w", n, err)
		}
		outputs, err := changedRaw(c.reportedOutputs, st.flow.OutputSchemas)
		if err != nil {
			return fmt.Errorf("runner: epoch %d: %w", n, err)
		}
		if len(inputs) > 0 || len(outputs) > 0 {
			c.job.OnSchemas(e, inputs, outputs)
		}
	}
	if c.job.OnCheckpoint != nil {
		c.job.OnCheckpoint(rec, collapsed)
	}
	return nil
}

// acknowledge acknowledges the epoch before rec upstream, and prunes now and
// then.
func (c *coordinator) acknowledge(ctx context.Context, st *epochState, rec *checkpoint.Record) {
	// Trail by one checkpoint, so recovery can fall back one record.
	if ack, ok := c.job.Source.(source.Acknowledger); ok && c.ackStates != nil {
		states, splits := acknowledged(c.ackStates)
		if err := ack.Committed(ctx, states, splits); err != nil {
			c.log.Warn("upstream acknowledgement failed; the next commit retries it", "epoch", rec.Epoch-1, "err", err)
		}
	}
	c.ackStates = st.splits

	// A long job prunes as it goes, not only when it starts.
	if c.sincePrune++; c.sincePrune >= c.job.keep() {
		c.sincePrune = 0
		prune(ctx, c.sess, rec, c.job.keep(), c.job.Staging, c.job.HeldURL != "", c.log)
		if c.afterPrune != nil {
			c.afterPrune(ctx, rec)
		}
	}
}

// dropStaged drops the staging of the epoch recorded before e, now that e's
// record is HEAD and it can't be recovered again. It's best effort: the
// attempt's pruning sweeps what it leaves.
func (c *coordinator) dropStaged(ctx context.Context, e destination.Epoch, staged bool) {
	if prev := c.lastStaged; !prev.IsZero() {
		if err := c.job.Staging.Drop(ctx, prev); err != nil {
			c.log.Warn("dropping a staged epoch failed", "epoch", prev.N, "generation", prev.Generation, "err", err)
		}
		c.lastStaged = destination.Epoch{}
	}
	if staged {
		c.lastStaged = e
	}
}

// growEpochs raises the byte threshold when commits take more than a quarter
// of an epoch's time, since one committer manages only a few a second.
func (c *coordinator) growEpochs(commit, epoch time.Duration) {
	if c.job.Settings.CommitMaxBytesSet || commit*4 <= epoch {
		return
	}
	if b := c.pump.maxBytes.Load(); b > 0 && b < maxEpochBytes {
		c.pump.maxBytes.Store(min(b*2, maxEpochBytes))
		c.log.Debug("epochs grow", "bytes", min(b*2, maxEpochBytes), "commit", commit, "epoch", epoch)
	}
}

func (c *coordinator) measure(ctx context.Context, rec *checkpoint.Record, ready *Ready, collapsed map[string]uint64, commit, epoch time.Duration) {
	job := c.attrs("")
	epochs.Add(ctx, 1, job)
	commitLength.Record(ctx, commit.Seconds(), job)
	epochLength.Record(ctx, epoch.Seconds(), job)
	quarantined := map[string]uint64{}
	for _, q := range rec.Quarantine {
		quarantined[q.Stream] += q.Rows
	}
	for name, st := range rec.Streams {
		attrs := c.attrs(name)
		rowsCommitted.Add(ctx, ints.Int64Clamped(st.Rows), attrs)
		if q := quarantined[name]; q > 0 {
			rowsQuarantined.Add(ctx, ints.Int64Clamped(q), attrs)
		}
		if n := collapsed[name]; n > 0 {
			rowsCollapsed.Add(ctx, ints.Int64Clamped(n), attrs)
		}
		c.measureHolds(ctx, name, st.Holds, ready.Holds[name], rec.CreatedAt)
	}
}

func (c *coordinator) measureHolds(ctx context.Context, stream string, h *checkpoint.Holds, r *ReadyHolds, now time.Time) {
	column := func(col string) metric.MeasurementOption {
		return metric.WithAttributes(attribute.String("openetl.org", c.job.Org), attribute.String("openetl.pipeline", c.job.Pipeline),
			attribute.String("openetl.stream", stream), attribute.String("openetl.column", col))
	}
	if r != nil {
		for col, cell := range r.Cells {
			cellsHeld.Add(ctx, ints.Int64Clamped(cell.Cells), column(col))
		}
		var bytes uint64
		for _, f := range r.Files {
			bytes += f.Bytes
		}
		if bytes > 0 {
			heldBytes.Add(ctx, int64(bytes), c.attrs(stream))
		}
	}
	if h == nil {
		return
	}
	for _, col := range h.Columns {
		if !col.Discarded && !col.Started.IsZero() {
			holdAge.Record(ctx, now.Sub(col.Started).Seconds(), column(col.Column))
		}
	}
	for name, cell := range h.Cells {
		if !cell.Started.IsZero() {
			holdAge.Record(ctx, now.Sub(cell.Started).Seconds(), column(name))
		}
	}
}

// attrs returns metrics' attributes for a stream, or for the job.
func (c *coordinator) attrs(stream string) metric.MeasurementOption {
	if m, ok := c.measures[stream]; ok {
		return m
	}
	kv := []attribute.KeyValue{attribute.String("openetl.org", c.job.Org), attribute.String("openetl.pipeline", c.job.Pipeline)}
	if stream != "" {
		kv = append(kv, attribute.String("openetl.stream", stream))
	}
	m := metric.WithAttributes(kv...)
	c.measures[stream] = m
	return m
}

func retry(ctx context.Context, f func() error, fatal error) error {
	backoff := 100 * time.Millisecond
	var err error
	for i := range commitAttempts {
		if err = f(); err == nil || errors.Is(err, fatal) || ctx.Err() != nil {
			return err
		}
		if i == commitAttempts-1 {
			break
		}
		t := time.NewTimer(backoff)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		}
		backoff *= 2
	}
	return err
}

// changedRaw returns the schemas that differ from those in known, and
// records them there; nil if none do.
func changedRaw(known map[string]*arrow.Schema, schemas map[string][]byte) (map[string][]byte, error) {
	changed, err := changedSchemas(known, schemas)
	if err != nil || len(changed) == 0 {
		return nil, err
	}
	raw := make(map[string][]byte, len(changed))
	for _, s := range changed {
		raw[s.Name] = schemas[s.Name]
		known[s.Name] = s.Schema
	}
	return raw, nil
}

// applySchemas applies the schemas that differ from those in applied, if the
// writer has a schema, and records them there; refused ones aren't.
func applySchemas(ctx context.Context, w destination.Writer, applied map[string]*arrow.Schema, schemas map[string][]byte) (destination.SchemaApplied, error) {
	sa, ok := w.(destination.SchemaApplier)
	if !ok {
		return destination.SchemaApplied{}, nil
	}
	changed, err := changedSchemas(applied, schemas)
	if err != nil || len(changed) == 0 {
		return destination.SchemaApplied{}, err
	}
	a, err := sa.ApplySchema(ctx, changed)
	if err != nil {
		return a, fmt.Errorf("apply schemas: %w", err)
	}
	for _, s := range changed {
		if !slices.ContainsFunc(a.Refused, func(r destination.RefusedColumn) bool { return r.Stream == s.Name }) {
			applied[s.Name] = s.Schema
		}
	}
	return a, nil
}

// changedSchemas decodes the schemas that differ from those in known, sorted
// by stream.
func changedSchemas(known map[string]*arrow.Schema, schemas map[string][]byte) ([]destination.StreamSchema, error) {
	var changed []destination.StreamSchema
	for name, b := range schemas {
		if b == nil {
			continue
		}
		s, err := decodeSchema(b)
		if err != nil {
			return nil, fmt.Errorf("stream %s schema: %w", name, err)
		}
		if prev := known[name]; prev == nil || !sameSchema(prev, s) {
			changed = append(changed, destination.StreamSchema{Name: name, Schema: s})
		}
	}
	slices.SortFunc(changed, func(a, b destination.StreamSchema) int { return strings.Compare(a.Name, b.Name) })
	return changed, nil
}

func decodeSchema(b []byte) (*arrow.Schema, error) {
	r, err := ipc.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer r.Release()
	return r.Schema(), nil
}

// sameSchema compares fields and metadata; the engine encodes metadata in
// no particular order.
func sameSchema(a, b *arrow.Schema) bool {
	if !slices.EqualFunc(a.Fields(), b.Fields(), func(x, y arrow.Field) bool { return x.Equal(y) }) {
		return false
	}
	am, bm := a.Metadata(), b.Metadata()
	if am.Len() != bm.Len() {
		return false
	}
	for i, k := range am.Keys() {
		if v, ok := bm.GetValue(k); !ok || v != am.Values()[i] {
			return false
		}
	}
	return true
}

// syncSoFar is where the sync started and the rows it has committed, from the
// record an attempt restored. A record of the same sync from before records
// carried them leaves the start unknown.
func syncSoFar(restored *checkpoint.Record, sync string) (*checkpoint.RecordRef, map[string]uint64) {
	switch {
	case restored == nil:
		return &checkpoint.RecordRef{}, map[string]uint64{}
	case restored.Sync != sync:
		ref := restored.Ref()
		return &ref, map[string]uint64{}
	}
	rows := maps.Clone(restored.SyncRows)
	if rows == nil {
		rows = map[string]uint64{}
	}
	return restored.SyncStart, rows
}
