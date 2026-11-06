package runner

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	bridgev1 "github.com/openetl-io/openetl/gen/openetl/bridge/v1"
	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// Bridge serves openetl.bridge.v1 to the Flink jobs a runner deploys
// (internal/bridge).
type Bridge interface {
	// Register serves h to calls carrying token, until unregister.
	Register(token string, h bridgev1.BridgeServiceServer) (unregister func())
	// Address is where jobs reach the bridge, as host:port.
	Address() string
	// Fingerprint is the SHA-256 of its certificate, in lowercase hex.
	Fingerprint() string
}

const (
	defaultFlinkConnectTimeout = time.Minute
	// How long a job whose tasks restart may take to say hello again.
	flinkRestartTimeout = 5 * time.Minute
	// How long a new segment waits for a schema of every stream before the
	// job is deployed without the ones it doesn't have.
	flinkSchemaWait = 30 * time.Second
)

// errRestarting: the job's tasks are restarting, so the segment's epoch in
// flight is dropped, and the next segment starts where Flink restores.
var errRestarting = errors.New("runner: the Flink job is restarting")

// schemaChanged: a stream's batch has a schema other than the one the job
// was deployed with. The job is deployed again with it.
type schemaChanged struct {
	stream string
	schema []byte
}

func (s *schemaChanged) Error() string {
	return fmt.Sprintf("runner: stream %q's schema changed, so the Flink job is deployed again", s.stream)
}

// flinkAttempt is one attempt of a continuous pipeline with a Flink job
// (ADR-0022 §3). It deploys the job once, and runs the dataflow around it in
// segments: a new segment, under a new generation, starts after the job's
// tasks restart, and a new deploy after a stream's schema changes.
type flinkAttempt struct {
	bridgev1.UnimplementedBridgeServiceServer

	job   *Job
	log   *slog.Logger
	res   *Result
	token string

	// The attempt's own goroutine's.
	sess     *checkpoint.Session
	w        destination.Writer
	fjob     FlinkJob
	from     string
	jobDone  chan error
	held     *coordinatorState // what outlives a segment's coordinator
	stopped  bool
	segments int
	// Set by segments' loops, one at a time: the drain began, and the stop
	// it asked for, if the job was running.
	draining bool
	stopDir  string
	stopDone chan error

	// Enumerator messages, for the attempt between segments and a segment's
	// coordinator during one.
	coord chan coordMsg

	mu       sync.Mutex
	schemas  map[string][]byte // what Describe gives, by stream
	seg      *flinkSegment
	segReady chan struct{} // closed when seg changes
	doneCh   chan struct{} // closed when the attempt ends
	call     uint64        // the enumerator's current call
	calls    uint64
	ended    bool
}

type coordMsg struct {
	call  uint64
	req   *bridgev1.CoordinateRequest
	reply chan error
	// The call ended.
	ended bool
}

// coordinatorState is what the coordinator keeps across segments.
type coordinatorState struct {
	reported        map[string]*arrow.Schema
	reportedOutputs map[string]*arrow.Schema
	measures        map[string]metricOption
}

func runFlink(ctx context.Context, job Job) (res Result, err error) {
	log := job.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log = log.With("pipeline", job.Pipeline, "owner", job.Owner)
	if job.Progress != nil {
		live.Store(job.Progress, struct{}{})
		defer live.Delete(job.Progress)
	}
	token := make([]byte, 24)
	if _, err := rand.Read(token); err != nil {
		return res, err
	}
	a := &flinkAttempt{
		job: &job, log: log, res: &res, token: hex.EncodeToString(token),
		coord: make(chan coordMsg), schemas: map[string][]byte{}, segReady: make(chan struct{}), doneCh: make(chan struct{}),
		held: &coordinatorState{reported: map[string]*arrow.Schema{}, reportedOutputs: map[string]*arrow.Schema{}, measures: map[string]metricOption{}},
	}
	unregister := job.Bridge.Register(a.token, a)
	defer unregister()
	defer a.end()
	defer func() {
		if a.w != nil {
			if cerr := a.w.Close(context.WithoutCancel(ctx)); err == nil && cerr != nil {
				err = fmt.Errorf("runner: close destination: %w", cerr)
			}
		}
	}()
	defer func() {
		// A job left running fails on its own once the bridge is gone; the
		// next attempt cancels it anyway.
		if a.fjob != nil && !a.stopped {
			if cerr := a.fjob.Cancel(context.WithoutCancel(ctx)); cerr != nil {
				log.Warn("cancelling the Flink job failed", "job", a.fjob.ID(), "err", cerr)
			}
		}
	}()
	defer func() {
		if err == nil || errors.Is(err, ErrDrained) || errors.Is(err, checkpoint.ErrFenced) || errors.Is(err, destination.ErrFenced) || a.sess == nil {
			return
		}
		if ferr := a.sess.Check(context.WithoutCancel(ctx)); errors.Is(ferr, checkpoint.ErrFenced) {
			err = fmt.Errorf("%w; the attempt failed with: %w", ferr, err)
		}
	}()
	return res, a.run(ctx)
}

func (a *flinkAttempt) run(ctx context.Context) error {
	var hello *coordMsg
	var prevSeg *flinkSegment
	for {
		var seg *flinkSegment
		var err error
		if hello == nil {
			seg, err = a.deploy(ctx)
		} else {
			seg, err = a.restart(ctx, prevSeg, hello)
		}
		if errors.Is(err, errDeployAgain) {
			hello, prevSeg = nil, nil
			continue
		}
		if err != nil {
			return err
		}
		err = seg.run(ctx)
		a.setSegment(nil)
		var changed *schemaChanged
		switch {
		case errors.Is(err, errRestarting):
			if hello, err = a.awaitHello(ctx, seg); err != nil {
				return err
			}
			prevSeg = seg
		case errors.As(err, &changed):
			a.log.Info("a stream's schema changed; deploying the Flink job again", "stream", changed.stream)
			a.mu.Lock()
			a.schemas[changed.stream] = changed.schema
			a.mu.Unlock()
			hello, prevSeg = nil, nil
			if err := a.fjob.Cancel(ctx); err != nil {
				return fmt.Errorf("runner: cancel the Flink job: %w", err)
			}
		default:
			return err
		}
	}
}

// claim claims the pipeline under a new generation, and opens the
// destination for it. The writer before is closed.
func (a *flinkAttempt) claim(ctx context.Context) (head, prev *checkpoint.Record, err error) {
	if a.w != nil {
		if err := a.w.Close(ctx); err != nil {
			a.log.Warn("closing the destination failed", "err", err)
		}
		a.w = nil
	}
	sess, err := a.job.Log.Claim(ctx, a.job.Owner)
	if err != nil {
		return nil, nil, err
	}
	a.sess = sess
	a.res.Generation = sess.Generation()
	if head, err = sess.Load(ctx); err != nil {
		return nil, nil, err
	}
	if head != nil && head.Previous != nil {
		if prev, err = a.job.Log.Record(ctx, *head.Previous); err != nil {
			return nil, nil, err
		}
	}
	forced := make(map[string]bool)
	for _, r := range []*checkpoint.Record{head, prev} {
		if r != nil {
			for _, s := range r.Forced {
				forced[s] = true
			}
		}
	}
	if a.w, err = a.job.Destination.Open(ctx, a.job.openConfig(sess.Generation(), forced, 0)); err != nil {
		return nil, nil, fmt.Errorf("runner: open destination: %w", err)
	}
	return head, prev, nil
}

// deploy cancels the pipeline's jobs, settles HEAD's epoch and chooses where
// Flink restores from, starts a segment there, and deploys the job once the
// segment knows every stream's schema.
func (a *flinkAttempt) deploy(ctx context.Context) (*flinkSegment, error) {
	// A zombie must not take the pipeline back from the attempt that
	// replaced it.
	if a.sess != nil {
		if err := a.sess.Check(ctx); err != nil {
			return nil, err
		}
	}
	if err := a.job.Flink.CancelAll(ctx); err != nil {
		return nil, fmt.Errorf("runner: cancel the pipeline's Flink jobs: %w", err)
	}
	head, prev, err := a.claim(ctx)
	if err != nil {
		return nil, err
	}
	restored, from, err := a.settle(ctx, head, prev)
	if err != nil {
		return nil, err
	}
	a.from = from
	a.mu.Lock()
	for _, s := range a.job.Streams {
		if _, ok := a.schemas[s.Name]; !ok && restored != nil && restored.Streams[s.Name].Schema != nil {
			a.schemas[s.Name] = restored.Streams[s.Name].Schema
		}
	}
	a.mu.Unlock()
	seg, err := a.start(ctx, restored, true)
	if err != nil {
		return nil, err
	}
	select {
	case <-seg.learned:
	case <-time.After(flinkSchemaWait):
		a.log.Warn("deploying the Flink job without a schema for every stream; it's deployed again once they come", "streams", seg.unlearned())
	case <-seg.ended:
		return seg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	a.mu.Lock()
	for name, schema := range seg.learnedSchemas() {
		if _, ok := a.schemas[name]; !ok {
			a.schemas[name] = schema
		}
	}
	a.mu.Unlock()
	fjob, err := a.job.Flink.Deploy(ctx, FlinkDeploy{
		Generation:         a.sess.Generation(),
		RestoreFrom:        from,
		Bridge:             a.job.Bridge.Address(),
		Fingerprint:        a.job.Bridge.Fingerprint(),
		Token:              a.token,
		CheckpointInterval: a.job.Settings.CommitInterval,
	})
	if err != nil {
		seg.stop(err)
		return nil, fmt.Errorf("runner: deploy the Flink job: %w", err)
	}
	a.fjob, a.stopped = fjob, false
	// The job before's outcome mustn't reach this one's channel.
	done := make(chan error, 1)
	a.jobDone = done
	go func() { done <- fjob.Wait(context.WithoutCancel(ctx)) }()
	a.log.Info("deployed the Flink job", "job", fjob.ID(), "from", from, "ui", fjob.UI())
	seg.deployed(fjob.ID())
	return seg, nil
}

// settle decides HEAD's epoch, before a deploy: the record the source reads
// from, and the checkpoint or savepoint Flink restores from ("" for none).
func (a *flinkAttempt) settle(ctx context.Context, head, prev *checkpoint.Record) (*checkpoint.Record, string, error) {
	if head == nil || head.Flink == nil {
		restored, err := a.recover(ctx, head, prev, true)
		return restored, "", err
	}
	hf := head.Flink
	completed, at, err := a.completed(ctx, hf)
	if err != nil {
		return nil, "", err
	}
	if completed {
		restored, err := a.recover(ctx, head, prev, true)
		if err != nil || restored == head {
			return restored, at, err
		}
		before, err := a.before(ctx, hf, prev)
		return restored, before, err
	}
	// The checkpoint didn't complete, so the epoch is lost, and its record
	// moves back one before the destination sees it.
	before, err := a.before(ctx, hf, prev)
	if err != nil {
		return nil, "", err
	}
	if err := a.rewind(ctx, prev); err != nil {
		return nil, "", err
	}
	return prev, before, nil
}

// completed says whether hf's checkpoint or savepoint completed, and where
// Flink restores from if it did: the newest checkpoint of its job, which is
// hf's or one after it that held no rows.
func (a *flinkAttempt) completed(ctx context.Context, hf *checkpoint.FlinkCheckpoint) (bool, string, error) {
	if hf.Savepoint != "" {
		p, err := a.job.Flink.Savepoint(ctx, hf.Savepoint)
		if err != nil {
			return false, "", fmt.Errorf("runner: find the savepoint below %s: %w", hf.Savepoint, err)
		}
		return p != "", p, nil
	}
	cs, err := a.job.Flink.Completed(ctx, hf.Job)
	if err != nil {
		return false, "", fmt.Errorf("runner: list the Flink job's checkpoints: %w", err)
	}
	if len(cs) == 0 || cs[len(cs)-1].ID < hf.Checkpoint {
		return false, "", nil
	}
	return true, cs[len(cs)-1].Path, nil
}

// before is where Flink restores from when hf's epoch is lost: the newest
// checkpoint of hf's job before hf's, or else where the job was deployed
// from. Either is the record before's checkpoint or one after it that held
// no rows.
func (a *flinkAttempt) before(ctx context.Context, hf *checkpoint.FlinkCheckpoint, prev *checkpoint.Record) (string, error) {
	cs, err := a.job.Flink.Completed(ctx, hf.Job)
	if err != nil {
		return "", fmt.Errorf("runner: list the Flink job's checkpoints: %w", err)
	}
	for _, c := range slices.Backward(cs) {
		if c.ID < hf.Checkpoint {
			if prev != nil && prev.Flink != nil && prev.Flink.Job == hf.Job && c.ID < prev.Flink.Checkpoint {
				break
			}
			return c.Path, nil
		}
	}
	switch {
	case prev != nil && prev.Flink != nil && prev.Flink.Job == hf.Job:
	case hf.From != "":
		return hf.From, nil
	case prev == nil || prev.Flink == nil:
		return "", nil
	}
	return "", errs.New(errs.PositionLost, "runner: Flink no longer retains the checkpoint of epoch %d or of the one before it, so the job can't restore where the pipeline's records are; restoring the job by hand, or losing its checkpoint storage, does this, and only a new snapshot recovers", prev.Epoch+1)
}

// recover recovers HEAD's epoch as a batch attempt does, rewinding to prev
// if it's lost.
func (a *flinkAttempt) recover(ctx context.Context, head, prev *checkpoint.Record, hand bool) (*checkpoint.Record, error) {
	var e destination.Epoch
	var staged []destination.Staged
	if head != nil {
		e, staged = destination.Epoch{Generation: head.Generation, N: head.Epoch}, committables(head)
		if _, err := applySchemas(ctx, a.w, map[string]*arrow.Schema{}, outputSchemas(head)); err != nil {
			return nil, err
		}
		if hand && a.job.staged() {
			if err := handOver(ctx, a.job.Staging, a.w, e, stagedFiles(head.Files)); err != nil {
				return nil, err
			}
		}
	}
	rec, err := a.w.Recover(ctx, a.sess.Generation(), e, staged)
	if err != nil {
		return nil, fmt.Errorf("runner: recover epoch %d: %w", e.N, err)
	}
	a.res.Recovery = rec
	if head != nil && a.job.staged() {
		flush(ctx, a.job.Staging, e, a.log)
	}
	if rec != destination.Lost {
		return head, nil
	}
	if err := a.rewind(ctx, prev); err != nil {
		return nil, err
	}
	return prev, nil
}

// rewind moves HEAD back to prev, whose epoch was committed before HEAD's
// record was written, and recovers it, which raises the destination's
// fence and lets it drop what HEAD's epoch staged.
func (a *flinkAttempt) rewind(ctx context.Context, prev *checkpoint.Record) error {
	var to *checkpoint.RecordRef
	if prev != nil {
		ref := prev.Ref()
		to = &ref
	}
	if err := a.sess.Rewind(ctx, to); err != nil {
		return err
	}
	restored, err := a.recover(ctx, prev, nil, false)
	if err != nil {
		return err
	}
	if restored != prev {
		return fmt.Errorf("runner: epoch %d was committed before the next one's record, and the destination now reports it lost", prev.Epoch)
	}
	return nil
}

// awaitHello waits for the enumerator of the job's restarted tasks.
func (a *flinkAttempt) awaitHello(ctx context.Context, seg *flinkSegment) (*coordMsg, error) {
	if m := seg.hello; m != nil {
		return m, nil
	}
	timeout := time.NewTimer(flinkRestartTimeout)
	defer timeout.Stop()
	for {
		select {
		case m := <-a.coord:
			if m.ended || m.call != a.currentCall() {
				if m.reply != nil {
					m.reply <- errors.New("runner: a call of an enumerator that's gone")
				}
				continue
			}
			if m.req.GetHello() != nil {
				return &m, nil
			}
			m.reply <- errors.New("runner: the enumerator sent a checkpoint before hello")
		case err := <-a.jobDone:
			a.jobDone <- err
			return nil, a.jobEnded(err)
		case <-timeout.C:
			return nil, errs.New(errs.Transient, "runner: the Flink job's tasks restarted, and its enumerator didn't connect again within %s", flinkRestartTimeout)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (a *flinkAttempt) jobEnded(err error) error {
	var failed *FlinkFailed
	switch {
	case errors.As(err, &failed):
		return err
	case err != nil:
		return errs.Wrap(errs.Transient, err, "runner: watch the Flink job")
	default:
		return errs.New(errs.Transient, "runner: the Flink job %s finished, which only a stop the runner asked for should do", a.fjob.ID())
	}
}

// restart starts a segment after the job's tasks restarted from checkpoint
// R, which hello carries. HEAD's epoch is committed first if R holds it,
// under the generation that recorded it, since a new one could lose it.
// Otherwise it's lost, and its record moves back one.
func (a *flinkAttempt) restart(ctx context.Context, prev *flinkSegment, hello *coordMsg) (*flinkSegment, error) {
	h := hello.req.GetHello()
	if h.GetJobId() != a.fjob.ID() {
		hello.reply <- errors.New("runner: another job's enumerator")
		return nil, errs.New(errs.Internal, "runner: the enumerator of Flink job %s connected, and this attempt deployed %s", h.GetJobId(), a.fjob.ID())
	}
	if err := a.sess.Check(ctx); err != nil {
		hello.reply <- err
		return nil, err
	}
	r := h.GetRestoredCheckpoint()
	head, before := prev.head, prev.headPrev
	var restored *checkpoint.Record
	switch {
	case head == nil || head.Flink == nil || head.Flink.Job != a.fjob.ID():
		restored = head
	case r >= head.Flink.Checkpoint:
		if err := prev.commitHead(ctx); err != nil {
			return nil, err
		}
		restored = head
	case !prev.headCompleted && (before == nil || before.Flink == nil || before.Flink.Job != a.fjob.ID() || r >= before.Flink.Checkpoint):
		restored = before
	default:
		// Only a JobManager that restarts without high availability goes
		// back past a checkpoint that completed, as HEAD's did, or past the
		// one before it. A deploy settles HEAD afresh, from the checkpoints
		// Flink still retains.
		hello.reply <- errors.New("runner: the job restored a checkpoint the pipeline's records don't stand at")
		a.log.Warn("the Flink job restored a checkpoint older than the pipeline's records; deploying it again", "restored", r, "head", head.Flink.Checkpoint)
		return nil, errDeployAgain
	}
	cur, _, err := a.claim(ctx)
	if err != nil {
		return nil, err
	}
	if !sameRecord(cur, head) {
		// Another attempt claimed the pipeline and wrote to it since, so
		// what the job restored says nothing about HEAD.
		hello.reply <- errors.New("runner: the pipeline moved on")
		a.log.Info("another attempt moved HEAD while the Flink job restarted; deploying the job again")
		return nil, errDeployAgain
	}
	if restored == head {
		_, err = a.recover(ctx, head, nil, true)
	} else {
		err = a.rewind(ctx, restored)
	}
	if err != nil {
		return nil, err
	}
	a.log.Info("the Flink job's tasks restarted", "restored", r, "epoch", epochOf(restored), "generation", a.sess.Generation())
	seg, err := a.start(ctx, restored, false)
	if err != nil {
		return nil, err
	}
	seg.deployed(a.fjob.ID())
	hello.reply <- nil
	return seg, nil
}

// errDeployAgain: what the restarted tasks restored can't be settled in
// place, because another attempt moved HEAD, or because the job went back
// past HEAD's checkpoint. The attempt settles HEAD afresh and deploys again.
var errDeployAgain = errors.New("runner: the Flink job is deployed again")

func sameRecord(a, b *checkpoint.Record) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Ref() == b.Ref()
}

func epochOf(r *checkpoint.Record) uint64 {
	if r == nil {
		return 0
	}
	return r.Epoch
}

// start starts a segment from restored, under the current generation.
func (a *flinkAttempt) start(ctx context.Context, restored *checkpoint.Record, learn bool) (*flinkSegment, error) {
	if restored != nil {
		a.res.Restored = restored.Epoch
	}
	a.res.Last = epochOf(restored)
	prune(ctx, a.sess, restored, a.job.keep(), a.job.Staging, false, a.log)
	a.pruneFlink(ctx, restored)
	a.segments++
	seg := newFlinkSegment(a, restored, learn)
	if err := seg.begin(ctx); err != nil {
		return nil, err
	}
	a.setSegment(seg)
	return seg, nil
}

// pruneFlink deletes the checkpoints and savepoints of jobs no kept record
// names, keeping the deployed job's and where it was deployed from.
func (a *flinkAttempt) pruneFlink(ctx context.Context, head *checkpoint.Record) {
	pr, ok := a.job.Flink.(interface {
		Prune(ctx context.Context, keep []string) error
	})
	if !ok {
		return
	}
	keep := map[string]bool{}
	add := func(id, from, savepoint string) {
		if id != "" {
			keep[id] = true
		}
		if savepoint != "" {
			keep[savepoint] = true
		}
		if from != "" {
			// {dir}/{job}/chk-N, or a savepoint's directory below the one
			// it was taken into. These are URIs, which path.Dir would
			// clean.
			dir, base := splitURI(from)
			if strings.HasPrefix(base, "chk-") {
				_, job := splitURI(dir)
				keep[job] = true
			} else {
				keep[dir] = true
			}
		}
	}
	if a.fjob != nil {
		add(a.fjob.ID(), a.from, "")
	} else {
		add("", a.from, "")
	}
	rec := head
	for range a.job.keep() {
		if rec == nil {
			break
		}
		if rec.Flink != nil {
			add(rec.Flink.Job, rec.Flink.From, rec.Flink.Savepoint)
		}
		if rec.Previous == nil {
			break
		}
		next, err := a.job.Log.Record(ctx, *rec.Previous)
		if err != nil {
			// A record pruned already names nothing to keep.
			break
		}
		rec = next
	}
	ids := make([]string, 0, len(keep))
	for k := range keep {
		ids = append(ids, k)
	}
	slices.Sort(ids)
	if err := pr.Prune(ctx, ids); err != nil {
		a.log.Warn("pruning the Flink job's checkpoints failed", "err", err)
	}
}

// splitURI splits a URI at its last slash, ignoring a trailing one.
func splitURI(uri string) (dir, base string) {
	uri = strings.TrimSuffix(uri, "/")
	i := strings.LastIndex(uri, "/")
	if i < 0 {
		return "", uri
	}
	return uri[:i], uri[i+1:]
}

func (a *flinkAttempt) setSegment(seg *flinkSegment) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.seg = seg
	close(a.segReady)
	a.segReady = make(chan struct{})
}

// end makes calls waiting for a segment give up.
func (a *flinkAttempt) end() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.ended = true
	close(a.doneCh)
	a.seg = nil
	close(a.segReady)
	a.segReady = make(chan struct{})
}

// segment waits for a segment that takes new calls.
func (a *flinkAttempt) segment(ctx context.Context) (*flinkSegment, error) {
	for {
		a.mu.Lock()
		seg, ready, ended := a.seg, a.segReady, a.ended
		a.mu.Unlock()
		if ended {
			return nil, status.Error(codes.Unavailable, "runner: the attempt ended")
		}
		if seg != nil && seg.open() {
			return seg, nil
		}
		select {
		case <-ready:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (a *flinkAttempt) currentCall() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.call
}

// Describe gives the streams' schemas the job is deployed with, and the
// outputs'.
func (a *flinkAttempt) Describe(context.Context, *bridgev1.DescribeRequest) (*bridgev1.DescribeResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	res := &bridgev1.DescribeResponse{Pipeline: a.job.Pipeline}
	changelog := false
	for _, s := range a.job.Streams {
		res.Streams = append(res.Streams, &bridgev1.Schema{Name: s.Name, Arrow: a.schemas[s.Name], Changelog: s.Read == source.CDC, PrimaryKey: s.PrimaryKey})
		changelog = changelog || s.Read == source.CDC
	}
	// Over a changelog, outputs keep each row's op, appended ones too, as
	// they would without the transform.
	for _, o := range a.job.Outputs {
		res.Outputs = append(res.Outputs, &bridgev1.Schema{Name: o.Name, PrimaryKey: o.PrimaryKey, Changelog: changelog})
	}
	return res, nil
}

// Coordinate hands the enumerator's messages to the attempt, or to its
// segment's coordinator, and acks each once it's taken.
func (a *flinkAttempt) Coordinate(st grpc.BidiStreamingServer[bridgev1.CoordinateRequest, bridgev1.CoordinateResponse]) error {
	ctx := st.Context()
	a.mu.Lock()
	a.calls++
	call := a.calls
	a.mu.Unlock()
	deliver := func(m coordMsg) bool {
		select {
		case a.coord <- m:
			return true
		case <-ctx.Done():
			return false
		}
	}
	for {
		req, err := st.Recv()
		if err != nil {
			go func() {
				select {
				case a.coord <- coordMsg{call: call, ended: true}:
				case <-a.doneCh:
				}
			}()
			return err
		}
		if req.GetHello() != nil {
			a.mu.Lock()
			a.call = call
			a.mu.Unlock()
		}
		m := coordMsg{call: call, req: req, reply: make(chan error, 1)}
		if !deliver(m) {
			return ctx.Err()
		}
		select {
		case err := <-m.reply:
			if err != nil {
				return status.Error(codes.Aborted, err.Error())
			}
		case <-ctx.Done():
			return ctx.Err()
		}
		var id int64
		switch {
		case req.GetStarted() != nil:
			id = req.GetStarted().GetCheckpoint()
		case req.GetOutcome() != nil:
			id = req.GetOutcome().GetCheckpoint()
		}
		if err := st.Send(&bridgev1.CoordinateResponse{Ack: &bridgev1.Ack{Checkpoint: id}}); err != nil {
			return err
		}
	}
}

// Read serves a split from the segment that takes it.
func (a *flinkAttempt) Read(req *bridgev1.ReadRequest, st grpc.ServerStreamingServer[bridgev1.ReadResponse]) error {
	if !slices.ContainsFunc(a.job.Streams, func(s Stream) bool { return s.Name == req.GetStream() }) {
		return status.Errorf(codes.NotFound, "runner: the pipeline has no stream %q", req.GetStream())
	}
	for {
		seg, err := a.segment(st.Context())
		if err != nil {
			return err
		}
		if err := seg.serveRead(st, req); !errors.Is(err, errTaken) {
			return err
		}
	}
}

// Write takes a sink subtask's batches and barriers into the segment that
// takes it.
func (a *flinkAttempt) Write(st grpc.BidiStreamingServer[bridgev1.WriteRequest, bridgev1.WriteResponse]) error {
	req, err := st.Recv()
	if err != nil {
		return err
	}
	h := req.GetHello()
	if h == nil {
		return status.Error(codes.InvalidArgument, "runner: a sink's first message must be hello")
	}
	if !slices.ContainsFunc(a.job.Outputs, func(o Stream) bool { return o.Name == h.GetOutput() }) {
		return status.Errorf(codes.NotFound, "runner: the pipeline has no output %q", h.GetOutput())
	}
	if h.GetParallelism() <= 0 || h.GetSubtask() < 0 || h.GetSubtask() >= h.GetParallelism() {
		return status.Errorf(codes.InvalidArgument, "runner: subtask %d of %d", h.GetSubtask(), h.GetParallelism())
	}
	for {
		seg, err := a.segment(st.Context())
		if err != nil {
			return err
		}
		if err := seg.serveWrite(st, h); !errors.Is(err, errTaken) {
			return err
		}
	}
}

// flinkSavepointName names a drain's savepoint.
func flinkSavepointName(gen uint64) string {
	return "g" + strconv.FormatUint(gen, 10) + "-" + time.Now().UTC().Format("20060102T150405Z")
}
