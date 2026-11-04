// Package flinktest is a fake Flink cluster and job for the runner's tests
// and the simulation. Its job plays openetl-flink's side of the bridge in
// process: an enumerator, a reader per stream and a sink per output, with
// each stream passed through to the output of the same name. Checkpoints are
// induced at the runner's barriers, as openetl-flink's are.
package flinktest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	bridgev1 "github.com/unsuitable001/openetl/gen/openetl/bridge/v1"
	"github.com/unsuitable001/openetl/internal/runner"
)

// Bridge is an in-process runner.Bridge.
type Bridge struct {
	mu       sync.Mutex
	handlers map[string]bridgev1.BridgeServiceServer
}

func NewBridge() *Bridge { return &Bridge{handlers: map[string]bridgev1.BridgeServiceServer{}} }

func (b *Bridge) Register(token string, h bridgev1.BridgeServiceServer) func() {
	b.mu.Lock()
	b.handlers[token] = h
	b.mu.Unlock()
	return func() {
		b.mu.Lock()
		delete(b.handlers, token)
		b.mu.Unlock()
	}
}

func (b *Bridge) Address() string     { return "inprocess:0" }
func (b *Bridge) Fingerprint() string { return "fake" }

func (b *Bridge) handler(token string) (bridgev1.BridgeServiceServer, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	h := b.handlers[token]
	if h == nil {
		return nil, errors.New("flinktest: unknown token")
	}
	return h, nil
}

// Faults says what the cluster does wrong, by checkpoint. Each is consulted
// once per checkpoint of a job's run, before the checkpoint starts.
type Faults struct {
	// The checkpoint is aborted once every sink's barrier is acked, so after
	// its record, and the job's tasks restart.
	DeclineAfterRecord func(c int64) bool
	// The job's tasks fail partway through the checkpoint's epoch, and
	// restart from the newest completed checkpoint.
	FailTasks func(c int64) bool
	// A savepoint taken by hand, in place of the checkpoint.
	SavepointByHand func(c int64) bool
	// A stop aborts the checkpoint in flight, as Flink's stop-with-savepoint
	// does: half the sinks are inside it, and the other readers pass over
	// its barrier.
	AbortOnStop bool
	// Consulted when the job's tasks restart: the JobManager restarts
	// without high availability, so they restore from the checkpoint the
	// job was deployed from, not the newest.
	NoHA func() bool
}

// Cluster is a fake runner.Flink.
type Cluster struct {
	Bridge *Bridge
	// How often jobs checkpoint; zero: the deploy's interval.
	Interval time.Duration
	Faults   Faults
	// Logf traces what the cluster does; nil: nothing.
	Logf func(format string, args ...any)
	// Point, if set, is called at each step of a job's tasks, with the run
	// of tasks it's in, numbered from 1 across the cluster. It applies the
	// step, or fails the run with an error.
	Point func(ctx context.Context, run int, op string, apply func() error) error
	// A job whose tasks restart this many times without completing a
	// checkpoint fails, as a bounded restart strategy makes it. Zero: never.
	MaxRestarts int
	// After a task failure, the job fails once more as soon as its tasks
	// have connected, and restores from the same checkpoint again, as
	// openetl-flink's enumerator makes it.
	FailOnceMore bool

	mu         sync.Mutex
	jobs       map[string]*Job
	completed  map[string][]runner.FlinkCheckpoint
	savepoints map[string]savepoint
	deploys    int
	runs       int
	// Every checkpoint ever completed, and every savepoint taken by hand,
	// by job.
	everCompleted map[string]map[int64]bool
	byHand        map[string]map[int64]bool
}

type savepoint struct {
	path string
	id   int64
}

func NewCluster(b *Bridge) *Cluster {
	return &Cluster{Bridge: b, jobs: map[string]*Job{}, completed: map[string][]runner.FlinkCheckpoint{}, savepoints: map[string]savepoint{},
		everCompleted: map[string]map[int64]bool{}, byHand: map[string]map[int64]bool{}}
}

// EverCompleted says whether job's checkpoint id completed.
func (c *Cluster) EverCompleted(job string, id int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.everCompleted[job][id]
}

// TakenByHand says whether job's checkpoint id was a savepoint taken by
// hand.
func (c *Cluster) TakenByHand(job string, id int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.byHand[job][id]
}

// Running counts the jobs not ended.
func (c *Cluster) Running() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.jobs)
}

func mark(m map[string]map[int64]bool, job string, id int64) {
	if m[job] == nil {
		m[job] = map[int64]bool{}
	}
	m[job][id] = true
}

func (c *Cluster) logf(format string, args ...any) {
	if c.Logf != nil {
		c.Logf(format, args...)
	}
}

// Deploys counts the jobs deployed.
func (c *Cluster) Deploys() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.deploys
}

func (c *Cluster) CancelAll(ctx context.Context) error {
	c.mu.Lock()
	jobs := make([]*Job, 0, len(c.jobs))
	for _, j := range c.jobs {
		jobs = append(jobs, j)
	}
	c.mu.Unlock()
	for _, j := range jobs {
		if err := j.Cancel(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (c *Cluster) Completed(_ context.Context, job string) ([]runner.FlinkCheckpoint, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.completed[job]), nil
}

func (c *Cluster) SavepointDir(name string) string { return "fake://savepoints/" + name }

func (c *Cluster) Savepoint(_ context.Context, dir string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.savepoints[dir].path, nil
}

func (c *Cluster) Prune(context.Context, []string) error { return nil }

// restoredID is the checkpoint a path holds.
func (c *Cluster) restoredID(path string) (int64, error) {
	if path == "" {
		return 0, nil
	}
	c.mu.Lock()
	for _, sp := range c.savepoints {
		if sp.path == path {
			c.mu.Unlock()
			return sp.id, nil
		}
	}
	c.mu.Unlock()
	i := strings.LastIndex(path, "/chk-")
	if i < 0 {
		return 0, fmt.Errorf("flinktest: %q is no checkpoint", path)
	}
	return strconv.ParseInt(path[i+len("/chk-"):], 10, 64)
}

func (c *Cluster) Deploy(_ context.Context, d runner.FlinkDeploy) (runner.FlinkJob, error) {
	r, err := c.restoredID(d.RestoreFrom)
	if err != nil {
		return nil, err
	}
	h, err := c.Bridge.handler(d.Token)
	if err != nil {
		return nil, err
	}
	interval := c.Interval
	if interval == 0 {
		interval = d.CheckpointInterval
	}
	j := &Job{cluster: c, id: fmt.Sprintf("job-g%d", d.Generation), h: h, interval: interval, from: r, next: r + 1, done: make(chan struct{})}
	c.mu.Lock()
	c.jobs[j.id] = j
	c.deploys++
	c.mu.Unlock()
	c.logf("flink: deploy %s from %q (checkpoint %d)", j.id, d.RestoreFrom, r)
	j.start(r, false)
	return j, nil
}

func (c *Cluster) complete(job string, id int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cs := append(c.completed[job], runner.FlinkCheckpoint{ID: id, Path: fmt.Sprintf("fake://%s/chk-%d", job, id)})
	if len(cs) > 2 {
		cs = cs[len(cs)-2:]
	}
	c.completed[job] = cs
	mark(c.everCompleted, job, id)
}

// Job is a deployed fake job. Its tasks run as a series of runs; a failure
// ends a run and starts the next from the newest completed checkpoint.
type Job struct {
	cluster  *Cluster
	id       string
	h        bridgev1.BridgeServiceServer
	interval time.Duration
	from     int64 // the checkpoint it was deployed from

	mu             sync.Mutex
	next           int64 // the next checkpoint ID
	run            *taskRun
	stopping       string
	abortedForStop bool
	stopErr        chan error
	restarts       int // since the last completed checkpoint
	ended          bool
	result         error
	done           chan struct{}
}

func (j *Job) ID() string { return j.id }
func (j *Job) UI() string { return "" }

func (j *Job) Wait(ctx context.Context) error {
	select {
	case <-j.done:
		return j.result
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (j *Job) Cancel(context.Context) error {
	j.finish(errors.New("flinktest: cancelled"))
	return nil
}

// Stop takes a synchronous savepoint into dir at the next checkpoint, and
// finishes the job once it completes.
func (j *Job) Stop(ctx context.Context, dir string) error {
	j.mu.Lock()
	if j.ended {
		j.mu.Unlock()
		return errors.New("flinktest: the job ended")
	}
	j.stopping = dir
	stopErr := make(chan error, 1)
	j.stopErr = stopErr
	j.mu.Unlock()
	select {
	case err := <-stopErr:
		return err
	case <-j.done:
		select {
		case err := <-stopErr:
			return err
		default:
			return errors.New("flinktest: the job ended before its stop's savepoint")
		}
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (j *Job) finish(result error) {
	j.mu.Lock()
	if j.ended {
		j.mu.Unlock()
		return
	}
	j.ended, j.result = true, result
	run := j.run
	j.mu.Unlock()
	if run != nil {
		run.cancel()
		run.wg.Wait()
	}
	j.cluster.mu.Lock()
	delete(j.cluster.jobs, j.id)
	j.cluster.mu.Unlock()
	close(j.done)
}

// start starts a run of the job's tasks, restored from checkpoint r.
func (j *Job) start(r int64, failOnce bool) {
	ctx, cancel := context.WithCancel(context.Background())
	j.cluster.mu.Lock()
	j.cluster.runs++
	id := j.cluster.runs
	j.cluster.mu.Unlock()
	run := &taskRun{job: j, id: id, restored: r, failOnce: failOnce, ctx: ctx, cancel: cancel}
	j.mu.Lock()
	if j.ended {
		j.mu.Unlock()
		cancel()
		return
	}
	run.wg.Add(1)
	j.run = run
	j.mu.Unlock()
	go func() {
		defer run.wg.Done()
		err := run.tasks()
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, errFinished) {
			go j.finish(nil)
			return
		}
		j.cluster.logf("flink: %s's tasks failed: %v", j.id, err)
		cancel()
		// A stop whose savepoint fails leaves the job running.
		j.mu.Lock()
		if j.stopping != "" {
			j.stopErr <- fmt.Errorf("flinktest: the stop's savepoint failed: %w", err)
			j.stopping, j.stopErr = "", nil
		}
		j.restarts++
		exhausted := j.cluster.MaxRestarts > 0 && j.restarts > j.cluster.MaxRestarts
		j.mu.Unlock()
		if exhausted {
			go j.finish(&runner.FlinkFailed{Message: "the restart strategy gave up: " + err.Error()})
			return
		}
		if run.failOnce {
			go j.start(run.restored, false)
			return
		}
		// Restart from the newest completed checkpoint, as Flink does with
		// high availability.
		r := run.restored
		if cs, _ := j.cluster.Completed(context.Background(), j.id); len(cs) > 0 {
			r = cs[len(cs)-1].ID
		}
		if f := j.cluster.Faults.NoHA; f != nil && f() {
			r = j.from
			j.cluster.logf("flink: %s's JobManager restarts without HA, from checkpoint %d", j.id, r)
		}
		go j.start(r, j.cluster.FailOnceMore)
	}()
}

var errFinished = errors.New("flinktest: finished")

// taskRun is one run of a job's tasks.
type taskRun struct {
	job      *Job
	id       int
	restored int64
	// The run fails once it has opened its calls (Cluster.FailOnceMore).
	failOnce bool
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// point runs a step of the tasks; nil apply is a step with no effect.
func (r *taskRun) point(op string, apply func() error) error {
	if apply == nil {
		apply = func() error { return nil }
	}
	if p := r.job.cluster.Point; p != nil {
		return p(r.ctx, r.id, op, apply)
	}
	return apply()
}

func (r *taskRun) tasks() error {
	ctx := r.ctx
	j := r.job
	desc, err := j.h.Describe(ctx, &bridgev1.DescribeRequest{})
	if err != nil {
		return err
	}
	if err := r.point("flink.hello", nil); err != nil {
		return err
	}
	coord := newBidi[bridgev1.CoordinateRequest, bridgev1.CoordinateResponse](ctx)
	go func() { coord.serverDone(j.h.Coordinate(coord)) }()
	if err := coord.call(&bridgev1.CoordinateRequest{Message: &bridgev1.CoordinateRequest_Hello{Hello: &bridgev1.EnumeratorHello{JobId: j.id, RestoredCheckpoint: r.restored}}}); err != nil {
		return fmt.Errorf("hello: %w", err)
	}
	sinks := map[string]*bidi[bridgev1.WriteRequest, bridgev1.WriteResponse]{}
	for _, o := range desc.GetOutputs() {
		w := newBidi[bridgev1.WriteRequest, bridgev1.WriteResponse](ctx)
		go func() { w.serverDone(j.h.Write(w)) }()
		if err := w.call(&bridgev1.WriteRequest{Message: &bridgev1.WriteRequest_Hello{Hello: &bridgev1.SinkHello{Output: o.GetName(), Parallelism: 1, RestoredCheckpoint: r.restored}}}); err != nil {
			return fmt.Errorf("sink %s: %w", o.GetName(), err)
		}
		sinks[o.GetName()] = w
	}
	// Each stream's reader forwards its batches to the output of its name,
	// and its barriers to the checkpoint loop.
	barriers := make(chan readerBarrier)
	errc := make(chan error, len(desc.GetStreams())+1)
	for _, s := range desc.GetStreams() {
		sink := sinks[s.GetName()]
		if sink == nil {
			return fmt.Errorf("flinktest: no output for stream %s", s.GetName())
		}
		rd := newServerStream[bridgev1.ReadResponse](ctx)
		go func() {
			rd.serverDone(j.h.Read(&bridgev1.ReadRequest{Stream: s.GetName(), RestoredCheckpoint: r.restored}, rd))
		}()
		go func() { errc <- r.read(rd, sink, barriers) }()
	}
	if r.failOnce {
		return r.point("flink.failOnceMore", func() error {
			return errors.New("flinktest: the enumerator fails the job once more after a restart")
		})
	}
	tick := time.NewTicker(j.interval)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
		case err := <-errc:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := r.checkpoint(coord, barriers, errc, len(desc.GetStreams())); err != nil {
			return err
		}
	}
}

type readerBarrier struct {
	c       int64
	release chan bridgev1.BarrierType
	acked   chan error
}

func (r *taskRun) read(rd *serverStream[bridgev1.ReadResponse], sink *bidi[bridgev1.WriteRequest, bridgev1.WriteResponse], barriers chan<- readerBarrier) error {
	for {
		res, err := rd.recv()
		if err != nil {
			return err
		}
		switch m := res.GetMessage().(type) {
		case *bridgev1.ReadResponse_Batch:
			if err := r.point("flink.batch", nil); err != nil {
				return err
			}
			if err := sink.send(&bridgev1.WriteRequest{Message: &bridgev1.WriteRequest_Batch{Batch: m.Batch}}); err != nil {
				return err
			}
		case *bridgev1.ReadResponse_Barrier:
			b := readerBarrier{c: m.Barrier.GetCheckpoint(), release: make(chan bridgev1.BarrierType, 1), acked: make(chan error, 1)}
			select {
			case barriers <- b:
			case <-r.ctx.Done():
				return r.ctx.Err()
			}
			var typ bridgev1.BarrierType
			select {
			case typ = <-b.release:
			case <-r.ctx.Done():
				return r.ctx.Err()
			}
			if typ == bridgev1.BarrierType_BARRIER_TYPE_UNSPECIFIED {
				// Passed over: its checkpoint was aborted.
				b.acked <- nil
				continue
			}
			if err := r.point("flink.barrier", nil); err != nil {
				b.acked <- err
				return err
			}
			b.acked <- sink.call(&bridgev1.WriteRequest{Message: &bridgev1.WriteRequest_Barrier{Barrier: &bridgev1.SinkBarrier{Checkpoint: b.c, Type: typ}}})
		}
	}
}

// checkpoint runs one checkpoint: it starts it on the enumerator, waits for
// every reader to reach its barrier, sends the sinks' barriers, and reports
// the outcome.
func (r *taskRun) checkpoint(coord *bidi[bridgev1.CoordinateRequest, bridgev1.CoordinateResponse], barriers <-chan readerBarrier, errc <-chan error, streams int) error {
	j := r.job
	f := j.cluster.Faults
	j.mu.Lock()
	c := j.next
	j.next++
	stopDir, stopErr := j.stopping, j.stopErr
	abortForStop := stopDir != "" && f.AbortOnStop && !j.abortedForStop
	j.abortedForStop = j.abortedForStop || abortForStop
	j.mu.Unlock()
	typ := bridgev1.BarrierType_BARRIER_TYPE_CHECKPOINT
	switch {
	case abortForStop:
	case stopDir != "":
		typ = bridgev1.BarrierType_BARRIER_TYPE_SYNC_SAVEPOINT
	case f.SavepointByHand != nil && f.SavepointByHand(c):
		typ = bridgev1.BarrierType_BARRIER_TYPE_SAVEPOINT
	}
	// A stop's savepoint is never faulted: a failed one fails the stop.
	stop := typ == bridgev1.BarrierType_BARRIER_TYPE_SYNC_SAVEPOINT
	failTasks := !stop && f.FailTasks != nil && f.FailTasks(c)
	decline := !stop && f.DeclineAfterRecord != nil && f.DeclineAfterRecord(c)
	if err := r.point("flink.start", nil); err != nil {
		return err
	}
	if err := coord.call(&bridgev1.CoordinateRequest{Message: &bridgev1.CoordinateRequest_Started{Started: &bridgev1.CheckpointStarted{Checkpoint: c}}}); err != nil {
		return fmt.Errorf("start checkpoint %d: %w", c, err)
	}
	if failTasks {
		return fmt.Errorf("flinktest: a TaskManager failed in checkpoint %d", c)
	}
	var held []readerBarrier
	for len(held) < streams {
		select {
		case b := <-barriers:
			if b.c != c {
				return fmt.Errorf("flinktest: a reader's barrier for checkpoint %d in checkpoint %d", b.c, c)
			}
			held = append(held, b)
		case err := <-errc:
			return err
		case <-r.ctx.Done():
			return r.ctx.Err()
		}
	}
	if abortForStop {
		return r.abortForStop(coord, c, held, errc)
	}
	for _, b := range held {
		b.release <- typ
	}
	for _, b := range held {
		var err error
		select {
		case err = <-b.acked:
		case err = <-errc:
		case <-r.ctx.Done():
			return r.ctx.Err()
		}
		if err != nil {
			return fmt.Errorf("checkpoint %d: a sink's barrier was refused: %w", c, err)
		}
	}
	if decline {
		j.cluster.logf("flink: %s declines checkpoint %d after its record", j.id, c)
		_ = coord.call(&bridgev1.CoordinateRequest{Message: &bridgev1.CoordinateRequest_Outcome{Outcome: &bridgev1.CheckpointOutcome{Checkpoint: c}}})
		return fmt.Errorf("flinktest: checkpoint %d declined", c)
	}
	if typ == bridgev1.BarrierType_BARRIER_TYPE_SAVEPOINT {
		j.cluster.logf("flink: %s took savepoint %d by hand", j.id, c)
		j.cluster.mu.Lock()
		mark(j.cluster.byHand, j.id, c)
		j.cluster.mu.Unlock()
		return nil
	}
	// A failure here leaves the checkpoint undecided; after it, completed
	// with no notice.
	err := r.point("flink.complete", func() error {
		if !stop {
			j.cluster.complete(j.id, c)
			j.mu.Lock()
			j.restarts = 0
			j.mu.Unlock()
			return nil
		}
		j.cluster.mu.Lock()
		j.cluster.savepoints[stopDir] = savepoint{path: fmt.Sprintf("%s/savepoint-%d", stopDir, c), id: c}
		j.cluster.mu.Unlock()
		return nil
	})
	if err != nil {
		return err
	}
	if err := r.point("flink.notify", nil); err != nil {
		return err
	}
	if err := coord.call(&bridgev1.CoordinateRequest{Message: &bridgev1.CoordinateRequest_Outcome{Outcome: &bridgev1.CheckpointOutcome{Checkpoint: c, Completed: true}}}); err != nil {
		return fmt.Errorf("checkpoint %d's outcome: %w", c, err)
	}
	if typ == bridgev1.BarrierType_BARRIER_TYPE_SYNC_SAVEPOINT {
		j.cluster.logf("flink: %s stopped with savepoint %d", j.id, c)
		j.mu.Lock()
		j.stopping, j.stopErr = "", nil
		j.mu.Unlock()
		stopErr <- nil
		return errFinished
	}
	return nil
}

// abortForStop aborts checkpoint c with half its sinks inside it. The
// runner takes the abort, since the job is stopping, and releases them; the
// other readers pass over the barrier.
func (r *taskRun) abortForStop(coord *bidi[bridgev1.CoordinateRequest, bridgev1.CoordinateResponse], c int64, held []readerBarrier, errc <-chan error) error {
	inside := held[:len(held)/2]
	for _, b := range inside {
		b.release <- bridgev1.BarrierType_BARRIER_TYPE_CHECKPOINT
	}
	// Let the sinks' barriers reach the runner before the abort.
	time.Sleep(time.Millisecond)
	r.job.cluster.logf("flink: %s's stop aborts checkpoint %d", r.job.id, c)
	if err := coord.call(&bridgev1.CoordinateRequest{Message: &bridgev1.CoordinateRequest_Outcome{Outcome: &bridgev1.CheckpointOutcome{Checkpoint: c}}}); err != nil {
		return fmt.Errorf("checkpoint %d's abort: %w", c, err)
	}
	for _, b := range held[len(held)/2:] {
		b.release <- bridgev1.BarrierType_BARRIER_TYPE_UNSPECIFIED
	}
	for _, b := range held {
		select {
		case err := <-b.acked:
			if err != nil {
				return fmt.Errorf("checkpoint %d: a barrier inside the aborted checkpoint was refused: %w", c, err)
			}
		case err := <-errc:
			return err
		case <-r.ctx.Done():
			return r.ctx.Err()
		}
	}
	return nil
}

// bidi is an in-process bidirectional stream: the fake job's end calls
// send, call and recv; the runner's handler gets it as its server stream.
type bidi[Req, Res any] struct {
	stream
	in   chan *Req
	out  chan *Res
	done chan struct{}
	err  error
}

func newBidi[Req, Res any](ctx context.Context) *bidi[Req, Res] {
	return &bidi[Req, Res]{stream: stream{ctx: ctx}, in: make(chan *Req), out: make(chan *Res, 1), done: make(chan struct{})}
}

func (b *bidi[Req, Res]) serverDone(err error) {
	b.err = err
	close(b.done)
}

// The server's side.
func (b *bidi[Req, Res]) Recv() (*Req, error) {
	select {
	case m := <-b.in:
		return m, nil
	case <-b.ctx.Done():
		return nil, b.ctx.Err()
	}
}

func (b *bidi[Req, Res]) Send(m *Res) error {
	select {
	case b.out <- m:
		return nil
	case <-b.ctx.Done():
		return b.ctx.Err()
	}
}

// The job's side.
func (b *bidi[Req, Res]) send(m *Req) error {
	select {
	case b.in <- m:
		return nil
	case <-b.done:
		return fmt.Errorf("flinktest: the call ended: %w", errOr(b.err))
	case <-b.ctx.Done():
		return b.ctx.Err()
	}
}

// call sends m and waits for one response.
func (b *bidi[Req, Res]) call(m *Req) error {
	if err := b.send(m); err != nil {
		return err
	}
	select {
	case <-b.out:
		return nil
	case <-b.done:
		return fmt.Errorf("flinktest: the call ended: %w", errOr(b.err))
	case <-b.ctx.Done():
		return b.ctx.Err()
	}
}

func errOr(err error) error {
	if err == nil {
		return io.EOF
	}
	return err
}

// serverStream is an in-process server stream.
type serverStream[Res any] struct {
	stream
	out  chan *Res
	done chan struct{}
	err  error
}

func newServerStream[Res any](ctx context.Context) *serverStream[Res] {
	return &serverStream[Res]{stream: stream{ctx: ctx}, out: make(chan *Res), done: make(chan struct{})}
}

func (s *serverStream[Res]) serverDone(err error) {
	s.err = err
	close(s.done)
}

func (s *serverStream[Res]) Send(m *Res) error {
	select {
	case s.out <- m:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *serverStream[Res]) recv() (*Res, error) {
	select {
	case m := <-s.out:
		return m, nil
	case <-s.done:
		return nil, fmt.Errorf("flinktest: the call ended: %w", errOr(s.err))
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

// stream is what grpc.ServerStream needs beyond the messages.
type stream struct{ ctx context.Context }

func (s stream) Context() context.Context   { return s.ctx }
func (stream) SetHeader(metadata.MD) error  { return nil }
func (stream) SendHeader(metadata.MD) error { return nil }
func (stream) SetTrailer(metadata.MD)       {}
func (stream) SendMsg(any) error            { return errors.New("flinktest: SendMsg") }
func (stream) RecvMsg(any) error            { return errors.New("flinktest: RecvMsg") }

var (
	_ grpc.BidiStreamingServer[bridgev1.CoordinateRequest, bridgev1.CoordinateResponse] = (*bidi[bridgev1.CoordinateRequest, bridgev1.CoordinateResponse])(nil)
	_ grpc.ServerStreamingServer[bridgev1.ReadResponse]                                 = (*serverStream[bridgev1.ReadResponse])(nil)
	_ runner.Flink                                                                      = (*Cluster)(nil)
)
