// Package dbosbackend runs workflows on DBOS Transact, a library that keeps
// them in a database of its own on the control plane's Postgres server, so a
// small install runs no Temporal (ADR-0028). What DBOS doesn't do it does in
// that database's openetl schema: IDs that start again, schedules, and
// replicas recovering each other.
package dbosbackend

import (
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/openetl-io/openetl/internal/orchestration"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
)

const (
	appName = "openetl"
	// A workflow's one mailbox.
	mailbox = "mailbox"

	// How many of a queue's workflows one orchestrator runs at a time. Most of
	// what they do is wait, and placement decides what runs on runners, so
	// these only keep a burst from swamping the database.
	syncsPerReplica = 1000
	opsPerReplica   = 200
	// How often a queue is looked at. Someone is waiting for an operation.
	queuePoll    = 500 * time.Millisecond
	opsQueuePoll = 200 * time.Millisecond

	// How often Run.Get looks.
	getPoll = 100 * time.Millisecond
	// How many times to try creating the system database.
	createTries = 5
	// How long a call from outside a workflow tries for, as DBOS's client
	// otherwise tries for as long as the database is down.
	callTimeout = 30 * time.Second
)

const (
	queueSyncs = "openetl-syncs"
	queueOps   = "openetl-ops"
	// Supervisors never end, so they mustn't count against a limit.
	queueLongLived = "openetl-long-lived"
)

// queueFor is the queue a workflow runs on.
func queueFor(workflow string) string {
	switch workflow {
	case contract.StreamSupervisorWorkflow:
		return queueLongLived
	case contract.SyncWorkflow, contract.RetentionWorkflow:
		return queueSyncs
	}
	return queueOps
}

// Stopped workflows end with this code, as errors keep only their fields on
// their way through DBOS.
const stoppedCode = "OPENETL_STOPPED"

func init() { gob.Register(&orchestration.Failure{}) }

// Options say how to reach the system database.
type Options struct {
	// DBOS creates the database if it can, and its tables.
	DatabaseURL string
	// Which orchestrator this is. One that starts again under its ID goes on
	// with its workflows at once; otherwise another recovers them when its
	// reports lapse. Empty: the host name.
	ExecutorID string
	// How long an orchestrator goes without reporting before another recovers
	// its workflows. Zero: 30 s.
	Lapse time.Duration
	// The version workflows started here run under, as only this version may
	// run them again. Empty: a hash of this binary, or DBOS__APPVERSION.
	Version string
	Logger  *slog.Logger
}

// Backend is a system database. Everything from outside a workflow goes
// through its client, so an API server runs no workflows and needs no
// registry of them.
type Backend struct {
	o      Options
	log    *slog.Logger
	pool   *pgxpool.Pool
	client dbos.Client
	// Wakes the loop that fires schedules.
	fire chan struct{}
}

var _ orchestration.Backend = (*Backend)(nil)

// SystemDatabaseURL is the system database's default URL: on the metadata
// database's server, named after it.
func SystemDatabaseURL(metadata string) (string, error) {
	u, err := url.Parse(metadata)
	if err != nil || u.Scheme == "" || u.Path == "" || u.Path == "/" {
		return "", errors.New("dbosbackend: can't name a system database after a metadata database URL that isn't postgres://host/name; set it")
	}
	u.Path = strings.TrimSuffix(u.Path, "/") + "_dbos"
	return u.String(), nil
}

// Open creates the system database if need be, and connects to it.
func Open(ctx context.Context, o Options) (*Backend, error) {
	log := o.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	if o.ExecutorID == "" {
		host, err := os.Hostname()
		if err != nil {
			return nil, fmt.Errorf("dbosbackend: naming this orchestrator: %w", err)
		}
		o.ExecutorID = host
	}
	dbosLog := slog.New(quiet{log.Handler()})
	if err := createSystemDatabase(ctx, o.DatabaseURL, dbosLog); err != nil {
		return nil, fmt.Errorf("dbosbackend: the system database: %w", err)
	}
	pool, err := pgxpool.New(ctx, o.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("dbosbackend: %w", err)
	}
	if err := migrate(ctx, pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("dbosbackend: the openetl schema: %w", err)
	}
	client, err := dbos.NewClient(ctx, dbos.ClientConfig{AppName: appName, DatabaseURL: o.DatabaseURL, Logger: dbosLog})
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("dbosbackend: %w", err)
	}
	b := &Backend{o: o, log: log, pool: pool, client: client, fire: make(chan struct{}, 1)}
	if err := b.registerQueues(); err != nil {
		b.Close()
		return nil, err
	}
	return b, nil
}

// createSystemDatabase makes the database and DBOS's tables, as a context does
// and a client doesn't. On a new install, a server and an orchestrator starting
// together race to create the database, and the one that loses tries again.
func createSystemDatabase(ctx context.Context, url string, log *slog.Logger) error {
	for try := 1; ; try++ {
		c, err := dbos.NewContext(ctx, dbos.Config{AppName: appName, DatabaseURL: url, Logger: log})
		if err == nil {
			if err := dbos.Shutdown(c, 5*time.Second); err != nil {
				log.Warn("closing the system database's first connection", "err", err)
			}
			return nil
		}
		if try == createTries || !strings.Contains(err.Error(), "failed to create database") {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func (b *Backend) Close() {
	if err := dbos.Shutdown(b.client, 5*time.Second); err != nil {
		b.log.Warn("closing the system database", "err", err)
	}
	b.pool.Close()
}

func (b *Backend) registerQueues() error {
	policy := dbos.WithQueueOnConflict(dbos.QueueConflictAlwaysUpdate)
	for name, opts := range map[string][]dbos.QueueOption{
		queueSyncs:     {dbos.WithWorkerConcurrency(syncsPerReplica), dbos.WithQueueBasePollingInterval(queuePoll), policy},
		queueOps:       {dbos.WithWorkerConcurrency(opsPerReplica), dbos.WithQueueBasePollingInterval(opsQueuePoll), policy},
		queueLongLived: {dbos.WithQueueBasePollingInterval(queuePoll), policy},
	} {
		if _, err := dbos.RegisterQueue(b.client, name, opts...); err != nil {
			return fmt.Errorf("dbosbackend: queue %s: %w", name, err)
		}
	}
	return nil
}

// with is the client for a call that ends with ctx. Without it, DBOS retries
// for as long as the database is down.
func (b *Backend) with(ctx context.Context) dbos.Client {
	c, ok := b.client.(dbos.Context)
	if !ok {
		return b.client
	}
	return dbos.From(c, ctx)
}

// Run runs workflows and steps here until ctx ends. Workflows left running
// stay as they are, for this orchestrator when it starts again or for another.
func (b *Backend) Run(ctx context.Context, workflows []orchestration.Workflow, steps []orchestration.Step) error {
	c, err := dbos.NewContext(context.WithoutCancel(ctx), dbos.Config{
		AppName: appName, DatabaseURL: b.o.DatabaseURL, ExecutorID: b.o.ExecutorID, ApplicationVersion: b.o.Version, Logger: slog.New(quiet{b.log.Handler()}),
	})
	if err != nil {
		return fmt.Errorf("dbosbackend: %w", err)
	}
	Register(c, workflows, steps, Hooks{Logger: b.log, Continue: b.startNext})
	// Before launching, so nothing recovers what this is about to.
	if err := b.report(ctx, c.GetApplicationVersion()); err != nil {
		return fmt.Errorf("dbosbackend: reporting: %w", err)
	}
	if err := dbos.Launch(c); err != nil {
		return fmt.Errorf("dbosbackend: %w", err)
	}
	defer func() {
		if err := dbos.Shutdown(c, 30*time.Second); err != nil {
			b.log.Warn("stopping workflows", "err", err)
		}
	}()
	// What the API enqueues runs on the latest version, which goes by when a
	// version first started, so this says so again after a rollback.
	if err := dbos.SetLatestApplicationVersion(c, c.GetApplicationVersion()); err != nil {
		return fmt.Errorf("dbosbackend: %w", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.runReplica(ctx, c.GetApplicationVersion())
	}()
	b.runSchedules(ctx)
	<-done
	return nil
}

// envelope is a workflow's input in the system database, with what the
// workflow knows about itself without having to ask.
type envelope struct {
	// The ID it was started under, which all its generations share.
	ID string
	// Which of them this is. Zero is the first.
	Generation int                      `json:",omitzero"`
	Attributes orchestration.Attributes `json:",omitzero"`
	// The trace of what started it.
	Trace map[string]string `json:",omitempty"`
	Input json.RawMessage
}

// execer is a pool or a transaction.
type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// generationID is the row of an ID's nth generation.
func generationID(id string, n int) string {
	if n <= 1 {
		return id
	}
	return fmt.Sprintf("%s@%d", id, n)
}

// generation is the ID's newest generation. DBOS keeps an ID for ever, so a
// workflow that starts again or continues gets a row of its own, and only IDs
// that have have a row here.
func (b *Backend) generation(ctx context.Context, id string) (int, error) {
	var n int
	err := b.pool.QueryRow(ctx, `SELECT generation FROM openetl.generations WHERE id = $1`, id).Scan(&n)
	if errors.Is(err, pgx.ErrNoRows) {
		return 1, nil
	}
	return n, err
}

// advance moves the ID on from generation from, unless something else has.
func advance(ctx context.Context, q execer, id string, from int) error {
	_, err := q.Exec(ctx, `INSERT INTO openetl.generations (id, generation) VALUES ($1, $2)
		ON CONFLICT (id) DO UPDATE SET generation = excluded.generation WHERE openetl.generations.generation < excluded.generation`, id, from+1)
	return err
}

// enqueue adds a workflow's row, or fails with dbos.ErrWorkflowIDInUse. tx may
// be nil.
func (b *Backend) enqueue(ctx context.Context, tx pgx.Tx, row, workflow string, env envelope, timeout time.Duration) error {
	opts := []dbos.EnqueueOption{
		dbos.WithEnqueueWorkflowID(row),
		dbos.WithEnqueueConfigName(workflow),
		dbos.WithEnqueueWorkflowIDReusePolicy(dbos.WorkflowIDReusePolicyReject),
		dbos.WithEnqueueAttributes(attributes(env.ID, env.Attributes)),
	}
	if timeout > 0 {
		opts = append(opts, dbos.WithEnqueueTimeout(timeout))
	}
	if tx != nil {
		opts = append(opts, dbos.WithEnqueueTransaction(tx))
	}
	_, err := dbos.Enqueue[json.RawMessage](b.with(ctx), queueFor(workflow), workflow, env, opts...)
	return err
}

// attributes are what ListRunning reads and an operator can find a workflow by.
func attributes(id string, a orchestration.Attributes) map[string]any {
	out := map[string]any{"id": id}
	for k, v := range map[string]string{"org": a.Org, "workspace": a.Workspace, "pipeline": a.Pipeline, "dataPlane": a.DataPlane} {
		if v != "" {
			out[k] = v
		}
	}
	if len(a.Connectors) > 0 {
		out["connectors"] = a.Connectors
	}
	return out
}

func active(s dbos.WorkflowStatusType) bool {
	return s == dbos.WorkflowStatusPending || s == dbos.WorkflowStatusEnqueued || s == dbos.WorkflowStatusDelayed
}

// isActive reports whether a row's workflow is waiting to run or running.
func (b *Backend) isActive(ctx context.Context, row string) (bool, error) {
	list, err := dbos.ListWorkflows(b.with(ctx), dbos.WithFilterWorkflowIDs(row), dbos.WithFilterLoadInput(false), dbos.WithFilterLoadOutput(false))
	if err != nil {
		return false, err
	}
	return len(list) > 0 && active(list[0].Status), nil
}

// start returns the row of the workflow running under o's ID, starting it if
// none is.
func (b *Backend) start(ctx context.Context, o orchestration.StartOptions) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	input, err := json.Marshal(o.Input)
	if err != nil {
		return "", err
	}
	env := envelope{ID: o.ID, Attributes: o.Attributes, Trace: traceHeaders(ctx), Input: input}
	for {
		n, err := b.generation(ctx, o.ID)
		if err != nil {
			return "", err
		}
		row := generationID(o.ID, n)
		env.Generation = n
		err = b.enqueue(ctx, nil, row, o.Workflow, env, o.Timeout)
		if err == nil {
			return row, nil
		}
		if !errors.Is(err, dbos.ErrWorkflowIDInUse) {
			return "", err
		}
		running, err := b.isActive(ctx, row)
		if err != nil {
			return "", err
		}
		if running {
			return row, nil
		}
		// It ended, so the ID moves on, with the row it moves on to. Of two
		// starting at once, one finds the other's running.
		next := generationID(o.ID, n+1)
		env.Generation = n + 1
		err = pgx.BeginFunc(ctx, b.pool, func(tx pgx.Tx) error {
			if err := advance(ctx, tx, o.ID, n); err != nil {
				return err
			}
			return b.enqueue(ctx, tx, next, o.Workflow, env, o.Timeout)
		})
		if err == nil {
			return next, nil
		}
		if !errors.Is(err, dbos.ErrWorkflowIDInUse) {
			return "", err
		}
	}
}

func (b *Backend) Start(ctx context.Context, o orchestration.StartOptions) (orchestration.Run, error) {
	row, err := b.start(ctx, o)
	if err != nil {
		return nil, err
	}
	return started{b, row}, nil
}

type started struct {
	b   *Backend
	row string
}

// Get follows a workflow through its generations.
func (r started) Get(ctx context.Context, out any) error {
	row := r.row
	for {
		h, err := dbos.RetrieveWorkflow[json.RawMessage](r.b.with(ctx), row)
		if err != nil {
			return err
		}
		raw, err := h.GetResult(dbos.WithHandlePollingInterval(getPoll))
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return FailureOf(err)
		}
		var next continuedOutput
		if json.Unmarshal(raw, &next) == nil && next.ContinuedTo != "" {
			row = next.ContinuedTo
			continue
		}
		if out == nil || len(raw) == 0 {
			return nil
		}
		return json.Unmarshal(raw, out)
	}
}

// continuedOutput is what a generation that continued ends with.
type continuedOutput struct {
	ContinuedTo string `json:"openetl.continuedTo"`
}

// FailureOf is the error a workflow ended with, as Run.Get says it.
func FailureOf(err error) error {
	if f, ok := errors.AsType[*orchestration.Failure](err); ok {
		if f.Code == stoppedCode {
			return orchestration.ErrStopped
		}
		return f
	}
	if errors.Is(err, dbos.ErrAwaitedWorkflowCancelled) {
		return errors.New("dbosbackend: the workflow ran out of time or was lost")
	}
	return err
}

func (b *Backend) Send(ctx context.Context, workflowID string, m orchestration.Message) error {
	n, err := b.generation(ctx, workflowID)
	if err != nil {
		return err
	}
	return b.send(ctx, generationID(workflowID, n), m)
}

// send ignores a row that doesn't exist. One that has ended takes the message
// and never reads it.
func (b *Backend) send(ctx context.Context, row string, m orchestration.Message, opts ...dbos.SendOption) error {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	err := dbos.Send(b.with(ctx), row, m, mailbox, opts...)
	if errors.Is(err, dbos.ErrNonExistentWorkflow) {
		return nil
	}
	return err
}

// SendWithStart isn't atomic: a crash between the two loses the message, and
// a workflow that has just started looks at everything a message would say.
func (b *Backend) SendWithStart(ctx context.Context, o orchestration.StartOptions, m orchestration.Message) error {
	row, err := b.start(ctx, o)
	if err != nil {
		return err
	}
	return b.send(ctx, row, m)
}

// Cancel doesn't use DBOS's cancelling, after which a workflow can't run a
// step. The idempotency key makes it tell each generation once.
func (b *Backend) Cancel(ctx context.Context, workflowID string) error {
	n, err := b.generation(ctx, workflowID)
	if err != nil {
		return err
	}
	return b.send(ctx, generationID(workflowID, n), orchestration.Message{Kind: orchestration.Stop}, dbos.WithIdempotencyKey(orchestration.Stop))
}

func (b *Backend) ListRunning(ctx context.Context, workflow string) ([]orchestration.RunningWorkflow, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	list, err := dbos.ListWorkflows(b.with(ctx),
		dbos.WithFilterName(workflow),
		dbos.WithFilterStatus(dbos.WorkflowStatusPending, dbos.WorkflowStatusEnqueued, dbos.WorkflowStatusDelayed),
		dbos.WithFilterLoadInput(false), dbos.WithFilterLoadOutput(false))
	if err != nil {
		return nil, err
	}
	out := make([]orchestration.RunningWorkflow, 0, len(list))
	for _, w := range list {
		id, _ := w.Attributes["id"].(string)
		org, _ := w.Attributes["org"].(string)
		if id == "" {
			id = w.ID
		}
		out = append(out, orchestration.RunningWorkflow{ID: id, Org: org})
	}
	return out, nil
}

// quiet keeps out what DBOS logs in the normal run of things at a warning, such
// as every wait that timed out.
type quiet struct{ slog.Handler }

func (q quiet) Enabled(ctx context.Context, l slog.Level) bool {
	return l >= slog.LevelWarn && q.Handler.Enabled(ctx, l)
}

func (q quiet) Handle(ctx context.Context, r slog.Record) error {
	if strings.HasSuffix(r.Message, "timeout reached") || strings.HasSuffix(r.Message, "context cancelled") || strings.HasPrefix(r.Message, "enqueue timeout") {
		return nil
	}
	return q.Handler.Handle(ctx, r)
}

func (q quiet) WithAttrs(a []slog.Attr) slog.Handler { return quiet{q.Handler.WithAttrs(a)} }
func (q quiet) WithGroup(n string) slog.Handler      { return quiet{q.Handler.WithGroup(n)} }
