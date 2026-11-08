package ops

import (
	"cmp"
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/openetl-io/openetl/internal/events"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/platform/ints"
	"github.com/openetl-io/openetl/internal/store"
	"github.com/openetl-io/openetl/internal/store/db"
)

const (
	sweepEvery = 5 * time.Second
	// How many teardowns one orchestrator runs at a time.
	sweepConcurrency = 8
	// Longer than an operation can take, so a teardown isn't claimed again
	// while its try runs. An orchestrator that dies leaves its claims to
	// run out.
	teardownLease = contract.OpTimeout + 5*time.Minute
	// How long a teardown is retried before it's given up on.
	teardownRetention = 7 * 24 * time.Hour
	maxTeardownError  = 2048
)

// The waits between tries, the last repeating.
var teardownBackoff = []time.Duration{time.Minute, 5 * time.Minute, 30 * time.Minute, time.Hour, 6 * time.Hour}

// OpRunner runs a connector operation and waits for its result.
type OpRunner interface {
	Run(ctx context.Context, in contract.ConnectorOpInput) (contract.ConnectorOpResult, error)
}

// Teardowns retries what deleted pipelines' connectors and Flink jobs have
// left to remove, until it succeeds or teardownRetention passes. Any number
// of orchestrators can run it: each claims rows the others haven't.
type Teardowns struct {
	Store  *store.Store
	Ops    OpRunner
	Logger *slog.Logger
	// How often it looks for teardowns that are due. Zero: 5 s.
	Every time.Duration
}

// Run works until ctx ends.
func (t *Teardowns) Run(ctx context.Context) {
	slots := make(chan struct{}, sweepConcurrency)
	tick := time.NewTicker(cmp.Or(t.Every, sweepEvery))
	defer tick.Stop()
	for {
		if free := cap(slots) - len(slots); free > 0 {
			rows, err := t.claim(ctx, free)
			if err != nil && ctx.Err() == nil {
				t.Logger.Warn("claiming teardowns failed", "err", err)
			}
			for _, r := range rows {
				slots <- struct{}{}
				go func() {
					defer func() { <-slots }()
					t.run(ctx, r)
				}()
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// claim leases up to n teardowns that are due, across the orgs.
func (t *Teardowns) claim(ctx context.Context, n int) ([]db.Teardown, error) {
	orgs, err := t.Store.ListOrgs(ctx)
	if err != nil {
		return nil, err
	}
	var out []db.Teardown
	for _, org := range orgs {
		rows, err := t.Store.ClaimTeardowns(ctx, db.ClaimTeardownsParams{OrgID: org, LeaseSeconds: teardownLease.Seconds(), MaxRows: ints.Int32(n - len(out))})
		if err != nil {
			return out, err
		}
		if out = append(out, rows...); len(out) >= n {
			break
		}
	}
	return out, nil
}

func (t *Teardowns) run(ctx context.Context, r db.Teardown) {
	log := t.Logger.With("teardown", r.ID, "pipeline", r.PipelineID, "of", r.Name)
	in := contract.ConnectorOpInput{
		OpID: r.ID.String(), Try: r.Tries, OrgID: r.OrgID.String(), WorkspaceID: r.WorkspaceID.String(), DataPlane: r.DataPlaneID,
		Connector: r.Connector, Op: r.Op, PipelineID: r.PipelineID.String(),
	}
	if r.SourceID != nil {
		in.SourceID = r.SourceID.String()
	}
	if r.DestinationID != nil {
		in.DestinationID = r.DestinationID.String()
	}
	_, err := t.Ops.Run(ctx, in)
	if ctx.Err() != nil {
		// The claim runs out, and another orchestrator tries.
		return
	}
	if err == nil {
		log.Info("teardown done", "tries", r.Tries)
		if err := t.Store.DeleteTeardown(ctx, db.DeleteTeardownParams{OrgID: r.OrgID, ID: r.ID}); err != nil {
			log.Warn("removing a finished teardown failed; it runs again", "err", err)
		}
		return
	}
	msg := err.Error()
	if len(msg) > maxTeardownError {
		msg = strings.ToValidUTF8(msg[:maxTeardownError], "")
	}
	if time.Since(r.CreateTime) >= teardownRetention {
		t.giveUp(ctx, r, msg, log)
		return
	}
	wait := teardownBackoff[min(int(r.Tries)-1, len(teardownBackoff)-1)]
	log.Warn("teardown failed; trying again", "try", r.Tries, "in", wait, "err", err)
	if err := t.Store.RetryTeardown(ctx, db.RetryTeardownParams{OrgID: r.OrgID, ID: r.ID, LastError: &msg, WaitSeconds: wait.Seconds()}); err != nil {
		log.Warn("recording a failed teardown failed; it runs again", "err", err)
	}
}

// giveUp stops trying and says so, since nothing else would: the pipeline
// the teardown cleans up after is gone, so no incident can name it.
func (t *Teardowns) giveUp(ctx context.Context, r db.Teardown, msg string, log *slog.Logger) {
	log.Error("giving up on a teardown; what it removes is left behind", "tries", r.Tries, "err", msg)
	err := t.Store.Tx(ctx, func(q *db.Queries) error {
		if _, err := events.Record(ctx, q, r.OrgID, events.Event{
			Workspace: r.WorkspaceID, Type: events.PipelineTeardownFailed, Pipeline: r.Pipeline,
			Data: events.Pipeline{Pipeline: r.Pipeline, Connector: r.Name, Error: msg},
		}); err != nil {
			return err
		}
		return q.DeleteTeardown(ctx, db.DeleteTeardownParams{OrgID: r.OrgID, ID: r.ID})
	})
	if err != nil {
		log.Warn("recording that a teardown was given up on failed; it's tried again", "err", err)
	}
}
