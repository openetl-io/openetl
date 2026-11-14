package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"connectrpc.com/connect"

	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
	"github.com/openetl-io/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
	"github.com/openetl-io/openetl/internal/platform/ints"
)

// Pod runs one worker of an attempt, in a pod its launcher made beside
// the attempt's coordinator's. It takes no part in the attempt's lease or
// result, which are the coordinator's: a worker that fails tells the
// coordinator, which fails the attempt.
type Pod struct {
	API dataplanev1connect.DataPlaneServiceClient
	// Unique to this process.
	ID      string
	Attempt Attempt
	// Which worker, counting from 0.
	Worker int
	// Runs the worker's part of the attempt, which the work's Join says
	// where to join.
	Run    func(ctx context.Context, w *dataplanev1.Work) error
	Logger *slog.Logger
	// How long it waits for its coordinator to say where it joins. Zero: 10
	// minutes, a pod's start timeout and the time workers have to join.
	Wait time.Duration
	// How often it asks. Zero: a second.
	Every time.Duration
}

func (p *Pod) log() *slog.Logger {
	if p.Logger == nil {
		return slog.New(slog.DiscardHandler)
	}
	return p.Logger
}

// Serve waits for the coordinator to say where the worker joins, and runs
// the worker's part. It returns nil if the attempt's placement moved on
// before then, or once the part has run, however it ended: the coordinator
// reports the attempt's outcome.
func (p *Pod) Serve(ctx context.Context) error {
	req := &dataplanev1.PollWorkRequest{
		Runner: p.ID, Attempt: &dataplanev1.Launch{Attempt: p.Attempt.ID, Placement: uint32(max(0, p.Attempt.Placement))},
		Worker: new(ints.Uint32(p.Worker)),
	}
	every := p.Every
	if every <= 0 {
		every = time.Second
	}
	wait := p.Wait
	if wait <= 0 {
		wait = 10 * time.Minute
	}
	deadline := time.Now().Add(wait)
	var failing error
	for {
		pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		res, err := p.API.PollWork(pctx, connect.NewRequest(req))
		cancel()
		switch {
		case ctx.Err() != nil:
			return nil
		case err != nil:
			failing = err
		case len(res.Msg.GetWork()) == 0:
			p.log().Info("the attempt isn't this worker's to run any more", "attempt", p.Attempt.ID, "placement", p.Attempt.Placement, "worker", p.Worker)
			return nil
		case res.Msg.GetWork()[0].GetJoin() != nil:
			w := res.Msg.GetWork()[0]
			p.log().Info("worker joining its coordinator", "attempt", w.GetId(), "worker", p.Worker, "coordinator", w.GetJoin().GetAddress())
			if err := p.Run(ctx, w); err != nil {
				p.log().Warn("the worker's part of the attempt failed", "attempt", w.GetId(), "worker", p.Worker, "err", err)
			}
			return nil
		}
		if time.Now().After(deadline) {
			if failing != nil {
				return fmt.Errorf("worker: taking attempt %s's worker %d from the API: %w", p.Attempt.ID, p.Worker, failing)
			}
			p.log().Warn("the attempt's coordinator didn't say where to join in time", "attempt", p.Attempt.ID, "worker", p.Worker, "waited", wait)
			return nil
		}
		select {
		case <-time.After(every):
		case <-ctx.Done():
			return nil
		}
	}
}
