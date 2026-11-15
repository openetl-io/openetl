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
	// How long it waits for its coordinator to say where it joins, and
	// after each part for a newer generation to join. Zero: 10 minutes, a
	// pod's start timeout and the time workers have to join.
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
// the worker's part, however it ends: the coordinator reports the attempt's
// outcome. It runs a part again for each newer generation the coordinator
// says to join, as it does when the destination refused a column and the
// coordinator claims the pipeline again to hold it. It returns nil once the
// attempt's placement has moved on, or Wait after the last part without a
// newer one.
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
	// The generation of the part run last; 0: none yet.
	var ran uint64
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
			if ran == 0 {
				p.log().Info("the attempt isn't this worker's to run any more", "attempt", p.Attempt.ID, "placement", p.Attempt.Placement, "worker", p.Worker+1)
			}
			return nil
		case res.Msg.GetWork()[0].GetJoin().GetGeneration() > ran:
			w := res.Msg.GetWork()[0]
			p.log().Info("worker joining its coordinator", "attempt", w.GetId(), "worker", p.Worker+1, "coordinator", w.GetJoin().GetAddress(), "generation", w.GetJoin().GetGeneration())
			if err := p.Run(ctx, w); err != nil {
				p.log().Warn("the worker's part of the attempt failed", "attempt", w.GetId(), "worker", p.Worker+1, "err", err)
			}
			ran, failing, deadline = w.GetJoin().GetGeneration(), nil, time.Now().Add(wait)
			continue
		}
		if time.Now().After(deadline) {
			switch {
			case failing != nil:
				return fmt.Errorf("worker: taking attempt %s's worker %d from the API: %w", p.Attempt.ID, p.Worker+1, failing)
			case ran == 0:
				p.log().Warn("the attempt's coordinator didn't say where to join in time", "attempt", p.Attempt.ID, "worker", p.Worker+1, "waited", wait)
			}
			return nil
		}
		select {
		case <-time.After(every):
		case <-ctx.Done():
			return nil
		}
	}
}
