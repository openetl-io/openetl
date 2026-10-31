//go:build e2e

package e2e

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/unsuitable001/openetl/connectors/native/faker"
	"github.com/unsuitable001/openetl/connectors/native/postgres"
	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/delivery"
	"github.com/unsuitable001/openetl/internal/engineclient"
	runnerpkg "github.com/unsuitable001/openetl/internal/runner"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

// A zombie attempt that reaches Commit(2) after its replacement has raised
// the destination's fence is refused by the fence row. The replacement then
// redoes epoch 2, and every row lands once.
func TestZombieCommitIsFenced(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	name := unique("fence")
	store, err := checkpoint.NewFSStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	log := checkpoint.NewLog(store, "org", name)
	pg, err := postgres.NewDestination(fmt.Appendf(nil, `{"dsn": %q, "schema": %q}`, env.dbURL, schema(name)))
	if err != nil {
		t.Fatal(err)
	}

	zombie := &zombieDest{Destination: pg, committing: make(chan struct{}), release: make(chan struct{})}
	zerr := make(chan error, 1)
	go func() {
		_, err := runnerpkg.RunAttempt(ctx, fencingJob(t, log, "zombie", zombie))
		zerr <- err
	}()
	select {
	case <-zombie.committing:
	case err := <-zerr:
		t.Fatalf("the zombie ended before Commit(2): %v", err)
	}

	replacement := &replacementDest{Destination: pg, fenceRaised: make(chan struct{}), proceed: make(chan struct{})}
	rres := make(chan error, 1)
	var res runnerpkg.Result
	go func() {
		var err error
		res, err = runnerpkg.RunAttempt(ctx, fencingJob(t, log, "replacement", replacement))
		rres <- err
	}()
	select {
	case <-replacement.fenceRaised:
	case err := <-rres:
		t.Fatalf("the replacement ended before raising the fence: %v", err)
	}

	close(zombie.release)
	if err := <-zerr; !errors.Is(err, destination.ErrFenced) {
		t.Fatalf("the zombie ended with %v, want the destination's fence", err)
	}
	close(replacement.proceed)
	if err := <-rres; err != nil {
		t.Fatal(err)
	}
	if res.Generation != 2 || res.Recovery != destination.Redone || res.Restored != 2 {
		t.Errorf("replacement: generation %d, %v, restored %d; want 2, redone, 2", res.Generation, res.Recovery, res.Restored)
	}
	checkRows(t, schema(name), 300, "users", "purchases")
	marks, err := query[int](t, "SELECT count(*) FROM "+pgx.Identifier{schema(name), "_openetl_loads"}.Sanitize()+" WHERE epoch = 2 AND generation = 1")
	if err != nil {
		t.Fatal(err)
	}
	if marks == 0 {
		t.Error("epoch 2 has no markers")
	}
}

func fencingJob(t *testing.T, log *checkpoint.Log, owner string, dst destination.Destination) runnerpkg.Job {
	t.Helper()
	src, err := faker.New([]byte(`{"seed": 7, "count": 300, "batchSize": 50, "delay": "5ms"}`))
	if err != nil {
		t.Fatal(err)
	}
	var streams []runnerpkg.Stream
	for _, s := range []string{"users", "purchases"} {
		streams = append(streams, runnerpkg.Stream{Name: s, Replay: source.Exact, Mode: destination.Append, PrimaryKey: []string{"id"}})
	}
	return runnerpkg.Job{
		Owner:       owner,
		Pipeline:    "fencing",
		Log:         log,
		Source:      src,
		Streams:     streams,
		Destination: dst,
		Dataflow:    &engineclient.Dataflow{Options: engineclient.Options{Binary: env.engine}},
		Settings: delivery.Settings{
			Require:        delivery.ExactlyOnce,
			CommitMaxBytes: 1,
			Scope:          destination.ScopePipeline,
			Dedupe:         delivery.DedupeNone,
		},
	}
}

// zombieDest stops in Commit(2) until released.
type zombieDest struct {
	destination.Destination
	committing chan struct{}
	release    chan struct{}
	once       sync.Once
}

func (d *zombieDest) Open(ctx context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	w, err := d.Destination.Open(ctx, cfg)
	return &zombieWriter{Writer: w, d: d}, err
}

type zombieWriter struct {
	destination.Writer
	d *zombieDest
}

func (w *zombieWriter) Commit(ctx context.Context, e destination.Epoch, s destination.Staged) error {
	if e.N == 2 {
		w.d.once.Do(func() { close(w.d.committing) })
		select {
		case <-w.d.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return w.Writer.Commit(ctx, e, s)
}

// replacementDest raises the fence, waits until it may proceed, then
// recovers.
type replacementDest struct {
	destination.Destination
	fenceRaised chan struct{}
	proceed     chan struct{}
}

func (d *replacementDest) Open(ctx context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	w, err := d.Destination.Open(ctx, cfg)
	return &replacementWriter{Writer: w, d: d}, err
}

type replacementWriter struct {
	destination.Writer
	d *replacementDest
}

func (w *replacementWriter) Recover(ctx context.Context, gen uint64, e destination.Epoch, s destination.Staged) (destination.Recovery, error) {
	if _, err := w.Writer.Recover(ctx, gen, destination.Epoch{}, nil); err != nil {
		return 0, err
	}
	close(w.d.fenceRaised)
	select {
	case <-w.d.proceed:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	return w.Writer.Recover(ctx, gen, e, s)
}
