//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/unsuitable001/openetl/connectors/native/faker"
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
	pg := postgresDestination(t, fmt.Appendf(nil, `{"dsn": %q, "schema": %q}`, env.dbURL, schema(name)))

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

// A runner cut off from Temporal and the API keeps committing its job, since
// it still reaches the checkpoint log and the destination. Once Temporal has
// timed the job out, the runner that takes it over fences it: the cut-off
// job can't commit again, and stops. When the partition heals, the job stays
// with its new runner. No row lands twice.
func TestPartitionedRunnerIsFenced(t *testing.T) {
	temporal, api := startProxy(t, env.temporal), startProxy(t, strings.TrimPrefix(env.apiURL, "http://"))
	a := startRunnerVia(t, temporal.addr(), "http://"+api.addr())
	name := unique("partitioned")
	startContinuous(t, name)
	waitRows(t, name, 100)
	host(t, a)
	gen := generation(t, name)

	temporal.cut(true)
	api.cut(true)
	// Three of e2e's heartbeat timeouts.
	time.Sleep(12 * time.Second)
	waitRows(t, name, 100)
	if !a.hosts() {
		t.Fatal("the cut-off runner stopped its job")
	}
	if g := generation(t, name); g != gen {
		t.Fatalf("generation %d, want %d: the job moved while no other runner ran", g, gen)
	}
	if !strings.Contains(a.out.String(), "heartbeat failed") {
		t.Errorf("the cut-off runner's heartbeats didn't fail:\n%s", a.out.String())
	}

	b := startRunner(t)
	host(t, b)
	eventually(t, 30*time.Second, "the cut-off job to be fenced and stop", func() bool {
		return !a.hosts() && strings.Contains(a.out.String(), "code=FENCED")
	})
	if g := generation(t, name); g <= gen {
		t.Errorf("generation %d, want above %d", g, gen)
	}
	waitRows(t, name, 100)

	temporal.cut(false)
	api.cut(false)
	time.Sleep(8 * time.Second)
	waitRows(t, name, 100)
	if a.hosts() || !b.hosts() {
		t.Errorf("after the partition healed, the first runner hosts the job: %v, the second: %v", a.hosts(), b.hosts())
	}
	if got := statuses(runs(t, name)); got != "running" {
		t.Errorf("runs %q, want one running", got)
	}
}

// generation is the pipeline's generation in its checkpoint log's HEAD.
func generation(t *testing.T, pipeline string) uint64 {
	t.Helper()
	dir := mustQuery[string](t, "SELECT org_id || '/' || id FROM pipelines WHERE name = $1", pipeline)
	data, err := os.ReadFile(filepath.Join(env.checkpoints, dir, "_checkpoints", "HEAD"))
	if err != nil {
		t.Fatal(err)
	}
	var h checkpoint.Head
	if err := json.Unmarshal(data, &h); err != nil {
		t.Fatal(err)
	}
	return h.Generation
}

// proxy forwards TCP connections to target. Cut, it closes the connections
// it carries, so the far end sees its clients go, and holds new ones without
// forwarding anything, as a network partition does. Healed, it closes those
// it held, and forwards again.
type proxy struct {
	ln     net.Listener
	target string
	mu     sync.Mutex
	isCut  bool
	conns  map[net.Conn]struct{}
}

func startProxy(t *testing.T, target string) *proxy {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &proxy{ln: ln, target: target, conns: map[net.Conn]struct{}{}}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go p.forward(c)
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		p.cut(true)
	})
	return p
}

func (p *proxy) addr() string { return p.ln.Addr().String() }

func (p *proxy) cut(cut bool) {
	p.mu.Lock()
	p.isCut = cut
	conns := p.conns
	p.conns = map[net.Conn]struct{}{}
	p.mu.Unlock()
	for c := range conns {
		_ = c.Close()
	}
}

func (p *proxy) forward(c net.Conn) {
	p.mu.Lock()
	p.conns[c] = struct{}{}
	held := p.isCut
	p.mu.Unlock()
	if held {
		return
	}
	var d net.Dialer
	up, err := d.Dial("tcp", p.target)
	if err == nil {
		p.mu.Lock()
		if _, ok := p.conns[c]; ok {
			p.conns[up] = struct{}{}
		} else {
			_ = up.Close()
			err = net.ErrClosed
		}
		p.mu.Unlock()
	}
	if err != nil {
		p.drop(c)
		return
	}
	go func() {
		_, _ = io.Copy(up, c)
		p.drop(up)
	}()
	_, _ = io.Copy(c, up)
	p.drop(c)
}

func (p *proxy) drop(c net.Conn) {
	p.mu.Lock()
	delete(p.conns, c)
	p.mu.Unlock()
	_ = c.Close()
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

func (w *zombieWriter) Commit(ctx context.Context, e destination.Epoch, s destination.Staged) (destination.Committed, error) {
	if e.N == 2 {
		w.d.once.Do(func() { close(w.d.committing) })
		select {
		case <-w.d.release:
		case <-ctx.Done():
			return destination.Committed{}, ctx.Err()
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
