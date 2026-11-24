package runner_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/delivery"
	"github.com/openetl-io/openetl/internal/runner"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// A read that replaces the table, by full_refresh or by overwrite, starts
// with no state, though its source reads from the last read's: otherwise
// each sync's overwrite keeps only the rows at or past the last floor.
func TestAReadThatReplacesTheTableStartsWithNoState(t *testing.T) {
	const rows = 5
	incremental := sdk.Incremental("s", "updated_at", func(_ *sdk.Ctx[struct{}], since sdk.Cursor, out *sdk.Out) error {
		from := 1
		if !since.IsZero() {
			from, _ = strconv.Atoi(since.String())
		}
		for i := from; i <= rows; i++ {
			if err := out.Emit(map[string]int{"id": i, "updated_at": i}); err != nil {
				return err
			}
		}
		return nil
	})
	resumable := sdk.Resumable("s", func(_ *sdk.Ctx[struct{}], state json.RawMessage, out *sdk.Out) error {
		after := 0
		if state != nil {
			if err := json.Unmarshal(state, &after); err != nil {
				return err
			}
		}
		for i := after + 1; i <= rows; i++ {
			if err := out.Emit(map[string]int{"id": i}); err != nil {
				return err
			}
			if err := out.Checkpoint(i); err != nil {
				return err
			}
		}
		return nil
	})
	for _, tc := range []struct {
		name   string
		stream sdk.Stream[struct{}]
		read   source.SyncMode
	}{
		{"an incremental stream read with full_refresh", incremental, source.FullRefresh},
		{"a resumable stream written with overwrite", resumable, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
				defer cancel()
				src, err := (&sdk.Source[struct{}]{Name: "src", Streams: []sdk.Stream[struct{}]{tc.stream}}).New(nil)
				if err != nil {
					t.Fatal(err)
				}
				log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
				dest := &overwriteDest{}
				for n, sync := range []string{"s1", "s2", "s3"} {
					committed := make(chan uint64, 64)
					j := job(log, "a"+sync, src, runner.Passthrough{Convert: columnless}, committed)
					j.Sync, j.Destination, j.Settings.Require = sync, dest, delivery.AtLeastOnce
					j.Streams = []runner.Stream{{Name: "s", Replay: source.Superset, Read: tc.read, Mode: destination.Overwrite}}
					if _, err := runner.RunAttempt(ctx, j); err != nil {
						t.Fatalf("sync %d: %v", n+1, err)
					}
					time.Sleep(time.Second)
					if got := dest.table["s"]; got != rows {
						t.Fatalf("after sync %d the table has %d rows, want %d", n+1, got, rows)
					}
				}
			})
		})
	}
}

// A retry of a read that replaces the table continues it from its resume
// point.
func TestARetryOfAReadThatReplacesTheTableContinuesIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
		dest := &overwriteDest{}
		var starts []string
		attempt := func(sync string, fail bool) error {
			committed := make(chan uint64, 64)
			src := sdk.Resumable("s", func(_ *sdk.Ctx[struct{}], state json.RawMessage, out *sdk.Out) error {
				starts = append(starts, string(state))
				after := 0
				if state != nil {
					if err := json.Unmarshal(state, &after); err != nil {
						return err
					}
				}
				for i := after + 1; i <= 3; i++ {
					if err := out.Emit(map[string]int{"id": i}); err != nil {
						return err
					}
					if err := out.Checkpoint(i); err != nil {
						return err
					}
					select {
					case <-committed:
					case <-ctx.Done():
						return ctx.Err()
					}
					if fail {
						return errors.New("the source broke")
					}
				}
				return nil
			})
			bound, err := (&sdk.Source[struct{}]{Name: "src", Streams: []sdk.Stream[struct{}]{src}}).New(nil)
			if err != nil {
				return err
			}
			j := job(log, "a"+strconv.Itoa(len(starts)), bound, runner.Passthrough{Convert: columnless}, committed)
			j.Sync, j.Destination, j.Settings.Require = sync, dest, delivery.AtLeastOnce
			j.Streams = []runner.Stream{{Name: "s", Replay: source.Superset, Read: source.FullRefresh, Mode: destination.Overwrite}}
			_, err = runner.RunAttempt(ctx, j)
			time.Sleep(time.Second)
			return err
		}
		if err := attempt("s1", false); err != nil {
			t.Fatal(err)
		}
		if err := attempt("s2", true); err == nil {
			t.Fatal("the failing attempt succeeded")
		}
		if err := attempt("s2", false); err != nil {
			t.Fatal(err)
		}
		if len(starts) != 3 || starts[0] != "" || starts[1] != "" || starts[2] != "1" {
			t.Fatalf("the reads started from %q, want the start, the start, then the retry from 1", starts)
		}
		if got := dest.table["s"]; got != 3 {
			t.Fatalf("the table has %d rows, want 3", got)
		}
	})
}

// overwriteDest replaces a stream's table with what its read committed, as
// the commit of the epoch the read ended in. A read that StartReads gives
// another start than the last is a new one. Rows of an epoch an attempt
// didn't commit go with the attempt. The runner calls Write and Commit from
// different goroutines, so mu guards the maps.
type overwriteDest struct {
	countingDest
	mu      sync.Mutex
	start   map[string]time.Time
	read    map[string]int64
	written map[uint64]map[string]int64
	ended   map[uint64][]string
	table   map[string]int64
}

func (d *overwriteDest) Capabilities() destination.Capabilities {
	c := d.countingDest.Capabilities()
	c.WriteModes = []destination.WriteMode{destination.Append, destination.Overwrite}
	return c
}

func (d *overwriteDest) Open(context.Context, destination.OpenConfig) (destination.Writer, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.start == nil {
		d.start, d.read, d.table = map[string]time.Time{}, map[string]int64{}, map[string]int64{}
	}
	d.written, d.ended = map[uint64]map[string]int64{}, map[uint64][]string{}
	return d, nil
}

func (d *overwriteDest) StartReads(_ context.Context, starts map[string]time.Time) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for s, at := range starts {
		if !d.start[s].Equal(at) {
			d.start[s], d.read[s] = at, 0
		}
	}
	return nil
}

func (d *overwriteDest) Write(_ context.Context, e destination.Epoch, s string, b arrow.RecordBatch) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.written[e.N] == nil {
		d.written[e.N] = map[string]int64{}
	}
	d.written[e.N][s] += b.NumRows()
	return nil
}

func (d *overwriteDest) EndRead(_ context.Context, e destination.Epoch, s string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ended[e.N] = append(d.ended[e.N], s)
	return nil
}

func (d *overwriteDest) Commit(_ context.Context, e destination.Epoch, _ []destination.Staged) (destination.Committed, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for s, n := range d.written[e.N] {
		d.read[s] += n
	}
	for _, s := range d.ended[e.N] {
		d.table[s] = d.read[s]
	}
	delete(d.written, e.N)
	delete(d.ended, e.N)
	return destination.Committed{}, nil
}

// A switch from incremental to full_refresh reads the whole stream, and one
// back goes on from where the full read completed.
func TestASwitchOfReadModeStartsTheReadItNeeds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		// Ordered, so that a completed read saves its highest cursor as the
		// next floor, without an unordered stream's one-read lag.
		stream := sdk.Incremental("s", "updated_at", func(_ *sdk.Ctx[struct{}], since sdk.Cursor, out *sdk.Out) error {
			from := 1
			if !since.IsZero() {
				from, _ = strconv.Atoi(since.String())
			}
			for i := from; i <= 5; i++ {
				if err := out.Emit(map[string]int{"id": i, "updated_at": i}); err != nil {
					return err
				}
			}
			return nil
		}, sdk.Ordered())
		bound, err := (&sdk.Source[struct{}]{Name: "src", Streams: []sdk.Stream[struct{}]{stream}}).New(nil)
		if err != nil {
			t.Fatal(err)
		}
		var starts []string
		src := startsSource{Reader: bound, starts: &starts}
		log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
		dest := &overwriteDest{}
		for n, s := range []runner.Stream{
			{Name: "s", Replay: source.Superset, Read: source.Incremental, Mode: destination.Append},
			{Name: "s", Replay: source.Superset, Read: source.FullRefresh, Mode: destination.Overwrite},
			{Name: "s", Replay: source.Superset, Read: source.Incremental, Mode: destination.Append},
		} {
			committed := make(chan uint64, 64)
			j := job(log, "a"+strconv.Itoa(n), src, runner.Passthrough{Convert: columnless}, committed)
			j.Sync, j.Destination, j.Settings.Require = "s"+strconv.Itoa(n), dest, delivery.AtLeastOnce
			j.Streams = []runner.Stream{s}
			if _, err := runner.RunAttempt(ctx, j); err != nil {
				t.Fatalf("sync %d: %v", n+1, err)
			}
			time.Sleep(time.Second)
		}
		if want := []string{"", "", `{"cursor":5}`}; !slices.Equal(starts, want) {
			t.Fatalf("the reads started from %q, want %q", starts, want)
		}
	})
}

// A switch of read or write mode while a read is unfinished starts a new
// read, with no state and a new start, where the unfinished one doesn't
// hold what the stream needs now: an overwrite that went on with an
// incremental read would replace the table with what changed since the
// last read. A read that keeps its mode goes on.
func TestASwitchOfModeDuringAReadStartsANewOne(t *testing.T) {
	incremental := runner.Stream{Name: "s", Replay: source.Superset, Read: source.Incremental, Mode: destination.Append}
	full := runner.Stream{Name: "s", Replay: source.Superset, Read: source.FullRefresh, Mode: destination.Append}
	overwrite := runner.Stream{Name: "s", Replay: source.Superset, Read: source.FullRefresh, Mode: destination.Overwrite}
	for _, tc := range []struct {
		name          string
		before, after runner.Stream
		// The state the read after the switch starts from, and the rows the
		// table has after it; -1 for an appended table, which isn't checked.
		from string
		rows int64
	}{
		{"incremental to overwrite", incremental, overwrite, "", 9},
		{"full_refresh to overwrite", full, overwrite, "", 9},
		{"overwrite to full_refresh", overwrite, full, "", -1},
		{"overwrite to incremental", overwrite, incremental, "", -1},
		{"full_refresh to incremental", full, incremental, "", -1},
		{"incremental kept", incremental, incremental, `{"cursor":6,"since":5,"page":"7"}`, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
				defer cancel()
				log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
				dest := &overwriteDest{}
				var starts []string
				upstream := 5
				// Reads pages of two records from the floor, and with fail,
				// fails on its second page, once an epoch is committed.
				attempt := func(sync string, fail bool, s runner.Stream) error {
					committed := make(chan uint64, 64)
					stream := sdk.Incremental("s", "updated_at", func(_ *sdk.Ctx[struct{}], since sdk.Cursor, out *sdk.Out) error {
						from := 1
						if !since.IsZero() {
							from, _ = strconv.Atoi(since.String())
						}
						// The token is the first id of the next page.
						pages := 0
						for page, err := range sdk.Paginate(out, func(_ context.Context, token string) ([]int, string, error) {
							start := from
							if token != "" {
								start, _ = strconv.Atoi(token)
							}
							var ids []int
							for i := start; i <= upstream && len(ids) < 2; i++ {
								ids = append(ids, i)
							}
							next := ""
							if start+2 <= upstream {
								next = strconv.Itoa(start + 2)
							}
							return ids, next, nil
						}) {
							if err != nil {
								return err
							}
							for _, i := range page {
								if err := out.Emit(map[string]int{"id": i, "updated_at": i}); err != nil {
									return err
								}
							}
							if pages++; fail && pages == 2 {
								select {
								case <-committed:
								case <-ctx.Done():
									return ctx.Err()
								}
								return errors.New("the source broke")
							}
						}
						return nil
					}, sdk.Ordered())
					bound, err := (&sdk.Source[struct{}]{Name: "src", Streams: []sdk.Stream[struct{}]{stream}}).New(nil)
					if err != nil {
						return err
					}
					j := job(log, "a"+strconv.Itoa(len(starts)), startsSource{Reader: bound, starts: &starts}, runner.Passthrough{Convert: columnless}, committed)
					j.Sync, j.Destination, j.Settings.Require = sync, dest, delivery.AtLeastOnce
					j.Streams = []runner.Stream{s}
					_, err = runner.RunAttempt(ctx, j)
					time.Sleep(time.Second)
					return err
				}
				if err := attempt("s0", false, tc.before); err != nil {
					t.Fatal(err)
				}
				upstream = 9
				if err := attempt("s1", true, tc.before); err == nil {
					t.Fatal("the failing attempt succeeded")
				}
				if err := attempt("s2", false, tc.after); err != nil {
					t.Fatal(err)
				}
				if len(starts) != 3 || starts[2] != tc.from {
					t.Fatalf("the reads started from %q, want the last from %q", starts, tc.from)
				}
				if got := dest.table["s"]; tc.rows >= 0 && got != tc.rows {
					t.Fatalf("the overwritten table has %d rows, want %d", got, tc.rows)
				}
			})
		})
	}
}

// startsSource keeps the state each read of its one stream starts from.
type startsSource struct {
	source.Reader
	starts *[]string
}

func (s startsSource) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	*s.starts = append(*s.starts, string(req.Streams[0].State))
	return s.Reader.Read(ctx, req, out)
}

// On workers too, a full refresh's read starts with no state, though its
// source goes on from where the last read's splits ended.
func TestAFullRefreshOnWorkersStartsWithNoState(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	var mu sync.Mutex
	var sources []*rangeSource
	p := newPool(t, nil)
	p.reader = func() source.Reader {
		rs := ranges(2, 5, 5)()
		mu.Lock()
		defer mu.Unlock()
		sources = append(sources, rs)
		return goesOnSource{rs}
	}
	p.change = func(j *runner.Job) {
		j.Sync = j.Owner
		j.Streams[0].Read = source.FullRefresh
	}
	var first int
	for _, owner := range []string{"a1", "a2"} {
		if _, err := runner.RunAttempt(ctx, p.workers(owner, 2)); err != nil {
			t.Fatalf("%s: %v", owner, err)
		}
		for i, err := range p.wait(owner) {
			if err != nil {
				t.Fatalf("%s's worker %d: %v", owner, i, err)
			}
		}
		mu.Lock()
		if first == 0 {
			first = len(sources)
		}
		mu.Unlock()
	}
	var from []string
	for _, rs := range sources[first:] {
		rs.mu.Lock()
		from = append(from, rs.from...)
		rs.mu.Unlock()
	}
	slices.Sort(from)
	if want := []string{"s0@", "s1@"}; !slices.Equal(from, want) {
		t.Fatalf("the second sync's splits started at %q, want %q", from, want)
	}
}

// On workers too, a switch to full_refresh while a read is unfinished
// starts a new read, with no state.
func TestASwitchOfModeOnWorkersDuringAReadStartsANewOne(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	// Each split waits after its first page, so the first attempt stops
	// with its read unfinished.
	gate := make(chan struct{})
	p := newPool(t, func() *rangeSource {
		s := ranges(2, 20, 5)()
		s.gate = gate
		return s
	})
	j := p.workers("a1", 2)
	var mu sync.Mutex
	positioned := map[uint64]bool{}
	j.OnCheckpoint = func(rec *checkpoint.Record, _, _ map[string]uint64) {
		mu.Lock()
		defer mu.Unlock()
		positioned[rec.Epoch] = slices.ContainsFunc(rec.Splits("s"), func(sp checkpoint.SplitState) bool { return sp.State != nil })
	}
	var once sync.Once
	j.OnCommit = func(n uint64) {
		mu.Lock()
		defer mu.Unlock()
		if positioned[n] {
			once.Do(func() { p.kill("a1", 0) })
		}
	}
	if _, err := runner.RunAttempt(ctx, j); err == nil {
		t.Fatal("the attempt went on without its worker")
	}
	p.wait("a1")
	close(gate)
	p.change = func(j *runner.Job) {
		j.Sync = j.Owner
		j.Streams[0].Read = source.FullRefresh
	}
	if _, err := runner.RunAttempt(ctx, p.workers("a2", 2)); err != nil {
		t.Fatal(err)
	}
	for i, err := range p.wait("a2") {
		if err != nil {
			t.Fatalf("worker %d: %v", i, err)
		}
	}
	var from []string
	for _, rs := range p.sources["a2"] {
		rs.mu.Lock()
		from = append(from, rs.from...)
		rs.mu.Unlock()
	}
	slices.Sort(from)
	if want := []string{"s0@", "s1@"}; !slices.Equal(from, want) {
		t.Fatalf("the full refresh's splits started at %q, want %q", from, want)
	}
}

// goesOnSource starts a read's splits where the last read's ended.
type goesOnSource struct{ *rangeSource }

func (s goesOnSource) Splits(ctx context.Context, cs source.ConfiguredStream, ended []source.Split) ([]source.Split, error) {
	if ended != nil {
		return ended, nil
	}
	return s.rangeSource.Splits(ctx, cs, ended)
}
