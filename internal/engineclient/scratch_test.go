package engineclient

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/delivery"
	"github.com/openetl-io/openetl/internal/platform/resources"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/internal/sqlplan"
	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// wideSource reads n rows of distinct keys in one minute, a thousand at a
// time, and then each tenth key again.
type wideSource struct{ n int }

func wideKey(i int) string { return fmt.Sprintf("key-%08d-%s", i, strings.Repeat("x", 40)) }

func (s *wideSource) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	return source.ReadSplits(ctx, req, out, func(_ context.Context, _ source.ConfiguredStream, _ source.Split, e source.SplitEmitter) error {
		row := func(i int) json.RawMessage {
			return json.RawMessage(fmt.Sprintf(`{"id": %d, "region": %q, "at": %q}`, i, wideKey(i), at(minutes+int64(i%60)*1_000_000)))
		}
		var keys []int
		for i := range s.n {
			keys = append(keys, i)
		}
		for i := 0; i < s.n; i += 10 {
			keys = append(keys, i)
		}
		for at := 0; at < len(keys); at += 1000 {
			var rows []json.RawMessage
			for _, k := range keys[at:min(at+1000, len(keys))] {
				rows = append(rows, row(k))
			}
			if err := e.Rows(rows); err != nil {
				return err
			}
			if err := e.ResumePoint([]byte(strconv.Itoa(at))); err != nil {
				return err
			}
		}
		return e.Complete([]byte("done"))
	})
}

// A group window run as the runner's activities run one, with the job's
// size and scratch set on the engine as a runner's are: its lanes flush
// their changes early as they pass their part of the engine's memory, and
// later reads of those windows come from the disk cache. Every window's
// row is right; the engine's directory, its scratch with it, is gone once
// the job ends, and so is one a dead runner left, while a live runner's
// stays.
func TestAWindowsStateGoesToScratchAndOutOfMemory(t *testing.T) {
	// Short, for the engine's sockets.
	tmp, err := os.MkdirTemp("", "s")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(tmp) })
	t.Setenv("TMPDIR", tmp)
	// One a dead runner left, its lock let go, and one a live runner holds.
	dead, live := filepath.Join(tmp, "oe-dead"), filepath.Join(tmp, "oe-live")
	for _, d := range []string{dead, live} {
		if err := os.MkdirAll(filepath.Join(d, scratchDir, "state"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(d, dirLock), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	held, err := os.Open(filepath.Join(live, dirLock))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Close() }()
	if err := syscall.Flock(int(held.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}

	opts := options(t, nil)
	opts.Size = resources.Size{CPU: 4000, Memory: 64 * resources.MiB}
	opts.Scratch = 256 * resources.MiB
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "region", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
	}, nil)
	declared, err := arrowipc.EncodeSchema(schema)
	if err != nil {
		t.Fatal(err)
	}
	const sql = "INSERT INTO minutes SELECT window_start, region, count(*) AS n, sum(id) AS total FROM s GROUP BY tumble(at, INTERVAL '1 minute'), region"
	a, err := Plan(ctx, planEngine(t), sqlplan.Request{SQL: sql, Streams: []sqlplan.Stream{{Name: "s", DeclaredSchema: declared, EventTime: "at"}}})
	if err != nil || len(a.Errors) != 0 {
		t.Fatal(a.Errors, err)
	}
	exp := expected(t, a)
	exp[0].Operator = a.Statements[0].Operator

	const keys = 30_000
	stateDir := t.TempDir()
	var mu sync.Mutex
	var named int
	var disk checkpoint.Requests
	var dirs []string
	d := newKeyDest(1)
	job := runner.Job{
		Pipeline: "scratch", Owner: "a1", Sync: "s1",
		Log: checkpoint.NewLog(checkpoint.NewMemStore(), "org", "scratch"), Source: &wideSource{n: keys},
		Streams: []runner.Stream{{
			Name: "s", Replay: source.Exact, Mode: destination.Append, Read: source.FullRefresh, Schema: schema,
			EventTime: &runner.EventTime{Column: "at", Held: true, Idle: 300 * time.Millisecond},
		}},
		Outputs:     []runner.Stream{{Name: "minutes", Replay: source.Exact, Mode: destination.Merge, PrimaryKey: []string{"window_start", "region"}, Transformed: true, From: "s"}},
		SQL:         &runner.SQLTransform{SQL: sql, Expected: exp, Operators: []string{a.Statements[0].Operator}},
		Destination: d, Dataflow: &Dataflow{Options: opts}, Lanes: opts.Size.Threads(), Logger: opts.Logger,
		StateURL: "file://" + stateDir,
		Settings: delivery.Settings{Require: delivery.AtLeastOnce, CommitInterval: time.Minute, Scope: destination.ScopePipeline},
		OnCheckpoint: func(rec *checkpoint.Record, _, _ map[string]uint64) {
			mu.Lock()
			defer mu.Unlock()
			for _, s := range rec.Snapshots {
				for _, r := range s.Runs {
					if strings.Contains(r, ".early") {
						named++
					}
				}
			}
			for _, o := range rec.Operators {
				disk.Count += o.Disk.Count
				disk.Bytes += o.Disk.Bytes
			}
			got, _ := filepath.Glob(filepath.Join(tmp, "oe-*", scratchDir, "state"))
			dirs = append(dirs, got...)
		},
	}
	if _, err := runner.RunAttempt(ctx, job); err != nil {
		t.Fatal(err)
	}

	rows := d.applied("minutes")
	if len(rows) != keys {
		t.Fatalf("%d windows' rows, not %d", len(rows), keys)
	}
	for _, r := range rows {
		k := strings.TrimPrefix(r.cols["region"], "key-")
		i, err := strconv.Atoi(k[:8])
		if err != nil {
			t.Fatal(r.cols)
		}
		n := 1
		if i%10 == 0 {
			n = 2
		}
		if r.cols["n"] != strconv.Itoa(n) || r.cols["total"] != strconv.Itoa(n*i) {
			t.Fatalf("window of %s: %v", r.cols["region"], r.cols)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	// The one record is at the end of input, where every window has gone
	// and it names no state: the early runs are on storage, for retention.
	var early int
	_ = filepath.WalkDir(stateDir, func(p string, _ os.DirEntry, _ error) error {
		if strings.Contains(filepath.Base(p), ".early") {
			early++
		}
		return nil
	})
	if early == 0 {
		t.Error("no lane flushed early")
	}
	if disk.Count == 0 {
		t.Error("the disk cache answered no read")
	}
	if len(dirs) == 0 {
		t.Error("the engine had no scratch while it ran")
	}
	if left, _ := filepath.Glob(filepath.Join(tmp, "oe-*")); len(left) != 1 || left[0] != live {
		t.Errorf("left in the temporary directory: %v", left)
	}
	t.Logf("%d early runs, %d named, %d reads from disk (%d bytes)", early, named, disk.Count, disk.Bytes)
}
