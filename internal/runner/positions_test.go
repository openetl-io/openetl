package runner_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/delivery"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// statesSource is an ackSource that keeps the state each read starts from.
type statesSource struct {
	ackSource
	starts *[]string
}

func (s statesSource) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	*s.starts = append(*s.starts, string(req.Streams[0].State))
	return s.ackSource.Read(ctx, req, out)
}

// runPages runs an attempt of a read of pages pages under setup, and
// returns the state it started from and its error.
func runPages(ctx context.Context, log *checkpoint.Log, owner string, pages int, setup func(*runner.Job)) (string, runner.Result, error) {
	committed := make(chan uint64, 16)
	var starts []string
	src := statesSource{ackSource: ackSource{pages: pages, committed: committed, acks: new([]string)}, starts: &starts}
	j := job(log, owner, src, runner.Passthrough{Convert: columnless}, committed)
	if setup != nil {
		setup(&j)
	}
	res, err := runner.RunAttempt(ctx, j)
	if len(starts) == 0 {
		return "", res, err
	}
	return starts[0], res, err
}

func TestResetsReadStreamsAgainOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
		if _, _, err := runPages(ctx, log, "a1", 3, nil); err != nil {
			t.Fatal(err)
		}
		reset := func(j *runner.Job) { j.Resets = []runner.Reset{{ID: "0001"}} }
		start, _, err := runPages(ctx, log, "a2", 3, reset)
		if err != nil {
			t.Fatal(err)
		}
		if start != "" {
			t.Fatalf("the reset read started from %q, want the start", start)
		}
		head, err := log.Latest(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if head.Reset != "0001" {
			t.Fatalf("HEAD's record carries reset %q, want 0001", head.Reset)
		}
		if start, _, err = runPages(ctx, log, "a3", 3, reset); err != nil {
			t.Fatal(err)
		}
		if start != "3" {
			t.Fatalf("a reset already taken read from %q again, want 3", start)
		}
		// A reset of another stream leaves s where it was.
		other := func(j *runner.Job) { j.Resets = []runner.Reset{{ID: "0001"}, {ID: "0002", Streams: []string{"t"}}} }
		if start, _, err = runPages(ctx, log, "a4", 3, other); err != nil {
			t.Fatal(err)
		}
		if start != "3" {
			t.Fatalf("a reset of another stream read s from %q, want 3", start)
		}
		if head, err = log.Latest(ctx); err != nil || head.Reset != "0002" {
			t.Fatalf("HEAD's record carries reset %q (%v), want 0002", head.Reset, err)
		}
	})
}

// unreadHead deletes the record HEAD points to, and returns its ref and the
// one before it.
func unreadHead(ctx context.Context, t *testing.T, store checkpoint.Store, log *checkpoint.Log) (checkpoint.RecordRef, checkpoint.RecordRef) {
	t.Helper()
	head, err := log.Latest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if head.Previous == nil {
		t.Fatal("HEAD's record is the first")
	}
	key := fmt.Sprintf("org/p/_checkpoints/%020d-g%020d.json", head.Epoch, head.Generation)
	if err := store.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	if _, err := log.Latest(ctx); !errors.Is(err, checkpoint.ErrUnreadable) {
		t.Fatalf("Latest after deleting HEAD's record: %v, want ErrUnreadable", err)
	}
	return head.Ref(), *head.Previous
}

func TestRewindAfterAnUnreadableRecord(t *testing.T) {
	cases := []struct {
		name    string
		require delivery.Level
		rewind  runner.Rewind
		// Empty: it restarts from the record before.
		fails string
	}{
		{"never", delivery.AtLeastOnce, runner.Rewind{}, "heal mode leaves to a person"},
		{"safe, and the replay lowers the guarantee", delivery.ExactlyOnce, runner.Rewind{Policy: runner.RewindSafe}, `stream "s" would get rows twice`},
		{"safe, and at least once anyway", delivery.AtLeastOnce, runner.Rewind{Policy: runner.RewindSafe}, ""},
		{"approved", delivery.ExactlyOnce, runner.Rewind{Policy: runner.RewindAny, Approval: "r1"}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
				defer cancel()
				store := checkpoint.NewMemStore()
				log := checkpoint.NewLog(store, "org", "p")
				// Pages commit one by one, and HEAD ends at the last.
				if _, _, err := runPages(ctx, log, "a1", 4, nil); err != nil {
					t.Fatal(err)
				}
				unreadable, before := unreadHead(ctx, t, store, log)
				prev, err := log.Record(ctx, before)
				if err != nil {
					t.Fatal(err)
				}
				var rewound []runner.Rewound
				start, _, err := runPages(ctx, log, "a2", 4, func(j *runner.Job) {
					j.Settings.Require, j.Rewind = tc.require, tc.rewind
					j.OnRewind = func(r runner.Rewound) { rewound = append(rewound, r) }
				})
				if tc.fails != "" {
					if errs.CodeOf(err) != errs.CheckpointCorrupt || !strings.Contains(err.Error(), tc.fails) {
						t.Fatalf("err = %v, want CHECKPOINT_CORRUPT saying %q", err, tc.fails)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if want := string(prev.Streams["s"].State); start != want {
					t.Fatalf("the read started from %q, want the record before's %q", start, want)
				}
				if len(rewound) != 1 || rewound[0].Unreadable != unreadable || rewound[0].Restored == nil || *rewound[0].Restored != before ||
					rewound[0].Approval != tc.rewind.Approval {
					t.Fatalf("rewound %+v, want once from %v to %v", rewound, unreadable, before)
				}
				// A newer generation's records follow the one restored.
				rec, err := log.Latest(ctx)
				for err == nil && rec.Previous != nil && *rec.Previous != before {
					rec, err = log.Record(ctx, *rec.Previous)
				}
				if err != nil {
					t.Fatal(err)
				}
				if rec.Previous == nil || *rec.Previous != before || rec.Generation <= unreadable.Generation {
					t.Fatalf("the record after the restart is %d-g%d after %v; want a newer generation's after %v", rec.Epoch, rec.Generation, rec.Previous, before)
				}
			})
		})
	}
}
