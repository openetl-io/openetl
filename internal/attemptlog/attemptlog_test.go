package attemptlog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/openetl-io/openetl/internal/checkpoint"
)

const syncID = "019a4b2e-0000-7000-8000-000000000001"

func newWriter(store checkpoint.Store, step, attempt int) *Writer {
	return NewWriter(store, SyncPrefix("org", "pipe", syncID)+Task(step, attempt)+"/", slog.New(slog.DiscardHandler))
}

func readAll(t *testing.T, store checkpoint.Store, c Cursor, limit int) ([]Line, Cursor) {
	t.Helper()
	var all []Line
	for {
		lines, next, err := Read(t.Context(), store, SyncPrefix("org", "pipe", syncID), c, limit)
		if err != nil {
			t.Fatal(err)
		}
		if len(lines) == 0 {
			return all, next
		}
		all, c = append(all, lines...), next
	}
}

func messages(t *testing.T, lines []Line) []string {
	t.Helper()
	var out []string
	for _, l := range lines {
		var m struct{ Msg string }
		if err := json.Unmarshal([]byte(l.Text), &m); err != nil {
			t.Fatalf("%q: %v", l.Text, err)
		}
		out = append(out, l.Task+" "+m.Msg)
	}
	return out
}

// A read while the attempt runs gets what was flushed, and the next read
// from its cursor only what came after, through the next attempt's log.
func TestReadFollowsTheAttempts(t *testing.T) {
	store := checkpoint.NewMemStore()
	w := newWriter(store, 0, 1)
	log := slog.New(w.Handler()).With("component", "engine")
	log.Info("one")
	log.Debug("not kept")
	log.Warn("two", "n", 2)
	w.flush(t.Context())
	got, c := readAll(t, store, Cursor{}, 1<<20)
	if want := []string{"attempt-001 one", "attempt-001 two"}; !slices.Equal(messages(t, got), want) {
		t.Fatalf("got %v, want %v", messages(t, got), want)
	}
	if !strings.Contains(got[1].Text, `"component":"engine"`) || !strings.Contains(got[1].Text, `"n":2`) {
		t.Errorf("attrs missing: %s", got[1].Text)
	}

	log.Info("three")
	w.Close(t.Context())
	log.Info("after close")
	w2 := newWriter(store, 0, 2)
	slog.New(w2.Handler()).Info("four")
	w2.Close(t.Context())
	w3 := newWriter(store, 1, 1)
	slog.New(w3.Handler()).Info("five")
	w3.Close(t.Context())

	got, _ = readAll(t, store, c, 1<<20)
	if want := []string{"attempt-001 three", "attempt-002 four", "step-01-attempt-001 five"}; !slices.Equal(messages(t, got), want) {
		t.Fatalf("got %v, want %v", messages(t, got), want)
	}
}

// Small pages hold one line each, and together the whole log.
func TestReadPages(t *testing.T) {
	store := checkpoint.NewMemStore()
	w := newWriter(store, 0, 1)
	log := slog.New(w.Handler())
	var want []string
	for i := range 10 {
		log.Info(fmt.Sprint("line ", i))
		want = append(want, fmt.Sprint("attempt-001 line ", i))
	}
	w.Close(t.Context())
	var got []Line
	c := Cursor{}
	for {
		lines, next, err := Read(t.Context(), store, SyncPrefix("org", "pipe", syncID), c, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(lines) == 0 {
			break
		}
		if len(lines) != 1 {
			t.Fatalf("a page of %d lines", len(lines))
		}
		got, c = append(got, lines...), next
	}
	if !slices.Equal(messages(t, got), want) {
		t.Fatalf("got %v, want %v", messages(t, got), want)
	}
}

func TestChunksRollOver(t *testing.T) {
	store := checkpoint.NewMemStore()
	w := newWriter(store, 0, 1)
	line := append(bytes.Repeat([]byte("x"), 100<<10), '\n')
	for range 25 {
		_, _ = w.Write(line)
		w.flush(t.Context())
	}
	w.Close(t.Context())
	keys, err := store.List(t.Context(), SyncPrefix("org", "pipe", syncID))
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 3 || !strings.HasSuffix(keys[2], "/attempt-001/000002.jsonl") {
		t.Fatalf("keys %v, want 3 chunks", keys)
	}
	if got, _ := readAll(t, store, Cursor{}, 1<<20); len(got) != 25 {
		t.Fatalf("read %d lines, want 25", len(got))
	}
	if len(w.chunks) != 1 {
		t.Errorf("the writer holds %d chunks, want only the open one", len(w.chunks))
	}
}

func TestLogStopsAtItsLimit(t *testing.T) {
	store := checkpoint.NewMemStore()
	w := newWriter(store, 0, 1)
	line := append(bytes.Repeat([]byte("x"), 1<<20-1), '\n')
	for range maxBytes>>20 + 3 {
		_, _ = w.Write(line)
		w.flush(t.Context())
	}
	w.Close(t.Context())
	got, _ := readAll(t, store, Cursor{}, 4<<20)
	if len(got) != maxBytes>>20+1 {
		t.Fatalf("%d lines, want %d and the note", len(got), maxBytes>>20)
	}
	if last := got[len(got)-1].Text; !strings.Contains(last, "stops here") {
		t.Fatalf("the last line is %.80q", last)
	}
}

// failOnce applies the first Create, then fails it.
type failOnce struct {
	checkpoint.Store
	failed bool
}

func (f *failOnce) Create(ctx context.Context, key string, data []byte) (checkpoint.Version, error) {
	v, err := f.Store.Create(ctx, key, data)
	if !f.failed {
		f.failed = true
		return "", errors.New("timeout")
	}
	return v, err
}

// A write that failed but was applied leaves a stale version, which the
// next flush reads before it rewrites the chunk.
func TestFlushRecoversFromAnAppliedFailure(t *testing.T) {
	store := &failOnce{Store: checkpoint.NewMemStore()}
	w := newWriter(store, 0, 1)
	log := slog.New(w.Handler())
	log.Info("one")
	w.flush(t.Context())
	log.Info("two")
	w.Close(t.Context())
	got, _ := readAll(t, store, Cursor{}, 1<<20)
	if want := []string{"attempt-001 one", "attempt-001 two"}; !slices.Equal(messages(t, got), want) {
		t.Fatalf("got %v, want %v", messages(t, got), want)
	}
}

func TestPrune(t *testing.T) {
	store := checkpoint.NewMemStore()
	now := time.Now()
	v7At := func(at time.Time) string {
		id := uuid.Must(uuid.NewV7())
		ms := uint64(at.UnixMilli())
		for i := range 6 {
			id[i] = byte(ms >> (40 - 8*i))
		}
		return id.String()
	}
	old, recent, v4 := v7At(now.Add(-40*24*time.Hour)), v7At(now.Add(-time.Hour)), uuid.NewString()
	for _, s := range []string{old, recent, v4} {
		for _, task := range []string{Task(0, 1), Task(1, 1)} {
			if _, err := store.Create(t.Context(), SyncPrefix("org", "pipe", s)+task+"/000000.jsonl", []byte("{}\n")); err != nil {
				t.Fatal(err)
			}
		}
	}
	n, err := Prune(t.Context(), store, "org", "pipe", now.Add(-30*24*time.Hour))
	if err != nil || n != 2 {
		t.Fatalf("deleted %d, error %v; want 2", n, err)
	}
	keys, _ := store.List(t.Context(), "org/pipe/"+Dir+"/")
	for _, k := range keys {
		if strings.Contains(k, old) {
			t.Errorf("%s survived", k)
		}
	}
	if len(keys) != 4 {
		t.Errorf("%d keys left, want 4", len(keys))
	}
}

func TestTask(t *testing.T) {
	for _, c := range [][2]int{{0, 1}, {0, 12}, {3, 2}} {
		step, attempt, ok := ParseTask(Task(c[0], c[1]))
		if !ok || step != c[0] || attempt != c[1] {
			t.Errorf("%v: got %d %d %v", c, step, attempt, ok)
		}
	}
	for _, bad := range []string{"attempt-1", "step-00-attempt-001", "x"} {
		if _, _, ok := ParseTask(bad); ok {
			t.Errorf("%q parsed", bad)
		}
	}
}
