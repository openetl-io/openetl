// Package attemptlog keeps the logs of syncs' attempts in the data plane's
// object storage, next to the pipeline's checkpoints, as JSON lines:
// {org}/{pipeline}/_logs/{sync}/{task}/{chunk}.jsonl. A writer rewrites its
// last chunk as it grows, so an attempt's log can be read while it runs.
package attemptlog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/openetl-io/openetl/internal/checkpoint"
)

const Dir = "_logs"

const (
	chunkSize = 1 << 20
	// An attempt's log stops here, so a chatty connector can't fill the
	// runner's memory while storage is down, or the bucket.
	maxBytes   = 32 << 20
	flushEvery = 5 * time.Second
	putTimeout = 30 * time.Second
)

// SyncPrefix holds a sync's logs.
func SyncPrefix(org, pipeline, sync string) string {
	return org + "/" + pipeline + "/" + Dir + "/" + sync + "/"
}

// Task names an attempt's directory, so they sort in the order they ran:
// attempt-{n} for the sync's attempts, step-{s}-attempt-{n} for a post-sync
// step's.
func Task(step, attempt int) string {
	if step == 0 {
		return fmt.Sprintf("attempt-%03d", attempt)
	}
	return fmt.Sprintf("step-%02d-attempt-%03d", step, attempt)
}

// ParseTask is Task's inverse.
func ParseTask(task string) (step, attempt int, ok bool) {
	if _, err := fmt.Sscanf(task, "attempt-%d", &attempt); err == nil && Task(0, attempt) == task {
		return 0, attempt, true
	}
	if _, err := fmt.Sscanf(task, "step-%d-attempt-%d", &step, &attempt); err == nil && step > 0 && Task(step, attempt) == task {
		return step, attempt, true
	}
	return 0, 0, false
}

type chunk struct {
	key  string
	data []byte
	// How much of data is in storage, and its version there; under
	// Writer.mu and Writer.flushMu respectively.
	written int
	version checkpoint.Version
}

// Writer is an attempt's log. Its Handler's records go to storage every few
// seconds, and when it closes. Failing to write them doesn't fail the
// attempt.
type Writer struct {
	store  checkpoint.Store
	prefix string
	log    *slog.Logger

	mu     sync.Mutex
	chunks []*chunk // not all written; the last is open
	next   int
	total  int
	full   bool
	closed bool

	flushMu sync.Mutex
	warned  bool

	stop, done chan struct{}
}

// NewWriter writes an attempt's log under prefix, SyncPrefix and Task. log
// gets the writer's own failures.
func NewWriter(store checkpoint.Store, prefix string, log *slog.Logger) *Writer {
	w := &Writer{store: store, prefix: prefix, log: log, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(w.done)
		t := time.NewTicker(flushEvery)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				w.flush(context.Background())
			case <-w.stop:
				return
			}
		}
	}()
	return w
}

// Handler writes Info and above to the log.
func (w *Writer) Handler() slog.Handler {
	return slog.NewJSONHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo})
}

// Write takes one line; slog's JSON handler writes a record at a time.
func (w *Writer) Write(p []byte) (int, error) {
	n := len(p)
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.full {
		return n, nil
	}
	if w.total+len(p) > maxBytes {
		p = fmt.Appendf(nil, `{"time":%q,"level":"WARN","msg":"the attempt's log stops here, at %d MiB"}`+"\n", time.Now().Format(time.RFC3339Nano), maxBytes>>20)
		w.full = true
	}
	w.total += len(p)
	var c *chunk
	if len(w.chunks) > 0 {
		c = w.chunks[len(w.chunks)-1]
	}
	if c == nil || (len(c.data) > 0 && len(c.data)+len(p) > chunkSize) {
		c = &chunk{key: fmt.Sprintf("%s%06d.jsonl", w.prefix, w.next)}
		w.next++
		w.chunks = append(w.chunks, c)
	}
	c.data = append(c.data, p...)
	return n, nil
}

// Close writes what's left. Lines after it are dropped.
func (w *Writer) Close(ctx context.Context) {
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	close(w.stop)
	<-w.done
	w.flush(ctx)
}

func (w *Writer) flush(ctx context.Context) {
	w.flushMu.Lock()
	defer w.flushMu.Unlock()
	type put struct {
		c    *chunk
		data []byte
	}
	var puts []put
	w.mu.Lock()
	for _, c := range w.chunks {
		if len(c.data) > c.written {
			puts = append(puts, put{c, c.data[:len(c.data):len(c.data)]})
		}
	}
	w.mu.Unlock()
	for _, p := range puts {
		pctx, cancel := context.WithTimeout(ctx, putTimeout)
		err := w.put(pctx, p.c, p.data)
		cancel()
		if err != nil {
			if !w.warned {
				w.warned = true
				w.log.Warn("writing the attempt's log failed; it will have gaps if this goes on", "key", p.c.key, "err", err)
			}
			return
		}
		w.mu.Lock()
		p.c.written = len(p.data)
		w.mu.Unlock()
	}
	w.mu.Lock()
	for len(w.chunks) > 1 && w.chunks[0].written == len(w.chunks[0].data) {
		w.chunks = w.chunks[1:]
	}
	w.mu.Unlock()
}

func (w *Writer) put(ctx context.Context, c *chunk, data []byte) error {
	var err error
	for range 2 {
		var v checkpoint.Version
		if c.version == "" {
			v, err = w.store.Create(ctx, c.key, data)
		} else {
			v, err = w.store.Swap(ctx, c.key, data, c.version)
		}
		if err == nil {
			c.version = v
			return nil
		}
		if !errors.Is(err, checkpoint.ErrPrecondition) {
			return err
		}
		// A write that failed was applied, so the version is stale.
		if _, c.version, err = w.store.Get(ctx, c.key); err != nil {
			return err
		}
	}
	return err
}

// Line is a line of a sync's log.
type Line struct {
	Task string
	// A JSON object.
	Text string
}

// Cursor is where a read ended: a chunk, and how many of its lines were
// read. The zero Cursor is the start.
type Cursor struct {
	Key   string
	Lines int
}

// Read returns the lines of the sync's log under prefix after c, up to limit
// bytes but at least one line, and where they end. Attempts' chunks are
// read in turn.
func Read(ctx context.Context, store checkpoint.Store, prefix string, c Cursor, limit int) ([]Line, Cursor, error) {
	keys, err := store.List(ctx, prefix)
	if err != nil {
		return nil, c, err
	}
	var lines []Line
	size := 0
	for _, k := range keys {
		task, name, ok := strings.Cut(strings.TrimPrefix(k, prefix), "/")
		if !ok || !strings.HasSuffix(name, ".jsonl") || k < c.Key {
			continue
		}
		skip := 0
		if k == c.Key {
			skip = c.Lines
		}
		data, _, err := store.Get(ctx, k)
		if errors.Is(err, checkpoint.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, c, err
		}
		n := 0
		for len(data) > 0 {
			var line []byte
			line, data, _ = bytes.Cut(data, []byte{'\n'})
			if n++; n <= skip {
				continue
			}
			if len(lines) > 0 && size+len(line) > limit {
				return lines, Cursor{k, n - 1}, nil
			}
			lines = append(lines, Line{Task: task, Text: string(line)})
			size += len(line)
		}
		c = Cursor{k, n}
	}
	return lines, c, nil
}

// Prune deletes the logs of a pipeline's syncs that started before before.
// Syncs are named by UUIDv7s, which hold when they started; it leaves other
// names alone.
func Prune(ctx context.Context, store checkpoint.Store, org, pipeline string, before time.Time) (deleted int, err error) {
	prefix := org + "/" + pipeline + "/" + Dir + "/"
	keys, err := store.List(ctx, prefix)
	if err != nil {
		return 0, err
	}
	for _, k := range keys {
		name, _, _ := strings.Cut(strings.TrimPrefix(k, prefix), "/")
		id, err := uuid.Parse(name)
		if err != nil || id.Version() != 7 {
			continue
		}
		if sec, nsec := id.Time().UnixTime(); !time.Unix(sec, nsec).Before(before) {
			continue
		}
		if err := store.Delete(ctx, k); err != nil && !errors.Is(err, checkpoint.ErrNotFound) {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}
