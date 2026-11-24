package engineclient

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/delivery"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// typeDest refuses v as anything but a bigint until it takes the change,
// as a destination with typeChanges: refuse does until its column is
// altered, and keeps each committed row's v by id.
type typeDest struct {
	*holdDest
	takes, noHolds bool
	pending        map[uint64]map[int64]string
	vs             map[int64]string
}

func newTypeDest() *typeDest {
	return &typeDest{holdDest: &holdDest{memDest: newMemDest()}, pending: map[uint64]map[int64]string{}, vs: map[int64]string{}}
}

func (d *typeDest) Capabilities() destination.Capabilities {
	c := d.holdDest.Capabilities()
	c.WriteModes = []destination.WriteMode{destination.Append, destination.Overwrite, destination.Merge}
	if d.noHolds {
		c.Holds = destination.Holds{}
	}
	return c
}

func (d *typeDest) Open(context.Context, destination.OpenConfig) (destination.Writer, error) {
	return d, nil
}

func (d *typeDest) ApplySchema(_ context.Context, streams []destination.StreamSchema) (destination.SchemaApplied, error) {
	var a destination.SchemaApplied
	for _, s := range streams {
		if fs := s.Schema.FieldIndices("v"); len(fs) == 1 && s.Schema.Field(fs[0]).Type.ID() != arrow.INT64 && !d.takes {
			a.Refused = append(a.Refused, destination.RefusedColumn{
				Stream: s.Name, Column: "v", FromType: "bigint", ToType: "text", Reason: "it doesn't change a column's type",
				Fix: "ALTER TABLE users ALTER COLUMN v TYPE text",
			})
		}
	}
	return a, nil
}

func (d *typeDest) Write(ctx context.Context, e destination.Epoch, stream string, batch arrow.RecordBatch) error {
	ids, vs := batch.Schema().FieldIndices("id"), batch.Schema().FieldIndices("v")
	if len(ids) == 1 && len(vs) == 1 {
		d.mu.Lock()
		if d.pending[e.N] == nil {
			d.pending[e.N] = map[int64]string{}
		}
		for i := range int(batch.NumRows()) {
			v := "<null>"
			if col := batch.Column(vs[0]); !col.IsNull(i) {
				v = col.ValueStr(i)
			}
			d.pending[e.N][batch.Column(ids[0]).(*array.Int64).Value(i)] = v
		}
		d.mu.Unlock()
	}
	return d.memDest.Write(ctx, e, stream, batch)
}

func (d *typeDest) Patch(_ context.Context, _ destination.Epoch, _ string, batch arrow.RecordBatch) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	col := batch.Column(batch.Schema().FieldIndices("v")[0])
	for i := range int(batch.NumRows()) {
		d.patched = append(d.patched, col.ValueStr(i))
	}
	return nil
}

func (d *typeDest) Commit(ctx context.Context, e destination.Epoch, staged []destination.Staged) (destination.Committed, error) {
	d.mu.Lock()
	for id, v := range d.pending[e.N] {
		d.vs[id] = v
	}
	delete(d.pending, e.N)
	d.mu.Unlock()
	return d.memDest.Commit(ctx, e, staged)
}

func (d *typeDest) Recover(context.Context, uint64, destination.Epoch, []destination.Staged) (destination.Recovery, error) {
	d.mu.Lock()
	clear(d.pending)
	clear(d.memDest.pending)
	d.mu.Unlock()
	return destination.Applied, nil
}

// A column held whole and discarded drops its values until it's released:
// a release, once the destination takes the change, delivers its later
// values as the stream has them, and patches in none of those it dropped.
func TestADiscardedColumnHoldEndsWhenReleased(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	opts := options(t, mem)
	defer mem.AssertSize(t, 0)
	ctx := testContext(t)

	dest := newTypeDest()
	root := t.TempDir()
	store, err := checkpoint.NewFSStore(root)
	if err != nil {
		t.Fatal(err)
	}
	log := checkpoint.NewLog(store, "org", "pipe")
	n := 0
	sync := func(releases []runner.Release, page ...string) {
		t.Helper()
		n++
		j := runner.Job{
			Owner: fmt.Sprintf("a%d", n), Pipeline: "pipe", Log: log, Destination: dest, Dataflow: &Dataflow{Options: opts},
			Source:  &pagedSource{pages: map[string][][]string{"users": {page}}, afterPage: dest.waitCommits},
			HeldURL: "file://" + filepath.Join(root, "org", "pipe", checkpoint.HeldDir),
			Streams: []runner.Stream{{
				Name: "users", Replay: source.Exact, Mode: destination.Append, PrimaryKey: []string{"id"},
				Holds: &runner.StreamHolds{},
			}},
			Releases: releases,
			Settings: delivery.Settings{Require: delivery.AtLeastOnce, Scope: destination.ScopePipeline, CommitMaxBytes: 1},
		}
		if _, err := runner.RunAttempt(ctx, j); err != nil {
			t.Fatalf("sync %d: %v", n, err)
		}
	}
	holds := func() *checkpoint.Holds {
		t.Helper()
		head, err := log.Latest(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return head.Streams["users"].Holds
	}

	sync(nil, `{"id": 1, "name": "a", "v": 1}`)
	sync(nil, `{"id": 2, "name": "b", "v": "x"}`)
	h := holds()
	if h == nil || len(h.Columns) != 1 || h.Columns[0].Column != "v" {
		t.Fatalf("v isn't held: %+v", h)
	}
	since := h.Columns[0].Since
	sync([]runner.Release{{ID: "d", Stream: "users", Column: "v", Since: since, Discard: true}}, `{"id": 3, "name": "c", "v": "y"}`)
	if h := holds(); h == nil || len(h.Columns) != 1 || !h.Columns[0].Discarded {
		t.Fatalf("v's hold isn't discarded: %+v", h)
	}
	if dest.vs[3] != "<null>" {
		t.Fatalf("row 3's v is %s while v's hold is discarded", dest.vs[3])
	}

	dest.takes = true
	sync([]runner.Release{{ID: "r", Stream: "users", Column: "v", Since: since}}, `{"id": 4, "name": "d", "v": "z"}`)
	if h := holds(); h != nil {
		t.Errorf("the discarded hold didn't end at its release: %+v", h)
	}
	if dest.vs[4] != "z" {
		t.Errorf("row 4's v is %s after the discarded hold's release, not z", dest.vs[4])
	}
	if len(dest.patched) != 0 {
		t.Errorf("the release patched in %v, which the discard dropped", dest.patched)
	}
}

// heldFiles are the held files below root's pipeline.
func heldFiles(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(filepath.Join(root, "org", "pipe", checkpoint.HeldDir), func(path string, d fs.DirEntry, err error) error {
		if errors.Is(err, fs.ErrNotExist) {
			return filepath.SkipDir
		}
		if err == nil && !d.IsDir() && strings.HasSuffix(path, ".parquet") {
			out = append(out, path)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// A version or a catalog under which a stream with open holds holds no
// longer fails the sync with how to go on, and keeps the holds and their
// files, until they're discarded; one that overwrites the stream reads its
// values again, so its holds go. Each change comes with the destination
// taking the column the stream held, as after its fix, or at a new one.
func TestAStreamThatStopsHoldingKeepsItsHolds(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*runner.Job, *typeDest)
		why    string
		// The stream's held values are read again, so its holds go.
		reread bool
	}{
		{name: "removed", change: func(j *runner.Job, _ *typeDest) { j.Streams = j.Streams[1:] }, why: "the pipeline doesn't read it now"},
		{name: "a write mode the destination doesn't patch", change: func(j *runner.Job, _ *typeDest) {
			j.Streams[0].Mode, j.Streams[0].Holds = destination.Merge, nil
		}, why: "the destination doesn't patch a stream it writes with merge"},
		{name: "no key", change: func(j *runner.Job, _ *typeDest) { j.Streams[0].PrimaryKey, j.Streams[0].Holds = nil, nil }, why: "it has no primary key"},
		{name: "a destination without holds", change: func(j *runner.Job, d *typeDest) {
			d.noHolds, j.Streams[0].Holds, j.Streams[1].Holds = true, nil, nil
		}, why: "the destination doesn't patch"},
		{name: "no held storage", change: func(j *runner.Job, _ *typeDest) { j.HeldURL = "" }, why: "no storage for held values"},
		{name: "overwritten", change: func(j *runner.Job, _ *typeDest) { j.Streams[0].Mode, j.Streams[0].Holds = destination.Overwrite, nil }, reread: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			opts := options(t, mem)
			defer mem.AssertSize(t, 0)
			ctx := testContext(t)

			dest := newTypeDest()
			root := t.TempDir()
			store, err := checkpoint.NewFSStore(root)
			if err != nil {
				t.Fatal(err)
			}
			log := checkpoint.NewLog(store, "org", "pipe")
			n := 0
			job := func(page string) runner.Job {
				n++
				return runner.Job{
					Owner: fmt.Sprintf("a%d", n), Pipeline: "pipe", Log: log, Destination: dest, Dataflow: &Dataflow{Options: opts},
					Source: &pagedSource{pages: map[string][][]string{
						"users": {{page}}, "teams": {{fmt.Sprintf(`{"id": %d, "name": "t"}`, 100+n)}},
					}, afterPage: dest.waitCommits},
					HeldURL: "file://" + filepath.Join(root, "org", "pipe", checkpoint.HeldDir),
					Streams: []runner.Stream{
						{Name: "users", Replay: source.Exact, Mode: destination.Append, PrimaryKey: []string{"id"}, Holds: &runner.StreamHolds{}},
						{Name: "teams", Replay: source.Exact, Mode: destination.Append, PrimaryKey: []string{"id"}, Holds: &runner.StreamHolds{}},
					},
					Settings: delivery.Settings{Require: delivery.AtLeastOnce, Scope: destination.ScopePipeline, CommitMaxBytes: 1},
				}
			}
			holds := func() *checkpoint.Holds {
				t.Helper()
				head, err := log.Latest(ctx)
				if err != nil {
					t.Fatal(err)
				}
				return head.HoldsOf("users")
			}
			for i, page := range []string{`{"id": 1, "name": "a", "v": 1}`, `{"id": 2, "name": "b", "v": "x"}`} {
				if _, err := runner.RunAttempt(ctx, job(page)); err != nil {
					t.Fatalf("sync %d: %v", i+1, err)
				}
			}
			h, files := holds(), heldFiles(t, root)
			if h == nil || len(h.Columns) != 1 || len(h.Epochs) == 0 || len(files) == 0 {
				t.Fatalf("v isn't held: %+v, held files %v", h, files)
			}
			since := h.Columns[0].Since

			dest.takes = true
			changed := job(`{"id": 3, "name": "c", "v": "y"}`)
			tc.change(&changed, dest)
			_, err = runner.RunAttempt(ctx, changed)
			dest.noHolds = false
			// The sync after it prunes the held files the record doesn't name.
			_, after := runner.RunAttempt(ctx, job(`{"id": 4, "name": "d"}`))
			if tc.reread {
				if err != nil || after != nil {
					t.Fatalf("overwritten: %v; the sync after: %v", err, after)
				}
				return
			}
			if errs.CodeOf(err) != errs.SchemaDrift || !strings.Contains(err.Error(), tc.why) || !strings.Contains(err.Error(), "openetl holds discard PIPELINE users v") {
				t.Errorf("the changed sync: %v, want SCHEMA_DRIFT that says %q and how to discard", err, tc.why)
			}
			if h := holds(); h == nil || len(h.Columns) != 1 || h.Columns[0].Since != since {
				t.Errorf("v's hold went: %+v", h)
			}
			if kept := heldFiles(t, root); !slices.Equal(kept[:min(len(files), len(kept))], files) {
				t.Errorf("held files %v, then %v", files, kept)
			}

			// Discarded, the holds go with the change.
			changed = job(`{"id": 5, "name": "e", "v": "w"}`)
			tc.change(&changed, dest)
			changed.Releases = []runner.Release{{ID: "d", Stream: "users", Column: "v", Since: since, Discard: true}}
			if _, err := runner.RunAttempt(ctx, changed); err != nil {
				t.Errorf("discarded, the changed sync: %v", err)
			}
		})
	}
}
