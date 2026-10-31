//go:build integration

package postgres

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/jackc/pgx/v5"

	"github.com/unsuitable001/openetl/sdk/go/destination"
)

// Runs against the compose Postgres (make deps-up) or
// OPENETL_TEST_POSTGRES_DSN.
func dsn() string {
	return cmp.Or(os.Getenv("OPENETL_TEST_POSTGRES_DSN"), "postgres://openetl:openetl@localhost:5432/openetl")
}

type env struct {
	t      *testing.T
	d      *Destination
	conn   *pgx.Conn
	schema string
}

func newEnv(t *testing.T) *env {
	t.Helper()
	schema := "t_" + strings.ToLower(rand.Text())[:10]
	cfg, _ := json.Marshal(DestinationConfig{DSN: dsn(), Schema: schema})
	d, err := NewDestination(cfg)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.Connect(t.Context(), dsn())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = conn.Exec(ctx, "DROP SCHEMA IF EXISTS "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		_ = conn.Close(ctx)
	})
	return &env{t: t, d: d, conn: conn, schema: schema}
}

func (e *env) open(gen uint64, scope destination.Scope, streams ...string) destination.Writer {
	e.t.Helper()
	cfg := destination.OpenConfig{Pipeline: "p1", Generation: gen, Scope: scope}
	for _, s := range streams {
		cfg.Streams = append(cfg.Streams, destination.Stream{Name: s, Mode: destination.Append})
	}
	w, err := e.d.Open(e.t.Context(), cfg)
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { _ = w.Close(context.Background()) })
	return w
}

func (e *env) count(q string) int {
	e.t.Helper()
	var n int
	if err := e.conn.QueryRow(e.t.Context(), strings.ReplaceAll(q, "$schema", pgx.Identifier{e.schema}.Sanitize())).Scan(&n); err != nil {
		e.t.Fatal(err)
	}
	return n
}

func batch(t *testing.T, fields []arrow.Field, rows string) arrow.RecordBatch {
	t.Helper()
	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrow.NewSchema(fields, nil), strings.NewReader(rows))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rec.Release)
	return rec
}

var (
	idField   = arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}
	nameField = arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true}
	tagsField = arrow.Field{Name: "tags", Type: arrow.BinaryTypes.String, Nullable: true,
		Metadata: arrow.NewMetadata([]string{extensionNameKey}, []string{jsonExtension})}
)

func recov(t *testing.T, w destination.Writer, gen uint64, e destination.Epoch, st destination.Staged) destination.Recovery {
	t.Helper()
	r, err := w.Recover(t.Context(), gen, e, st)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func precommit(t *testing.T, w destination.Writer, e destination.Epoch) destination.Staged {
	t.Helper()
	st, err := w.PreCommit(t.Context(), e)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestCommitAppliesOnce(t *testing.T) {
	env := newEnv(t)
	ctx := t.Context()
	w := env.open(1, destination.ScopePipeline, "users")
	if r := recov(t, w, 1, destination.Epoch{}, nil); r != destination.Applied {
		t.Fatalf("Recover = %v", r)
	}
	e := destination.Epoch{Generation: 1, N: 1}
	if err := w.Write(ctx, e, "users", batch(t, []arrow.Field{idField}, `[{"id": 1}, {"id": 2}]`)); err != nil {
		t.Fatal(err)
	}
	// A column appears mid-epoch.
	if err := w.Write(ctx, e, "users", batch(t, []arrow.Field{idField, nameField, tagsField}, `[{"id": 3, "name": "c", "tags": "[\"x\"]"}]`)); err != nil {
		t.Fatal(err)
	}
	st := precommit(t, w, e)
	if n := env.count("SELECT count(*) FROM pg_tables WHERE schemaname = '" + env.schema + "' AND tablename = 'users'"); n != 0 {
		t.Fatal("PreCommit made the target visible")
	}
	for range 2 {
		if err := w.Commit(ctx, e, st); err != nil {
			t.Fatal(err)
		}
	}
	if n := env.count("SELECT count(*) FROM $schema.users"); n != 3 {
		t.Errorf("users has %d rows, want 3", n)
	}
	if n := env.count("SELECT count(*) FROM $schema.users WHERE tags @> '[\"x\"]'::jsonb"); n != 1 {
		t.Errorf("tags isn't jsonb with the row's value: %d matches", n)
	}
	if n := env.count("SELECT count(*) FROM $schema._openetl_loads WHERE pipeline = 'p1' AND stream = 'users' AND generation = 1 AND epoch = 1"); n != 1 {
		t.Errorf("%d markers, want 1", n)
	}
	if n := env.count("SELECT count(*) FROM pg_tables WHERE schemaname = '" + env.schema + "' AND tablename LIKE '\\_openetl\\_stage\\_%'"); n != 0 {
		t.Errorf("%d staging tables left", n)
	}
}

func TestRecoverRedoesAStagedEpoch(t *testing.T) {
	env := newEnv(t)
	ctx := t.Context()
	w1 := env.open(1, destination.ScopePipeline, "users", "orders")
	recov(t, w1, 1, destination.Epoch{}, nil)
	e := destination.Epoch{Generation: 1, N: 1}
	if err := w1.Write(ctx, e, "users", batch(t, []arrow.Field{idField}, `[{"id": 1}]`)); err != nil {
		t.Fatal(err)
	}
	if err := w1.Write(ctx, e, "orders", batch(t, []arrow.Field{idField}, `[{"id": 10}, {"id": 11}]`)); err != nil {
		t.Fatal(err)
	}
	st := precommit(t, w1, e)
	// The attempt dies here, after its checkpoint record but before Commit.

	w2 := env.open(2, destination.ScopePipeline, "users", "orders")
	if r := recov(t, w2, 2, e, st); r != destination.Redone {
		t.Fatalf("Recover = %v, want redone", r)
	}
	w3 := env.open(3, destination.ScopePipeline, "users", "orders")
	if r := recov(t, w3, 3, e, st); r != destination.Applied {
		t.Fatalf("second Recover = %v, want applied", r)
	}
	if n := env.count("SELECT count(*) FROM $schema.users") + env.count("SELECT count(*) FROM $schema.orders"); n != 3 {
		t.Errorf("%d rows, want 3", n)
	}
}

func TestRecoverReportsLost(t *testing.T) {
	env := newEnv(t)
	ctx := t.Context()
	w1 := env.open(1, destination.ScopePipeline, "users")
	recov(t, w1, 1, destination.Epoch{}, nil)
	e := destination.Epoch{Generation: 1, N: 1}
	if err := w1.Write(ctx, e, "users", batch(t, []arrow.Field{idField}, `[{"id": 1}]`)); err != nil {
		t.Fatal(err)
	}
	st := precommit(t, w1, e)
	if _, err := env.conn.Exec(ctx, "DROP TABLE "+pgx.Identifier{env.schema, stagingName("p1", "users", e)}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	w2 := env.open(2, destination.ScopePipeline, "users")
	if r := recov(t, w2, 2, e, st); r != destination.Lost {
		t.Fatalf("Recover = %v, want lost", r)
	}
}

func TestStaleWriterIsFenced(t *testing.T) {
	env := newEnv(t)
	ctx := t.Context()
	w1 := env.open(1, destination.ScopeStream, "users")
	recov(t, w1, 1, destination.Epoch{}, nil)
	e := destination.Epoch{Generation: 1, N: 1}
	if err := w1.Write(ctx, e, "users", batch(t, []arrow.Field{idField}, `[{"id": 1}]`)); err != nil {
		t.Fatal(err)
	}
	st := precommit(t, w1, e)

	w2 := env.open(2, destination.ScopeStream, "users")
	if r := recov(t, w2, 2, destination.Epoch{}, nil); r != destination.Applied {
		t.Fatalf("Recover = %v", r)
	}
	if err := w1.Commit(ctx, e, st); !errors.Is(err, destination.ErrFenced) {
		t.Fatalf("stale Commit: %v, want fenced", err)
	}
	if _, err := w1.Recover(ctx, 1, destination.Epoch{}, nil); !errors.Is(err, destination.ErrFenced) {
		t.Fatalf("stale Recover: %v, want fenced", err)
	}
	if n := env.count("SELECT count(*) FROM $schema._openetl_loads"); n != 0 {
		t.Errorf("a fenced commit wrote %d markers", n)
	}
}

func TestBatchWithoutColumns(t *testing.T) {
	env := newEnv(t)
	ctx := t.Context()
	w := env.open(1, destination.ScopePipeline, "empty")
	recov(t, w, 1, destination.Epoch{}, nil)
	e := destination.Epoch{Generation: 1, N: 1}
	rec := array.NewRecordBatch(arrow.NewSchema(nil, nil), nil, 3)
	defer rec.Release()
	if err := w.Write(ctx, e, "empty", rec); err != nil {
		t.Fatal(err)
	}
	if err := w.Write(ctx, e, "empty", batch(t, []arrow.Field{idField}, `[{"id": 1}]`)); err != nil {
		t.Fatal(err)
	}
	if err := w.Commit(ctx, e, precommit(t, w, e)); err != nil {
		t.Fatal(err)
	}
	if n := env.count("SELECT count(*) FROM $schema.empty"); n != 4 {
		t.Errorf("%d rows, want 4", n)
	}
}

func TestOnlyAppend(t *testing.T) {
	env := newEnv(t)
	_, err := env.d.Open(t.Context(), destination.OpenConfig{Pipeline: "p1", Generation: 1, Scope: destination.ScopePipeline,
		Streams: []destination.Stream{{Name: "users", Mode: destination.Merge, PrimaryKey: []string{"id"}}}})
	if err == nil {
		t.Fatal("Open accepted a merge stream")
	}
}
