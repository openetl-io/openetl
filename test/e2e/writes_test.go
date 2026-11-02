//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// writeDocs syncs Postgres schema in into schema out with the write mode, in
// epochs of a few KB, so that each read spans several.
func writeDocs(t *testing.T, name, in, out, write string, setup ...string) []string {
	t.Helper()
	run(t, append([]string{"CREATE SCHEMA " + pgx.Identifier{in}.Sanitize()}, setup...)...)
	src := fmt.Sprintf(`{"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}, "schema": %q, "chunkRows": 50}}`, in)
	dst := `{"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}}}`
	d := docs(name, src, dst, `{}`)
	d[2] = fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
		"spec": {"schedule": {"every": "1h"}, "source": {"ref": %q}, "destination": {"ref": %q, "namespace": %q, "write": %q},
		"delivery": {"commit": {"maxBytes": "2KB"}}}}`, name, name, name, out, write)
	return d
}

func run(t *testing.T, queries ...string) {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), env.dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	for _, q := range queries {
		if _, err := conn.Exec(t.Context(), q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
}

func mustQuery[T any](t *testing.T, q string, args ...any) T {
	t.Helper()
	v, err := query[T](t, q, args...)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return v
}

// Each sync replaces each table with what it read, rows the source holds
// twice included, so nothing piles up across syncs, and a view on the
// table keeps working.
func TestOverwrite(t *testing.T) {
	startRunner(t)
	name := unique("overwrite")
	in, out := schema(name), schema(name)+"_out"
	src := func(table string) string { return pgx.Identifier{in, table}.Sanitize() }
	dst := func(table string) string { return pgx.Identifier{out, table}.Sanitize() }
	apply(t, writeDocs(t, name, in, out, "overwrite",
		"CREATE TABLE "+src("users")+" (id bigint PRIMARY KEY, name text)",
		"INSERT INTO "+src("users")+" SELECT i, 'u' || i FROM generate_series(1, 500) i",
		"CREATE TABLE "+src("views")+" (user_id bigint, path text)",
		"INSERT INTO "+src("views")+" SELECT i % 150, '/p' || (i % 150) FROM generate_series(1, 300) i")...)
	if status, stats := waitSync(t, name, 60*time.Second); status != "succeeded" {
		t.Fatalf("first sync %s: %s", status, stats)
	}
	if got := mustQuery[string](t, "SELECT (SELECT count(*) FROM "+dst("users")+") || ' ' || (SELECT count(*) FROM "+dst("views")+")"); got != "500 300" {
		t.Fatalf("after the first sync, users and views have %s rows, want 500 300", got)
	}
	run(t, "CREATE VIEW "+dst("names")+" AS SELECT name FROM "+dst("users"),
		"DELETE FROM "+src("users")+" WHERE id > 400",
		"UPDATE "+src("users")+" SET name = 'v' || id WHERE id <= 100",
		"INSERT INTO "+src("users")+" SELECT i, 'u' || i FROM generate_series(501, 550) i")

	if status, stats := syncNow(t, name); status != "succeeded" {
		t.Fatalf("second sync %s: %s", status, stats)
	}
	same := "SELECT count(*) FROM " + dst("users") + " o JOIN " + src("users") + " i USING (id) WHERE o.name = i.name"
	if got := mustQuery[string](t, "SELECT (SELECT count(*) FROM "+dst("users")+") || ' ' || ("+same+") || ' ' || (SELECT count(*) FROM "+dst("views")+")"); got != "450 450 300" {
		t.Errorf("after the second sync: users has %s rows, matching ones, and views rows; want 450 450 300", got)
	}
	if n := mustQuery[int](t, "SELECT count(*) FROM "+dst("names")); n != 450 {
		t.Errorf("the view on users has %d rows, want 450", n)
	}
	if n := mustQuery[int](t, "SELECT count(DISTINCT epoch) FROM "+dst("_openetl_loads")+" WHERE stream = 'users'"); n < 3 {
		t.Errorf("users was applied in %d epochs; the read should span several", n)
	}

	run(t, "DELETE FROM "+src("views"))
	if status, stats := syncNow(t, name); status != "succeeded" {
		t.Fatalf("third sync %s: %s", status, stats)
	}
	if got := mustQuery[string](t, "SELECT (SELECT count(*) FROM "+dst("users")+") || ' ' || (SELECT count(*) FROM "+dst("views")+")"); got != "450 0" {
		t.Errorf("after the source's views emptied, users and views have %s rows, want 450 0", got)
	}
	if n := mustQuery[int](t, `SELECT count(*) FROM pg_tables WHERE schemaname = $1 AND tablename LIKE '\_openetl\_read\_%'`, out); n != 0 {
		t.Errorf("%d read tables left", n)
	}
}

// Each sync upserts on the primary key. A full refresh doesn't see deletes,
// so rows the source deleted stay, not marked deleted.
func TestMerge(t *testing.T) {
	startRunner(t)
	name := unique("merge")
	in, out := schema(name), schema(name)+"_out"
	src, dst := pgx.Identifier{in, "users"}.Sanitize(), pgx.Identifier{out, "users"}.Sanitize()
	apply(t, writeDocs(t, name, in, out, "merge",
		"CREATE TABLE "+src+" (id bigint PRIMARY KEY, name text)",
		"INSERT INTO "+src+" SELECT i, 'u' || i FROM generate_series(1, 500) i")...)
	if status, stats := waitSync(t, name, 60*time.Second); status != "succeeded" {
		t.Fatalf("first sync %s: %s", status, stats)
	}
	if got := mustQuery[string](t, "SELECT count(*) || ' ' || count(*) FILTER (WHERE NOT _openetl_deleted) FROM "+dst); got != "500 500" {
		t.Fatalf("after the first sync, users has %s rows and live ones, want 500 500", got)
	}
	run(t, "UPDATE "+src+" SET name = 'v' || id WHERE id <= 100",
		"DELETE FROM "+src+" WHERE id > 450",
		"INSERT INTO "+src+" SELECT i, 'u' || i FROM generate_series(501, 520) i")
	if status, stats := syncNow(t, name); status != "succeeded" {
		t.Fatalf("second sync %s: %s", status, stats)
	}
	got := mustQuery[string](t, "SELECT count(*) || ' ' || count(DISTINCT id) || ' ' || count(*) FILTER (WHERE name = 'v' || id) || ' ' || count(*) FILTER (WHERE _openetl_deleted) FROM "+dst)
	if got != "520 520 100 0" {
		t.Errorf("users has %s rows, distinct ids, updated names and deleted rows; want 520 520 100 0", got)
	}
	if n := mustQuery[int](t, "SELECT count(*) FROM pg_indexes WHERE schemaname = $1 AND tablename = 'users' AND indexdef LIKE 'CREATE UNIQUE INDEX%(id)'", out); n != 1 {
		t.Errorf("users has %d unique indexes on id, want 1", n)
	}
}
