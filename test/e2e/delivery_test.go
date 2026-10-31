//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// The retry re-reads 150 rows per stream that are already applied. The
// default, exactly_once, dedupes them at apply.
func TestSupersetKilledMidSync(t *testing.T) {
	r := startRunner(t)
	name := unique("superset-killed")
	faker := `{"seed": 3, "count": 2000, "batchSize": 50, "delay": "50ms", "overlap": 150}`
	apply(t, pipelineDocs(name, faker, `{"commit": {"interval": "1s"}}`)...)
	pid := r.engine(t)
	eventually(t, 30*time.Second, "a committed epoch", func() bool { return committedEpochs(t, schema(name)) > 0 })
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	status, stats := waitSync(t, name, 90*time.Second)
	if status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	if !strings.Contains(stats, `"Generation": 2`) {
		t.Errorf("stats %s: want the second attempt to have finished", stats)
	}
	checkRows(t, schema(name), 2000, "users", "purchases")
}

// A table without a primary key isn't deduped, so rows it holds twice land
// twice.
func TestDuplicateRowsLand(t *testing.T) {
	startRunner(t)
	name := unique("duplicate-rows")
	in, out := pgx.Identifier{schema(name)}.Sanitize(), schema(name)+"_out"
	conn, err := pgx.Connect(t.Context(), env.dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	for _, q := range []string{
		"CREATE SCHEMA " + in,
		"CREATE TABLE " + in + ".views (user_id bigint, path text)",
		"INSERT INTO " + in + ".views SELECT i % 350, '/p' || (i % 350) FROM generate_series(1, 700) i",
	} {
		if _, err := conn.Exec(t.Context(), q); err != nil {
			t.Fatal(err)
		}
	}
	src := fmt.Sprintf(`{"connector": "postgres", "config": {"dsn": %q, "schema": %q}}`, env.dbURL, schema(name))
	dst := fmt.Sprintf(`{"connector": "postgres", "config": {"dsn": %q, "schema": %q}}`, env.dbURL, out)
	apply(t, docs(name, src, dst, `{}`)...)
	if status, stats := waitSync(t, name, 60*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	got, err := query[string](t, "SELECT count(*) || ' ' || count(DISTINCT (user_id, path)) FROM "+pgx.Identifier{out, "views"}.Sanitize())
	if err != nil {
		t.Fatal(err)
	}
	if got != "700 350" {
		t.Errorf("rows and distinct rows are %s, want 700 350", got)
	}
	if n, err := query[int](t, "SELECT count(*) FROM pg_indexes WHERE schemaname = $1 AND tablename = 'views'", out); err != nil || n != 0 {
		t.Errorf("views has %d indexes (%v), want none", n, err)
	}
}

func TestPlanChecksFailTheSync(t *testing.T) {
	startRunner(t)
	name := unique("plan-checks")
	src := `{"connector": "faker", "config": {"count": 10, "overlap": 5}}`
	apply(t, docs(name, src, parquetDest(t.TempDir()), `{}`)...)
	status, msg := waitSync(t, name, 60*time.Second)
	if status != "failed" {
		t.Fatalf("sync %s: %s", status, msg)
	}
	for _, stream := range []string{"users", "purchases"} {
		if want := `stream "` + stream + `" gets at_least_once (superset replay, and the destination can't dedupe), below exactly_once`; !strings.Contains(msg, want) {
			t.Errorf("error %q doesn't say %q", msg, want)
		}
	}
}
