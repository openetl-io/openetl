//go:build e2e

package e2e

import (
	"fmt"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
)

// A paused stream is skipped by syncs while the others carry on, and once
// resumed reads on from where it stopped rather than from the start.
func TestStreamPause(t *testing.T) {
	startRunner(t)
	name := unique("streampause")
	in, out := schema(name), schema(name)+"_out"
	next := "nextval('" + pgx.Identifier{in, "at"}.Sanitize() + "')"
	stmts := []string{"CREATE SCHEMA " + pgx.Identifier{in}.Sanitize(), "CREATE SEQUENCE " + pgx.Identifier{in, "at"}.Sanitize()}
	for _, table := range []string{"a", "b"} {
		tbl := pgx.Identifier{in, table}.Sanitize()
		stmts = append(stmts, "CREATE TABLE "+tbl+" (id bigint PRIMARY KEY, at bigint NOT NULL DEFAULT "+next+")",
			"INSERT INTO "+tbl+" (id) SELECT i FROM generate_series(1, 100) i")
	}
	run(t, stmts...)
	apply(t,
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Source", "metadata": {"name": %q},
			"spec": {"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}, "schema": %q}}}`, name, in),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Destination", "metadata": {"name": %q},
			"spec": {"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}}}}`, name),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
			"spec": {"schedule": {"every": "720h"}, "source": {"ref": %q, "streams": [
				{"name": "a", "sync": "incremental", "cursor": "at"}, {"name": "b", "sync": "incremental", "cursor": "at"}]},
			"destination": {"ref": %q, "namespace": %q, "write": "merge"}, "delivery": {"require": "effectively_once"}}}`, name, name, name, out))
	if status, stats := waitSync(t, name, 60*time.Second); status != "succeeded" {
		t.Fatalf("first sync %s: %s", status, stats)
	}
	add := func(from int) {
		for _, table := range []string{"a", "b"} {
			run(t, fmt.Sprintf("INSERT INTO %s (id) SELECT i FROM generate_series(%d, %d) i", pgx.Identifier{in, table}.Sanitize(), from, from+49))
		}
	}
	counts := func() string {
		return mustQuery[string](t, fmt.Sprintf(`SELECT (SELECT count(*) FROM %s) || ' ' || (SELECT count(*) FROM %s)`,
			pgx.Identifier{out, "a"}.Sanitize(), pgx.Identifier{out, "b"}.Sanitize()))
	}
	// Rows the pipeline's latest sync read of stream b.
	readOfB := func() int64 {
		return mustQuery[int64](t, `SELECT coalesce(sum((st ->> 'rows')::bigint), 0)::bigint FROM checkpoint_mirror c
			JOIN syncs s ON s.org_id = c.org_id AND s.pipeline_id = c.pipeline_id AND s.run_id = c.run_id
			CROSS JOIN LATERAL jsonb_array_elements(c.streams) st
			WHERE st ->> 'name' = 'b' AND s.id = (
				SELECT s2.id FROM syncs s2 JOIN pipelines p ON p.id = s2.pipeline_id WHERE p.name = $1 ORDER BY s2.start_time DESC LIMIT 1)`, name)
	}
	pipeline := "workspaces/default/pipelines/" + name

	if _, err := pipelines().PausePipeline(t.Context(), connect.NewRequest(&apiv1.PausePipelineRequest{Name: pipeline, Stream: "b"})); err != nil {
		t.Fatal(err)
	}
	_, err := pipelines().PausePipeline(t.Context(), connect.NewRequest(&apiv1.PausePipelineRequest{Name: pipeline, Stream: "a"}))
	wantCode(t, err, connect.CodeFailedPrecondition, "pausing the last stream that isn't paused")
	add(101)
	if status, stats := syncNow(t, name); status != "succeeded" {
		t.Fatalf("sync with b paused %s: %s", status, stats)
	}
	if got := counts(); got != "150 100" {
		t.Errorf("with b paused, the destination has %s rows of a and b, want 150 100", got)
	}
	if n := readOfB(); n != 0 {
		t.Errorf("with b paused, the sync read %d of its rows", n)
	}

	if _, err := pipelines().ResumePipeline(t.Context(), connect.NewRequest(&apiv1.ResumePipelineRequest{Name: pipeline, Stream: "b"})); err != nil {
		t.Fatal(err)
	}
	if status, stats := syncNow(t, name); status != "succeeded" {
		t.Fatalf("sync after b was resumed %s: %s", status, stats)
	}
	if got := counts(); got != "150 150" {
		t.Errorf("after b was resumed, the destination has %s rows of a and b, want 150 150", got)
	}
	// The 50 new rows, and those at the cursor's last value; not all 150.
	if n := readOfB(); n < 50 || n >= 100 {
		t.Errorf("after b was resumed, the sync read %d of its rows, want 50 and those at its cursor", n)
	}
}
