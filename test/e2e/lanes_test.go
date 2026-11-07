//go:build e2e

package e2e

import (
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// laneDocs syncs a table of rows read in key ranges, at size l, which gives it
// four lanes.
func laneDocs(t *testing.T, name, dest, write string, rows int) []string {
	t.Helper()
	in := schema(name)
	table := pgx.Identifier{in, "items"}.Sanitize()
	run(t, "CREATE SCHEMA "+pgx.Identifier{in}.Sanitize(),
		"CREATE TABLE "+table+" (id bigint PRIMARY KEY, name text, updated timestamptz NOT NULL DEFAULT now())",
		fmt.Sprintf("INSERT INTO %s SELECT i, 'n' || i FROM generate_series(1, %d) i", table, rows),
		"ANALYZE "+table)
	src := fmt.Sprintf(`{"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}, "schema": %q, "chunkRows": 500}}`, in)
	d := docs(name, src, dest, `{}`)
	d[2] = fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
		"spec": {"schedule": {"every": "720h"}, "source": {"ref": %q}, "destination": {"ref": %q, "namespace": %q, "write": %q},
		"resources": {"size": "l"}, "delivery": {"commit": {"maxBytes": "64KB"}}}}`, name, name, name, in+"_out", write)
	return d
}

// ranOnLanes checks the engine ran the sync on this many lanes.
func ranOnLanes(t *testing.T, pipeline string, lanes int) {
	t.Helper()
	for _, e := range wholeLog(t, latestSync(t, pipeline).GetName()) {
		if e.GetMessage() == "job started" && int(e.GetAttrs().GetFields()["lanes"].GetNumberValue()) == lanes {
			return
		}
	}
	t.Errorf("no attempt of %s started the engine with %d lanes", pipeline, lanes)
}

// A table read in key ranges on several lanes lands each row once, in each
// write mode, into Postgres.
func TestLanesIntoPostgres(t *testing.T) {
	startRunner(t)
	for _, write := range []string{"append", "merge", "overwrite"} {
		t.Run(write, func(t *testing.T) {
			name := unique("lanes-" + write)
			dst := `{"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}}}`
			apply(t, laneDocs(t, name, dst, write, 20_000)...)
			if status, stats := waitSync(t, name, 3*time.Minute); status != "succeeded" {
				t.Fatalf("sync %s: %s", status, stats)
			}
			out := pgx.Identifier{schema(name) + "_out", "items"}.Sanitize()
			got := mustQuery[string](t, "SELECT count(*) || ' ' || count(DISTINCT id) || ' ' || sum(id) FROM "+out)
			if want := "20000 20000 200010000"; got != want {
				t.Errorf("rows, distinct ids and their sum: %s, want %s", got, want)
			}
			if n := mustQuery[int64](t, "SELECT count(*) FROM pg_tables WHERE schemaname = $1 AND tablename LIKE '_openetl_stage_%'", schema(name)+"_out"); n != 0 {
				t.Errorf("%d staging tables left behind", n)
			}
			ranOnLanes(t, name, 4)
		})
	}
}

// As into Iceberg, in one commit per epoch whatever the lanes, and an
// overwrite ends up with exactly the rows of its read.
func TestLanesIntoIceberg(t *testing.T) {
	startRunner(t)
	for _, write := range []string{"append", "overwrite"} {
		t.Run(write, func(t *testing.T) {
			name := unique("lanes-ice-" + write)
			apply(t, laneDocs(t, name, icebergDest(t, schema(name)+"_out"), write, 20_000)...)
			if status, stats := waitSync(t, name, 3*time.Minute); status != "succeeded" {
				t.Fatalf("sync %s: %s", status, stats)
			}
			checkIceberg(t, schema(name)+"_out", 20_000, "items")
			ranOnLanes(t, name, 4)
		})
	}
}

// A runner killed mid-sync on several lanes leaves no row out or in twice,
// once another finishes the read from its record.
func TestLanesThroughARunnerKill(t *testing.T) {
	r := startRunner(t)
	name := unique("lanes-kill")
	dst := `{"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}}}`
	apply(t, laneDocs(t, name, dst, "append", 200_000)...)
	eventually(t, 2*time.Minute, "a first commit", func() bool { return committedEpochs(t, schema(name)+"_out") > 0 })
	r.kill(t)
	startRunner(t)
	if status, stats := waitSync(t, name, 5*time.Minute); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	out := pgx.Identifier{schema(name) + "_out", "items"}.Sanitize()
	got := mustQuery[string](t, "SELECT count(*) || ' ' || count(DISTINCT id) FROM "+out)
	if want := "200000 200000"; got != want {
		t.Errorf("rows and distinct ids: %s, want %s", got, want)
	}
}

// A CDC table on several lanes: its rows are routed by key group, and later
// changes to a key supersede earlier ones whichever writer lane has it. A
// soft delete keeps the other columns as the target had them.
func TestLanesCDCRoutedByKey(t *testing.T) {
	startRunner(t)
	name := unique("lanes-cdc")
	in := schema(name)
	table := pgx.Identifier{in, "items"}.Sanitize()
	run(t, "CREATE SCHEMA "+pgx.Identifier{in}.Sanitize(),
		"CREATE TABLE "+table+" (id bigint PRIMARY KEY, v int)",
		"INSERT INTO "+table+" SELECT i, 0 FROM generate_series(1, 2000) i")
	src := fmt.Sprintf(`{"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}, "schema": %q, "tables": ["items"], "slot": %q, "publication": %q}}`, in, "s_"+in, "p_"+in)
	dst := `{"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}}}`
	d := docs(name, src, dst, `{}`)
	d[2] = fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
		"spec": {"mode": "continuous", "source": {"ref": %q, "streams": [{"name": "items", "sync": "cdc"}]},
		"destination": {"ref": %q, "namespace": %q, "write": "merge"}, "resources": {"size": "m"}, "delivery": {"commit": {"interval": "1s"}}}}`, name, name, name, in+"_out")
	apply(t, d...)
	out := pgx.Identifier{in + "_out", "items"}.Sanitize()
	count := func() int64 {
		n, err := query[int64](t, "SELECT count(*) FROM "+out)
		if err != nil {
			return 0
		}
		return n
	}
	eventually(t, 2*time.Minute, "the snapshot", func() bool { return count() == 2000 })
	for v := 1; v <= 5; v++ {
		run(t, fmt.Sprintf("UPDATE %s SET v = %d", table, v))
	}
	run(t, "DELETE FROM "+table+" WHERE id % 2 = 0")
	state := ""
	defer func() {
		if t.Failed() {
			t.Logf("live rows, deleted rows, live rows' lowest v and the highest: %s", state)
			groups, err := query[string](t, "SELECT string_agg(g, '; ') FROM (SELECT _openetl_deleted || ' v=' || v || ': ' || count(*) || ' ids ' || min(id) || '..' || max(id) || ' unchanged ' || coalesce(max(_openetl_unchanged::text), '-') AS g FROM "+out+" GROUP BY _openetl_deleted, v ORDER BY 1) x")
			t.Logf("by deleted and v: %s %v", groups, err)
		}
	}()
	eventually(t, 2*time.Minute, "the changes", func() bool {
		s, err := query[string](t, "SELECT count(*) FILTER (WHERE NOT _openetl_deleted) || ' ' || count(*) FILTER (WHERE _openetl_deleted AND id % 2 = 0) || ' ' || min(v) FILTER (WHERE NOT _openetl_deleted) || ' ' || max(v) FROM "+out)
		state = fmt.Sprint(s, err)
		return err == nil && s == "1000 1000 5 5"
	})
}
