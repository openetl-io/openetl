//go:build e2e

package e2e

import (
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// Scheduled syncs read a table incrementally by a sequence-backed cursor,
// each from the last one's highest value on, so the rows at it are read
// again. Merge, effectively once, ends equal to the source; append under
// exactly_once dedupes those rows, and holds every version a sync saw.
func TestPostgresIncremental(t *testing.T) {
	startRunner(t)
	name := unique("incremental")
	in := schema(name)
	src := pgx.Identifier{in, "items"}.Sanitize()
	next := "nextval('" + pgx.Identifier{in, "items_at"}.Sanitize() + "')"
	run(t, "CREATE SCHEMA "+pgx.Identifier{in}.Sanitize(),
		"CREATE SEQUENCE "+pgx.Identifier{in, "items_at"}.Sanitize(),
		"CREATE TABLE "+src+" (id bigint PRIMARY KEY, v int NOT NULL, at bigint NOT NULL DEFAULT "+next+")",
		"INSERT INTO "+src+" (id, v) SELECT i, 1 FROM generate_series(1, 300) i")
	out := func(write string) string { return in + "_" + write }
	for write, require := range map[string]string{"merge": "effectively_once", "append": "exactly_once"} {
		n := name + "-" + write
		apply(t,
			fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Source", "metadata": {"name": %q},
				"spec": {"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}, "schema": %q, "chunkRows": 40}}}`, n, in),
			fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Destination", "metadata": {"name": %q},
				"spec": {"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}}}}`, n),
			fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
				"spec": {"schedule": {"every": "1h"}, "source": {"ref": %q, "streams": [{"name": "items", "sync": "incremental", "cursor": "at"}]},
				"destination": {"ref": %q, "namespace": %q, "write": %q}, "delivery": {"require": %q, "commit": {"maxBytes": "2KB"}}}}`,
				n, n, n, out(write), write, require))
		if status, stats := waitSync(t, n, 60*time.Second); status != "succeeded" {
			t.Fatalf("%s: first sync %s: %s", write, status, stats)
		}
	}
	for round := range 3 {
		run(t, fmt.Sprintf("UPDATE %s SET v = v + 1, at = %s WHERE id %% 3 = %d", src, next, round),
			fmt.Sprintf("INSERT INTO %s (id, v) SELECT i, 1 FROM generate_series(%d, %d) i", src, 301+50*round, 350+50*round))
		for _, write := range []string{"merge", "append"} {
			if status, stats := syncNow(t, name+"-"+write); status != "succeeded" {
				t.Fatalf("%s: sync %d %s: %s", write, round+2, status, stats)
			}
		}
	}

	merged := pgx.Identifier{out("merge"), "items"}.Sanitize()
	if v := mustQuery[string](t, `SELECT (SELECT count(*) FROM `+merged+`) || ' ' || (SELECT count(*) FROM `+src+`) || ' ' ||
		(SELECT count(*) FROM `+src+` s LEFT JOIN `+merged+` d USING (id) WHERE d.v IS DISTINCT FROM s.v)`); v != "450 450 0" {
		t.Errorf("merged rows, source rows, and rows that differ: %s, want 450 450 0", v)
	}
	appended := pgx.Identifier{out("append"), "items"}.Sanitize()
	if v := mustQuery[string](t, `SELECT count(*) || ' ' || count(DISTINCT _openetl_id) || ' ' || count(DISTINCT (id, v)) FROM `+appended); v != "800 800 800" {
		t.Errorf("appended rows, distinct ids, and distinct versions: %s, want 800 each", v)
	}
	if n := mustQuery[int](t, `SELECT count(*) FROM `+src+` s WHERE NOT EXISTS (SELECT 1 FROM `+appended+` d WHERE d.id = s.id AND d.v = s.v)`); n != 0 {
		t.Errorf("%d of the source's rows have no appended version", n)
	}
}
