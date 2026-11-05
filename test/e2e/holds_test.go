//go:build e2e

package e2e

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
)

func listHolds(t *testing.T, pipeline string) []*apiv1.Hold {
	t.Helper()
	resp, err := pipelines().ListHolds(t.Context(), connect.NewRequest(&apiv1.ListHoldsRequest{Parent: "workspaces/default/pipelines/" + pipeline}))
	if err != nil {
		t.Fatal(err)
	}
	return resp.Msg.GetHolds()
}

func releaseHold(t *testing.T, pipeline, stream, column string) {
	t.Helper()
	_, err := pipelines().ReleaseHold(t.Context(), connect.NewRequest(&apiv1.ReleaseHoldRequest{
		Parent: "workspaces/default/pipelines/" + pipeline, Stream: stream, Column: column,
	}))
	if err != nil {
		t.Fatal(err)
	}
}

// dumpHolds logs what a pipeline's checkpoint log and held files say, for
// a release that didn't land.
func dumpHolds(t *testing.T, pipeline string) {
	t.Helper()
	id, err := query[string](t, "SELECT id::text FROM pipelines WHERE name = $1", pipeline)
	if err != nil {
		t.Logf("pipeline id: %v", err)
		return
	}
	for _, q := range []string{
		"SELECT string_agg(stream || '.' || column_name || ' ' || whole || ' ' || since_generation || '/' || since_epoch || ' releasing=' || releasing, '; ') FROM holds WHERE pipeline_id = $1::uuid",
		"SELECT string_agg(stream || '.' || column_name || ' ' || whole || ' ' || since_generation || '/' || since_epoch || ' discard=' || discard, '; ') FROM hold_releases WHERE pipeline_id = $1::uuid",
	} {
		v, err := query[*string](t, q, id)
		if err == nil && v != nil {
			t.Logf("%s", *v)
		} else {
			t.Logf("%v", err)
		}
	}
	root := filepath.Join(env.checkpoints)
	_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.Contains(p, id) && (strings.Contains(p, "_held") || strings.HasSuffix(p, "HEAD")) {
			t.Logf("file %s", strings.TrimPrefix(p, root))
		}
		return nil
	})
	matches, _ := filepath.Glob(filepath.Join(root, "*", id, "_checkpoints", "*.json"))
	if len(matches) > 0 {
		b, _ := os.ReadFile(matches[len(matches)-1])
		if i := strings.Index(string(b), `"holds"`); i >= 0 {
			b = b[i:]
		}
		t.Logf("last record %s: %s", filepath.Base(matches[len(matches)-1]), b)
	}
}

// heldSetup creates a source table in its own schema, with a sequence as its
// cursor, and a scheduled merge pipeline from it into Postgres.
func heldSetup(t *testing.T, prefix, columns, rows, streamSpec string) (name, src, dst string) {
	t.Helper()
	name = unique(prefix)
	in := schema(name)
	src = pgx.Identifier{in, "items"}.Sanitize()
	dst = pgx.Identifier{in + "_dst", "items"}.Sanitize()
	next := "nextval('" + pgx.Identifier{in, "items_at"}.Sanitize() + "')"
	run(t, "CREATE SCHEMA "+pgx.Identifier{in}.Sanitize(),
		"CREATE SEQUENCE "+pgx.Identifier{in, "items_at"}.Sanitize(),
		"CREATE TABLE "+src+" (id bigint PRIMARY KEY, "+columns+", at bigint NOT NULL DEFAULT "+next+")",
		"INSERT INTO "+src+" "+rows)
	apply(t,
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Source", "metadata": {"name": %q},
			"spec": {"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}, "schema": %q}}}`, name, in),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Destination", "metadata": {"name": %q},
			"spec": {"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}}}}`, name),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
			"spec": {"schedule": {"every": "1h"}, "source": {"ref": %q, "streams": [%s]},
			"destination": {"ref": %q, "namespace": %q, "write": "merge"}, "delivery": {"require": "effectively_once"}}}`,
			name, name, streamSpec, name, in+"_dst"))
	return name, src, dst
}

// A column whose type the destination can't change, because a view depends
// on it, is held instead of failing the sync: the rest of each row lands,
// and once the view is gone a release widens the column and patches its
// values in.
func TestAColumnAViewDependsOnIsHeldAndReleased(t *testing.T) {
	startRunner(t)
	name, src, dst := heldSetup(t, "heldview", "v int NOT NULL", "(id, v) SELECT i, i FROM generate_series(1, 50) i",
		`{"name": "items", "sync": "incremental", "cursor": "at"}`)
	if status, stats := syncNow(t, name); status != "succeeded" {
		t.Fatalf("first sync %s: %s", status, stats)
	}
	view := pgx.Identifier{schema(name) + "_dst", "items_v"}.Sanitize()
	run(t, "CREATE VIEW "+view+" AS SELECT id, v FROM "+dst,
		"ALTER TABLE "+src+" ALTER COLUMN v TYPE bigint",
		"UPDATE "+src+" SET v = 3000000000 + id, at = nextval('"+pgx.Identifier{schema(name), "items_at"}.Sanitize()+"') WHERE id <= 10")
	if status, stats := syncNow(t, name); status != "succeeded" {
		t.Fatalf("the sync after the change %s: %s", status, stats)
	}
	// The read takes the cursor's last row again, so row 50 is held too.
	held, err := query[int64](t, "SELECT count(*) FROM "+dst+" WHERE v IS NULL AND _openetl_held ? 'v' AND id <= 10")
	if err != nil || held != 10 {
		t.Fatalf("%d of the changed rows hold v, want 10: %v", held, err)
	}
	if n, _ := query[int64](t, "SELECT count(*) FROM "+dst+" WHERE v IS NOT NULL"); n < 39 {
		t.Errorf("only %d rows kept their v", n)
	}
	if typ, _ := query[string](t, "SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = '"+dst+"'::regclass AND attname = 'v'"); typ != "integer" {
		t.Errorf("v became %s while held", typ)
	}
	holds := listHolds(t, name)
	if len(holds) != 1 || holds[0].GetColumn() != "v" || !holds[0].GetWhole() {
		t.Fatalf("holds %v", holds)
	}

	run(t, "DROP VIEW "+view)
	releaseHold(t, name, "items", "v")
	if status, stats := syncNow(t, name); status != "succeeded" {
		t.Fatalf("the sync after the release %s: %s", status, stats)
	}
	patched := func() bool {
		v, err := query[bool](t, `SELECT NOT EXISTS (SELECT 1 FROM `+src+` s JOIN `+dst+` d USING (id)
			WHERE d.v IS DISTINCT FROM s.v OR d._openetl_held ? 'v')`)
		return err == nil && v
	}
	for deadline := time.Now().Add(30 * time.Second); !patched(); time.Sleep(200 * time.Millisecond) {
		if time.Now().After(deadline) {
			dumpHolds(t, name)
			rows, _ := query[string](t, "SELECT string_agg(id || '=' || coalesce(v::text, 'null') || coalesce(_openetl_held::text, ''), ' ' ORDER BY id) FROM "+dst+" WHERE id <= 12")
			t.Fatalf("the held values weren't patched in: %s", rows)
		}
	}
	if typ, _ := query[string](t, "SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = '"+dst+"'::regclass AND attname = 'v'"); typ != "bigint" {
		t.Errorf("v is %s after the release", typ)
	}
	if holds := listHolds(t, name); len(holds) != 0 {
		t.Errorf("holds after the release: %v", holds)
	}
}

// A value that doesn't fit its type holds its cell when the stream says
// quarantine: cell, and the rest of its row lands. Once the column's type
// is overridden to one that takes it, a release patches it in.
func TestAHeldCellIsPatchedInAfterAnOverride(t *testing.T) {
	startRunner(t)
	stream := `{"name": "items", "sync": "incremental", "cursor": "at", "quarantine": "cell"}`
	name, _, dst := heldSetup(t, "heldcell", "p numeric(10, 2), note text",
		"(id, p, note) SELECT i, CASE WHEN i = 7 THEN 'NaN'::numeric ELSE i END, 'n' || i FROM generate_series(1, 20) i", stream)
	if status, stats := syncNow(t, name); status != "succeeded" {
		t.Fatalf("first sync %s: %s", status, stats)
	}
	row, err := query[string](t, "SELECT coalesce(p::text, '<null>') || ' ' || note || ' ' || (_openetl_held ? 'p')::text FROM "+dst+" WHERE id = 7")
	if err != nil || row != "<null> n7 true" {
		t.Fatalf("row 7 is %q, %v; want its p held and its note delivered", row, err)
	}
	if n, _ := query[int64](t, "SELECT count(*) FROM "+dst); n != 20 {
		t.Fatalf("%d rows landed, want 20", n)
	}
	holds := listHolds(t, name)
	if len(holds) != 1 || holds[0].GetColumn() != "p" || holds[0].GetWhole() || holds[0].GetCells() != 1 {
		t.Fatalf("holds %v", holds)
	}

	in := schema(name)
	apply(t, fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
		"spec": {"schedule": {"every": "1h"}, "source": {"ref": %q, "streams": [{"name": "items", "sync": "incremental", "cursor": "at",
			"quarantine": "cell", "columns": {"p": {"type": "string"}}}]},
		"destination": {"ref": %q, "namespace": %q, "write": "merge"}, "delivery": {"require": "effectively_once"}}}`,
		name, name, name, in+"_dst"))
	releaseHold(t, name, "items", "p")
	if status, stats := syncNow(t, name); status != "succeeded" {
		t.Fatalf("the sync after the release %s: %s", status, stats)
	}
	eventually(t, 30*time.Second, "the held cell to be patched in", func() bool {
		v, err := query[string](t, "SELECT p::text || ' ' || (_openetl_held IS NULL OR NOT _openetl_held ? 'p')::text FROM "+dst+" WHERE id = 7")
		return err == nil && v == "NaN true"
	})
	if holds := listHolds(t, name); len(holds) != 0 {
		t.Errorf("holds after the release: %v", holds)
	}
}
