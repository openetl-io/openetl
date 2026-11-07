//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
)

func listHolds(t *testing.T, pipeline string) []*apiv1.Hold {
	t.Helper()
	resp, err := pipelines().ListHolds(t.Context(), connect.NewRequest(&apiv1.ListHoldsRequest{Parent: "workspaces/default/pipelines/" + pipeline}))
	if err != nil {
		t.Fatal(err)
	}
	return resp.Msg.GetHolds()
}

func releaseHold(t *testing.T, pipeline, stream, column string, newVersion ...bool) {
	t.Helper()
	_, err := pipelines().ReleaseHold(t.Context(), connect.NewRequest(&apiv1.ReleaseHoldRequest{
		Parent: "workspaces/default/pipelines/" + pipeline, Stream: stream, Column: column, NewVersion: slices.Contains(newVersion, true),
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
			"spec": {"schedule": {"every": "720h"}, "source": {"ref": %q, "streams": [%s]},
			"destination": {"ref": %q, "namespace": %q, "write": "merge"}, "delivery": {"require": "effectively_once"}}}`,
			name, name, streamSpec, name, in+"_dst"))
	return name, src, dst
}

// A column whose type widened upstream is held, since Postgres doesn't
// change a column's type: the rest of each row lands, and once the user has
// altered the column a release patches its values in.
func TestAWidenedColumnIsHeldAndReleased(t *testing.T) {
	startRunner(t)
	name, src, dst := heldSetup(t, "heldview", "v int NOT NULL", "(id, v) SELECT i, i FROM generate_series(1, 50) i",
		`{"name": "items", "sync": "incremental", "cursor": "at"}`)
	if status, stats := syncNow(t, name); status != "succeeded" {
		t.Fatalf("first sync %s: %s", status, stats)
	}
	run(t, "ALTER TABLE "+src+" ALTER COLUMN v TYPE bigint",
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
	c := holds[0].GetCause()
	if c.GetKind() != apiv1.HoldCause_KIND_REFUSED || c.GetFrom() != "int32" || c.GetTo() != "int64" ||
		c.GetFromType() != "integer" || c.GetToType() != "bigint" || !strings.Contains(c.GetFix(), `ALTER COLUMN "v" TYPE bigint`) {
		t.Errorf("cause %v", c)
	}
	if r := holds[0].GetReason(); !strings.HasPrefix(r, "v changed from int32 to int64 (integer to bigint in the destination), and the destination refused it: ") {
		t.Errorf("reason %q", r)
	}

	// Released before the ALTER, the destination refuses it again: the hold
	// says so, and the release isn't tried again.
	releaseHold(t, name, "items", "v")
	for range 2 {
		if status, stats := syncNow(t, name); status != "succeeded" {
			t.Fatalf("a sync after a release the destination refuses %s: %s", status, stats)
		}
		holds = listHolds(t, name)
		if len(holds) != 1 || holds[0].GetRefused() == nil || holds[0].GetPending() || len(holds[0].GetWaysOut()) != 3 {
			t.Fatalf("holds after a refused release %v", holds)
		}
	}

	run(t, "ALTER TABLE "+dst+" ALTER COLUMN v TYPE bigint")
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

// A stream whose spec allows type changes has a widened column changed in
// place: nothing is held, and every row lands with its new value.
func TestAWidenedColumnLandsInPlaceWhenTheSpecAllows(t *testing.T) {
	startRunner(t)
	name, src, dst := heldSetup(t, "widened", "v int NOT NULL", "(id, v) SELECT i, i FROM generate_series(1, 50) i",
		`{"name": "items", "sync": "incremental", "cursor": "at", "typeChanges": "allow"}`)
	if status, stats := syncNow(t, name); status != "succeeded" {
		t.Fatalf("first sync %s: %s", status, stats)
	}
	run(t, "ALTER TABLE "+src+" ALTER COLUMN v TYPE bigint",
		"UPDATE "+src+" SET v = 3000000000 + id, at = nextval('"+pgx.Identifier{schema(name), "items_at"}.Sanitize()+"') WHERE id <= 10")
	if status, stats := syncNow(t, name); status != "succeeded" {
		t.Fatalf("the sync after the change %s: %s", status, stats)
	}
	if typ, _ := query[string](t, "SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = '"+dst+"'::regclass AND attname = 'v'"); typ != "bigint" {
		t.Errorf("v is %s, want bigint", typ)
	}
	if n, err := query[int64](t, `SELECT count(*) FROM `+src+` s FULL JOIN `+dst+` d USING (id) WHERE d.v IS DISTINCT FROM s.v`); err != nil || n != 0 {
		t.Errorf("%d rows differ from the source: %v", n, err)
	}
	if holds := listHolds(t, name); len(holds) != 0 {
		t.Errorf("holds %v", holds)
	}
}

// A widened column released to a new version needs no ALTER: the column
// keeps its type, v__v2 is added with the new one, and the held values,
// and every value from then on, land in it.
func TestAWidenedColumnIsReleasedToANewVersion(t *testing.T) {
	startRunner(t)
	name, src, dst := heldSetup(t, "heldv2", "v int NOT NULL", "(id, v) SELECT i, i FROM generate_series(1, 50) i",
		`{"name": "items", "sync": "incremental", "cursor": "at"}`)
	if status, stats := syncNow(t, name); status != "succeeded" {
		t.Fatalf("first sync %s: %s", status, stats)
	}
	next := "nextval('" + pgx.Identifier{schema(name), "items_at"}.Sanitize() + "')"
	run(t, "ALTER TABLE "+src+" ALTER COLUMN v TYPE bigint",
		"UPDATE "+src+" SET v = 3000000000 + id, at = "+next+" WHERE id <= 10")
	if status, stats := syncNow(t, name); status != "succeeded" {
		t.Fatalf("the sync after the change %s: %s", status, stats)
	}
	if holds := listHolds(t, name); len(holds) != 1 || holds[0].GetColumn() != "v" || !holds[0].GetWhole() {
		t.Fatalf("holds %v", holds)
	}

	releaseHold(t, name, "items", "v", true)
	run(t, "UPDATE "+src+" SET v = 4000000000 + id, at = "+next+" WHERE id IN (20, 21)")
	if status, stats := syncNow(t, name); status != "succeeded" {
		t.Fatalf("the sync after the release %s: %s", status, stats)
	}
	landed := func() string {
		v, err := query[string](t, `SELECT string_agg(id || '=' || coalesce(v::text, '-') || '/' || coalesce(v__v2::text, '-') || coalesce(_openetl_held::text, ''), ' ' ORDER BY id)
			FROM `+dst+` WHERE id IN (1, 10, 11, 20, 21)`)
		if err != nil {
			return err.Error()
		}
		return v
	}
	want := "1=-/3000000001 10=-/3000000010 11=11/- 20=-/4000000020 21=-/4000000021"
	eventually(t, 30*time.Second, "the held values to land in v__v2", func() bool { return landed() == want })
	typ := func(c string) string {
		v, _ := query[string](t, "SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = '"+dst+"'::regclass AND attname = '"+c+"'")
		return v
	}
	if typ("v") != "integer" || typ("v__v2") != "bigint" {
		t.Errorf("v is %s and v__v2 is %s", typ("v"), typ("v__v2"))
	}
	if holds := listHolds(t, name); len(holds) != 0 {
		t.Errorf("holds after the release: %v", holds)
	}
}

// A value that doesn't fit its type holds its cell when the stream says
// quarantine: cell, and the rest of its row lands. Once the column's type
// is overridden to one that takes it, and the user has altered the column
// to match, a release patches it in.
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
		"spec": {"schedule": {"every": "720h"}, "source": {"ref": %q, "streams": [{"name": "items", "sync": "incremental", "cursor": "at",
			"quarantine": "cell", "columns": {"p": {"type": "string"}}}]},
		"destination": {"ref": %q, "namespace": %q, "write": "merge"}, "delivery": {"require": "effectively_once"}}}`,
		name, name, name, in+"_dst"))
	run(t, "ALTER TABLE "+dst+" ALTER COLUMN p TYPE text")
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

// Under block, a CDC stream's new column is held instead of stopping the
// run, which goes on delivering the rest of each row. A release restarts the
// run, adds the column and patches its held values in.
func TestAContinuousRunHoldsANewColumnUnderBlock(t *testing.T) {
	startRunner(t)
	name := unique("heldcdc")
	in := schema(name)
	src := pgx.Identifier{in, "users"}.Sanitize()
	dst := pgx.Identifier{in + "_dst", "users"}.Sanitize()
	run(t, "CREATE SCHEMA "+pgx.Identifier{in}.Sanitize(),
		"CREATE TABLE "+src+" (id bigint PRIMARY KEY, name text)",
		"INSERT INTO "+src+" SELECT i, 'u' || i FROM generate_series(1, 20) i",
		"CREATE PUBLICATION "+pgx.Identifier{in}.Sanitize()+" FOR TABLE "+src)
	t.Cleanup(func() {
		_, _ = pipelines().DeletePipeline(context.Background(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: "workspaces/default/pipelines/" + name}))
		if conn, err := pgx.Connect(context.Background(), env.dbURL); err == nil {
			_, _ = conn.Exec(context.Background(), "DROP PUBLICATION IF EXISTS "+pgx.Identifier{in}.Sanitize())
			_ = conn.Close(context.Background())
		}
	})
	apply(t,
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Source", "metadata": {"name": %q},
			"spec": {"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}, "schema": %q, "publication": %q}}}`, name, in, in),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Destination", "metadata": {"name": %q},
			"spec": {"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}}}}`, name),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
			"spec": {"mode": "continuous", "schemaPolicy": "block", "source": {"ref": %q}, "destination": {"ref": %q, "namespace": %q},
			"delivery": {"commit": {"interval": "1s"}}}}`, name, name, name, in+"_dst"))
	eventually(t, 90*time.Second, "the snapshot", func() bool {
		n, err := query[int64](t, "SELECT count(*) FROM "+dst)
		return err == nil && n == 20
	})

	run(t, "ALTER TABLE "+src+" ADD COLUMN email text",
		"UPDATE "+src+" SET email = 'e' || id, name = name || '!' WHERE id <= 5",
		"INSERT INTO "+src+" VALUES (21, 'u21!', 'e21')")
	changed := func() bool {
		n, err := query[int64](t, "SELECT count(*) FROM "+dst+" WHERE name LIKE '%!' AND _openetl_held ? 'email'")
		return err == nil && n == 6
	}
	for deadline := time.Now().Add(90 * time.Second); !changed(); time.Sleep(200 * time.Millisecond) {
		if time.Now().After(deadline) {
			rows, err := query[*string](t, "SELECT string_agg(id || ' ' || name, '; ' ORDER BY id) FROM "+dst+" WHERE id <= 5 OR id = 21")
			syncs, _ := query[*string](t, "SELECT string_agg(s.status || ' ' || coalesce(s.error, ''), '; ') FROM syncs s JOIN pipelines p ON p.id = s.pipeline_id WHERE p.name = $1", name)
			t.Fatalf("the changed rows: %v %v; runs: %v", deref(rows), err, deref(syncs))
		}
	}
	if has, _ := query[bool](t, "SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = '"+dst+"'::regclass AND attname = 'email')"); has {
		t.Error("the held column was added")
	}
	holds := listHolds(t, name)
	if len(holds) != 1 || holds[0].GetColumn() != "email" || !holds[0].GetWhole() {
		t.Fatalf("holds %v", holds)
	}

	releaseHold(t, name, "users", "email")
	eventually(t, 120*time.Second, "the held values to be patched in", func() bool {
		v, err := query[bool](t, `SELECT count(*) = 21 AND bool_and(d.email IS NOT DISTINCT FROM s.email AND NOT coalesce(d._openetl_held ? 'email', false))
			FROM `+src+` s JOIN `+dst+` d USING (id)`)
		return err == nil && v
	})
	run(t, "UPDATE "+src+" SET email = 'again' WHERE id = 1")
	eventually(t, 60*time.Second, "the column delivered again", func() bool {
		v, err := query[string](t, "SELECT email FROM "+dst+" WHERE id = 1")
		return err == nil && v == "again"
	})
	if holds := listHolds(t, name); len(holds) != 0 {
		t.Errorf("holds after the release: %v", holds)
	}
}

func deref(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}
