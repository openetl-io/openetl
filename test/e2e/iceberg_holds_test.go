//go:build e2e

package e2e

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/jackc/pgx/v5"
)

// icebergHeldSetup creates a source table v int in its own schema, with a
// sequence as its cursor, and a scheduled merge pipeline from it into an
// Iceberg namespace of its own, and runs its first sync.
func icebergHeldSetup(t *testing.T, prefix string) (name, src, ns string) {
	t.Helper()
	name = unique(prefix)
	in := schema(name)
	ns = in + "_dst"
	dst := icebergDest(t, ns)
	src = pgx.Identifier{in, "items"}.Sanitize()
	run(t, "CREATE SCHEMA "+pgx.Identifier{in}.Sanitize(),
		"CREATE SEQUENCE "+pgx.Identifier{in, "items_at"}.Sanitize(),
		"CREATE TABLE "+src+" (id bigint PRIMARY KEY, v int NOT NULL, at bigint NOT NULL DEFAULT nextval('"+pgx.Identifier{in, "items_at"}.Sanitize()+"'))",
		"INSERT INTO "+src+" (id, v) SELECT i, i FROM generate_series(1, 50) i")
	apply(t,
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Source", "metadata": {"name": %q},
			"spec": {"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}, "schema": %q}}}`, name, in),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Destination", "metadata": {"name": %q}, "spec": %s}`, name, dst),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
			"spec": {"schedule": {"every": "1h"}, "source": {"ref": %q, "streams": [{"name": "items", "sync": "incremental", "cursor": "at"}]},
			"destination": {"ref": %q, "write": "merge"}, "delivery": {"require": "effectively_once", "commit": {"interval": "1s"}}}}`,
			name, name, name))
	if status, stats := syncNow(t, name); status != "succeeded" {
		t.Fatalf("first sync %s: %s", status, stats)
	}
	return name, src, ns
}

// widen widens the source's v and gives rows 1 to 10 values only a bigint
// takes, then syncs: the destination refuses the change, and v is held.
func widen(t *testing.T, name, src string) {
	t.Helper()
	run(t, "ALTER TABLE "+src+" ALTER COLUMN v TYPE bigint",
		"UPDATE "+src+" SET v = 3000000000 + id, at = nextval('"+pgx.Identifier{schema(name), "items_at"}.Sanitize()+"') WHERE id <= 10")
	if status, stats := syncNow(t, name); status != "succeeded" {
		t.Fatalf("the sync after the change %s: %s", status, stats)
	}
	if holds := listHolds(t, name); len(holds) != 1 || holds[0].GetColumn() != "v" || !holds[0].GetWhole() {
		t.Fatalf("holds %v", holds)
	}
}

// icebergValues returns the table's rows with the given ids, sorted, each as
// id=, cols' values joined by '/' with '-' for null, and its held columns
// in brackets.
func icebergValues(t *testing.T, ns string, ids []int64, cols ...string) string {
	t.Helper()
	tbl, err := icebergTable(t, ns, "items")
	if err != nil {
		return err.Error()
	}
	_, it, err := tbl.Scan().ToArrowRecords(t.Context())
	if err != nil {
		return err.Error()
	}
	type row struct {
		id  int64
		val string
	}
	var out []row
	for b, err := range it {
		if err != nil {
			return err.Error()
		}
		sc := b.Schema()
		for r := range int(b.NumRows()) {
			id := b.Column(sc.FieldIndices("id")[0]).(*array.Int64).Value(r)
			if !slices.Contains(ids, id) {
				continue
			}
			vals := make([]string, len(cols))
			for i, c := range cols {
				vals[i] = "-"
				if idx := sc.FieldIndices(c); len(idx) > 0 && !b.Column(idx[0]).IsNull(r) {
					vals[i] = b.Column(idx[0]).ValueStr(r)
				}
			}
			held := ""
			if idx := sc.FieldIndices("_openetl_held"); len(idx) > 0 {
				if m, ok := b.Column(idx[0]).(*array.Map); ok && !m.IsNull(r) {
					start, end := m.ValueOffsets(r)
					for i := start; i < end; i++ {
						held += "[" + m.Keys().ValueStr(int(i)) + "]"
					}
				}
			}
			out = append(out, row{id, fmt.Sprintf("%d=%s%s", id, strings.Join(vals, "/"), held)})
		}
		b.Release()
	}
	slices.SortFunc(out, func(a, b row) int { return int(a.id - b.id) })
	vals := make([]string, len(out))
	for i, r := range out {
		vals[i] = r.val
	}
	return strings.Join(vals, " ")
}

// eventuallyValues waits for the rows with the given ids to be want.
func eventuallyValues(t *testing.T, name, ns string, ids []int64, want string, cols ...string) {
	t.Helper()
	got := ""
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		if got = icebergValues(t, ns, ids, cols...); got == want {
			return
		}
		if time.Now().After(deadline) {
			dumpHolds(t, name)
			t.Fatalf("got %s, want %s", got, want)
		}
	}
}

func icebergType(t *testing.T, ns, column string) string {
	t.Helper()
	tbl, err := icebergTable(t, ns, "items")
	if err != nil {
		t.Fatal(err)
	}
	f, ok := tbl.Schema().FindFieldByName(column)
	if !ok {
		return ""
	}
	return f.Type.String()
}

// Iceberg never changes a column's type either, though it could promote
// int to long by metadata: a widened column is held, the rest of each row
// lands, and once the user has promoted the column a release patches its
// values in, rewriting their rows.
func TestAWidenedColumnIsHeldInIcebergAndReleased(t *testing.T) {
	startRunner(t)
	name, src, ns := icebergHeldSetup(t, "iceheld")
	widen(t, name, src)
	if got, want := icebergValues(t, ns, []int64{1, 10, 11}, "v"), "1=-[v] 10=-[v] 11=11"; got != want {
		t.Fatalf("after the change: %s, want %s", got, want)
	}
	if typ := icebergType(t, ns, "v"); typ != "int" {
		t.Errorf("v became %s while held", typ)
	}

	tbl, err := icebergTable(t, ns, "items")
	if err != nil {
		t.Fatal(err)
	}
	tx := tbl.NewTransaction()
	err = tx.UpdateSchema(true, false).
		UpdateColumn([]string{"v"}, table.ColumnUpdate{FieldType: iceberg.Optional[iceberg.Type]{Valid: true, Val: iceberg.PrimitiveTypes.Int64}}).Commit()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	releaseHold(t, name, "items", "v")
	if status, stats := syncNow(t, name); status != "succeeded" {
		t.Fatalf("the sync after the release %s: %s", status, stats)
	}
	eventuallyValues(t, name, ns, []int64{1, 10, 11, 50}, "1=3000000001 10=3000000010 11=11 50=50", "v")
	if typ := icebergType(t, ns, "v"); typ != "long" {
		t.Errorf("v is %s after the release", typ)
	}
	if holds := listHolds(t, name); len(holds) != 0 {
		t.Errorf("holds after the release: %v", holds)
	}
	checkIceberg(t, ns, 50, "items")
}

// A widened column released to a new version needs no change to the
// table: v keeps its type, v__v2 is added with the new one, and the held
// values, and every value from then on, land in it.
func TestAWidenedColumnIsReleasedToANewVersionInIceberg(t *testing.T) {
	startRunner(t)
	name, src, ns := icebergHeldSetup(t, "icev2")
	widen(t, name, src)

	releaseHold(t, name, "items", "v", true)
	run(t, "UPDATE "+src+" SET v = 4000000000 + id, at = nextval('"+pgx.Identifier{schema(name), "items_at"}.Sanitize()+"') WHERE id IN (20, 21)")
	if status, stats := syncNow(t, name); status != "succeeded" {
		t.Fatalf("the sync after the release %s: %s", status, stats)
	}
	eventuallyValues(t, name, ns, []int64{1, 10, 11, 20, 21}, "1=-/3000000001 10=-/3000000010 11=11/- 20=-/4000000020 21=-/4000000021", "v", "v__v2")
	if typ, typ2 := icebergType(t, ns, "v"), icebergType(t, ns, "v__v2"); typ != "int" || typ2 != "long" {
		t.Errorf("v is %s and v__v2 is %s", typ, typ2)
	}
	if holds := listHolds(t, name); len(holds) != 0 {
		t.Errorf("holds after the release: %v", holds)
	}
	checkIceberg(t, ns, 50, "items")
}
