//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/jackc/pgx/v5"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
)

// A scheduled pipeline whose SQL renames columns and filters rows: the
// statement's output lands under its own name, and the stream it reads
// passes through as well.
func TestSQLTransformRenamesAndFilters(t *testing.T) {
	startRunner(t)
	name := unique("sql-rename")
	sch := pgx.Identifier{schema(name)}.Sanitize()
	conn, err := pgx.Connect(t.Context(), env.dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	for _, q := range []string{
		"CREATE SCHEMA " + sch,
		"CREATE TABLE " + sch + ".customers (id bigint PRIMARY KEY, email text, active boolean NOT NULL)",
		"INSERT INTO " + sch + ".customers SELECT i, 'User' || i || '@Example.COM', i % 3 <> 0 FROM generate_series(1, 900) i",
	} {
		if _, err := conn.Exec(t.Context(), q); err != nil {
			t.Fatal(err)
		}
	}
	dir := t.TempDir()
	sql, err := json.Marshal("INSERT INTO contacts\nSELECT id AS customer_id, lower(email) AS address\nFROM customers\nWHERE active")
	if err != nil {
		t.Fatal(err)
	}
	src := fmt.Sprintf(`{"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}, "schema": %q}}`, schema(name))
	apply(t,
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Source", "metadata": {"name": %q}, "spec": %s}`, name, src),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Destination", "metadata": {"name": %q}, "spec": %s}`, name, parquetDest(dir)),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
			"spec": {"schedule": {"every": "720h"}, "source": {"ref": %q}, "destination": {"ref": %q}, "transform": {"sql": %s}}}`, name, name, name, sql),
	)
	if status, stats := waitSync(t, name, 90*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	checkParquet(t, dir, 900, "customers")
	addresses, rows := contactRows(t, dir)
	if rows != 600 || len(addresses) != 600 {
		t.Errorf("contacts: %d rows of %d customers, want each of the 600 active ones once", rows, len(addresses))
	}
	for id := int64(1); id <= 900; id++ {
		a, ok := addresses[id]
		switch {
		case id%3 == 0 && ok:
			t.Errorf("contacts has inactive customer %d", id)
		case id%3 != 0 && a != fmt.Sprintf("user%d@example.com", id):
			t.Errorf("customer %d's address is %q", id, a)
		}
	}
}

// A continuous CDC pipeline whose SQL renames columns, the key among them,
// and filters rows, merged into Postgres: each row the filter keeps lands
// once, under the statement's names; a row an update takes out of the
// filter leaves the table, and one an update brings in lands. A runner
// killed mid-stream hands the job to another, and the same holds after.
func TestSQLTransformOnCDC(t *testing.T) {
	a, b := startRunner(t), startRunner(t)
	name := unique("sql-cdc")
	in, out := schema(name), schema(name)+"_out"
	src := pgx.Identifier{in, "users"}.Sanitize()
	dst := pgx.Identifier{out, "users"}.Sanitize()
	plans := "(ARRAY['free', 'pro', 'team'])[i % 3 + 1]"
	run(t, "CREATE SCHEMA "+pgx.Identifier{in}.Sanitize(),
		"CREATE TABLE "+src+" (id bigint PRIMARY KEY, name text, plan text NOT NULL)",
		"INSERT INTO "+src+" SELECT i, 'u' || i, "+plans+" FROM generate_series(1, 300) i")
	t.Cleanup(func() {
		if conn, err := pgx.Connect(context.Background(), env.dbURL); err == nil {
			_, _ = conn.Exec(context.Background(), "DROP PUBLICATION IF EXISTS "+pgx.Identifier{in}.Sanitize())
			_ = conn.Close(context.Background())
		}
	})
	sql, err := json.Marshal("INSERT INTO users\nSELECT id AS user_id, upper(name) AS full_name, plan AS tier\nFROM users\nWHERE plan <> 'free'")
	if err != nil {
		t.Fatal(err)
	}
	apply(t,
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Source", "metadata": {"name": %q},
			"spec": {"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}, "schema": %q, "publication": %q, "snapshotChunkRows": 50}}}`, name, in, in),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Destination", "metadata": {"name": %q},
			"spec": {"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}}}}`, name),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
			"spec": {"mode": "continuous", "source": {"ref": %q}, "destination": {"ref": %q, "namespace": %q},
			"delivery": {"commit": {"interval": "1s"}}, "transform": {"sql": %s}}}`, name, name, name, out, sql))
	t.Cleanup(func() {
		_, _ = pipelines().DeletePipeline(context.Background(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: "workspaces/default/pipelines/" + name}))
	})

	// The table's live rows are the source's that the filter keeps, each
	// once, under the statement's names.
	matches := func() bool {
		v, err := query[bool](t, `SELECT
			NOT EXISTS (SELECT user_id, full_name, tier FROM `+dst+` WHERE NOT _openetl_deleted
				EXCEPT SELECT id, upper(name), plan FROM `+src+` WHERE plan <> 'free')
			AND NOT EXISTS (SELECT id, upper(name), plan FROM `+src+` WHERE plan <> 'free'
				EXCEPT SELECT user_id, full_name, tier FROM `+dst+` WHERE NOT _openetl_deleted)
			AND (SELECT count(*) = count(DISTINCT user_id) FROM `+dst+`)`)
		return err == nil && v
	}
	eventually(t, 60*time.Second, "the snapshot", matches)
	if n := mustQuery[int](t, "SELECT count(*) FROM "+dst+" WHERE NOT _openetl_deleted"); n != 200 {
		t.Errorf("%d live rows, want the 200 not on the free plan", n)
	}
	columns := mustQuery[string](t, `SELECT string_agg(column_name, ',' ORDER BY column_name) FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = 'users' AND column_name NOT LIKE '\_openetl\_%'`, out)
	if columns != "full_name,tier,user_id" {
		t.Errorf("the table's columns are %s, want the statement's: full_name, tier and user_id", columns)
	}

	// Of users 1 to 30, the 20 not on the free plan leave it, and leave the
	// table; of 31 to 60, the 10 on it join another, and land.
	run(t, "UPDATE "+src+" SET plan = 'free' WHERE id BETWEEN 1 AND 30 AND plan <> 'free'",
		"UPDATE "+src+" SET plan = 'pro' WHERE id BETWEEN 31 AND 60 AND plan = 'free'",
		"UPDATE "+src+" SET name = 'renamed' || id WHERE id BETWEEN 61 AND 90",
		"DELETE FROM "+src+" WHERE id BETWEEN 91 AND 100",
		"INSERT INTO "+src+" SELECT i, 'new' || i, "+plans+" FROM generate_series(301, 360) i")
	eventually(t, 60*time.Second, "the changes", matches)
	if n := mustQuery[int](t, "SELECT count(*) FROM "+dst+" WHERE user_id BETWEEN 1 AND 30 AND user_id % 3 <> 0 AND _openetl_deleted"); n != 20 {
		t.Errorf("%d of the 20 rows that left the filter are deleted", n)
	}

	host(t, a, b).kill(t)
	run(t, "UPDATE "+src+" SET plan = CASE WHEN plan = 'free' THEN 'team' ELSE 'free' END WHERE id > 300")
	eventually(t, 90*time.Second, "the changes after the runner died", matches)
	if got := statuses(runs(t, name)); got != "running" {
		t.Errorf("runs %q, want one running", got)
	}
}

// contactRows reads the contacts output's committed files: each row's
// address by its customer_id, and how many rows there are.
func contactRows(t *testing.T, dir string) (map[int64]string, int) {
	t.Helper()
	names, err := filepath.Glob(filepath.Join(dir, "contacts", "*.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	addresses := map[int64]string{}
	rows := 0
	for _, name := range names {
		f, err := os.Open(name)
		if err != nil {
			t.Fatal(err)
		}
		tbl, err := pqarrow.ReadTable(t.Context(), f, parquet.NewReaderProperties(nil), pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
		_ = f.Close()
		if err != nil {
			t.Fatal(err)
		}
		// Row i of one column is row i of the other, whatever their chunks.
		var ids []int64
		for _, chunk := range tbl.Column(tbl.Schema().FieldIndices("customer_id")[0]).Data().Chunks() {
			for i := range chunk.Len() {
				ids = append(ids, chunk.(*array.Int64).Value(i))
			}
		}
		j := 0
		for _, chunk := range tbl.Column(tbl.Schema().FieldIndices("address")[0]).Data().Chunks() {
			for i := range chunk.Len() {
				addresses[ids[j]] = chunk.(*array.String).Value(i)
				j++
			}
		}
		rows += len(ids)
		tbl.Release()
	}
	return addresses, rows
}
