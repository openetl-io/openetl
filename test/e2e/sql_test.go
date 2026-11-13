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

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/jackc/pgx/v5"
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
