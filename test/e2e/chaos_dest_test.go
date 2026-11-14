//go:build e2e && chaos

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// chaosDest is a run's destination. Iceberg tables and Kafka topics are
// copied into Postgres tables of the same names before each check, so the
// same queries check every destination.
type chaosDest struct {
	kind string
	// The Destination's spec.
	spec string
	// The destination's namespace, and the schema checks read.
	out    string
	tables []string
	// The column that holds v: v, or a SQL run's name for it.
	value string
	// What plants did to the copies, done again after each refresh.
	planted []string
}

// columns are the ones a copy keeps.
func (d *chaosDest) columns() []string {
	return []string{"id", d.value, "_openetl_op", "_openetl_deleted"}
}

func newChaosDest(t *testing.T, kind, out string, tables []string) *chaosDest {
	t.Helper()
	d := &chaosDest{kind: kind, out: out, tables: tables, value: "v", spec: `{"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}}}`}
	switch kind {
	case "iceberg":
		d.spec = icebergDest(t, out)
	case "kafka":
		d.spec = kafkaDest(t, out)
	}
	if kind != "postgres" {
		run(t, "CREATE SCHEMA "+pgx.Identifier{out}.Sanitize())
	}
	return d
}

// refresh copies what the destination holds into Postgres, unless it is
// Postgres.
func (d *chaosDest) refresh(ctx context.Context) error {
	if d.kind == "postgres" {
		return nil
	}
	rows := map[string][][]any{}
	for _, tbl := range d.tables {
		var err error
		if d.kind == "iceberg" {
			rows[tbl], err = d.icebergRows(ctx, tbl)
		} else {
			rows[tbl], err = d.kafkaRows(ctx, d.out+"."+tbl)
		}
		if err != nil {
			return fmt.Errorf("%s: %w", tbl, err)
		}
	}
	conn, err := pgx.Connect(ctx, env.dbURL)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.Background()) }()
	return pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
		for _, tbl := range d.tables {
			ident := pgx.Identifier{d.out, tbl}
			if _, err := tx.Exec(ctx, "DROP TABLE IF EXISTS "+ident.Sanitize()); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, "CREATE TABLE "+ident.Sanitize()+" (id bigint, "+pgx.Identifier{d.value}.Sanitize()+" bigint, _openetl_op text, _openetl_deleted boolean)"); err != nil {
				return err
			}
			if _, err := tx.CopyFrom(ctx, ident, d.columns(), pgx.CopyFromRows(rows[tbl])); err != nil {
				return err
			}
		}
		for _, q := range d.planted {
			if _, err := tx.Exec(ctx, q); err != nil {
				return err
			}
		}
		return nil
	})
}

// exec runs statements against what checks read: the destination itself
// if it's Postgres, or else the copies, after every refresh from now on.
func (d *chaosDest) exec(t *testing.T, qs ...string) {
	t.Helper()
	if d.kind != "postgres" {
		d.planted = append(d.planted, qs...)
	}
	run(t, qs...)
}

func icebergCatalog(ctx context.Context) (*rest.Catalog, error) {
	return rest.NewCatalog(ctx, "chaos", icebergURI(), rest.WithAdditionalProps(iceberg.Properties{
		"s3.endpoint": s3Endpoint(), "s3.region": "us-east-1", "s3.access-key-id": "openetl", "s3.secret-access-key": "openetl",
	}))
}

func (d *chaosDest) icebergRows(ctx context.Context, name string) ([][]any, error) {
	cat, err := icebergCatalog(ctx)
	if err != nil {
		return nil, err
	}
	tbl, err := cat.LoadTable(ctx, table.Identifier{d.out, name})
	if err != nil {
		return nil, err
	}
	columns := d.columns()
	var fields []string
	for _, c := range columns {
		if _, ok := tbl.Schema().FindFieldByName(c); ok {
			fields = append(fields, c)
		}
	}
	_, it, err := tbl.Scan(table.WithSelectedFields(fields...)).ToArrowRecords(ctx)
	if err != nil {
		return nil, err
	}
	var out [][]any
	for b, err := range it {
		if err != nil {
			return nil, err
		}
		cols := make([]arrow.Array, len(columns))
		for i, c := range columns {
			if idx := b.Schema().FieldIndices(c); len(idx) == 1 {
				cols[i] = b.Column(idx[0])
			}
		}
		for r := range int(b.NumRows()) {
			row := make([]any, len(columns))
			for i, col := range cols {
				if col != nil {
					row[i] = arrowValue(col, r)
				}
			}
			out = append(out, row)
		}
		b.Release()
	}
	return out, nil
}

func arrowValue(col arrow.Array, i int) any {
	if col.IsNull(i) {
		return nil
	}
	switch c := col.(type) {
	case *array.Int64:
		return c.Value(i)
	case *array.Int32:
		return int64(c.Value(i))
	case *array.String:
		return c.Value(i)
	case *array.LargeString:
		return c.Value(i)
	case *array.Boolean:
		return c.Value(i)
	}
	return col.ValueStr(i)
}

// kafkaRows reads every committed record of topic.
func (d *chaosDest) kafkaRows(ctx context.Context, topic string) ([][]any, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(kafkaBrokers()))
	if err != nil {
		return nil, err
	}
	ends, err := kadm.NewClient(cl).ListCommittedOffsets(ctx, topic)
	cl.Close()
	if err != nil {
		return nil, err
	}
	starts, left := map[int32]kgo.Offset{}, map[int32]int64{}
	if err := ends.Error(); err != nil {
		return nil, err
	}
	ends.Each(func(o kadm.ListedOffset) {
		if o.Offset > 0 {
			starts[o.Partition], left[o.Partition] = kgo.NewOffset().AtStart(), o.Offset
		}
	})
	if len(left) == 0 {
		return nil, nil
	}
	cl, err = kgo.NewClient(kgo.SeedBrokers(kafkaBrokers()), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: starts}),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.KeepControlRecords())
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var out [][]any
	for len(left) > 0 {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err := fs.Err0(); err != nil && !errors.Is(err, context.Canceled) {
			return nil, err
		}
		for _, r := range fs.Records() {
			if r.Offset+1 >= left[r.Partition] {
				delete(left, r.Partition)
			}
			if r.Attrs.IsControl() {
				continue
			}
			row, err := kafkaRow(r.Value, d.columns())
			if err != nil {
				return nil, fmt.Errorf("offset %d/%d: %w", r.Partition, r.Offset, err)
			}
			out = append(out, row)
		}
	}
	return out, nil
}

// kafkaRow is a record's columns, the first two numbers.
func kafkaRow(value []byte, columns []string) ([]any, error) {
	var v map[string]any
	dec := json.NewDecoder(bytes.NewReader(value))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("value %q: %w", value, err)
	}
	row := make([]any, len(columns))
	for i, c := range columns {
		switch x := v[c].(type) {
		case json.Number:
			n, err := x.Int64()
			if err != nil {
				return nil, fmt.Errorf("%s: %w", c, err)
			}
			row[i] = n
		case string:
			if i < 2 {
				n, err := strconv.ParseInt(x, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("%s: %w", c, err)
				}
				row[i] = n
			} else {
				row[i] = x
			}
		case bool:
			row[i] = x
		}
	}
	return row, nil
}

func (d *chaosDest) drop() {
	ctx := context.Background()
	switch d.kind {
	case "iceberg":
		cat, err := icebergCatalog(ctx)
		if err != nil {
			return
		}
		for _, tbl := range d.tables {
			_ = cat.PurgeTable(ctx, table.Identifier{d.out, tbl})
		}
		_ = cat.DropNamespace(ctx, table.Identifier{d.out})
	case "kafka":
		cl, err := kgo.NewClient(kgo.SeedBrokers(kafkaBrokers()))
		if err != nil {
			return
		}
		defer cl.Close()
		topics := make([]string, len(d.tables))
		for i, tbl := range d.tables {
			topics[i] = d.out + "." + tbl
		}
		_, _ = kadm.NewClient(cl).DeleteTopics(ctx, topics...)
	}
}
