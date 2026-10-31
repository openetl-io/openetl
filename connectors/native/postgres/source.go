package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/jackc/pgx/v5"

	"github.com/unsuitable001/openetl/sdk/go/source"
)

type SourceConfig struct {
	// A connection URL or a keyword/value connection string.
	DSN string `json:"dsn"`
	// Default "public".
	Schema string `json:"schema"`
	// The tables to offer as streams. Empty: every table in the schema.
	Tables []string `json:"tables"`
	// Rows per query on a table with a primary key; each chunk ends at a
	// resume point. Default 100000.
	ChunkRows int `json:"chunkRows"`
	// Approximate bytes of values per Arrow batch. Default 4 MiB.
	BatchBytes int `json:"batchBytes"`
}

// Source reads full snapshots of tables. A table with a primary key is read
// in key order, a chunk per query, so a resumed read yields exactly the rows
// after its last chunk. A table without one is read in a single query.
type Source struct {
	cfg  SourceConfig
	conn *pgx.ConnConfig
}

var _ source.Source = (*Source)(nil)

func NewSource(config []byte) (*Source, error) {
	cfg := SourceConfig{Schema: "public", ChunkRows: 100_000, BatchBytes: 4 << 20}
	dec := json.NewDecoder(bytes.NewReader(config))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("postgres: config: %w", err)
	}
	switch {
	case cfg.DSN == "":
		return nil, errors.New("postgres: config needs a dsn")
	case cfg.ChunkRows <= 0 || cfg.BatchBytes <= 0:
		return nil, errors.New("postgres: chunkRows and batchBytes must be positive")
	}
	cc, err := pgx.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres: dsn: %w", err)
	}
	// Text values, and the keys that resume a read, are spelled in these.
	for k, v := range map[string]string{"DateStyle": "ISO, YMD", "TimeZone": "UTC", "IntervalStyle": "iso_8601", "extra_float_digits": "3"} {
		cc.RuntimeParams[k] = v
	}
	return &Source{cfg: cfg, conn: cc}, nil
}

type table struct {
	name string
	cols []sourceColumn
	// Indexes into cols, in key order.
	key []int
}

type sourceColumn struct {
	name   string
	oid    uint32
	typmod int32
}

func (s *Source) Discover(ctx context.Context) (source.Catalog, error) {
	conn, err := pgx.ConnectConfig(ctx, s.conn)
	if err != nil {
		return source.Catalog{}, fmt.Errorf("postgres: connect: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	tables, err := s.tables(ctx, conn)
	if err != nil {
		return source.Catalog{}, err
	}
	var c source.Catalog
	for _, t := range tables {
		cs := source.CatalogStream{Name: t.name, Replay: source.Exact}
		for _, i := range t.key {
			cs.PrimaryKey = append(cs.PrimaryKey, t.cols[i].name)
		}
		if len(t.key) == 0 {
			// Its only resume point is at the end.
			cs.Replay = source.Superset
		}
		c.Streams = append(c.Streams, cs)
	}
	return c, nil
}

// tables lists the configured tables, in name order.
func (s *Source) tables(ctx context.Context, conn *pgx.Conn) ([]table, error) {
	rows, err := conn.Query(ctx, `SELECT c.relname, a.attname, a.atttypid, a.atttypmod,
			coalesce((SELECT k.ord FROM unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord) WHERE k.attnum = a.attnum), 0)
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
		LEFT JOIN pg_index i ON i.indrelid = c.oid AND i.indisprimary
		WHERE n.nspname = $1 AND c.relkind IN ('r', 'p') AND NOT c.relispartition
			AND (coalesce(cardinality($2::text[]), 0) = 0 OR c.relname = ANY($2))
		ORDER BY c.relname, a.attnum`, s.cfg.Schema, s.cfg.Tables)
	if err != nil {
		return nil, fmt.Errorf("postgres: list tables: %w", err)
	}
	var out []table
	var keyPos []int
	finish := func() {
		if len(out) == 0 {
			return
		}
		t := &out[len(out)-1]
		for pos := 1; ; pos++ {
			i := indexOf(keyPos, pos)
			if i < 0 {
				break
			}
			t.key = append(t.key, i)
		}
	}
	for rows.Next() {
		var name string
		var c sourceColumn
		var pos int64
		if err := rows.Scan(&name, &c.name, &c.oid, &c.typmod, &pos); err != nil {
			return nil, fmt.Errorf("postgres: list tables: %w", err)
		}
		if len(out) == 0 || out[len(out)-1].name != name {
			finish()
			out = append(out, table{name: name})
			keyPos = keyPos[:0]
		}
		t := &out[len(out)-1]
		t.cols = append(t.cols, c)
		keyPos = append(keyPos, int(pos))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: list tables: %w", err)
	}
	finish()
	for _, want := range s.cfg.Tables {
		if !hasTable(out, want) {
			return nil, fmt.Errorf("postgres: no table %q in schema %q", want, s.cfg.Schema)
		}
	}
	return out, nil
}

func indexOf(xs []int, x int) int {
	for i, v := range xs {
		if v == x {
			return i
		}
	}
	return -1
}

func hasTable(ts []table, name string) bool {
	for _, t := range ts {
		if t.name == name {
			return true
		}
	}
	return false
}

type sourceState struct {
	// The primary key of the last row read, spelled as text.
	After []string `json:"after,omitempty"`
	// Set once a table without a primary key has been read.
	Done bool `json:"done,omitempty"`
}

func (s *Source) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	conn, err := pgx.ConnectConfig(ctx, s.conn)
	if err != nil {
		return fmt.Errorf("postgres: connect: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	tables, err := s.tables(ctx, conn)
	if err != nil {
		return err
	}
	for _, cs := range req.Streams {
		i := 0
		for i < len(tables) && tables[i].name != cs.Name {
			i++
		}
		if i == len(tables) {
			return fmt.Errorf("postgres: unknown stream %q", cs.Name)
		}
		var st sourceState
		if cs.State != nil {
			if err := json.Unmarshal(cs.State, &st); err != nil {
				return fmt.Errorf("postgres: %s state: %w", cs.Name, err)
			}
		}
		if err := s.readTable(ctx, conn, tables[i], st, out); err != nil {
			return err
		}
	}
	return nil
}

func (s *Source) readTable(ctx context.Context, conn *pgx.Conn, t table, st sourceState, out source.Emitter) error {
	b, err := newBatcher(memory.DefaultAllocator, t.cols)
	if err != nil {
		return fmt.Errorf("postgres: %s: %w", t.name, err)
	}
	defer b.release()
	ident := pgx.Identifier{s.cfg.Schema, t.name}.Sanitize()
	cols := make([]string, len(t.cols))
	for i, c := range t.cols {
		cols[i] = pgx.Identifier{c.name}.Sanitize()
	}
	list := strings.Join(cols, ", ")

	if len(t.key) == 0 {
		if !st.Done {
			if _, _, err := s.query(ctx, conn, b, t.name, "SELECT "+list+" FROM "+ident, nil, 0, out); err != nil {
				return err
			}
			if err := resume(out, t.name, sourceState{Done: true}); err != nil {
				return err
			}
		}
		return out.Complete(t.name)
	}

	key := make([]string, len(t.key))
	keyText := make([]string, len(t.key))
	params := make([]string, len(t.key))
	for i, k := range t.key {
		key[i] = cols[k]
		keyText[i] = cols[k] + "::text AS _openetl_key_" + strconv.Itoa(i)
		params[i] = "$" + strconv.Itoa(i+1)
	}
	head := "SELECT " + list + ", " + strings.Join(keyText, ", ") + " FROM " + ident
	tail := " ORDER BY " + strings.Join(key, ", ") + " LIMIT " + strconv.Itoa(s.cfg.ChunkRows)
	after := st.After
	if after != nil && len(after) != len(t.key) {
		return fmt.Errorf("postgres: %s state has %d key columns, the table has %d", t.name, len(after), len(t.key))
	}
	for {
		sql := head + tail
		if after != nil {
			sql = head + " WHERE (" + strings.Join(key, ", ") + ") > (" + strings.Join(params, ", ") + ")" + tail
		}
		n, last, err := s.query(ctx, conn, b, t.name, sql, after, len(t.key), out)
		if err != nil {
			return err
		}
		if n > 0 {
			after = last
		}
		if err := resume(out, t.name, sourceState{After: after}); err != nil {
			return err
		}
		if n < s.cfg.ChunkRows {
			return out.Complete(t.name)
		}
	}
}

// query runs sql and emits its rows. The last keyCols result columns are the
// key as text; query returns the last row's. Every row is emitted before it
// returns.
func (s *Source) query(ctx context.Context, conn *pgx.Conn, b *batcher, stream, sql string, args []string, keyCols int, out source.Emitter) (int, []string, error) {
	params := make([][]byte, len(args))
	for i, a := range args {
		params[i] = []byte(a)
	}
	formats := make([]int16, 0, len(b.cols)+keyCols)
	for _, c := range b.cols {
		formats = append(formats, c.format)
	}
	for range keyCols {
		formats = append(formats, pgx.TextFormatCode)
	}
	rr := conn.PgConn().ExecParams(ctx, sql, params, nil, nil, formats)
	n := 0
	var last []string
	var err error
	for rr.NextRow() {
		vals := rr.Values()
		if err = b.add(vals[:len(b.cols)]); err != nil {
			break
		}
		n++
		if b.bytes >= s.cfg.BatchBytes {
			if err = b.flush(stream, out); err != nil {
				break
			}
		}
		if keyCols > 0 {
			last = last[:0]
			for _, v := range vals[len(b.cols):] {
				last = append(last, string(v))
			}
		}
	}
	if _, cerr := rr.Close(); err == nil && cerr != nil {
		err = fmt.Errorf("postgres: read %s: %w", stream, cerr)
	}
	if err == nil {
		err = b.flush(stream, out)
	}
	return n, last, err
}

func resume(out source.Emitter, stream string, st sourceState) error {
	b, err := json.Marshal(st)
	if err != nil {
		return err
	}
	return out.ResumePoint(stream, b)
}

// batcher builds one table's Arrow batches.
type batcher struct {
	schema *arrow.Schema
	cols   []*colBuilder
	rows   int
	bytes  int
}

func newBatcher(mem memory.Allocator, cols []sourceColumn) (*batcher, error) {
	b := &batcher{}
	fields := make([]arrow.Field, len(cols))
	for i, c := range cols {
		cb, err := newColBuilder(mem, c)
		if err != nil {
			b.release()
			return nil, err
		}
		b.cols = append(b.cols, cb)
		fields[i] = arrow.Field{Name: c.name, Type: cb.b.Type(), Nullable: true}
	}
	b.schema = arrow.NewSchema(fields, nil)
	return b, nil
}

func (b *batcher) add(vals [][]byte) error {
	for i, c := range b.cols {
		v := vals[i]
		if v == nil {
			c.b.AppendNull()
			continue
		}
		if err := c.add(v); err != nil {
			return fmt.Errorf("postgres: column %q: %w", b.schema.Field(i).Name, err)
		}
		b.bytes += len(v)
	}
	b.rows++
	return nil
}

func (b *batcher) flush(stream string, out source.Emitter) error {
	if b.rows == 0 {
		return nil
	}
	arrs := make([]arrow.Array, len(b.cols))
	for i, c := range b.cols {
		arrs[i] = c.b.NewArray()
	}
	rec := array.NewRecordBatch(b.schema, arrs, int64(b.rows))
	for _, a := range arrs {
		a.Release()
	}
	b.rows, b.bytes = 0, 0
	if err := out.Records(stream, rec); err != nil {
		rec.Release()
		return err
	}
	return nil
}

func (b *batcher) release() {
	for _, c := range b.cols {
		c.b.Release()
	}
}
