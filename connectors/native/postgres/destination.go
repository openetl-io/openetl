// Package postgres has the Postgres destination: append writes, staged into
// one table per stream and epoch, and applied behind a fence row with a
// marker per stream in _openetl_loads.
package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/unsuitable001/openetl/sdk/go/destination"
)

type DestinationConfig struct {
	// A connection URL or a keyword/value connection string.
	DSN string `json:"dsn"`
	// Used when the pipeline names no namespace. Default "public".
	Schema string `json:"schema"`
}

type Destination struct {
	cfg  DestinationConfig
	pool *pgxpool.Config
}

var _ destination.Destination = (*Destination)(nil)

func NewDestination(config []byte) (*Destination, error) {
	cfg := DestinationConfig{Schema: "public"}
	dec := json.NewDecoder(bytes.NewReader(config))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("postgres: config: %w", err)
	}
	if cfg.DSN == "" {
		return nil, errors.New("postgres: config needs a dsn")
	}
	pc, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres: dsn: %w", err)
	}
	pc.MaxConns = 4
	return &Destination{cfg: cfg, pool: pc}, nil
}

func (d *Destination) Capabilities() destination.Capabilities {
	return destination.Capabilities{
		Class:                 destination.StageAndApply,
		Scopes:                []destination.Scope{destination.ScopePipeline, destination.ScopeStream},
		DefaultCommitInterval: 10 * time.Second,
		MinCommitInterval:     time.Second,
	}
}

func (d *Destination) Open(ctx context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	for _, s := range cfg.Streams {
		if s.Mode != destination.Append {
			return nil, fmt.Errorf("postgres: stream %q: only append writes are supported", s.Name)
		}
		if s.Dedupe {
			return nil, fmt.Errorf("postgres: stream %q: dedupe at apply isn't supported yet", s.Name)
		}
	}
	pool, err := pgxpool.NewWithConfig(ctx, d.pool)
	if err != nil {
		return nil, fmt.Errorf("postgres: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: connect: %w", err)
	}
	w := &writer{
		pool:    pool,
		cfg:     cfg,
		schema:  cfg.Namespace,
		staging: map[uint64]map[string]*stagingTable{},
		streams: map[string]bool{},
	}
	if w.schema == "" {
		w.schema = d.cfg.Schema
	}
	for _, s := range cfg.Streams {
		w.streams[s.Name] = true
	}
	return w, nil
}

type writer struct {
	pool    *pgxpool.Pool
	cfg     destination.OpenConfig
	schema  string
	streams map[string]bool
	// Only Write and PreCommit use it.
	staging map[uint64]map[string]*stagingTable
}

type stagingTable struct {
	name string
	cols []column
}

// staged is the descriptor PreCommit returns.
type staged struct {
	Generation uint64         `json:"generation"`
	Epoch      uint64         `json:"epoch"`
	Streams    []stagedStream `json:"streams"`
}

type stagedStream struct {
	Name    string `json:"name"`
	Table   string `json:"table"`
	Staging string `json:"staging"`
}

func (w *writer) ident(name string) string { return pgx.Identifier{w.schema, name}.Sanitize() }

func stagingName(pipeline, stream string, e destination.Epoch) string {
	h := sha256.Sum256([]byte(pipeline + "\x00" + stream + "\x00" + strconv.FormatUint(e.Generation, 10) + "\x00" + strconv.FormatUint(e.N, 10)))
	return "_openetl_stage_" + hex.EncodeToString(h[:12])
}

func (w *writer) Write(ctx context.Context, e destination.Epoch, stream string, batch arrow.RecordBatch) error {
	if !w.streams[stream] {
		return fmt.Errorf("postgres: unknown stream %q", stream)
	}
	if batch.NumRows() == 0 {
		return nil
	}
	cols, err := columnsOf(batch.Schema())
	if err != nil {
		return err
	}
	byStream := w.staging[e.N]
	if byStream == nil {
		byStream = map[string]*stagingTable{}
		w.staging[e.N] = byStream
	}
	st := byStream[stream]
	if st == nil {
		st = &stagingTable{name: stagingName(w.cfg.Pipeline, stream, e)}
		if _, err := w.pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+w.ident(st.name)+" ("+columnDefs(cols)+")"); err != nil {
			return fmt.Errorf("postgres: create staging for %s: %w", stream, err)
		}
		st.cols = cols
		byStream[stream] = st
	} else if err := w.widen(ctx, st, cols); err != nil {
		return err
	}

	n := int(batch.NumRows())
	if len(cols) == 0 {
		_, err := w.pool.Exec(ctx, "INSERT INTO "+w.ident(st.name)+" SELECT FROM generate_series(1, $1)", n)
		return err
	}
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.name
	}
	arrs := batch.Columns()
	row := make([]any, len(cols))
	_, err = w.pool.CopyFrom(ctx, pgx.Identifier{w.schema, st.name}, names, pgx.CopyFromSlice(n, func(i int) ([]any, error) {
		for j, c := range cols {
			if arrs[j].IsNull(i) || c.value == nil {
				row[j] = nil
				continue
			}
			v := c.value(arrs[j], i)
			if err, ok := v.(error); ok {
				return nil, fmt.Errorf("postgres: column %q: %w", c.name, err)
			}
			row[j] = v
		}
		return row, nil
	}))
	if err != nil {
		return fmt.Errorf("postgres: stage %s: %w", stream, err)
	}
	return nil
}

// widen adds columns that appeared since the staging table was created.
func (w *writer) widen(ctx context.Context, st *stagingTable, cols []column) error {
	for _, c := range cols {
		i := slices.IndexFunc(st.cols, func(x column) bool { return x.name == c.name })
		if i >= 0 {
			if st.cols[i].typ != c.typ {
				return fmt.Errorf("postgres: column %q changed type from %s to %s within an epoch", c.name, st.cols[i].typ, c.typ)
			}
			continue
		}
		if _, err := w.pool.Exec(ctx, "ALTER TABLE "+w.ident(st.name)+" ADD COLUMN IF NOT EXISTS "+pgx.Identifier{c.name}.Sanitize()+" "+c.typ); err != nil {
			return fmt.Errorf("postgres: add column %q to staging: %w", c.name, err)
		}
		st.cols = append(st.cols, c)
	}
	return nil
}

func columnDefs(cols []column) string {
	defs := make([]string, len(cols))
	for i, c := range cols {
		defs[i] = pgx.Identifier{c.name}.Sanitize() + " " + c.typ
	}
	return strings.Join(defs, ", ")
}

// PreCommit has nothing to flush: every Write committed its rows to staging.
func (w *writer) PreCommit(_ context.Context, e destination.Epoch) (destination.Staged, error) {
	desc := staged{Generation: e.Generation, Epoch: e.N}
	byStream := w.staging[e.N]
	for _, s := range w.cfg.Streams {
		if st := byStream[s.Name]; st != nil {
			desc.Streams = append(desc.Streams, stagedStream{Name: s.Name, Table: tableName(s.Name), Staging: st.name})
		}
	}
	delete(w.staging, e.N)
	return json.Marshal(desc)
}

func (w *writer) Commit(ctx context.Context, _ destination.Epoch, s destination.Staged) error {
	var desc staged
	if err := json.Unmarshal(s, &desc); err != nil {
		return fmt.Errorf("postgres: staged descriptor: %w", err)
	}
	return w.apply(ctx, desc, desc.Streams)
}

// apply applies each stream that has no marker yet, then drops its staging.
func (w *writer) apply(ctx context.Context, desc staged, streams []stagedStream) error {
	todo, err := w.unapplied(ctx, desc, streams)
	if err != nil || len(todo) == 0 {
		return err
	}
	cols := make(map[string][]string, len(todo))
	for _, s := range todo {
		if cols[s.Name], err = w.ensureTarget(ctx, desc, s); err != nil {
			return err
		}
	}
	if w.cfg.Scope == destination.ScopeStream {
		for _, s := range todo {
			if err := w.applyTx(ctx, desc, []stagedStream{s}, cols, "FOR SHARE"); err != nil {
				return err
			}
		}
	} else if err := w.applyTx(ctx, desc, todo, cols, "FOR UPDATE"); err != nil {
		return err
	}
	w.dropStaging(ctx, todo)
	return nil
}

func (w *writer) applyTx(ctx context.Context, desc staged, streams []stagedStream, cols map[string][]string, lock string) error {
	return pgx.BeginFunc(ctx, w.pool, func(tx pgx.Tx) error {
		var fence uint64
		err := tx.QueryRow(ctx, "SELECT generation FROM "+w.ident("_openetl_fence")+" WHERE pipeline = $1 "+lock, w.cfg.Pipeline).Scan(&fence)
		if errors.Is(err, pgx.ErrNoRows) {
			return errors.New("postgres: the fence row is missing; Recover must run first")
		}
		if err != nil {
			return fmt.Errorf("postgres: lock fence: %w", err)
		}
		if fence > w.cfg.Generation {
			return fmt.Errorf("%w: fence is %d, applier is %d", destination.ErrFenced, fence, w.cfg.Generation)
		}
		for _, s := range streams {
			var done bool
			if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM "+w.ident("_openetl_loads")+" WHERE pipeline = $1 AND stream = $2 AND generation = $3 AND epoch = $4)",
				w.cfg.Pipeline, s.Name, desc.Generation, desc.Epoch).Scan(&done); err != nil {
				return fmt.Errorf("postgres: read marker for %s: %w", s.Name, err)
			}
			if done {
				continue
			}
			list := quoteAll(cols[s.Name])
			if _, err := tx.Exec(ctx, "INSERT INTO "+w.ident(s.Table)+" ("+list+") SELECT "+list+" FROM "+w.ident(s.Staging)); err != nil {
				return fmt.Errorf("postgres: apply %s: %w", s.Name, err)
			}
			if _, err := tx.Exec(ctx, "INSERT INTO "+w.ident("_openetl_loads")+" (pipeline, stream, generation, epoch) VALUES ($1, $2, $3, $4)",
				w.cfg.Pipeline, s.Name, desc.Generation, desc.Epoch); err != nil {
				return fmt.Errorf("postgres: write marker for %s: %w", s.Name, err)
			}
		}
		return nil
	})
}

func quoteAll(names []string) string {
	q := make([]string, len(names))
	for i, n := range names {
		q[i] = pgx.Identifier{n}.Sanitize()
	}
	return strings.Join(q, ", ")
}

// ensureTarget creates the target table, or adds the staging table's new
// columns to it, and returns the staging table's columns.
func (w *writer) ensureTarget(ctx context.Context, desc staged, s stagedStream) ([]string, error) {
	var exists bool
	if err := w.pool.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", w.ident(s.Staging)).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("postgres: staging for %s in epoch %d-g%d is missing", s.Name, desc.Epoch, desc.Generation)
	}
	rows, err := w.pool.Query(ctx, `SELECT attname, format_type(atttypid, atttypmod) FROM pg_attribute
		WHERE attrelid = to_regclass($1) AND attnum > 0 AND NOT attisdropped ORDER BY attnum`, w.ident(s.Staging))
	if err != nil {
		return nil, fmt.Errorf("postgres: read staging columns for %s: %w", s.Name, err)
	}
	type col struct{ name, typ string }
	cols, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (col, error) {
		var c col
		return c, r.Scan(&c.name, &c.typ)
	})
	if err != nil {
		return nil, fmt.Errorf("postgres: read staging columns for %s: %w", s.Name, err)
	}
	defs := make([]string, len(cols))
	names := make([]string, len(cols))
	for i, c := range cols {
		defs[i] = pgx.Identifier{c.name}.Sanitize() + " " + c.typ
		names[i] = c.name
	}
	if _, err := w.pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+w.ident(s.Table)+" ("+strings.Join(defs, ", ")+")"); err != nil {
		return nil, fmt.Errorf("postgres: create %s: %w", s.Table, err)
	}
	for _, d := range defs {
		if _, err := w.pool.Exec(ctx, "ALTER TABLE "+w.ident(s.Table)+" ADD COLUMN IF NOT EXISTS "+d); err != nil {
			return nil, fmt.Errorf("postgres: add column to %s: %w", s.Table, err)
		}
	}
	return names, nil
}

func (w *writer) unapplied(ctx context.Context, desc staged, streams []stagedStream) ([]stagedStream, error) {
	var todo []stagedStream
	for _, s := range streams {
		var done bool
		if err := w.pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM "+w.ident("_openetl_loads")+" WHERE pipeline = $1 AND stream = $2 AND generation = $3 AND epoch = $4)",
			w.cfg.Pipeline, s.Name, desc.Generation, desc.Epoch).Scan(&done); err != nil {
			return nil, fmt.Errorf("postgres: read marker for %s: %w", s.Name, err)
		}
		if !done {
			todo = append(todo, s)
		}
	}
	return todo, nil
}

// dropStaging is best effort: staging left behind is never applied twice,
// because its marker exists.
func (w *writer) dropStaging(ctx context.Context, streams []stagedStream) {
	for _, s := range streams {
		_, _ = w.pool.Exec(ctx, "DROP TABLE IF EXISTS "+w.ident(s.Staging))
	}
}

func (w *writer) Recover(ctx context.Context, gen uint64, e destination.Epoch, s destination.Staged) (destination.Recovery, error) {
	for _, q := range []string{
		"CREATE SCHEMA IF NOT EXISTS " + pgx.Identifier{w.schema}.Sanitize(),
		"CREATE TABLE IF NOT EXISTS " + w.ident("_openetl_fence") + " (pipeline text PRIMARY KEY, generation bigint NOT NULL)",
		"CREATE TABLE IF NOT EXISTS " + w.ident("_openetl_loads") + ` (pipeline text NOT NULL, stream text NOT NULL,
			generation bigint NOT NULL, epoch bigint NOT NULL, loaded_at timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (pipeline, stream, generation, epoch))`,
	} {
		if _, err := w.pool.Exec(ctx, q); err != nil {
			return 0, fmt.Errorf("postgres: prepare %s: %w", w.schema, err)
		}
	}
	// Waits for applies that hold the fence row.
	var raised uint64
	err := w.pool.QueryRow(ctx, "INSERT INTO "+w.ident("_openetl_fence")+` AS f (pipeline, generation) VALUES ($1, $2)
		ON CONFLICT (pipeline) DO UPDATE SET generation = EXCLUDED.generation WHERE f.generation <= EXCLUDED.generation
		RETURNING generation`, w.cfg.Pipeline, gen).Scan(&raised)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("%w: recovering as generation %d", destination.ErrFenced, gen)
	}
	if err != nil {
		return 0, fmt.Errorf("postgres: raise fence: %w", err)
	}
	if e.IsZero() {
		return destination.Applied, nil
	}

	var desc staged
	if err := json.Unmarshal(s, &desc); err != nil {
		return 0, fmt.Errorf("postgres: staged descriptor: %w", err)
	}
	missing, err := w.unapplied(ctx, desc, desc.Streams)
	if err != nil {
		return 0, err
	}
	present := 0
	for _, m := range missing {
		var ok bool
		if err := w.pool.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", w.ident(m.Staging)).Scan(&ok); err != nil {
			return 0, err
		}
		if ok {
			present++
		}
	}
	switch {
	case len(missing) == 0:
		w.dropStaging(ctx, desc.Streams)
		return destination.Applied, nil
	case present == len(missing):
		if err := w.apply(ctx, desc, missing); err != nil {
			return 0, err
		}
		return destination.Redone, nil
	case present == 0 && len(missing) == len(desc.Streams):
		return destination.Lost, nil
	default:
		return 0, fmt.Errorf("postgres: epoch %d-g%d is partly applied and the staging for the rest is gone", desc.Epoch, desc.Generation)
	}
}

func (w *writer) Close(context.Context) error {
	w.pool.Close()
	return nil
}
