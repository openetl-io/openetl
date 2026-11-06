//go:build e2e && chaos

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// A chaos run's source and what it checks in the destination. Every change
// a workload makes is also written to its truth table, in the same
// transaction where the source is Postgres.
type workload interface {
	// docs are the run's Source and Destination specs, and its pipeline's
	// source and destination settings.
	docs() (src, dst, pipeSource, pipeDest string)
	// loaded reports whether what the source held before the pipeline
	// started has landed. A run writes no more until it has.
	loaded(t *testing.T) bool
	// write changes the source until ctx ends.
	write(ctx context.Context, rng *rand.Rand) error
	// check compares the destination with the truth. Rows missing can
	// still land; rows too many never go away.
	check(t *testing.T) (missing, extra int, detail string)
	// plant takes a row the check needs from the destination, or adds
	// one it mustn't have, so a test can see the check notice. Under
	// merge, both are rows that differ, counted as missing.
	plant(t *testing.T, what string)
	// changes is how many changes the truth holds.
	changes(t *testing.T) int
	drop(t *testing.T)
}

// What plant does. A held plant leaves a row holding its added column.
const (
	plantLoss  = "loss"
	plantExtra = "extra"
	plantHeld  = "held"
)

// The rows each table or topic holds before the pipeline starts.
const preloaded = 200

var chaosTables = []string{"a", "b"}

type pgWork struct {
	c              chaosConfig
	in, out, truth string
	dest           *chaosDest
	next           map[string]int64
	live           map[string]*idSet
	deletes, incr  bool
	notDeleted     string
	gate           *pgGate
	// With holds: the source's tables have w, which each change since
	// sets to v.
	added bool
}

func newPGWork(t *testing.T, c chaosConfig, name string, gate *pgGate) *pgWork {
	t.Helper()
	w := &pgWork{c: c, gate: gate, in: schema(name), next: map[string]int64{}, live: map[string]*idSet{}, deletes: c.source == "cdc", incr: c.source == "incremental"}
	w.out, w.truth = w.in+"_out", w.in+"_truth"
	w.notDeleted = "true"
	if w.deletes && c.write == "merge" {
		w.notDeleted = "NOT _openetl_deleted"
	}
	in, truth := pgx.Identifier{w.in}.Sanitize(), pgx.Identifier{w.truth}.Sanitize()
	qs := []string{
		"CREATE SCHEMA " + in, "CREATE SCHEMA " + truth,
		"CREATE SEQUENCE " + in + ".seq",
		"CREATE TABLE " + truth + ".changes (tbl text NOT NULL, id bigint NOT NULL, v bigint NOT NULL, op text NOT NULL, w bigint)",
	}
	for _, tbl := range chaosTables {
		ident := pgx.Identifier{w.in, tbl}.Sanitize()
		qs = append(qs,
			"CREATE TABLE "+ident+" (id bigint PRIMARY KEY, v bigint NOT NULL, seq bigint NOT NULL DEFAULT nextval('"+w.in+".seq'), pad text)",
			"ALTER TABLE "+ident+" REPLICA IDENTITY FULL",
			fmt.Sprintf("INSERT INTO %s (id, v, pad) SELECT i, 1, md5(i::text) FROM generate_series(1, %d) i", ident, preloaded),
			fmt.Sprintf("INSERT INTO %s.changes SELECT %s, i, 1, 'insert' FROM generate_series(1, %d) i", truth, quote(tbl), preloaded))
		w.next[tbl] = preloaded + 1
		w.live[tbl] = &idSet{}
		for i := range int64(preloaded) {
			w.live[tbl].add(i + 1)
		}
	}
	run(t, qs...)
	w.dest = newChaosDest(t, c.dest, w.out, chaosTables)
	return w
}

func quote(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func (w *pgWork) docs() (src, dst, pipeSource, pipeDest string) {
	cfg := map[string]any{"dsn": map[string]string{"secret": "e2e-dsn"}, "schema": w.in, "chunkRows": 50}
	var streams []map[string]string
	for _, tbl := range chaosTables {
		if w.incr {
			streams = append(streams, map[string]string{"name": tbl, "sync": "incremental", "cursor": "seq"})
		} else {
			streams = append(streams, map[string]string{"name": tbl, "sync": "cdc"})
		}
	}
	if !w.incr {
		cfg["publication"], cfg["snapshotChunkRows"] = w.in, 50
	}
	return mustJSON(map[string]any{"connector": "postgres", "config": cfg}),
		w.dest.spec,
		mustJSON(map[string]any{"streams": streams}),
		mustJSON(map[string]string{"namespace": w.out, "write": w.c.write})
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func (w *pgWork) loaded(t *testing.T) bool {
	t.Helper()
	if w.dest.refresh(t.Context()) != nil {
		return false
	}
	for _, tbl := range chaosTables {
		n, err := query[int](t, "SELECT count(*) FROM "+pgx.Identifier{w.out, tbl}.Sanitize())
		if err != nil || n < preloaded {
			return false
		}
	}
	return true
}

func (w *pgWork) write(ctx context.Context, rng *rand.Rand) error {
	bg := context.Background()
	var conn *pgx.Conn
	defer func() {
		if conn != nil {
			_ = conn.Close(bg)
		}
	}()
	var addAt time.Time
	if deadline, ok := ctx.Deadline(); ok && w.c.holds {
		addAt = time.Now().Add(upTo(rng, time.Until(deadline)/2+1))
	}
	since := -1
	for ctx.Err() == nil {
		var err error
		w.gate.do(func() {
			if conn == nil || since != w.gate.restarts {
				if conn != nil {
					_ = conn.Close(bg)
				}
				if conn, err = pgx.Connect(bg, env.dbURL); err != nil {
					conn = nil
					return
				}
				since = w.gate.restarts
			}
			if w.c.holds && !w.added && time.Now().After(addAt) {
				err = w.addColumn(bg, conn)
				return
			}
			err = w.change(bg, conn, rng)
		})
		if err != nil {
			return err
		}
		pause(ctx, 5*time.Millisecond+upTo(rng, 35*time.Millisecond))
	}
	return nil
}

// addColumn adds w to the source's tables, which block holds until it's
// released.
func (w *pgWork) addColumn(bg context.Context, conn *pgx.Conn) error {
	tx, err := conn.Begin(bg)
	if err != nil {
		return err
	}
	for _, tbl := range chaosTables {
		if _, err := tx.Exec(bg, "ALTER TABLE "+pgx.Identifier{w.in, tbl}.Sanitize()+" ADD COLUMN w bigint"); err != nil {
			_ = tx.Rollback(bg)
			return err
		}
	}
	if err := tx.Commit(bg); err != nil {
		return err
	}
	w.added = true
	return nil
}

// change makes a transaction of a few changes, with their truth.
func (w *pgWork) change(bg context.Context, conn *pgx.Conn, rng *rand.Rand) error {
	tx, err := conn.Begin(bg)
	if err != nil {
		return err
	}
	var rows [][]any
	for range 1 + rng.IntN(5) {
		tbl := chaosTables[rng.IntN(len(chaosTables))]
		ident := pgx.Identifier{w.in, tbl}.Sanitize()
		live := w.live[tbl]
		op := "insert"
		switch r := rng.Float64(); {
		case live.len() < 20:
		case w.deletes && r < 0.15:
			op = "delete"
		case r < 0.6:
			op = "update"
		}
		var id, v int64
		var wv *int64
		set, returning, into := "v = v + 1", "v", []any{&v}
		if w.added {
			set, returning, into = "v = v + 1, w = v + 1", "v, w", []any{&v, &wv}
		}
		switch op {
		case "insert":
			id, v = w.next[tbl], 1
			w.next[tbl]++
			if w.added {
				wv = &v
				_, err = tx.Exec(bg, "INSERT INTO "+ident+" (id, v, pad, w) VALUES ($1, 1, md5($2), 1)", id, strconv.FormatInt(id, 10))
			} else {
				_, err = tx.Exec(bg, "INSERT INTO "+ident+" (id, v, pad) VALUES ($1, 1, md5($2))", id, strconv.FormatInt(id, 10))
			}
			live.add(id)
		case "update":
			id = live.pick(rng)
			err = tx.QueryRow(bg, "UPDATE "+ident+" SET "+set+", seq = nextval('"+w.in+".seq'), pad = md5(random()::text) WHERE id = $1 RETURNING "+returning, id).Scan(into...)
		case "delete":
			id = live.pick(rng)
			err = tx.QueryRow(bg, "DELETE FROM "+ident+" WHERE id = $1 RETURNING "+returning, id).Scan(into...)
			live.remove(id)
		}
		if err != nil {
			_ = tx.Rollback(bg)
			return fmt.Errorf("%s %s %d: %w", op, tbl, id, err)
		}
		rows = append(rows, []any{tbl, id, v, op, wv})
	}
	if _, err := tx.CopyFrom(bg, pgx.Identifier{w.truth, "changes"}, []string{"tbl", "id", "v", "op", "w"}, pgx.CopyFromRows(rows)); err != nil {
		_ = tx.Rollback(bg)
		return err
	}
	return tx.Commit(bg)
}

func pause(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

func (w *pgWork) check(t *testing.T) (missing, extra int, detail string) {
	t.Helper()
	if err := w.dest.refresh(t.Context()); err != nil {
		return 1, 0, "the destination: " + err.Error()
	}
	var details []string
	for _, tbl := range chaosTables {
		in, out := pgx.Identifier{w.in, tbl}.Sanitize(), pgx.Identifier{w.out, tbl}.Sanitize()
		truth := pgx.Identifier{w.truth, "changes"}.Sanitize() + " WHERE tbl = " + quote(tbl)
		// With holds, w too, once the source has it: a row that still
		// holds it is missing, and isn't extra.
		cols, outCols, notHeld, held := "id, v", "id, v", "true", "0"
		if w.c.holds && hasColumn(t, in, "w") {
			cols, outCols = "id, v, w", "id, v, NULL::bigint AS w"
			if hasColumn(t, out, "w") {
				outCols = cols
			}
			if hasColumn(t, out, "_openetl_held") {
				notHeld = "NOT coalesce(_openetl_held ? 'w', false)"
				held = "(SELECT count(*) FROM " + out + " WHERE _openetl_held ? 'w')"
			}
		}
		var q string
		switch {
		case w.c.write == "merge":
			// Rows that differ either way, which a later commit can still
			// fix, and keys twice, which it can't.
			q = `SELECT
				(SELECT count(*) FROM (SELECT ` + cols + ` FROM ` + in + ` EXCEPT SELECT ` + outCols + ` FROM ` + out + ` WHERE ` + w.notDeleted + `) x)
					+ (SELECT count(*) FROM (SELECT ` + outCols + ` FROM ` + out + ` WHERE ` + w.notDeleted + ` EXCEPT SELECT ` + cols + ` FROM ` + in + `) x)
					+ ` + held + `,
				(SELECT count(*) - count(DISTINCT id) FROM ` + out + `)`
		case w.incr:
			// Each version once, and every row's last.
			q = `SELECT
				(SELECT count(*) FROM (SELECT ` + cols + ` FROM ` + in + ` EXCEPT SELECT ` + outCols + ` FROM ` + out + `) x) + ` + held + `,
				(SELECT count(*) FROM (SELECT id, v FROM ` + out + ` GROUP BY id, v HAVING count(*) > 1) x)
					+ (SELECT count(*) FROM (SELECT ` + outCols + ` FROM ` + out + ` WHERE ` + notHeld + ` EXCEPT SELECT ` + cols + ` FROM ` + truth + `) x)`
		default:
			// Each change once.
			q = `SELECT
				(SELECT count(*) FROM (SELECT ` + cols + `, op FROM ` + truth + ` EXCEPT ALL SELECT ` + outCols + `, _openetl_op FROM ` + out + `) x) + ` + held + `,
				(SELECT count(*) FROM (SELECT ` + outCols + `, _openetl_op FROM ` + out + ` WHERE ` + notHeld + ` EXCEPT ALL SELECT ` + cols + `, op FROM ` + truth + `) x)`
		}
		m, e, err := counts(t, q)
		if err != nil {
			missing++
			details = append(details, fmt.Sprintf("%s: %v", tbl, err))
			continue
		}
		missing, extra = missing+m, extra+e
		if m > 0 || e > 0 {
			details = append(details, fmt.Sprintf("%s: %d missing, %d extra", tbl, m, e))
		}
	}
	return missing, extra, strings.Join(details, "; ")
}

// hasColumn reports whether table has column, as a sanitized identifier.
func hasColumn(t *testing.T, table, column string) bool {
	t.Helper()
	ok, err := query[bool](t, "SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass($1) AND attname = $2 AND NOT attisdropped)", table, column)
	return err == nil && ok
}

func (w *pgWork) plant(t *testing.T, what string) {
	t.Helper()
	out, in := pgx.Identifier{w.out, chaosTables[0]}.Sanitize(), pgx.Identifier{w.in, chaosTables[0]}.Sanitize()
	switch {
	case what == plantHeld:
		w.dest.exec(t, "UPDATE "+out+" SET w = NULL, _openetl_held = jsonb_build_object('w', _openetl_id) WHERE ctid = (SELECT ctid FROM "+out+" WHERE w IS NOT NULL LIMIT 1)")
	case what == plantLoss && w.incr:
		w.dest.exec(t, "DELETE FROM "+out+" WHERE (id, v) IN (SELECT id, v FROM "+in+" LIMIT 1)")
	case what == plantLoss:
		w.dest.exec(t, "DELETE FROM "+out+" WHERE ctid = (SELECT ctid FROM "+out+" WHERE "+w.notDeleted+" LIMIT 1)")
	case w.c.write == "merge":
		// Its keys are unique, so a row that was never in the source.
		w.dest.exec(t, "UPDATE "+out+" SET id = -id WHERE id = (SELECT id FROM "+out+" WHERE "+w.notDeleted+" LIMIT 1)")
	default:
		w.dest.exec(t, "INSERT INTO "+out+" SELECT * FROM "+out+" LIMIT 1")
	}
}

func (w *pgWork) changes(t *testing.T) int {
	t.Helper()
	n, _ := query[int](t, "SELECT count(*) FROM "+pgx.Identifier{w.truth, "changes"}.Sanitize())
	return n
}

func counts(t *testing.T, q string) (int, int, error) {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), env.dbURL)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = conn.Close(context.Background()) }()
	var a, b int
	err = conn.QueryRow(t.Context(), q).Scan(&a, &b)
	return a, b, err
}

func (w *pgWork) drop(t *testing.T) {
	t.Helper()
	w.dest.drop()
	conn, err := pgx.Connect(context.Background(), env.dbURL)
	if err != nil {
		return
	}
	defer func() { _ = conn.Close(context.Background()) }()
	for _, s := range []string{w.in, w.out, w.truth} {
		_, _ = conn.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+pgx.Identifier{s}.Sanitize()+" CASCADE")
	}
	_, _ = conn.Exec(context.Background(), "DROP PUBLICATION IF EXISTS "+pgx.Identifier{w.in}.Sanitize())
}

type kafkaWork struct {
	out, truth string
	dest       *chaosDest
	topics     []string
	versions   map[string]map[int64]int64
	next       map[string]int64
	cl         *kgo.Client
	gate       *pgGate
}

func newKafkaWork(t *testing.T, c chaosConfig, name string, gate *pgGate) *kafkaWork {
	t.Helper()
	sch := schema(name)
	w := &kafkaWork{out: sch + "_out", truth: sch + "_truth", versions: map[string]map[int64]int64{}, next: map[string]int64{}, gate: gate}
	for _, tbl := range chaosTables {
		w.topics = append(w.topics, sch+"_"+tbl)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(kafkaBrokers()))
	if err != nil {
		t.Fatal(err)
	}
	w.cl = cl
	if _, err := kadm.NewClient(cl).CreateTopics(t.Context(), 3, 1, nil, w.topics...); err != nil {
		t.Fatal(err)
	}
	run(t, "CREATE SCHEMA "+pgx.Identifier{w.truth}.Sanitize(),
		"CREATE TABLE "+pgx.Identifier{w.truth, "changes"}.Sanitize()+" (tbl text NOT NULL, id bigint NOT NULL, v bigint NOT NULL, op text NOT NULL)")
	var recs []*kgo.Record
	var rows [][]any
	for _, topic := range w.topics {
		w.versions[topic] = map[int64]int64{}
		for i := range int64(preloaded) {
			recs = append(recs, w.record(topic, i+1, 1))
			rows = append(rows, []any{topic, i + 1, int64(1), "insert"})
		}
		w.next[topic] = preloaded + 1
	}
	if err := w.produce(t.Context(), recs, rows); err != nil {
		t.Fatal(err)
	}
	w.dest = newChaosDest(t, c.dest, w.out, w.topics)
	return w
}

func (w *kafkaWork) record(topic string, id, v int64) *kgo.Record {
	w.versions[topic][id] = v
	return &kgo.Record{Topic: topic, Key: []byte(strconv.FormatInt(id, 10)),
		Value: fmt.Appendf(nil, `{"id": %d, "v": %d, "pad": "%x"}`, id, v, rand.Uint64())}
}

// produce writes the records, then their truth.
func (w *kafkaWork) produce(ctx context.Context, recs []*kgo.Record, rows [][]any) error {
	if err := w.cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		return fmt.Errorf("produce: %w", err)
	}
	conn, err := pgx.Connect(ctx, env.dbURL)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.Background()) }()
	_, err = conn.CopyFrom(ctx, pgx.Identifier{w.truth, "changes"}, []string{"tbl", "id", "v", "op"}, pgx.CopyFromRows(rows))
	return err
}

func (w *kafkaWork) docs() (src, dst, pipeSource, pipeDest string) {
	return mustJSON(map[string]any{"connector": "kafka", "config": map[string]any{"brokers": []string{kafkaBrokers()}, "topics": w.topics}}),
		w.dest.spec,
		`{}`,
		mustJSON(map[string]string{"namespace": w.out, "write": "append"})
}

func (w *kafkaWork) loaded(*testing.T) bool { return true }

func (w *kafkaWork) write(ctx context.Context, rng *rand.Rand) error {
	for ctx.Err() == nil {
		var recs []*kgo.Record
		var rows [][]any
		for range 1 + rng.IntN(10) {
			topic := w.topics[rng.IntN(len(w.topics))]
			id := w.next[topic]
			if rng.IntN(2) == 0 {
				id = 1 + rng.Int64N(w.next[topic]-1)
			} else {
				w.next[topic]++
			}
			v := w.versions[topic][id] + 1
			recs = append(recs, w.record(topic, id, v))
			rows = append(rows, []any{topic, id, v, "insert"})
		}
		var err error
		w.gate.do(func() { err = w.produce(context.Background(), recs, rows) })
		if err != nil {
			return err
		}
		pause(ctx, 5*time.Millisecond+upTo(rng, 35*time.Millisecond))
	}
	return nil
}

func (w *kafkaWork) check(t *testing.T) (missing, extra int, detail string) {
	t.Helper()
	if err := w.dest.refresh(t.Context()); err != nil {
		return 1, 0, "the destination: " + err.Error()
	}
	var details []string
	for _, topic := range w.topics {
		out := pgx.Identifier{w.out, topic}.Sanitize()
		truth := pgx.Identifier{w.truth, "changes"}.Sanitize() + " WHERE tbl = " + quote(topic)
		m, e, err := counts(t, `SELECT
			(SELECT count(*) FROM (SELECT id, v FROM `+truth+` EXCEPT ALL SELECT id, v FROM `+out+`) x),
			(SELECT count(*) FROM (SELECT id, v FROM `+out+` EXCEPT ALL SELECT id, v FROM `+truth+`) x)`)
		if err != nil {
			missing++
			details = append(details, fmt.Sprintf("%s: %v", topic, err))
			continue
		}
		missing, extra = missing+m, extra+e
		if m > 0 || e > 0 {
			details = append(details, fmt.Sprintf("%s: %d missing, %d extra", topic, m, e))
		}
	}
	return missing, extra, strings.Join(details, "; ")
}

func (w *kafkaWork) plant(t *testing.T, what string) {
	t.Helper()
	out := pgx.Identifier{w.out, w.topics[0]}.Sanitize()
	if what == plantLoss {
		w.dest.exec(t, "DELETE FROM "+out+" WHERE ctid = (SELECT ctid FROM "+out+" LIMIT 1)")
	} else {
		w.dest.exec(t, "INSERT INTO "+out+" SELECT * FROM "+out+" LIMIT 1")
	}
}

func (w *kafkaWork) changes(t *testing.T) int {
	t.Helper()
	n, _ := query[int](t, "SELECT count(*) FROM "+pgx.Identifier{w.truth, "changes"}.Sanitize())
	return n
}

func (w *kafkaWork) drop(t *testing.T) {
	t.Helper()
	_, _ = kadm.NewClient(w.cl).DeleteTopics(context.Background(), w.topics...)
	w.cl.Close()
	w.dest.drop()
	conn, err := pgx.Connect(context.Background(), env.dbURL)
	if err != nil {
		return
	}
	defer func() { _ = conn.Close(context.Background()) }()
	for _, s := range []string{w.out, w.truth} {
		_, _ = conn.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+pgx.Identifier{s}.Sanitize()+" CASCADE")
	}
}

// idSet is a set of ids that can pick one at random.
type idSet struct {
	ids []int64
	at  map[int64]int
}

func (s *idSet) len() int { return len(s.ids) }

func (s *idSet) add(id int64) {
	if s.at == nil {
		s.at = map[int64]int{}
	}
	s.at[id] = len(s.ids)
	s.ids = append(s.ids, id)
}

func (s *idSet) remove(id int64) {
	i, ok := s.at[id]
	if !ok {
		return
	}
	last := s.ids[len(s.ids)-1]
	s.ids[i], s.at[last] = last, i
	s.ids = s.ids[:len(s.ids)-1]
	delete(s.at, id)
}

func (s *idSet) pick(rng *rand.Rand) int64 { return s.ids[rng.IntN(len(s.ids))] }
