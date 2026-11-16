//go:build e2e && chaos

package e2e

import (
	"context"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// A chaos run with group windows: a SQL transform counts each of two
// Kafka topics' records by key, and sums their partitions, in a window of
// their timestamps that each topic draws, merged into Postgres. A JSON
// value's columns come from the records, so the statements read only the
// columns the source declares. Every record's timestamp is at or past the
// one produced before it, whatever its partition, so each partition reads
// them in order and none is ever late, before a failure or after: the
// destination's table is determined, each window's row with every record
// of it, as any run without failures leaves it. The timestamps start an
// hour ahead of the clock, which an idle partition's watermark moves to,
// so it never passes them. Once the writes stop, a record an hour past the
// last on every partition, of a key the check leaves out, closes every
// window.

const (
	windowPartitions = 3
	windowKeys       = 5
	// The key of the records that close the windows. Its own windows
	// never close.
	windowEnd = "~end"
)

// A topic's window: tumble, hop or session, by its size, a hop's slide
// and a session's gap, and whether HAVING keeps only windows whose sum is
// above 10.
type windowKind struct {
	kind        string
	size, slide time.Duration
	having      bool
}

func drawWindow(rng *rand.Rand) windowKind {
	k := windowKind{having: rng.IntN(3) == 0}
	switch rng.IntN(3) {
	case 0:
		k.kind, k.size = "tumble", 10*time.Second
	case 1:
		k.kind, k.size, k.slide = "hop", 15*time.Second, 5*time.Second
	default:
		k.kind, k.size = "session", 2*time.Second
	}
	return k
}

// sql is the statement that writes output from topic.
func (k windowKind) sql(output, topic string) string {
	interval := func(d time.Duration) string { return fmt.Sprintf("INTERVAL '%d seconds'", int(d.Seconds())) }
	var w string
	switch k.kind {
	case "tumble":
		w = fmt.Sprintf("tumble(_kafka_timestamp, %s)", interval(k.size))
	case "hop":
		w = fmt.Sprintf("hop(_kafka_timestamp, %s, %s)", interval(k.slide), interval(k.size))
	default:
		w = fmt.Sprintf("session(_kafka_timestamp, %s)", interval(k.size))
	}
	having := ""
	if k.having {
		having = " HAVING sum(_kafka_partition) > 10"
	}
	return fmt.Sprintf("INSERT INTO %s SELECT window_start, _kafka_key AS k, count(*) AS n, sum(_kafka_partition) AS total FROM %s GROUP BY %s, _kafka_key%s;\n",
		output, topic, w, having)
}

// starts are the starts of the tumbling or hopping windows a time falls
// in, all in microseconds.
func (k windowKind) starts(t int64) []int64 {
	slide, size := k.slide.Microseconds(), k.size.Microseconds()
	if k.kind == "tumble" {
		return []int64{t - t%size}
	}
	var out []int64
	for s := t - t%slide; s+size > t; s -= slide {
		out = append(out, s)
	}
	return out
}

type windowRecord struct {
	key       string
	partition int32
	at        time.Time
}

// A window's row: its start in microseconds and key, and its count and
// sum.
type (
	windowKey struct {
		start int64
		key   string
	}
	windowAgg struct{ n, total int64 }
)

type windowWork struct {
	c        chaosConfig
	out      string
	topics   []string
	kinds    []windowKind
	delay    string
	lateness string
	cl       *kgo.Client
	// Each topic's records produced, in the order of their times.
	recs [][]windowRecord
	// The latest timestamp produced.
	at time.Time
}

func newWindowWork(t *testing.T, c chaosConfig, name string) *windowWork {
	t.Helper()
	rng := rand.New(rand.NewPCG(c.seed, uint64(c.i)<<32|0x3d0e5))
	sch := schema(name)
	w := &windowWork{c: c, out: sch + "_out", delay: []string{"0s", "2s"}[rng.IntN(2)], lateness: []string{"0s", "30s"}[rng.IntN(2)],
		at: time.Now().Add(time.Hour).Truncate(time.Minute)}
	for _, tbl := range chaosTables {
		w.topics = append(w.topics, sch+"_"+tbl)
		w.kinds = append(w.kinds, drawWindow(rng))
		w.recs = append(w.recs, nil)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(kafkaBrokers()), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	w.cl = cl
	// Kafka refuses timestamps more than an hour ahead of its clock by
	// default; these topics take a day.
	day := "86400000"
	created, err := kadm.NewClient(cl).CreateTopics(t.Context(), windowPartitions, 1, map[string]*string{
		"message.timestamp.type":         kadm.StringPtr("CreateTime"),
		"message.timestamp.after.max.ms": &day,
	}, w.topics...)
	if err == nil {
		err = created.Error()
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := w.produce(t.Context(), rng, preloaded); err != nil {
		t.Fatal(err)
	}
	run(t, "CREATE SCHEMA "+pgx.Identifier{w.out}.Sanitize())
	return w
}

// produce writes n records, each to a topic and partition at random, at
// or past the one before.
func (w *windowWork) produce(ctx context.Context, rng *rand.Rand, n int) error {
	var recs []*kgo.Record
	type made struct {
		topic int
		r     windowRecord
	}
	var batch []made
	at := w.at
	for range n {
		step := time.Duration(rng.IntN(200)) * time.Millisecond
		if rng.IntN(25) == 0 {
			step = time.Duration(1000+rng.IntN(5000)) * time.Millisecond
		}
		at = at.Add(step)
		topic := rng.IntN(len(w.topics))
		r := windowRecord{key: fmt.Sprintf("k%d", rng.IntN(windowKeys)), partition: rng.Int32N(windowPartitions), at: at}
		batch = append(batch, made{topic, r})
		recs = append(recs, &kgo.Record{Topic: w.topics[topic], Partition: r.partition, Key: []byte(r.key),
			Value: []byte(`{"x": 1}`), Timestamp: at})
	}
	if err := w.cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		return fmt.Errorf("produce: %w", err)
	}
	for _, m := range batch {
		w.recs[m.topic] = append(w.recs[m.topic], m.r)
	}
	w.at = at
	return nil
}

func (w *windowWork) docs() (src, dst, pipeSource, pipeDest string) {
	var streams []map[string]any
	for _, topic := range w.topics {
		streams = append(streams, map[string]any{"name": topic, "eventTime": "_kafka_timestamp",
			"watermark": map[string]string{"delay": w.delay, "lateness": w.lateness}})
	}
	return mustJSON(map[string]any{"connector": "kafka", "config": map[string]any{"brokers": []string{kafkaBrokers()}, "topics": w.topics}}),
		`{"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}}}`,
		mustJSON(map[string]any{"streams": streams}),
		mustJSON(map[string]string{"namespace": w.out, "write": "merge"})
}

// transform is the pipeline's: a window's statement per topic, into the
// output of its table's name, and none of the topics passed through.
func (w *windowWork) transform() map[string]any {
	var b strings.Builder
	for i, tbl := range chaosTables {
		b.WriteString(w.kinds[i].sql(tbl, w.topics[i]))
	}
	return map[string]any{"sql": b.String(), "passthrough": "none"}
}

func (w *windowWork) loaded(*testing.T) bool { return true }

func (w *windowWork) write(ctx context.Context, rng *rand.Rand) error {
	for ctx.Err() == nil {
		if err := w.produce(context.Background(), rng, 1+rng.IntN(10)); err != nil {
			return err
		}
		pause(ctx, 5*time.Millisecond+upTo(rng, 35*time.Millisecond))
	}
	var ends []*kgo.Record
	for _, topic := range w.topics {
		for p := range int32(windowPartitions) {
			ends = append(ends, &kgo.Record{Topic: topic, Partition: p, Key: []byte(windowEnd), Value: []byte(`{"x": 1}`), Timestamp: w.at.Add(time.Hour)})
		}
	}
	if err := w.cl.ProduceSync(context.Background(), ends...).FirstErr(); err != nil {
		return fmt.Errorf("produce the windows' ends: %w", err)
	}
	return nil
}

// want is each window's row of topic i once every window has closed.
func (w *windowWork) want(i int) map[windowKey]windowAgg {
	k := w.kinds[i]
	out := map[windowKey]windowAgg{}
	add := func(start int64, r windowRecord) {
		a := out[windowKey{start, r.key}]
		a.n++
		a.total += int64(r.partition)
		out[windowKey{start, r.key}] = a
	}
	// A session is [first, last + gap]: a record joins one that's open at
	// its time.
	last, first := map[string]time.Time{}, map[string]int64{}
	for _, r := range w.recs[i] {
		if k.kind != "session" {
			for _, s := range k.starts(r.at.UnixMicro()) {
				add(s, r)
			}
			continue
		}
		if l, ok := last[r.key]; !ok || r.at.Sub(l) > k.size {
			first[r.key] = r.at.UnixMicro()
		}
		last[r.key] = r.at
		add(first[r.key], r)
	}
	if k.having {
		maps.DeleteFunc(out, func(_ windowKey, a windowAgg) bool { return a.total <= 10 })
	}
	return out
}

// check compares each output's table with its windows. A window's row
// lands once, when the window closes with every record of it, so a row
// that differs never goes away: it's extra, as well as missing.
func (w *windowWork) check(t *testing.T) (missing, extra int, detail string) {
	t.Helper()
	var details []string
	for i, tbl := range chaosTables {
		got, twice, err := windowTable(t, pgx.Identifier{w.out, tbl}.Sanitize())
		if err != nil {
			missing++
			details = append(details, fmt.Sprintf("%s: %v", tbl, err))
			continue
		}
		want := w.want(i)
		m, e := 0, twice
		var differ []string
		for k, a := range want {
			g, ok := got[k]
			if !ok {
				m++
			} else if g != a {
				m, e = m+1, e+1
				if len(differ) < 3 {
					differ = append(differ, fmt.Sprintf("%s at %d: %+v, want %+v", k.key, k.start, g, a))
				}
			}
		}
		for k := range got {
			if _, ok := want[k]; !ok {
				e++
			}
		}
		missing, extra = missing+m, extra+e
		if m > 0 || e > 0 {
			details = append(details, fmt.Sprintf("%s (%s): %d missing, %d extra of %d windows %v", tbl, w.kinds[i].kind, m, e, len(want), differ))
		}
	}
	return missing, extra, strings.Join(details, "; ")
}

// windowTable is a window output's live rows but the closing key's, and
// how many keys it has twice.
func windowTable(t *testing.T, table string) (map[windowKey]windowAgg, int, error) {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), env.dbURL)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = conn.Close(context.Background()) }()
	rows, err := conn.Query(t.Context(), `SELECT (extract(epoch FROM window_start) * 1000000)::bigint, k, n, total FROM `+table+
		` WHERE NOT _openetl_deleted AND k <> $1`, windowEnd)
	if err != nil {
		return nil, 0, err
	}
	out, twice := map[windowKey]windowAgg{}, 0
	for rows.Next() {
		var k windowKey
		var a windowAgg
		if err := rows.Scan(&k.start, &k.key, &a.n, &a.total); err != nil {
			return nil, 0, err
		}
		if _, ok := out[k]; ok {
			twice++
		}
		out[k] = a
	}
	return out, twice, rows.Err()
}

func (w *windowWork) plant(t *testing.T, what string) {
	t.Helper()
	out := pgx.Identifier{w.out, chaosTables[0]}.Sanitize()
	row := "(SELECT ctid FROM " + out + " WHERE NOT _openetl_deleted AND k <> " + quote(windowEnd) + " LIMIT 1)"
	if what == plantLoss {
		run(t, "DELETE FROM "+out+" WHERE ctid = "+row)
	} else {
		run(t, "UPDATE "+out+" SET n = n + 1 WHERE ctid = "+row)
	}
}

func (w *windowWork) changes(*testing.T) int {
	n := 0
	for _, recs := range w.recs {
		n += len(recs)
	}
	return n
}

func (w *windowWork) drop(*testing.T) {
	_, _ = kadm.NewClient(w.cl).DeleteTopics(context.Background(), w.topics...)
	w.cl.Close()
	conn, err := pgx.Connect(context.Background(), env.dbURL)
	if err != nil {
		return
	}
	defer func() { _ = conn.Close(context.Background()) }()
	_, _ = conn.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+pgx.Identifier{w.out}.Sanitize()+" CASCADE")
}

// TestChaosWindows runs group windows over Kafka's partitions, merged into
// Postgres, through each of the suite's faults, one a run: the runner
// killed and cut off from the API; the engine and the source killed; the
// runner killed at each of the commit protocol's crash points, one of them
// under the store's noise; the engine killed while the pipeline is
// rescaled onto other numbers of lanes; and in a pod, the pod killed.
// Each window's row lands once, with every record of it. With
// -chaos.window-workers, the runs in pods are on workers too, and a worker
// is killed.
func TestChaosWindows(t *testing.T) {
	s := newChaosSuite(t, 1, 1)
	type variant struct {
		fault    string
		pods     bool
		workers  int
		rescales int
		noise    float64
	}
	variants := []variant{
		{fault: faultKill}, {fault: faultEngine}, {fault: faultSource}, {fault: faultPartition},
		{fault: "crash " + crashStaged}, {fault: "crash " + crashRecorded}, {fault: "crash " + crashHead},
		{fault: "crash " + crashCommit, noise: 0.03},
		{fault: faultEngine, rescales: 2},
		{fault: faultKill, pods: true},
	}
	if *chaosWindowWorkers {
		variants = append(variants, variant{fault: faultWorker, pods: true, workers: 2}, variant{fault: "crash " + crashWorkerHead, pods: true, workers: 3})
	}
	sem := make(chan struct{}, *chaosParallel)
	for i, v := range variants {
		c := chaosConfig{
			i: i, seed: 1, source: "kafka", continuous: true, write: "merge", dest: "postgres", transform: "window", scope: "stream", interval: "1s",
			size: "m", pods: v.pods, workers: v.workers, rescales: v.rescales, noise: v.noise, faults: []string{v.fault}, writeFor: 30 * time.Second, strict: true,
		}
		name := fmt.Sprintf("%s-%s", c.name(), strings.ReplaceAll(v.fault, " ", "-"))
		if v.rescales > 0 {
			name += "-rescaled"
		}
		if *chaosOnly != "" && !slices.ContainsFunc(strings.Split(*chaosOnly, ","), func(s string) bool { return strings.Contains(name, s) }) {
			continue
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			sem <- struct{}{}
			defer func() { <-sem }()
			s.run(t, c)
		})
	}
}
