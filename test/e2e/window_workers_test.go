//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/checkpoint"
)

// A window test's records: record i at windowT0 + i*windowStep, of key
// k{i % windowTestKeys}, on partition i % the topic's partitions, at offset i /
// the partitions there, so a partition's records come in the order of their
// time and none is late.
const (
	windowTestKeys = 8
	windowStep     = 10 * time.Millisecond
	windowSize     = 10 * time.Second
)

// produceWindowRound writes records from..from+n-1 of a window test whose
// first record is at t0 to topic's partitions.
func produceWindowRound(t *testing.T, cl *kgo.Client, topic string, t0 time.Time, from, n int, partitions int32) {
	t.Helper()
	recs := make([]*kgo.Record, 0, n)
	for i := from; i < from+n; i++ {
		recs = append(recs, &kgo.Record{Topic: topic, Partition: int32(i) % partitions, Key: fmt.Appendf(nil, "k%d", i%windowTestKeys),
			Value: fmt.Appendf(nil, `{"i": %d}`, i), Timestamp: t0.Add(time.Duration(i) * windowStep)})
	}
	if err := cl.ProduceSync(t.Context(), recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// latestGeneration is the newest generation of the pipeline's records its
// data plane reported.
func latestGeneration(t *testing.T, name string) int64 {
	t.Helper()
	g, _ := query[int64](t, `SELECT coalesce(max(c.generation), 0) FROM checkpoint_mirror c JOIN pipelines p ON p.id = c.pipeline_id WHERE p.name = $1`, name)
	return g
}

// windowRow is one of a window's rows, as the window test's statement
// writes it: its key, its records' least offset, how many records and the
// sum of their offsets.
type windowRow struct {
	k               string
	first, n, total int
}

// A continuous count over windows of ten seconds from Kafka's partitions
// into Kafka runs on two workers: each worker reads its partitions, and the
// workers' engines exchange each record by its key's group to the keyed
// lane that owns it, whose windows feed its own writer lane, with no second
// exchange, as the plan shows. One worker is lost as the lanes of an epoch
// that closed windows commit: the next attempt finds the committed lanes'
// markers, and writes the lost lanes' part of the epoch again, from the
// state before it, reading every worker's partitions for their key groups
// and closing the windows the epoch closed. Then that attempt's coordinator
// is killed once it has committed a record of its own, and the next
// restores the windows' state from the record. A last record far ahead
// closes every window the test wrote to: each lands once, read committed,
// with every record of it.
func TestKafkaWindowOnWorkersThroughKills(t *testing.T) {
	l := startLaneLoss(t)
	name := unique("workers-window")
	topic, ns := schema(name), schema(name)+"_out"
	// Far enough ahead that an idle partition's watermark, which follows the
	// clock, can't reach the records while the test runs; the topic takes
	// records a day ahead.
	t0 := time.Now().UTC().Truncate(time.Minute).Add(20 * time.Minute)
	ahead := "86400000"
	if _, err := kadm.NewClient(kafkaClient(t)).CreateTopic(t.Context(), laneLossPartitions, 1, map[string]*string{"message.timestamp.after.max.ms": &ahead}, topic); err != nil {
		t.Fatal(err)
	}
	// Over the record's own columns: a topic's columns of its values come
	// from its records, so a job's start doesn't have them yet.
	sql, err := json.Marshal(fmt.Sprintf("INSERT INTO tens\nSELECT window_start, _kafka_key AS k, count(*) AS n, sum(_kafka_offset) AS total, min(_kafka_offset) AS first\nFROM %q\nGROUP BY tumble(_kafka_timestamp, INTERVAL '10 seconds'), _kafka_key", topic))
	if err != nil {
		t.Fatal(err)
	}
	docs := []string{
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Source", "metadata": {"name": %q}, "spec": %s}`, name, kafkaSource(t, topic)),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Destination", "metadata": {"name": %q}, "spec": %s}`, name, kafkaDest(t, ns)),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
			"spec": {"mode": "continuous", "dataPlane": %q, "resources": {"size": "m", "workers": 2},
			"source": {"ref": %q, "streams": [{"name": %q, "eventTime": "_kafka_timestamp"}]}, "destination": {"ref": %q},
			"transform": {"sql": %s, "passthrough": "none"}, "delivery": {"commit": {"interval": "1s"}}}}`, name, l.plane, name, topic, name, sql),
	}
	t.Cleanup(func() {
		_, _ = pipelines().DeletePipeline(context.Background(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: "workspaces/default/pipelines/" + name}))
	})
	apply(t, docs[0])
	discover(t, name, l.plane)
	plan := planOf(t, docs...)
	if plan.GetJob().GetWorkers() != 2 || len(plan.GetProblems()) > 0 || slices.ContainsFunc(plan.GetWarnings(), func(w *apiv1.PlanWarning) bool {
		return strings.Contains(w.GetMessage(), "one pod") || strings.Contains(w.GetMessage(), "one of its 2 workers")
	}) {
		t.Fatalf("the plan runs the job on %d workers, with problems %v and warnings %v; want 2 workers that read every partition between them",
			plan.GetJob().GetWorkers(), plan.GetProblems(), plan.GetWarnings())
	}
	// One exchange, by the window's key, before the statement; none before
	// the writer.
	var exchanges []string
	for _, st := range plan.GetStreams() {
		for _, op := range st.GetOperators() {
			if op.GetExchange() {
				exchanges = append(exchanges, fmt.Sprintf("%s %s %v", st.GetName(), op.GetKind(), op.GetNeeds()))
			}
		}
	}
	if want := []string{"tens PLAN_OPERATOR_KIND_STATEMENT [_kafka_key]"}; !slices.Equal(exchanges, want) {
		t.Errorf("the plan exchanges %v; want %v; its notes: %v", exchanges, want, plan.GetNotes())
	}
	// Stop a worker as an epoch that closed windows commits.
	window := func(us int64) int64 { return (us - t0.UnixMicro()) / windowSize.Microseconds() }
	l.proxy.mu.Lock()
	l.proxy.when = func(rec checkpoint.Record) bool {
		st := rec.Streams[topic]
		return st.Floor != nil && st.Closed != nil && *st.Floor >= t0.UnixMicro() && window(*st.Closed) > window(*st.Floor)
	}
	l.proxy.mu.Unlock()
	apply(t, docs...)

	producer := kafkaClient(t, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	const round = 10 * laneLossPartitions
	feed := func(from int) int {
		produceWindowRound(t, producer, topic, t0, from, round, laneLossPartitions)
		return round
	}
	rec, produced := l.loseFeeding(t, name, feed)
	// Each writer lane's committable names its key groups, and every
	// partition the epoch read for the window's output, by its key.
	for i, cov := range rec.Coverage {
		if cov.KeyGroups == nil {
			t.Errorf("committable %d of epoch %d names no key groups", i, rec.Epoch)
		}
		for _, s := range cov.Splits {
			if s.Output != "tens" || s.KeyGroups == nil || *s.KeyGroups != *cov.KeyGroups || !slices.Equal(s.Key, []string{"_kafka_key"}) {
				t.Errorf("committable %d of epoch %d names %+v", i, rec.Epoch, s)
			}
		}
	}

	// Fed until an attempt after the lost one commits a record; then its
	// coordinator is killed, and fed until the next one commits.
	feedUntil := func(what string, done func() bool) {
		t.Helper()
		for deadline := time.Now().Add(3 * time.Minute); !done(); {
			if failed := runFailure(t, name); failed != "" {
				t.Fatalf("waiting for a %s, the pipeline's run failed: %s", what, failed)
			}
			if time.Now().After(deadline) {
				t.Fatalf("no %s in 3 minutes", what)
			}
			produced += feed(produced)
			time.Sleep(100 * time.Millisecond)
		}
	}
	feedUntil("record after the lost worker's attempt", func() bool { return latestGeneration(t, name) > int64(rec.Generation) })
	killed := latestGeneration(t, name)
	pid, _ := podOf(t, name)
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		t.Fatal(err)
	}
	t.Logf("killed the coordinator, pod %d, after a record of generation %d; %d records written", pid, killed, produced)
	feedUntil("record after the killed coordinator's attempt", func() bool { return latestGeneration(t, name) > killed })

	// A record a minute past the last on every partition closes every
	// window the records fall in; its own stays open.
	last := t0.Add(time.Duration(produced)*windowStep + time.Minute)
	var flush []*kgo.Record
	for p := range int32(laneLossPartitions) {
		flush = append(flush, &kgo.Record{Topic: topic, Partition: p, Key: []byte("flush"), Value: []byte(`{"i": -1}`), Timestamp: last})
	}
	if err := producer.ProduceSync(t.Context(), flush...).FirstErr(); err != nil {
		t.Fatal(err)
	}

	want := map[windowRow]bool{}
	byWindow := map[[2]int]*windowRow{}
	for i := range produced {
		w := [2]int{int(time.Duration(i) * windowStep / windowSize), i % windowTestKeys}
		offset := i / laneLossPartitions
		r := byWindow[w]
		if r == nil {
			r = &windowRow{k: fmt.Sprintf("k%d", w[1]), first: offset}
			byWindow[w] = r
		}
		r.n++
		r.total += offset
	}
	for _, r := range byWindow {
		want[*r] = true
	}
	read := func() []windowRow {
		var out []windowRow
		for _, v := range topicValues(t, ns+".tens") {
			k, _ := v["k"].(string)
			if k == "flush" {
				continue
			}
			num := func(f string) int {
				x, _ := v[f].(float64)
				return int(x)
			}
			out = append(out, windowRow{k: k, first: num("first"), n: num("n"), total: num("total")})
		}
		return out
	}
	eventually(t, 3*time.Minute, "every window in the output topic", func() bool {
		if failed := runFailure(t, name); failed != "" {
			t.Fatalf("the pipeline's run failed: %s", failed)
		}
		return len(read()) >= len(want)
	})
	// A transaction's commit returns before its markers reach the
	// partitions; a moment more shows nothing else lands.
	time.Sleep(3 * time.Second)
	got := read()
	seen := map[windowRow]bool{}
	for _, r := range got {
		switch {
		case seen[r]:
			t.Errorf("window %+v landed twice", r)
		case !want[r]:
			t.Errorf("window %+v isn't one written: a key's window of ten seconds, with every record of it", r)
		}
		seen[r] = true
	}
	if len(seen) != len(want) {
		missing := 0
		for r := range want {
			if !seen[r] {
				missing++
			}
		}
		t.Errorf("%d windows landed, want %d; %d missing", len(seen), len(want), missing)
	}
	t.Logf("%d windows of %d records landed, through a lost worker and a killed coordinator", len(got), produced)
	l.finish(t, name, rec)
}
