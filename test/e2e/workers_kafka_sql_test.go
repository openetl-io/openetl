//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
)

// A continuous pipeline from Kafka's partitions into Kafka, whose SQL keeps
// two records of three and computes a column, runs on two workers, each
// committing its own writer lanes, as TestKafkaOnWorkersRedoesALostLane's
// does without SQL; the plan says so. One worker is lost once every lane has
// staged an epoch the record names. The next attempt finds the committed
// lanes' markers, and reads the lost lanes' part of the epoch from the
// source again, runs the statement on it, and commits the statement's
// output and the stream passed through as the lost lanes had them: each
// topic, read committed, has every record it should once, the statement's
// with the column it computes.
func TestKafkaSQLOnWorkersRedoesALostLane(t *testing.T) {
	l := startLaneLoss(t)
	name := unique("workers-kafka-sql")
	topic, ns := schema(name), schema(name)+"_out"
	sql, err := json.Marshal(fmt.Sprintf(`INSERT INTO kept SELECT *, CAST(_kafka_key AS BIGINT) * 2 AS twice FROM %q WHERE CAST(_kafka_key AS BIGINT) %% 3 <> 0`, topic))
	if err != nil {
		t.Fatal(err)
	}
	docs := l.docs(t, name, topic, ns, fmt.Sprintf(`"transform": {"sql": %s}`, sql))
	apply(t, docs[0])
	discover(t, name, l.plane)
	plan := planOf(t, docs...)
	if plan.GetJob().GetWorkers() != 2 || len(plan.GetProblems()) > 0 || slices.ContainsFunc(plan.GetWarnings(), func(w *apiv1.PlanWarning) bool {
		return strings.Contains(w.GetMessage(), "one pod") || strings.Contains(w.GetMessage(), "one of its 2 workers")
	}) || slices.ContainsFunc(plan.GetNotes(), func(n string) bool { return strings.Contains(n, "capabilities") }) {
		t.Fatalf("the plan runs the job on %d workers, with problems %v, warnings %v and notes %v; want 2 workers that read every partition between them",
			plan.GetJob().GetWorkers(), plan.GetProblems(), plan.GetWarnings(), plan.GetNotes())
	}
	apply(t, docs...)

	rec, produced := l.lose(t, name, topic)
	// Each committable names the partitions its lanes read, once for the
	// statement's output and once for the stream passed through.
	for i, cov := range rec.Coverage {
		outputs := map[string]bool{}
		for _, s := range cov.Splits {
			outputs[s.Output] = true
		}
		if !outputs["kept"] || !outputs[topic] || len(outputs) != 2 {
			t.Errorf("committable %d of epoch %d covers outputs %v, want kept and %s", i, rec.Epoch, outputs, topic)
		}
	}

	want := produced - (produced+2)/3
	eventually(t, 3*time.Minute, "every record in both topics", func() bool {
		return len(topicRecords(t, ns+"."+topic)) >= produced && len(topicRecords(t, ns+".kept")) >= want
	})
	checkTopics(t, ns, produced, topic)
	// A transaction's commit returns before its markers reach the
	// partitions, so its records can be a moment from being readable.
	vals := topicValues(t, ns+".kept")
	for deadline := time.Now().Add(30 * time.Second); len(vals) < want && time.Now().Before(deadline); {
		time.Sleep(200 * time.Millisecond)
		vals = topicValues(t, ns+".kept")
	}
	seen := map[int]bool{}
	for _, v := range vals {
		id, ok := v["id"].(float64)
		twice, _ := v["twice"].(float64)
		switch {
		case !ok || id != float64(int(id)) || int(id) < 0 || int(id) >= produced:
			t.Fatalf("kept has a record %v whose id isn't one produced", v)
		case int(id)%3 == 0:
			t.Fatalf("kept has id %d, which the statement leaves out", int(id))
		case seen[int(id)]:
			t.Fatalf("kept has id %d twice", int(id))
		case twice != 2*id:
			t.Fatalf("kept's record of id %d: %v", int(id), v)
		}
		seen[int(id)] = true
	}
	if len(seen) != want {
		t.Errorf("kept has %d records, want %d: one of each id the statement keeps", len(seen), want)
	}
	l.finish(t, name, rec)
}
