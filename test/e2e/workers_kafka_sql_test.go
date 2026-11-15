//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/platform/objectstore"
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
	plane, token := placementPlane(t)
	bucket := "sqllanes-" + strings.TrimPrefix(unique("x"), "x-")
	c, err := objectstore.S3Client(t.Context(), mustParse(t, "s3://"+bucket+"?anonymous=true&endpoint="+s3Endpoint()))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: &bucket}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	t.Cleanup(func() { dropBucket(t, c, bucket) })
	proxy := startLaneLossProxy(t)
	store := "s3://" + bucket + "?anonymous=true&endpoint=" + url.QueryEscape(proxy.srv.URL)
	launcher := startPodLauncher(t, plane, token, "--checkpoints", store)
	proxy.launcher.Store(launcher)
	startRunnerWith(t, token, env.apiURL, "--data-plane", plane, "--capacity", "1,2Gi", "--checkpoints", store)
	// A pipeline applied before the launcher reports runs on one pod.
	eventually(t, 30*time.Second, "the pod launcher to report", func() bool {
		n, err := query[int](t, "SELECT count(*) FROM runners WHERE data_plane_id = $1 AND launcher AND attempt = ''", plane)
		return err == nil && n == 1
	})

	name := unique("workers-kafka-sql")
	topic, ns := schema(name), schema(name)+"_out"
	dst := kafkaDest(t, ns)
	src := kafkaSource(t, topic)
	const partitions = 6
	if _, err := kadm.NewClient(kafkaClient(t)).CreateTopic(t.Context(), partitions, 1, nil, topic); err != nil {
		t.Fatal(err)
	}
	sql, err := json.Marshal(fmt.Sprintf(`INSERT INTO kept SELECT *, CAST(_kafka_key AS BIGINT) * 2 AS twice FROM %q WHERE CAST(_kafka_key AS BIGINT) %% 3 <> 0`, topic))
	if err != nil {
		t.Fatal(err)
	}
	docs := []string{
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Source", "metadata": {"name": %q}, "spec": %s}`, name, src),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Destination", "metadata": {"name": %q}, "spec": %s}`, name, dst),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
			"spec": {"mode": "continuous", "dataPlane": %q, "resources": {"size": "m", "workers": 2},
			"source": {"ref": %q}, "destination": {"ref": %q}, "transform": {"sql": %s}, "delivery": {"commit": {"interval": "1s"}}}}`, name, plane, name, name, sql),
	}
	apply(t, docs[0])
	discover(t, name, plane)
	plan := planOf(t, docs...)
	if plan.GetJob().GetWorkers() != 2 || len(plan.GetProblems()) > 0 || slices.ContainsFunc(plan.GetWarnings(), func(w *apiv1.PlanWarning) bool {
		return strings.Contains(w.GetMessage(), "one pod") || strings.Contains(w.GetMessage(), "one of its 2 workers")
	}) || slices.ContainsFunc(plan.GetNotes(), func(n string) bool { return strings.Contains(n, "capabilities") }) {
		t.Fatalf("the plan runs the job on %d workers, with problems %v, warnings %v and notes %v; want 2 workers that read every partition between them",
			plan.GetJob().GetWorkers(), plan.GetProblems(), plan.GetWarnings(), plan.GetNotes())
	}
	apply(t, docs...)
	t.Cleanup(func() {
		_, _ = pipelines().DeletePipeline(context.Background(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: "workspaces/default/pipelines/" + name}))
	})

	producer := kafkaClient(t, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	const round = 10 * partitions
	produced := 0
	for deadline := time.Now().Add(3 * time.Minute); ; {
		select {
		case <-proxy.done:
		default:
			if time.Now().After(deadline) {
				t.Fatalf("no epoch on workers had rows in every writer lane in 3 minutes; attempts on 2 workers: %d", attemptsOnTwoWorkers(t, name))
			}
			produceRound(t, producer, topic, produced, round, partitions)
			produced += round
			time.Sleep(100 * time.Millisecond)
			continue
		}
		break
	}
	proxy.mu.Lock()
	rec, pid, failed := proxy.rec, proxy.stopped, proxy.failed
	proxy.mu.Unlock()
	if failed != nil {
		t.Fatal(failed)
	}
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
	// Long enough for the other worker to commit its lanes, and well within
	// the coordinator's timeout for a silent worker, so the kill loses it.
	time.Sleep(3 * time.Second)
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		t.Fatal(err)
	}
	t.Logf("stopped worker pod %d before HEAD named epoch %d of generation %d, of %d writer lanes, then killed it; %d records written", pid, rec.Epoch, rec.Generation, len(rec.Committables), produced)

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
	if n := attemptsOnTwoWorkers(t, name); n < 2 {
		t.Errorf("%d attempts ran on 2 workers, want the lost one and the one after it", n)
	}
	redone := map[string]bool{}
	for _, m := range laneRedoing.FindAllStringSubmatch(launcher.out.String(), -1) {
		if m[2] == strconv.FormatUint(rec.Epoch, 10) && m[3] == strconv.FormatUint(rec.Generation, 10) {
			redone[m[1]] = true
		}
	}
	if len(redone) == 0 || len(redone) >= len(rec.Committables) {
		t.Errorf("of epoch %d's %d writer lanes, %d were read again from the source (%v); want some, and not those that committed", rec.Epoch, len(rec.Committables), len(redone), redone)
	}
	if _, err := pipelines().DeletePipeline(t.Context(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: "workspaces/default/pipelines/" + name})); err != nil {
		t.Fatal(err)
	}
	eventually(t, 2*time.Minute, "the stream's pods to go", func() bool { return launcher.pods(t) == 0 })
}
