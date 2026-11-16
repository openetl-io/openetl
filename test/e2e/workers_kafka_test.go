//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/platform/objectstore"
)

// laneLossProxy forwards a pod launcher's checkpoint requests to the S3
// store. The first time HEAD is about to point to a record of an attempt on
// workers whose every writer lane holds rows, it stops a worker's process
// before forwarding: every lane has staged the epoch, and the stopped
// worker's lanes can't commit it while the others' do.
type laneLossProxy struct {
	target   *url.URL
	tr       *http.Transport
	srv      *httptest.Server
	launcher atomic.Pointer[runner]

	// Says a record is one to stop at, beside every writer lane's holding
	// rows; nil: any.
	when func(checkpoint.Record) bool

	mu sync.Mutex
	// The last record written, until HEAD's write after it.
	pending string
	fired   bool
	// What the stop found: the record, the stopped worker's PID, or why it
	// couldn't stop one.
	rec     checkpoint.Record
	stopped int
	failed  error
	done    chan struct{}
}

func startLaneLossProxy(t *testing.T) *laneLossProxy {
	t.Helper()
	p := &laneLossProxy{target: mustParse(t, s3Endpoint()), tr: &http.Transport{MaxIdleConnsPerHost: 64}, done: make(chan struct{})}
	p.srv = httptest.NewServer(p)
	t.Cleanup(p.srv.Close)
	t.Cleanup(func() {
		if pid := p.stoppedPID(); pid > 0 {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	})
	return p
}

func (p *laneLossProxy) stoppedPID() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stopped
}

var (
	laneLossRecord = regexp.MustCompile(`/_checkpoints/\d{20}-g\d{20}\.json$`)
	// A worker pod's PID, from the line its runner logs as it starts.
	workerPodStarted = regexp.MustCompile(`msg="runner running one worker of an attempt" .*id=\S+:(\d+) attempt=`)
)

func (p *laneLossProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.EscapedPath()
	put := r.Method == http.MethodPut
	if put && strings.HasSuffix(path, "/_checkpoints/HEAD") {
		p.beforeHead(r.Context())
	}
	out := r.Clone(r.Context())
	out.RequestURI = ""
	out.URL.Scheme, out.URL.Host, out.Host = p.target.Scheme, p.target.Host, p.target.Host
	res, err := p.tr.RoundTrip(out)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer func() { _ = res.Body.Close() }()
	if put && res.StatusCode/100 == 2 && laneLossRecord.MatchString(path) {
		p.mu.Lock()
		p.pending = path
		p.mu.Unlock()
	}
	for k, vs := range res.Header {
		w.Header()[k] = vs
	}
	w.WriteHeader(res.StatusCode)
	_, _ = io.Copy(w, res.Body)
}

// beforeHead stops a worker if HEAD's write is the one to stop at.
func (p *laneLossProxy) beforeHead(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()
	path := p.pending
	p.pending = ""
	if p.fired || path == "" {
		return
	}
	rec, err := p.read(ctx, path)
	if err != nil {
		p.fail(fmt.Errorf("reading record %s: %w", path, err))
		return
	}
	if len(rec.Coverage) < 2 || len(rec.Coverage) != len(rec.Committables) {
		return
	}
	for _, c := range rec.Coverage {
		if len(c.Splits) == 0 {
			return
		}
	}
	if p.when != nil && !p.when(rec) {
		return
	}
	launcher := p.launcher.Load()
	if launcher == nil {
		return
	}
	m := workerPodStarted.FindAllStringSubmatch(launcher.out.String(), -1)
	if len(m) == 0 {
		p.fail(fmt.Errorf("epoch %d on workers, and no worker pod in the launcher's log", rec.Epoch))
		return
	}
	pid, _ := strconv.Atoi(m[len(m)-1][1])
	if err := syscall.Kill(pid, syscall.SIGSTOP); err != nil {
		p.fail(fmt.Errorf("stopping worker pod %d: %w", pid, err))
		return
	}
	p.fired, p.rec, p.stopped = true, rec, pid
	close(p.done)
}

// fail ends the wait for a stop; p.mu is held.
func (p *laneLossProxy) fail(err error) {
	p.fired, p.failed = true, err
	close(p.done)
}

// read reads a record from the store, past the proxy.
func (p *laneLossProxy) read(ctx context.Context, path string) (checkpoint.Record, error) {
	var rec checkpoint.Record
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.target.String()+path, nil)
	if err != nil {
		return rec, err
	}
	res, err := p.tr.RoundTrip(req)
	if err != nil {
		return rec, err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK {
		return rec, fmt.Errorf("status %s", res.Status)
	}
	err = json.NewDecoder(res.Body).Decode(&rec)
	return rec, err
}

// A lane-redo line of a worker's log: the lane, the epoch and its
// generation.
var laneRedoing = regexp.MustCompile(`msg="redoing a writer lane's part of an epoch from the source" .*lane=(\d+) epoch=(\d+) of_generation=(\d+)`)

// produceRound writes ids from..from+n-1 to topic, outside a transaction,
// each partition in turn from 0.
func produceRound(t *testing.T, cl *kgo.Client, topic string, from, n int, partitions int32) {
	t.Helper()
	recs := make([]*kgo.Record, 0, n)
	var p int32
	for i := from; i < from+n; i++ {
		recs = append(recs, &kgo.Record{Topic: topic, Partition: p, Key: []byte(strconv.Itoa(i)), Value: []byte(fmt.Sprintf(`{"id": %d, "name": "e%d"}`, i, i))})
		p = (p + 1) % partitions
	}
	if err := cl.ProduceSync(t.Context(), recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// laneLoss is a data plane of its own, with a pod launcher and a runner
// whose checkpoints go through a laneLossProxy.
type laneLoss struct {
	plane    string
	proxy    *laneLossProxy
	launcher *runner
}

// startLaneLoss starts a lane loss's data plane, and returns once its pod
// launcher has reported: a pipeline applied before then runs on one pod.
func startLaneLoss(t *testing.T) *laneLoss {
	t.Helper()
	plane, token := placementPlane(t)
	bucket := "lanes-" + strings.TrimPrefix(unique("x"), "x-")
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
	eventually(t, 30*time.Second, "the pod launcher to report", func() bool {
		n, err := query[int](t, "SELECT count(*) FROM runners WHERE data_plane_id = $1 AND launcher AND attempt = ''", plane)
		return err == nil && n == 1
	})
	return &laneLoss{plane: plane, proxy: proxy, launcher: launcher}
}

// laneLossPartitions are the partitions of a lane loss's topic: each worker
// reads three of them on two lanes.
const laneLossPartitions = 6

// docs makes pipeline name's topic, and returns its Source, Destination and
// Pipeline, continuous on two workers of l's data plane from the topic into
// Kafka's namespace ns, with transform, a "transform" field, if it's set.
func (l *laneLoss) docs(t *testing.T, name, topic, ns, transform string) []string {
	t.Helper()
	dst, src := kafkaDest(t, ns), kafkaSource(t, topic)
	if _, err := kadm.NewClient(kafkaClient(t)).CreateTopic(t.Context(), laneLossPartitions, 1, nil, topic); err != nil {
		t.Fatal(err)
	}
	if transform != "" {
		transform += ", "
	}
	t.Cleanup(func() {
		_, _ = pipelines().DeletePipeline(context.Background(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: "workspaces/default/pipelines/" + name}))
	})
	return []string{
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Source", "metadata": {"name": %q}, "spec": %s}`, name, src),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Destination", "metadata": {"name": %q}, "spec": %s}`, name, dst),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
			"spec": {"mode": "continuous", "dataPlane": %q, "resources": {"size": "m", "workers": 2},
			"source": {"ref": %q}, "destination": {"ref": %q}, %s"delivery": {"commit": {"interval": "1s"}}}}`, name, l.plane, name, name, transform),
	}
}

// lose writes to every partition of pipeline name's topic every 100 ms
// until the proxy stops a worker, then kills the worker once the other has
// had time to commit its lanes. It returns the record HEAD was about to
// name, and how many records it wrote.
func (l *laneLoss) lose(t *testing.T, name, topic string) (checkpoint.Record, int) {
	t.Helper()
	producer := kafkaClient(t, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	const round = 10 * laneLossPartitions
	return l.loseFeeding(t, name, func(from int) int {
		produceRound(t, producer, topic, from, round, laneLossPartitions)
		return round
	})
}

// loseFeeding is lose, writing a round of records with feed, from the id
// it's given, which says how many it wrote.
func (l *laneLoss) loseFeeding(t *testing.T, name string, feed func(from int) int) (checkpoint.Record, int) {
	t.Helper()
	produced := 0
	for deadline := time.Now().Add(3 * time.Minute); ; {
		select {
		case <-l.proxy.done:
		default:
			if time.Now().After(deadline) {
				t.Fatalf("no epoch on workers had rows in every writer lane in 3 minutes; attempts on 2 workers: %d", attemptsOnTwoWorkers(t, name))
			}
			produced += feed(produced)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		break
	}
	l.proxy.mu.Lock()
	rec, pid, failed := l.proxy.rec, l.proxy.stopped, l.proxy.failed
	l.proxy.mu.Unlock()
	if failed != nil {
		t.Fatal(failed)
	}
	// Long enough for the other worker to commit its lanes, and well within
	// the coordinator's timeout for a silent worker, so the kill loses it.
	time.Sleep(3 * time.Second)
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		t.Fatal(err)
	}
	t.Logf("stopped worker pod %d before HEAD named epoch %d of generation %d, of %d writer lanes, then killed it; %d records written", pid, rec.Epoch, rec.Generation, len(rec.Committables), produced)
	return rec, produced
}

// finish checks that pipeline name ran on two workers in the lost attempt
// and the one after it, and that some of rec's writer lanes were read again
// from the source, and not those that committed; then it deletes the
// pipeline, and waits for its pods to go.
func (l *laneLoss) finish(t *testing.T, name string, rec checkpoint.Record) {
	t.Helper()
	if n := attemptsOnTwoWorkers(t, name); n < 2 {
		t.Errorf("%d attempts ran on 2 workers, want the lost one and the one after it", n)
	}
	redone := map[string]bool{}
	for _, m := range laneRedoing.FindAllStringSubmatch(l.launcher.out.String(), -1) {
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
	eventually(t, 2*time.Minute, "the stream's pods to go", func() bool { return l.launcher.pods(t) == 0 })
}

// A continuous pipeline from Kafka's partitions into Kafka runs on two
// workers, each committing its own writer lanes. One worker is lost once
// every lane has staged an epoch the record names: its lanes' transactions
// are left open while the other worker's commit. The next attempt finds the
// committed lanes' markers, and reads the lost lanes' part of the epoch
// from the source again, up to where it ended, so the topic, read
// committed, has every record once.
//
// The pipeline never ends by itself, and the test writes to every
// partition every 100 ms until the stop: each partition is a split, each
// worker reads three of them on two lanes, and each lane writes on its own
// writer lane, so every 1 s epoch holds rows of every writer lane however
// fast or slow the runner.
func TestKafkaOnWorkersRedoesALostLane(t *testing.T) {
	l := startLaneLoss(t)
	name := unique("workers-kafka")
	topic, ns := schema(name), schema(name)+"_out"
	apply(t, l.docs(t, name, topic, ns, "")...)
	rec, produced := l.lose(t, name, topic)
	eventually(t, 3*time.Minute, "every record in the output topic", func() bool { return len(topicRecords(t, ns+"."+topic)) >= produced })
	checkTopics(t, ns, produced, topic)
	l.finish(t, name, rec)
}

// attemptsOnTwoWorkers is how many of the pipeline's attempts ran on 2
// workers.
func attemptsOnTwoWorkers(t *testing.T, pipeline string) int {
	t.Helper()
	n, _ := query[int](t, `SELECT count(*) FROM attempts a JOIN pipelines p ON p.org_id = a.org_id AND p.id = a.pipeline_id
		WHERE p.name = $1 AND a.workers = 2`, pipeline)
	return n
}
