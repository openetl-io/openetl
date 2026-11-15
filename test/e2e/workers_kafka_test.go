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

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/twmb/franz-go/pkg/kadm"

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

// A sync from Kafka's partitions into Kafka runs on two workers, each
// committing its own writer lanes. One worker is lost once every lane has
// staged an epoch the record names: its lanes' transactions are left open
// while the other worker's commit. The next attempt finds the committed
// lanes' markers, and reads the lost lanes' part of the epoch from the
// source again, up to where it ended, so the topic, read committed, has
// every record once.
func TestKafkaOnWorkersRedoesALostLane(t *testing.T) {
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
	// A pipeline applied before the launcher reports runs on one pod.
	eventually(t, 30*time.Second, "the pod launcher to report", func() bool {
		n, err := query[int](t, "SELECT count(*) FROM runners WHERE data_plane_id = $1 AND launcher AND attempt = ''", plane)
		return err == nil && n == 1
	})

	name := unique("workers-kafka")
	topic, ns := schema(name), schema(name)+"_out"
	dst := kafkaDest(t, ns)
	src := kafkaSource(t, topic)
	if _, err := kadm.NewClient(kafkaClient(t)).CreateTopic(t.Context(), 6, 1, nil, topic); err != nil {
		t.Fatal(err)
	}
	const records = 60_000
	produceJSON(t, topic, 0, records)
	apply(t,
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Source", "metadata": {"name": %q}, "spec": %s}`, name, src),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Destination", "metadata": {"name": %q}, "spec": %s}`, name, dst),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
			"spec": {"schedule": {"every": "720h"}, "dataPlane": %q, "resources": {"size": "m", "workers": 2},
			"source": {"ref": %q}, "destination": {"ref": %q}, "delivery": {"commit": {"maxBytes": "64KB"}}}}`, name, plane, name, name))

	eventually(t, 3*time.Minute, "an epoch on workers with rows in every writer lane", func() bool {
		select {
		case <-proxy.done:
			return true
		default:
		}
		if status, _ := query[string](t, `SELECT s.status FROM syncs s JOIN pipelines p ON p.id = s.pipeline_id
			WHERE p.name = $1 ORDER BY s.start_time DESC LIMIT 1`, name); status != "" && status != "running" {
			a := lastAttempt(t, name)
			t.Fatalf("the sync ended %s, on %d workers (%q), before an epoch had rows in every writer lane: %s", status, a.GetWorkers(), a.GetOnePod(), lastSyncError(t, name))
		}
		return false
	})
	proxy.mu.Lock()
	rec, pid, failed := proxy.rec, proxy.stopped, proxy.failed
	proxy.mu.Unlock()
	if failed != nil {
		t.Fatal(failed)
	}
	// Long enough for the other worker to commit its lanes, and well within
	// the coordinator's timeout for a silent worker, so the kill loses it.
	time.Sleep(3 * time.Second)
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		t.Fatal(err)
	}
	t.Logf("stopped worker pod %d before HEAD named epoch %d of generation %d, of %d writer lanes, then killed it", pid, rec.Epoch, rec.Generation, len(rec.Committables))

	status, stats := waitSync(t, name, 4*time.Minute)
	finishedLater(t, status, stats)
	checkTopics(t, ns, records, topic)
	if a := lastAttempt(t, name); a.GetWorkers() != 2 {
		t.Errorf("the sync's last attempt ran on %d workers (%q), want 2", a.GetWorkers(), a.GetOnePod())
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
}
