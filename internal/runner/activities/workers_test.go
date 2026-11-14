package activities

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/apache/arrow-go/v18/arrow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
	"github.com/openetl-io/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/engineclient"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/platform/resources"
	"github.com/openetl-io/openetl/internal/runner/pods"
	"github.com/openetl-io/openetl/internal/runner/worker"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/host"
)

// What this test binary runs as, when it's started as one of the processes
// TestAnAttemptOnWorkerProcesses needs.
const (
	processEnv = "OPENETL_TEST_PROCESS"
	apiEnv     = "OPENETL_TEST_API"
	storeEnv   = "OPENETL_TEST_STORE"
	dirEnv     = "OPENETL_TEST_CONNECTORS"
)

func TestMain(m *testing.M) {
	switch os.Getenv(processEnv) {
	case "connector":
		sdk.Serve(laneFiles)
		return
	case "worker":
		os.Exit(workerProcess())
	}
	os.Exit(m.Run())
}

// workerProcess is a worker's pod, as the process launcher starts one: the
// attempt, its placement and the worker in its environment.
func workerProcess() int {
	placement, _ := strconv.Atoi(os.Getenv("OPENETL_PLACEMENT"))
	n, _ := strconv.Atoi(os.Getenv("OPENETL_WORKER"))
	log := slog.New(slog.NewTextHandler(os.Stderr, nil)).With("worker", n-1)
	api := dataplanev1connect.NewDataPlaneServiceClient(http.DefaultClient, os.Getenv(apiEnv))
	store, err := checkpoint.NewFSStore(os.Getenv(storeEnv))
	if err != nil {
		log.Error("the checkpoint store", "err", err)
		return 1
	}
	s := &Sync{
		API: api, Checkpoints: store, Storage: os.Getenv(storeEnv), Logger: log,
		Engine:     engineclient.Options{Binary: os.Getenv("OPENETL_ENGINE")},
		Connectors: connectorhost.Env{Connectors: os.Getenv(dirEnv)},
	}
	pod := &worker.Pod{API: api, ID: fmt.Sprintf("worker-%d", n), Attempt: worker.Attempt{ID: os.Getenv("OPENETL_ATTEMPT"), Placement: int32(placement)},
		Worker: n - 1, Run: s.RunWorker, Logger: log, Every: 20 * time.Millisecond}
	if err := pod.Serve(context.Background()); err != nil {
		log.Error("the worker's pod failed", "err", err)
		return 1
	}
	return 0
}

// workersAPI is the control plane as an attempt on workers sees it: the
// pipeline's spec, where the workers join, and the checkpoints mirrored.
type workersAPI struct {
	dataplanev1connect.UnimplementedDataPlaneServiceHandler
	spec *dataplanev1.GetSyncSpecResponse
	work *dataplanev1.Work

	mu      sync.Mutex
	target  *dataplanev1.WorkerTarget
	joined  map[uint32]bool
	opened  int
	commits int
}

func (a *workersAPI) GetSyncSpec(context.Context, *connect.Request[dataplanev1.GetSyncSpecRequest]) (*connect.Response[dataplanev1.GetSyncSpecResponse], error) {
	return connect.NewResponse(a.spec), nil
}

func (a *workersAPI) OpenWorkers(_ context.Context, req *connect.Request[dataplanev1.OpenWorkersRequest]) (*connect.Response[dataplanev1.OpenWorkersResponse], error) {
	m := req.Msg
	a.mu.Lock()
	defer a.mu.Unlock()
	a.opened++
	ok := m.GetRunner() == "coordinator" && m.GetId() == a.work.GetId()
	if ok {
		a.target = &dataplanev1.WorkerTarget{Address: m.GetAddress(), Certificate: m.GetCertificate(), Token: m.GetToken(), Generation: m.GetGeneration(), Owner: m.GetOwner()}
	}
	return connect.NewResponse(&dataplanev1.OpenWorkersResponse{Accepted: ok}), nil
}

func (a *workersAPI) PollWork(_ context.Context, req *connect.Request[dataplanev1.PollWorkRequest]) (*connect.Response[dataplanev1.PollWorkResponse], error) {
	m := req.Msg
	if m.Worker == nil || m.GetAttempt().GetAttempt() != a.work.GetId() || m.GetAttempt().GetPlacement() != 1 || m.GetWorker() >= a.work.GetWorkers() {
		return connect.NewResponse(&dataplanev1.PollWorkResponse{}), nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	w := &dataplanev1.Work{Id: a.work.GetId(), Kind: a.work.GetKind(), Input: a.work.GetInput(), Size: a.work.GetSize(), Workers: a.work.GetWorkers()}
	if a.target != nil {
		t := proto.CloneOf(a.target)
		t.Worker = m.GetWorker()
		w.Join = t
		if a.joined == nil {
			a.joined = map[uint32]bool{}
		}
		a.joined[m.GetWorker()] = true
	}
	return connect.NewResponse(&dataplanev1.PollWorkResponse{Work: []*dataplanev1.Work{w}}), nil
}

func (a *workersAPI) ReportCheckpoint(context.Context, *connect.Request[dataplanev1.ReportCheckpointRequest]) (*connect.Response[dataplanev1.ReportCheckpointResponse], error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.commits++
	return connect.NewResponse(&dataplanev1.ReportCheckpointResponse{Added: true}), nil
}

func (a *workersAPI) ReportSchemas(context.Context, *connect.Request[dataplanev1.ReportSchemasRequest]) (*connect.Response[dataplanev1.ReportSchemasResponse], error) {
	return connect.NewResponse(&dataplanev1.ReportSchemasResponse{}), nil
}

func (a *workersAPI) ReportAttemptFailure(context.Context, *connect.Request[dataplanev1.ReportAttemptFailureRequest]) (*connect.Response[dataplanev1.ReportAttemptFailureResponse], error) {
	return connect.NewResponse(&dataplanev1.ReportAttemptFailureResponse{}), nil
}

// An attempt on two workers, each a process the process launcher starts as
// it would a pod: the coordinator tells the API where they join, they find
// it there, read and write their parts with connectors and engines of their
// own, and every row lands once. The coordinator closes the connectors it
// discovered with and reads and writes nothing.
func TestAnAttemptOnWorkerProcesses(t *testing.T) {
	engine := os.Getenv("OPENETL_ENGINE")
	if _, err := os.Stat(engine); engine == "" || err != nil {
		t.Skip("OPENETL_ENGINE names no engine binary")
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	connectors, store, out := filepath.Join(dir, "connectors"), filepath.Join(dir, "store"), filepath.Join(dir, "out")
	for _, d := range []string{connectors, store, out} {
		if err := os.MkdirAll(d, 0o750); err != nil {
			t.Fatal(err)
		}
	}
	script := fmt.Sprintf("#!/bin/sh\n%s=connector exec %q -test.run='^$'\n", processEnv, exe)
	if err := os.WriteFile(filepath.Join(connectors, "destination-lanes"), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	src, err := structpb.NewStruct(map[string]any{"seed": 4, "count": 3000, "batchSize": 100})
	if err != nil {
		t.Fatal(err)
	}
	dst, err := structpb.NewStruct(map[string]any{"dir": out})
	if err != nil {
		t.Fatal(err)
	}
	input, err := json.Marshal(contract.AttemptInput{OrgID: "org", PipelineID: "p", Version: "v1", SyncID: "s", RunKey: "run", Attempt: 1})
	if err != nil {
		t.Fatal(err)
	}
	api := &workersAPI{
		spec: &dataplanev1.GetSyncSpecResponse{
			OrgId: "org", PipelineId: "p", Version: "v1",
			Source:      &apiv1.ConnectorConfig{Connector: "faker", Config: src},
			Destination: &apiv1.ConnectorConfig{Connector: "lanes", Config: dst},
			Pipeline: &apiv1.PipelineSpec{Delivery: &apiv1.Delivery{Commit: &apiv1.Commit{
				MaxBytes: 64 << 10, Scope: apiv1.CommitScope_COMMIT_SCOPE_PIPELINE,
			}}},
		},
		work: &dataplanev1.Work{Id: "run/attempt/1", Kind: contract.RunSyncAttempt, Input: input, Size: &dataplanev1.Resources{CpuMillis: 1000, MemoryBytes: 1 << 30}, Workers: 2},
	}
	mux := http.NewServeMux()
	mux.Handle(dataplanev1connect.NewDataPlaneServiceHandler(api))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	launcher := &pods.Processes{
		Command: []string{exe, "-test.run=^$"},
		Environ: append(os.Environ(), processEnv+"=worker", apiEnv+"="+srv.URL, storeEnv+"="+store, dirEnv+"="+connectors),
		Pool:    "default",
	}
	t.Cleanup(launcher.Close)
	// The coordinator is this process: only the workers' processes start.
	workers := pods.Launch{Attempt: api.work.GetId(), Placement: 1, Kind: contract.RunSyncAttempt, Size: resources.S, Workers: 2, Coordinator: resources.XS}
	if err := launchWorkersOnly(t.Context(), launcher, workers); err != nil {
		t.Fatal(err)
	}

	checkpoints, err := checkpoint.NewFSStore(store)
	if err != nil {
		t.Fatal(err)
	}
	s := &Sync{
		API: dataplanev1connect.NewDataPlaneServiceClient(srv.Client(), srv.URL), Checkpoints: checkpoints, Storage: store,
		Engine: engineclient.Options{Binary: engine}, Connectors: connectorhost.Env{Connectors: connectors},
		Logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})).With("coordinator", true),
	}
	wk := worker.NewWork(worker.Spec{ID: api.work.GetId(), Runner: "coordinator", Workers: 2, Size: resources.S, Received: time.Now()}, nil)
	wk.Meter = resources.NewMeter()
	ctx, cancel := context.WithTimeout(worker.NewContext(t.Context(), wk), 3*time.Minute)
	defer cancel()
	res, err := s.RunSyncAttempt(ctx, contract.AttemptInput{OrgID: "org", PipelineID: "p", Version: "v1", SyncID: "s", RunKey: "run", Attempt: 1})
	if err != nil {
		t.Fatal(err)
	}
	if res.Committed < 2 {
		t.Errorf("committed %d epochs, want several", res.Committed)
	}
	api.mu.Lock()
	joined, opened := len(api.joined), api.opened
	api.mu.Unlock()
	if joined != 2 || opened != 1 {
		t.Errorf("%d workers joined, told once in %d, want 2 and 1", joined, opened)
	}
	for _, stream := range []string{"users", "purchases"} {
		rows := readRows(t, filepath.Join(out, stream+".jsonl"))
		ids := map[string]bool{}
		for _, r := range rows {
			if ids[r] {
				t.Fatalf("%s: row %s twice", stream, r)
			}
			ids[r] = true
		}
		if len(ids) == 0 {
			t.Errorf("%s: no rows", stream)
		}
	}
	if users := len(readRows(t, filepath.Join(out, "users.jsonl"))); users != 3000 {
		t.Errorf("%d users, want 3000", users)
	}
	if rows, _ := wk.Meter.Progress(); rows == 0 {
		t.Error("the workers' rows didn't reach the coordinator's meter")
	}
}

// launchWorkersOnly starts l's workers' processes, but not its own, which
// the test runs in its own process.
func launchWorkersOnly(ctx context.Context, p *pods.Processes, l pods.Launch) error {
	if err := p.Launch(ctx, l); err != nil {
		return err
	}
	all, err := p.Pods(ctx)
	if err != nil {
		return err
	}
	for _, pod := range all {
		if pod.Worker == 0 {
			return p.Delete(ctx, pod)
		}
	}
	return errors.New("no process for the attempt's own runner")
}

// readRows are the rows of a JSON lines file, each as its id.
func readRows(t *testing.T, path string) []string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	var out []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var row map[string]any
		if err := json.Unmarshal(sc.Bytes(), &row); err != nil {
			t.Fatal(err)
		}
		out = append(out, fmt.Sprint(row["id"]))
	}
	return out
}

// laneFiles is a stage-and-apply destination of JSON lines with 4 writer
// lanes: {dir}/{stream}.jsonl. Each lane stages its part of an epoch, and a
// commit applies every lane's, once, behind a fence.
var laneFiles = sdk.Connector{
	Name:         "lanes",
	ConfigSchema: []byte(`{"type": "object", "properties": {"dir": {"type": "string"}}, "required": ["dir"]}`),
	Egress:       sdk.Egress{Tier: sdk.EgressRaw},
	Destination: func(config []byte, _ host.Host) (destination.Destination, error) {
		var c struct{ Dir string }
		if err := json.Unmarshal(config, &c); err != nil || c.Dir == "" {
			return nil, fmt.Errorf("lanes: a dir, in %s: %w", config, err)
		}
		return &laneDir{dir: c.Dir}, nil
	},
	Capabilities: destination.Capabilities{
		Class: destination.StageAndApply, Scopes: []destination.Scope{destination.ScopePipeline}, MaxIdentifierLength: 63,
		WriteModes: []destination.WriteMode{destination.Append}, WriterLanes: 4,
	},
}

type laneDir struct{ dir string }

func (d *laneDir) Capabilities() destination.Capabilities { return destination.Capabilities{} }

func (d *laneDir) Open(_ context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	return &laneWriter{dir: d.dir, gen: cfg.Generation, lane: cfg.Lane, pending: map[string][]json.RawMessage{}}, os.MkdirAll(filepath.Join(d.dir, ".epochs"), 0o750)
}

type laneWriter struct {
	dir     string
	gen     uint64
	lane    int
	mu      sync.Mutex
	pending map[string][]json.RawMessage
}

func (w *laneWriter) path(name string) string { return filepath.Join(w.dir, ".epochs", name) }

func (w *laneWriter) Write(_ context.Context, _ destination.Epoch, stream string, b arrow.RecordBatch) error {
	j, err := b.MarshalJSON()
	if err != nil {
		return err
	}
	var rows []json.RawMessage
	if err := json.Unmarshal(j, &rows); err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.pending[stream] = append(w.pending[stream], rows...)
	return nil
}

func (w *laneWriter) PreCommit(_ context.Context, e destination.Epoch) (destination.Staged, error) {
	w.mu.Lock()
	rows := w.pending
	w.pending = map[string][]json.RawMessage{}
	w.mu.Unlock()
	b, err := json.Marshal(rows)
	if err != nil {
		return nil, err
	}
	name := fmt.Sprintf("%d-%d-%d", e.Generation, e.N, w.lane)
	return destination.Staged(name), os.WriteFile(w.path(name), b, 0o600)
}

func (w *laneWriter) fence() uint64 {
	b, _ := os.ReadFile(w.path("fence"))
	n, _ := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	return n
}

func (w *laneWriter) Commit(_ context.Context, e destination.Epoch, staged []destination.Staged) (destination.Committed, error) {
	if w.fence() > w.gen {
		return destination.Committed{}, destination.ErrFenced
	}
	return destination.Committed{}, w.apply(e, staged)
}

// apply appends every lane's staged rows of e to the streams' files, once.
func (w *laneWriter) apply(e destination.Epoch, staged []destination.Staged) error {
	marker := w.path(fmt.Sprintf("%d-%d.applied", e.Generation, e.N))
	if _, err := os.Stat(marker); err == nil {
		return nil
	}
	for _, s := range staged {
		b, err := os.ReadFile(w.path(string(s)))
		if err != nil {
			return err
		}
		var rows map[string][]json.RawMessage
		if err := json.Unmarshal(b, &rows); err != nil {
			return err
		}
		for stream, rs := range rows {
			f, err := os.OpenFile(filepath.Join(w.dir, stream+".jsonl"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
			if err != nil {
				return err
			}
			for _, r := range rs {
				_, err = f.Write(append(r, '\n'))
			}
			if err = errors.Join(err, f.Close()); err != nil {
				return err
			}
		}
	}
	return os.WriteFile(marker, nil, 0o600)
}

func (w *laneWriter) Recover(_ context.Context, gen uint64, e destination.Epoch, staged []destination.Staged) (destination.Recovery, error) {
	if w.fence() > gen {
		return 0, destination.ErrFenced
	}
	if err := os.WriteFile(w.path("fence"), []byte(strconv.FormatUint(gen, 10)), 0o600); err != nil {
		return 0, err
	}
	if e.IsZero() {
		return destination.Applied, nil
	}
	if _, err := os.Stat(w.path(fmt.Sprintf("%d-%d.applied", e.Generation, e.N))); err == nil {
		return destination.Applied, nil
	}
	return destination.Redone, w.apply(e, staged)
}

func (w *laneWriter) Close(context.Context) error { return nil }
