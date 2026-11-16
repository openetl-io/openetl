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
	"slices"
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
	"github.com/openetl-io/openetl/internal/plan"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/platform/resources"
	"github.com/openetl-io/openetl/internal/runner/pods"
	"github.com/openetl-io/openetl/internal/runner/worker"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/host"
	"github.com/openetl-io/openetl/sdk/go/source"
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
	case "timed":
		sdk.Serve(timedEvents)
		return
	case "locked":
		sdk.Serve(lockedSource)
		return
	case "dies":
		sdk.Serve(dyingSource)
		return
	case "chatty":
		sdk.Serve(chattySource)
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

// onWorkerProcesses runs an attempt of pipeline, from src into the lanes
// destination in its out directory, on two workers, each a process the
// process launcher starts as it would a pod, through the activities a
// runner runs: this process is their coordinator. The attempt's result, the
// API it ran against, its work and its out directory.
func onWorkerProcesses(t *testing.T, src *apiv1.ConnectorConfig, pipeline *apiv1.PipelineSpec) (contract.AttemptResult, *workersAPI, *worker.Work, string, error) {
	t.Helper()
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
	for name, as := range map[string]string{"destination-lanes": "connector", "source-timed": "timed"} {
		script := fmt.Sprintf("#!/bin/sh\n%s=%s exec %q -test.run='^$'\n", processEnv, as, exe)
		if err := os.WriteFile(filepath.Join(connectors, name), []byte(script), 0o700); err != nil {
			t.Fatal(err)
		}
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
			Source: src, Destination: &apiv1.ConnectorConfig{Connector: "lanes", Config: dst}, Pipeline: pipeline,
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
	return res, api, wk, out, err
}

// An attempt on two workers: the coordinator tells the API where they join,
// they find it there, read and write their parts with connectors and engines
// of their own, whose exchange sends each keyed row over TLS to the writer
// lane that owns its key group, in either worker, and every row lands once.
// The coordinator closes the connectors it discovered with and reads and
// writes nothing.
func TestAnAttemptOnWorkerProcesses(t *testing.T) {
	src, err := structpb.NewStruct(map[string]any{"seed": 4, "count": 3000, "batchSize": 100})
	if err != nil {
		t.Fatal(err)
	}
	res, api, wk, out, err := onWorkerProcesses(t, &apiv1.ConnectorConfig{Connector: "faker", Config: src}, &apiv1.PipelineSpec{
		Delivery: &apiv1.Delivery{Commit: &apiv1.Commit{MaxBytes: 64 << 10, Scope: apiv1.CommitScope_COMMIT_SCOPE_PIPELINE}},
	})
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
	// Each stream is read by one worker, and its rows, keyed, reach both
	// workers' writer lanes through the exchange between their engines.
	for _, stream := range []string{"users", "purchases"} {
		if lanes := stagedLanes(t, filepath.Join(out, ".epochs"))[stream]; len(lanes) != 2 {
			t.Errorf("%s's rows were staged on writer lanes %v, want both workers'", stream, lanes)
		}
	}
}

// A group window's job runs on the workers it was placed on, through the
// activities a runner runs: the coordinator's job has the engine's dataflow
// when it decides, so it isn't placed again on one pod. The workers'
// engines send each row to the keyed lane that owns its region, on either
// worker, and each window lands once, from both workers' writer lanes,
// with every row.
func TestAGroupWindowOnWorkerProcesses(t *testing.T) {
	sql := "INSERT INTO tens SELECT window_start, region, count(*) AS n FROM events GROUP BY tumble(at, INTERVAL '10 seconds'), region"
	res, api, _, out, err := onWorkerProcesses(t, &apiv1.ConnectorConfig{Connector: "timed", Config: &structpb.Struct{}}, &apiv1.PipelineSpec{
		Source:    &apiv1.PipelineSource{Streams: []*apiv1.PipelineStream{{Name: "events", EventTime: "at"}}},
		Transform: &apiv1.Transform{Engine: &apiv1.Transform_Sql{Sql: &apiv1.SqlTransform{Sql: sql, Passthrough: apiv1.Passthrough_PASSTHROUGH_NONE}}},
		Delivery:  &apiv1.Delivery{Commit: &apiv1.Commit{MaxBytes: 16 << 10, Scope: apiv1.CommitScope_COMMIT_SCOPE_PIPELINE}},
	})
	if onePod := (*worker.OnePod)(nil); errors.As(err, &onePod) {
		t.Fatalf("the job was placed again on one pod: %s", onePod.Why)
	}
	if err != nil {
		t.Fatal(err)
	}
	if res.Committed < 2 {
		t.Errorf("committed %d epochs, want several", res.Committed)
	}
	api.mu.Lock()
	joined := len(api.joined)
	api.mu.Unlock()
	if joined != 2 {
		t.Errorf("%d workers joined, want 2", joined)
	}
	b, err := os.ReadFile(filepath.Join(out, "tens.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	starts := map[string][]string{}
	got := map[string]int{}
	for line := range strings.Lines(string(b)) {
		var r struct {
			Start  string `json:"window_start"`
			Region string
			N      int
		}
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatal(err)
		}
		w := r.Start + "/" + r.Region
		if _, ok := got[w]; ok {
			t.Fatalf("window %s landed twice", w)
		}
		got[w] = r.N
		starts[r.Region] = append(starts[r.Region], r.Start)
	}
	// Each region's windows, in the order they start.
	want := timedWindows()
	for region, counts := range want {
		ss := starts[region]
		slices.Sort(ss)
		var ns []int
		for _, s := range ss {
			ns = append(ns, got[s+"/"+region])
		}
		if !slices.Equal(ns, counts) {
			t.Errorf("region %s's windows counted %v, want %v", region, ns, counts)
		}
	}
	if len(starts) != len(want) {
		t.Errorf("%d regions' windows landed, want %d", len(starts), len(want))
	}
	// Each worker writes with a destination process of its own.
	procs := writerProcesses(t, filepath.Join(out, ".epochs"))
	wrote := map[string]bool{}
	for l := range stagedLanes(t, filepath.Join(out, ".epochs"))["tens"] {
		wrote[procs[l]] = true
	}
	if len(wrote) != 2 {
		t.Errorf("the windows were staged from %d of the workers' processes %v, want both", len(wrote), procs)
	}
}

// A Spark or Flink transform's attempt placed on workers goes back to one
// pod before its transform is set up: this runner, which can run neither,
// would fail at that setup.
func TestAnOwnCodeTransformGoesToOnePodBeforeItsSetup(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	connectors, store := filepath.Join(dir, "connectors"), filepath.Join(dir, "store")
	for _, d := range []string{connectors, store} {
		if err := os.MkdirAll(d, 0o750); err != nil {
			t.Fatal(err)
		}
	}
	script := fmt.Sprintf("#!/bin/sh\n%s=connector exec %q -test.run='^$'\n", processEnv, exe)
	if err := os.WriteFile(filepath.Join(connectors, "destination-lanes"), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	dst, err := structpb.NewStruct(map[string]any{"dir": filepath.Join(dir, "out")})
	if err != nil {
		t.Fatal(err)
	}
	checkpoints, err := checkpoint.NewFSStore(store)
	if err != nil {
		t.Fatal(err)
	}
	for name, tr := range map[string]*apiv1.Transform{
		"spark": {Engine: &apiv1.Transform_Spark{Spark: &apiv1.SparkTransform{Cluster: "c"}}},
		"flink": {Engine: &apiv1.Transform_Flink{Flink: &apiv1.FlinkTransform{Cluster: "c"}}},
	} {
		api := &workersAPI{spec: &dataplanev1.GetSyncSpecResponse{
			OrgId: "org", PipelineId: "p", Version: "v1",
			Source:      &apiv1.ConnectorConfig{Connector: "faker", Config: &structpb.Struct{}},
			Destination: &apiv1.ConnectorConfig{Connector: "lanes", Config: dst},
			Pipeline:    &apiv1.PipelineSpec{Transform: tr},
		}}
		mux := http.NewServeMux()
		mux.Handle(dataplanev1connect.NewDataPlaneServiceHandler(api))
		srv := httptest.NewServer(mux)
		s := &Sync{
			API: dataplanev1connect.NewDataPlaneServiceClient(srv.Client(), srv.URL), Checkpoints: checkpoints, Storage: store,
			Connectors: connectorhost.Env{Connectors: connectors},
		}
		wk := worker.NewWork(worker.Spec{ID: "run/attempt/1", Runner: "coordinator", Workers: 2, Size: resources.S, Received: time.Now()}, nil)
		_, err := s.RunSyncAttempt(worker.NewContext(t.Context(), wk), contract.AttemptInput{OrgID: "org", PipelineID: "p", Version: "v1", SyncID: "s", RunKey: "run", Attempt: 1})
		srv.Close()
		var onePod *worker.OnePod
		if !errors.As(err, &onePod) || onePod.Why != "a Spark or Flink transform runs on one pod" {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// A statement tier 3 takes, as a dedupe by ROW_NUMBER() = 1, is refused
// as its sync's job is built, before the attempt is placed: on workers as on
// one pod.
func TestATier3StatementIsRefusedOnWorkersAsOnOnePod(t *testing.T) {
	engine := os.Getenv("OPENETL_ENGINE")
	if _, err := os.Stat(engine); engine == "" || err != nil {
		t.Skip("OPENETL_ENGINE names no engine binary")
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	connectors, store := filepath.Join(dir, "connectors"), filepath.Join(dir, "store")
	for _, d := range []string{connectors, store} {
		if err := os.MkdirAll(d, 0o750); err != nil {
			t.Fatal(err)
		}
	}
	for name, as := range map[string]string{"destination-lanes": "connector", "source-timed": "timed"} {
		script := fmt.Sprintf("#!/bin/sh\n%s=%s exec %q -test.run='^$'\n", processEnv, as, exe)
		if err := os.WriteFile(filepath.Join(connectors, name), []byte(script), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	dst, err := structpb.NewStruct(map[string]any{"dir": filepath.Join(dir, "out")})
	if err != nil {
		t.Fatal(err)
	}
	checkpoints, err := checkpoint.NewFSStore(store)
	if err != nil {
		t.Fatal(err)
	}
	sql := "INSERT INTO firsts SELECT id, region, at FROM events QUALIFY row_number() OVER (PARTITION BY region ORDER BY at) = 1"
	api := &workersAPI{spec: &dataplanev1.GetSyncSpecResponse{
		OrgId: "org", PipelineId: "p", Version: "v1",
		Source:      &apiv1.ConnectorConfig{Connector: "timed", Config: &structpb.Struct{}},
		Destination: &apiv1.ConnectorConfig{Connector: "lanes", Config: dst},
		Pipeline: &apiv1.PipelineSpec{
			Source:    &apiv1.PipelineSource{Streams: []*apiv1.PipelineStream{{Name: "events", EventTime: "at"}}},
			Transform: &apiv1.Transform{Engine: &apiv1.Transform_Sql{Sql: &apiv1.SqlTransform{Sql: sql, Passthrough: apiv1.Passthrough_PASSTHROUGH_NONE}}},
		},
	}}
	mux := http.NewServeMux()
	mux.Handle(dataplanev1connect.NewDataPlaneServiceHandler(api))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	s := &Sync{
		API: dataplanev1connect.NewDataPlaneServiceClient(srv.Client(), srv.URL), Checkpoints: checkpoints, Storage: store,
		Engine: engineclient.Options{Binary: engine}, Connectors: connectorhost.Env{Connectors: connectors},
	}
	for _, workers := range []int{0, 2} {
		wk := worker.NewWork(worker.Spec{ID: "run/attempt/1", Runner: "coordinator", Workers: workers, Size: resources.S, Received: time.Now()}, nil)
		_, err := s.RunSyncAttempt(worker.NewContext(t.Context(), wk), contract.AttemptInput{OrgID: "org", PipelineID: "p", Version: "v1", SyncID: "s", RunKey: "run", Attempt: 1})
		if errs.CodeOf(err) != errs.ConfigInvalid || !strings.Contains(err.Error(), "transform.sql:1:62: OVER: transforms don't run window functions yet (tier 3)") {
			t.Errorf("on %d workers: %v", workers, err)
		}
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	if api.opened > 0 {
		t.Error("the coordinator opened its workers")
	}
}

// A count over windows of a topic's records, by their own columns, as a
// Kafka source declares them, its headers JSON, plans for two workers
// through the catalog: one exchange, on the window's key, before the
// statement, and none before the writer.
func TestAWindowOverARecordsOwnColumnsPlansForWorkers(t *testing.T) {
	bin := os.Getenv("OPENETL_ENGINE")
	if _, err := os.Stat(bin); bin == "" || err != nil {
		t.Skip("OPENETL_ENGINE names no engine binary")
	}
	raw, err := arrowipc.EncodeSchema(arrow.NewSchema([]arrow.Field{
		{Name: "_kafka_partition", Type: arrow.PrimitiveTypes.Int32},
		{Name: "_kafka_offset", Type: arrow.PrimitiveTypes.Int64},
		{Name: "_kafka_timestamp", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
		{Name: "_kafka_key", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "_kafka_headers", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: arrow.NewMetadata([]string{"ARROW:extension:name"}, []string{"arrow.json"})},
	}, nil))
	if err != nil {
		t.Fatal(err)
	}
	discovered, err := arrowipc.DecodeSchema(raw)
	if err != nil {
		t.Fatal(err)
	}
	cat := catalogProto(source.Catalog{Streams: []source.CatalogStream{{
		Name: "topic", CursorField: "_kafka_offset", Replay: source.Exact, Schema: discovered, SyncModes: []source.SyncMode{source.Stream},
	}}})
	sql := "INSERT INTO tens\nSELECT window_start, _kafka_key AS k, count(*) AS n, sum(_kafka_offset) AS total, min(_kafka_offset) AS first\n" +
		"FROM topic\nGROUP BY tumble(_kafka_timestamp, INTERVAL '10 seconds'), _kafka_key"
	spec := &apiv1.PipelineSpec{
		Mode:      apiv1.PipelineMode_PIPELINE_MODE_CONTINUOUS,
		Resources: &apiv1.JobResources{Workers: 2},
		Source:    &apiv1.PipelineSource{Streams: []*apiv1.PipelineStream{{Name: "topic", EventTime: "_kafka_timestamp"}}},
		Transform: &apiv1.Transform{Engine: &apiv1.Transform_Sql{Sql: &apiv1.SqlTransform{Sql: sql, Passthrough: apiv1.Passthrough_PASSTHROUGH_NONE}}},
	}
	req, unknown := plan.SQLRequest(spec, cat, nil)
	if len(unknown) > 0 {
		t.Fatal(unknown)
	}
	a, err := engineclient.Plan(t.Context(), engineclient.Options{Binary: bin}, req)
	if err != nil || len(a.Errors) > 0 {
		t.Fatalf("%v, %v", err, a.Errors)
	}
	caps := destination.Capabilities{Class: destination.TransactionalStream, Scopes: []destination.Scope{destination.ScopePipeline}, WriterLanes: 64, LaneCommits: true}
	streams, problems, _ := plan.PipelineSQL(spec, cat, caps, &a)
	if len(problems) > 0 {
		t.Fatal(problems)
	}
	if workers, onePod, warnings := plan.Workers(spec, cat, caps, false, &a); workers != 2 || onePod != "" || len(warnings) > 0 {
		t.Errorf("%d workers, on one pod because %q, warned %v", workers, onePod, warnings)
	}
	var exchanges []string
	for _, st := range streams {
		for _, op := range st.GetOperators() {
			if op.GetExchange() {
				exchanges = append(exchanges, fmt.Sprintf("%s %s %v", st.GetName(), op.GetKind(), op.GetNeeds()))
			}
		}
	}
	if want := []string{"tens PLAN_OPERATOR_KIND_STATEMENT [_kafka_key]"}; !slices.Equal(exchanges, want) {
		t.Errorf("the plan exchanges %v, want %v", exchanges, want)
	}
}

// timedEvents is a source of one stream, events, in timedSplits splits,
// whose columns it declares. Split k's chunk c holds timedRows rows at c
// seconds and r milliseconds, the rth of region timedRegions[(k+r)%16], so
// each split's rows come in the order of their time.
var timedEvents = sdk.Connector{
	Name:         "timed",
	ConfigSchema: []byte(`{"type": "object"}`),
	Egress:       sdk.Egress{Tier: sdk.EgressRaw},
	Splits:       true,
	Source:       func([]byte, host.Host) (source.Source, error) { return timedSource{}, nil },
}

const (
	timedSplits = 4
	timedChunks = 30
	timedRows   = 16
)

var timedRegions = func() []string {
	out := make([]string, 16)
	for i := range out {
		out[i] = fmt.Sprintf("r%d", i)
	}
	return out
}()

// timedWindows are the counts of each region's ten-second windows of
// timedEvents' rows, in the order the windows start.
func timedWindows() map[string][]int {
	out := map[string][]int{}
	for k := range timedSplits {
		for c := range timedChunks {
			for r := range timedRows {
				region := timedRegions[(k+r)%len(timedRegions)]
				if out[region] == nil {
					out[region] = make([]int, timedChunks/10)
				}
				out[region][c/10]++
			}
		}
	}
	return out
}

type timedSource struct{}

func (timedSource) Discover(context.Context) (source.Catalog, error) {
	return source.Catalog{Streams: []source.CatalogStream{{
		Name: "events", Replay: source.Exact, SyncModes: []source.SyncMode{source.FullRefresh},
		Schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "region", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		}, nil),
	}}}, nil
}

func (timedSource) Splits(context.Context, source.ConfiguredStream, []source.Split) ([]source.Split, error) {
	out := make([]source.Split, timedSplits)
	for i := range out {
		out[i] = source.Split{ID: strconv.Itoa(i)}
	}
	return out, nil
}

func (timedSource) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return source.ReadSplits(ctx, req, out, func(_ context.Context, _ source.ConfiguredStream, sp source.Split, e source.SplitEmitter) error {
		k, err := strconv.Atoi(sp.ID)
		if err != nil {
			return err
		}
		from := 0
		if sp.State != nil {
			if from, err = strconv.Atoi(string(sp.State)); err != nil {
				return err
			}
		}
		for c := from; c < timedChunks; c++ {
			rows := make([]json.RawMessage, timedRows)
			for r := range rows {
				at := start.Add(time.Duration(c)*time.Second + time.Duration(r)*time.Millisecond)
				rows[r] = fmt.Appendf(nil, `{"id": %d, "region": %q, "at": %q}`, (k*timedChunks+c)*timedRows+r, timedRegions[(k+r)%len(timedRegions)], at.Format(time.RFC3339Nano))
			}
			if err := e.Rows(rows); err != nil {
				return err
			}
			if err := e.ResumePoint([]byte(strconv.Itoa(c + 1))); err != nil {
				return err
			}
		}
		return e.Complete([]byte(strconv.Itoa(timedChunks)))
	})
}

// stagedLanes are the writer lanes that staged rows of each stream, as
// laneFiles names its staging in dir.
func stagedLanes(t *testing.T, dir string) map[string]map[int]bool {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]map[int]bool{}
	for _, e := range entries {
		parts := strings.Split(e.Name(), "-")
		if len(parts) != 3 {
			continue
		}
		lane, err := strconv.Atoi(parts[2])
		if err != nil {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		var rows map[string][]json.RawMessage
		if err := json.Unmarshal(b, &rows); err != nil {
			t.Fatal(err)
		}
		for stream, rs := range rows {
			if len(rs) > 0 {
				if out[stream] == nil {
					out[stream] = map[int]bool{}
				}
				out[stream][lane] = true
			}
		}
	}
	return out
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

// Open keeps, as .epochs/writer-LANE, the process the lane writes from: one
// per worker, whose destination is its own.
func (d *laneDir) Open(_ context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	w := &laneWriter{dir: d.dir, gen: cfg.Generation, lane: cfg.Lane, pending: map[string][]json.RawMessage{}}
	if err := os.MkdirAll(filepath.Join(d.dir, ".epochs"), 0o750); err != nil {
		return nil, err
	}
	return w, os.WriteFile(w.path(fmt.Sprintf("writer-%d", cfg.Lane)), []byte(strconv.Itoa(os.Getpid())), 0o600)
}

// writerProcesses are the processes that opened each writer lane, as
// laneDir keeps them in dir.
func writerProcesses(t *testing.T, dir string) map[int]string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	out := map[int]string{}
	for _, e := range entries {
		lane, ok := strings.CutPrefix(e.Name(), "writer-")
		if !ok {
			continue
		}
		l, err := strconv.Atoi(lane)
		if err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		out[l] = string(b)
	}
	return out
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
