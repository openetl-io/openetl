package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/gen/openetl/api/v1/apiv1connect"
)

const syncName = "workspaces/default/pipelines/p/syncs/0193"

type fakeSyncs struct {
	apiv1connect.UnimplementedSyncServiceHandler
	reads []*apiv1.ReadSyncLogRequest
}

func (f *fakeSyncs) ListSyncs(_ context.Context, req *connect.Request[apiv1.ListSyncsRequest]) (*connect.Response[apiv1.ListSyncsResponse], error) {
	start := time.Date(2026, 11, 2, 9, 0, 0, 0, time.UTC)
	return connect.NewResponse(&apiv1.ListSyncsResponse{Syncs: []*apiv1.Sync{
		{Name: syncName, State: apiv1.SyncState_SYNC_STATE_RUNNING, StartTime: timestamppb.New(start), Version: "v2",
			Streams: []*apiv1.SyncStream{{Name: "a", Rows: 40}, {Name: "b", Rows: 2}}},
		{Name: req.Msg.GetParent() + "/syncs/0192", State: apiv1.SyncState_SYNC_STATE_FAILED, StartTime: timestamppb.New(start.Add(-time.Hour)),
			EndTime: timestamppb.New(start.Add(-time.Hour + 90*time.Second)), Version: "v1", Error: "TRANSIENT: boom\nmore"},
	}[:min(req.Msg.GetPageSize(), 2)]}), nil
}

// ReadSyncLog returns an entry, then nothing while the sync runs, then an
// entry and the end.
func (f *fakeSyncs) ReadSyncLog(_ context.Context, req *connect.Request[apiv1.ReadSyncLogRequest]) (*connect.Response[apiv1.ReadSyncLogResponse], error) {
	f.reads = append(f.reads, req.Msg)
	attrs, _ := structpb.NewStruct(map[string]any{"component": "engine", "rows": 10, "stream": "public users"})
	entry := &apiv1.LogEntry{Time: timestamppb.Now(), Level: "INFO", Message: "committed", Attempt: 2, Attrs: attrs}
	switch len(f.reads) {
	case 1:
		return connect.NewResponse(&apiv1.ReadSyncLogResponse{Entries: []*apiv1.LogEntry{entry}, NextPageToken: "a"}), nil
	case 2:
		return connect.NewResponse(&apiv1.ReadSyncLogResponse{NextPageToken: "a"}), nil
	case 3:
		entry.PostSyncStep, entry.Attempt, entry.Message, entry.Attrs = 1, 1, "dbt finished", nil
		return connect.NewResponse(&apiv1.ReadSyncLogResponse{Entries: []*apiv1.LogEntry{entry}, NextPageToken: "b"}), nil
	}
	return connect.NewResponse(&apiv1.ReadSyncLogResponse{}), nil
}

func TestSyncsAndLogs(t *testing.T) {
	f := &fakeSyncs{}
	mux := http.NewServeMux()
	mux.Handle(apiv1connect.NewSyncServiceHandler(f))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	t.Setenv("OPENETL_API_KEY", "oetl_test")
	followEvery = time.Millisecond

	var out bytes.Buffer
	if err := run(t.Context(), []string{"syncs", "--api-url", srv.URL, "p"}, &out); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 3 || !strings.Contains(lines[1], "0193  running") || !strings.Contains(lines[1], "  42    v2") || !strings.Contains(lines[2], "1m30s") || !strings.HasSuffix(lines[2], "TRANSIENT: boom") {
		t.Errorf("printed\n%s", out.String())
	}

	out.Reset()
	if err := run(t.Context(), []string{"logs", "--api-url", srv.URL, "p"}, &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); !strings.Contains(got, ` INFO  attempt 2 engine: committed rows=10 stream="public users"`) || strings.Count(got, "\n") != 1 {
		t.Errorf("printed %q", got)
	}
	if len(f.reads) != 2 || f.reads[0].GetName() != syncName || f.reads[1].GetPageToken() != "a" {
		t.Errorf("reads %v", f.reads)
	}

	f.reads, out = nil, bytes.Buffer{}
	if err := run(t.Context(), []string{"logs", "--api-url", srv.URL, "-f", "--sync", "0193", "p"}, &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); strings.Count(got, "\n") != 2 || !strings.Contains(got, "step 1 attempt 1 dbt finished") {
		t.Errorf("printed %q", got)
	}
	if len(f.reads) != 4 {
		t.Errorf("%d reads, want 4", len(f.reads))
	}
}

type fakeVersions struct {
	apiv1connect.UnimplementedPipelineServiceHandler
	reqs []*apiv1.ListPipelineVersionsRequest
}

func (f *fakeVersions) ListPipelineVersions(_ context.Context, req *connect.Request[apiv1.ListPipelineVersionsRequest]) (*connect.Response[apiv1.ListPipelineVersionsResponse], error) {
	f.reqs = append(f.reqs, req.Msg)
	created := timestamppb.New(time.Date(2026, 11, 2, 9, 0, 0, 0, time.UTC))
	if req.Msg.GetPageToken() == "" {
		return connect.NewResponse(&apiv1.ListPipelineVersionsResponse{PipelineVersions: []*apiv1.PipelineVersion{
			{Name: req.Msg.GetParent() + "/versions/v3", Current: true, CreateTime: created, Provenance: "cli:shop.yaml#3"},
		}, NextPageToken: "next"}), nil
	}
	return connect.NewResponse(&apiv1.ListPipelineVersionsResponse{PipelineVersions: []*apiv1.PipelineVersion{
		{Name: req.Msg.GetParent() + "/versions/v2", CreateTime: created, Provenance: "api:doc 1"},
	}}), nil
}

func TestVersions(t *testing.T) {
	f := &fakeVersions{}
	mux := http.NewServeMux()
	mux.Handle(apiv1connect.NewPipelineServiceHandler(f))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	t.Setenv("OPENETL_API_KEY", "oetl_test")

	var out bytes.Buffer
	if err := run(t.Context(), []string{"versions", "--api-url", srv.URL, "--workspace", "ws", "p"}, &out); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 3 || !strings.HasPrefix(strings.Join(strings.Fields(lines[1]), " "), "v3 * ") || !strings.HasSuffix(lines[1], "cli:shop.yaml#3") || !strings.HasSuffix(lines[2], "api:doc 1") {
		t.Errorf("printed\n%s", out.String())
	}
	if len(f.reqs) != 2 || f.reqs[0].GetParent() != "workspaces/ws/pipelines/p" || f.reqs[1].GetPageToken() != "next" {
		t.Errorf("requests %v", f.reqs)
	}
}
