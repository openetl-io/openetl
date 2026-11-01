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
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/gen/openetl/api/v1/apiv1connect"
)

type fakeAudit struct {
	apiv1connect.UnimplementedAuditServiceHandler
	reqs []*apiv1.ListAuditEntriesRequest
}

// ListAuditEntries returns two pages of two entries.
func (f *fakeAudit) ListAuditEntries(_ context.Context, req *connect.Request[apiv1.ListAuditEntriesRequest]) (*connect.Response[apiv1.ListAuditEntriesResponse], error) {
	f.reqs = append(f.reqs, req.Msg)
	res := &apiv1.ListAuditEntriesResponse{}
	for range min(req.Msg.GetPageSize(), 2) {
		res.Entries = append(res.Entries, &apiv1.AuditEntry{
			Time: timestamppb.Now(), Actor: "principals/ci", Method: "/openetl.api.v1.PipelineService/SyncPipeline",
			Resources: []string{"workspaces/default/pipelines/p"}, Code: "ok",
		})
	}
	if req.Msg.GetPageToken() == "" {
		res.NextPageToken = "2"
		res.Entries[0].Code = ""
	}
	return connect.NewResponse(res), nil
}

func TestAuditCommands(t *testing.T) {
	f := &fakeAudit{}
	mux := http.NewServeMux()
	mux.Handle(apiv1connect.NewAuditServiceHandler(f))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	t.Setenv("OPENETL_API_KEY", "oetl_test")

	var out bytes.Buffer
	if err := run(t.Context(), []string{"audit", "list", "--api-url", srv.URL, "--actor", "ci", "--method", "SyncPipeline", "--since", "7d", "--limit", "3"}, &out); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 4 || !strings.Contains(lines[1], "PipelineService/SyncPipeline") || !strings.Contains(lines[1], "unfinished") || !strings.Contains(lines[3], " ok ") {
		t.Errorf("printed\n%s", out.String())
	}
	if len(f.reqs) != 2 || f.reqs[0].GetPageSize() != 3 || f.reqs[1].GetPageSize() != 1 || f.reqs[1].GetPageToken() != "2" {
		t.Errorf("requests %v", f.reqs)
	}
	r := f.reqs[0]
	if since := time.Since(r.GetStartTime().AsTime()); r.GetActor() != "principals/ci" || r.GetMethod() != "SyncPipeline" || since < 7*24*time.Hour || since > 7*24*time.Hour+time.Minute {
		t.Errorf("request %v", r)
	}

	out.Reset()
	if err := run(t.Context(), []string{"audit", "list", "--api-url", srv.URL, "--json", "--limit", "1"}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(out.String(), "{") || strings.Count(out.String(), "\n") != 1 {
		t.Errorf("--json printed %q", out.String())
	}
}

func TestShortMethod(t *testing.T) {
	for m, want := range map[string]string{
		"/openetl.api.v1.PipelineService/SyncPipeline":       "PipelineService/SyncPipeline",
		"/openetl.dataplane.v1.DataPlaneService/GetSyncSpec": "DataPlaneService/GetSyncSpec",
		"openetld bootstrap":                                 "openetld bootstrap",
	} {
		if got := shortMethod(m); got != want {
			t.Errorf("%s: %s", m, got)
		}
	}
}
