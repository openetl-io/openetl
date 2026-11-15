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

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
)

type fakeQuarantine struct {
	apiv1connect.UnimplementedPipelineServiceHandler
	reqs []*apiv1.ListQuarantineRequest
}

func (f *fakeQuarantine) ListQuarantine(_ context.Context, req *connect.Request[apiv1.ListQuarantineRequest]) (*connect.Response[apiv1.ListQuarantineResponse], error) {
	f.reqs = append(f.reqs, req.Msg)
	if req.Msg.GetParent() != "workspaces/sales/pipelines/p" {
		return nil, connect.NewError(connect.CodeNotFound, nil)
	}
	at := timestamppb.New(time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC))
	const base = "s3://lake/org/pipe/_quarantine/"
	files := []*apiv1.QuarantineFile{
		{
			Stream: "orders", Path: "3/17/orders.jsonl", Url: base + "3/17/orders.jsonl", Rows: 312, Generation: 3, Epoch: 17, CommitTime: at,
			Causes: []*apiv1.QuarantineCause{{Code: "type", Column: "amount", Rows: 310}, {Code: "type", Column: "id", NotHeld: "key", Rows: 2}},
		},
		{
			Stream: "orders", Path: "3/16/orders.jsonl", Url: base + "3/16/orders.jsonl", Rows: 2, Generation: 3, Epoch: 16, CommitTime: at,
			Causes: []*apiv1.QuarantineCause{{Code: "type", Column: "amount", Rows: 2}},
		},
		{
			Stream: "daily", Output: true, Path: "_outputs/3/16/daily.jsonl", Url: base + "_outputs/3/16/daily.jsonl", Rows: 1, Generation: 3, Epoch: 16, CommitTime: at,
			Causes: []*apiv1.QuarantineCause{{Code: "statement", Rows: 1}},
		},
	}
	switch req.Msg.GetPageToken() {
	case "":
		return connect.NewResponse(&apiv1.ListQuarantineResponse{Files: files[:2], NextPageToken: "next"}), nil
	default:
		return connect.NewResponse(&apiv1.ListQuarantineResponse{Files: files[2:]}), nil
	}
}

func (f *fakeQuarantine) ReadQuarantineFile(_ context.Context, req *connect.Request[apiv1.ReadQuarantineFileRequest]) (*connect.Response[apiv1.ReadQuarantineFileResponse], error) {
	if req.Msg.GetParent() != "workspaces/sales/pipelines/p" || req.Msg.GetPath() != "3/17/orders.jsonl" {
		return nil, connect.NewError(connect.CodeNotFound, nil)
	}
	lines := []string{`{"stream":"orders","code":"type","record":"{\"amount\": \"x\"}"}`, `{"stream":"orders","code":"object","record":"[1]"}`}
	n := min(int(req.Msg.GetLimit()), len(lines))
	return connect.NewResponse(&apiv1.ReadQuarantineFileResponse{Lines: lines[:n], More: n < len(lines)}), nil
}

func TestQuarantineShow(t *testing.T) {
	f := &fakeQuarantine{}
	mux := http.NewServeMux()
	mux.Handle(apiv1connect.NewPipelineServiceHandler(f))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	t.Setenv("OPENETL_API_KEY", "oetl_test")

	var out, notes bytes.Buffer
	if err := showQuarantine(t.Context(), []string{"--api-url", srv.URL, "--workspace", "sales", "--limit", "1", "p", "3/17/orders.jsonl"}, &out, &notes); err != nil {
		t.Fatal(err)
	}
	if out.String() != `{"stream":"orders","code":"type","record":"{\"amount\": \"x\"}"}`+"\n" || !strings.Contains(notes.String(), "more lines than these 1") {
		t.Errorf("printed %q, noted %q", out.String(), notes.String())
	}
	out.Reset()
	if err := run(t.Context(), []string{"quarantine", "show", "--api-url", srv.URL, "--workspace", "sales", "p", "3/17/orders.jsonl"}, &out); err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(out.String(), "\n"); n != 2 {
		t.Errorf("printed %d lines: %s", n, out.String())
	}
	for _, args := range [][]string{{"--limit", "101", "p", "x"}, {"p"}} {
		if err := showQuarantine(t.Context(), append([]string{"--api-url", srv.URL}, args...), &out, &notes); err == nil {
			t.Errorf("show %v", args)
		}
	}
}

func TestQuarantine(t *testing.T) {
	f := &fakeQuarantine{}
	mux := http.NewServeMux()
	mux.Handle(apiv1connect.NewPipelineServiceHandler(f))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	t.Setenv("OPENETL_API_KEY", "oetl_test")

	var out bytes.Buffer
	if err := run(t.Context(), []string{"quarantine", "--api-url", srv.URL, "--workspace", "sales", "--stream", "orders", "--since", "1d", "p"}, &out); err != nil {
		t.Fatal(err)
	}
	if len(f.reqs) != 2 || f.reqs[0].GetStream() != "orders" || f.reqs[0].GetSince() == nil ||
		time.Since(f.reqs[0].GetSince().AsTime()).Round(time.Hour) != 24*time.Hour || f.reqs[1].GetPageToken() != "next" {
		t.Errorf("requests %v", f.reqs)
	}
	got := out.String()
	for _, want := range []string{
		"COMMITTED", "orders          3/17   312   3/17/orders.jsonl", "daily (output)", "_outputs/3/16/daily.jsonl",
		"Paths are below s3://lake/org/pipe/_quarantine/ in the data plane's storage.",
		"By cause:", "orders          312   type on amount\n", "orders          2     type on id, not held: key",
		"daily (output)  1     statement",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("printed\n%s\nwithout %q", got, want)
		}
	}

	out.Reset()
	f.reqs = nil
	if err := run(t.Context(), []string{"quarantine", "--api-url", srv.URL, "--workspace", "sales", "--limit", "2", "p"}, &out); err != nil {
		t.Fatal(err)
	}
	if len(f.reqs) != 1 || f.reqs[0].GetPageSize() != 2 || !strings.Contains(out.String(), "The newest 2 files; --limit or --since lists others.") {
		t.Errorf("requests %v, printed\n%s", f.reqs, out.String())
	}

	out.Reset()
	if err := run(t.Context(), []string{"quarantine", "--api-url", srv.URL, "--workspace", "sales", "--json", "p"}, &out); err != nil {
		t.Fatal(err)
	}
	if lines := strings.Split(strings.TrimSpace(out.String()), "\n"); len(lines) != 3 || !strings.Contains(lines[0], `"notHeld":"key"`) {
		t.Errorf("printed\n%s", out.String())
	}
}
