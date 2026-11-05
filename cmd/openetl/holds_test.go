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

type fakeHolds struct {
	apiv1connect.UnimplementedPipelineServiceHandler
	released, discarded []string
}

func (f *fakeHolds) ListHolds(_ context.Context, req *connect.Request[apiv1.ListHoldsRequest]) (*connect.Response[apiv1.ListHoldsResponse], error) {
	if req.Msg.GetParent() != "workspaces/sales/pipelines/p" {
		return nil, connect.NewError(connect.CodeNotFound, nil)
	}
	start := timestamppb.New(time.Now().Add(-90 * time.Minute))
	return connect.NewResponse(&apiv1.ListHoldsResponse{Holds: []*apiv1.Hold{
		{Stream: "orders", Column: "amount", Whole: true, Reason: "a view depends on it", SinceGeneration: 3, SinceEpoch: 41, StartTime: start, Pending: true},
		{Stream: "orders", Column: "note", Cells: 7, Reason: "type: column \"note\" isn't json", SinceGeneration: 3, SinceEpoch: 40, StartTime: start},
	}}), nil
}

func (f *fakeHolds) ReleaseHold(_ context.Context, req *connect.Request[apiv1.ReleaseHoldRequest]) (*connect.Response[apiv1.ReleaseHoldResponse], error) {
	f.released = append(f.released, req.Msg.GetParent()+" "+req.Msg.GetStream()+"."+req.Msg.GetColumn())
	return connect.NewResponse(&apiv1.ReleaseHoldResponse{Hold: &apiv1.Hold{Stream: req.Msg.GetStream(), Column: req.Msg.GetColumn()}}), nil
}

func (f *fakeHolds) DiscardHold(_ context.Context, req *connect.Request[apiv1.DiscardHoldRequest]) (*connect.Response[apiv1.DiscardHoldResponse], error) {
	f.discarded = append(f.discarded, req.Msg.GetParent()+" "+req.Msg.GetStream()+"."+req.Msg.GetColumn())
	return connect.NewResponse(&apiv1.DiscardHoldResponse{Hold: &apiv1.Hold{Stream: req.Msg.GetStream(), Column: req.Msg.GetColumn()}}), nil
}

func TestHolds(t *testing.T) {
	f := &fakeHolds{}
	mux := http.NewServeMux()
	mux.Handle(apiv1connect.NewPipelineServiceHandler(f))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	t.Setenv("OPENETL_API_KEY", "oetl_test")

	var out bytes.Buffer
	if err := run(t.Context(), []string{"holds", "--api-url", srv.URL, "--workspace", "sales", "p"}, &out); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 3 || !strings.Contains(lines[1], "amount  column  -") || !strings.Contains(lines[1], "3/41") ||
		!strings.Contains(lines[1], "1h30m") || !strings.Contains(lines[1], "pending") || !strings.Contains(lines[2], "note    cells   7") ||
		!strings.Contains(lines[2], " held ") || !strings.HasSuffix(lines[2], `type: column "note" isn't json`) {
		t.Errorf("printed\n%s", out.String())
	}

	for _, args := range [][]string{
		{"holds", "release", "--api-url", srv.URL, "p", "orders", "amount"},
		{"holds", "discard", "--api-url", srv.URL, "p", "orders", "note"},
	} {
		out.Reset()
		if err := run(t.Context(), args, &out); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(out.String(), "orders.") {
			t.Errorf("%v printed %q", args, out.String())
		}
	}
	if len(f.released) != 1 || f.released[0] != "workspaces/default/pipelines/p orders.amount" ||
		len(f.discarded) != 1 || f.discarded[0] != "workspaces/default/pipelines/p orders.note" {
		t.Errorf("released %v, discarded %v", f.released, f.discarded)
	}
	if err := run(t.Context(), []string{"holds", "release", "--api-url", srv.URL, "p", "orders"}, &out); err == nil {
		t.Error("release without a column worked")
	}
}
