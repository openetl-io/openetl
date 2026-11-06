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
		{
			Stream: "orders", Column: "amount", Whole: true, Reason: "amount changed from int32 to int64.", SinceGeneration: 3, SinceEpoch: 41, StartTime: start,
			Refused: &apiv1.HoldRefusal{Reason: "it doesn't change a column's type"},
			WaysOut: []*apiv1.HoldWayOut{
				{Action: apiv1.HoldWayOut_ACTION_RELEASE, First: "Run this on the destination.", Statement: "ALTER TABLE orders"},
				{Action: apiv1.HoldWayOut_ACTION_RELEASE_NEW_VERSION, Effect: "Sends its values to amount__v2."},
			},
		},
		{Stream: "orders", Column: "note", Cells: 7, Reason: "7 values of note don't fit its type.", SinceGeneration: 3, SinceEpoch: 40, StartTime: start, Pending: true},
	}}), nil
}

func (f *fakeHolds) ReleaseHold(_ context.Context, req *connect.Request[apiv1.ReleaseHoldRequest]) (*connect.Response[apiv1.ReleaseHoldResponse], error) {
	r := req.Msg.GetParent() + " " + req.Msg.GetStream() + "." + req.Msg.GetColumn()
	if req.Msg.GetNewVersion() {
		r += " (new version)"
	}
	f.released = append(f.released, r)
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
	if len(lines) < 3 || !strings.Contains(lines[1], "amount  column  -") || !strings.Contains(lines[1], "3/41") ||
		!strings.Contains(lines[1], "1h30m") || !strings.HasSuffix(lines[1], "refused") || !strings.Contains(lines[2], "note    cells   7") ||
		!strings.HasSuffix(lines[2], "pending") {
		t.Errorf("printed\n%s", out.String())
	}
	want := `
orders.amount
  amount changed from int32 to int64.
  Its last release didn't clear it: it doesn't change a column's type. Release it again once that's fixed.
  To clear it:
  - Run this on the destination.
      ALTER TABLE orders
    then: openetl holds release --workspace sales p orders amount
  - openetl holds release --new-version --workspace sales p orders amount
    Sends its values to amount__v2.

orders.note (cells)
  7 values of note don't fit its type.
`
	if !strings.HasSuffix(out.String(), want) {
		t.Errorf("printed\n%s\nwant it to end\n%s", out.String(), want)
	}

	for _, args := range [][]string{
		{"holds", "release", "--api-url", srv.URL, "p", "orders", "amount"},
		{"holds", "release", "--api-url", srv.URL, "--new-version", "p", "orders", "amount"},
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
	if len(f.released) != 2 || f.released[0] != "workspaces/default/pipelines/p orders.amount" ||
		f.released[1] != "workspaces/default/pipelines/p orders.amount (new version)" ||
		len(f.discarded) != 1 || f.discarded[0] != "workspaces/default/pipelines/p orders.note" {
		t.Errorf("released %v, discarded %v", f.released, f.discarded)
	}
	if err := run(t.Context(), []string{"holds", "release", "--api-url", srv.URL, "p", "orders"}, &out); err == nil {
		t.Error("release without a column worked")
	}
	if err := run(t.Context(), []string{"holds", "discard", "--api-url", srv.URL, "--new-version", "p", "orders", "note"}, &out); err == nil {
		t.Error("discard took --new-version")
	}
}
