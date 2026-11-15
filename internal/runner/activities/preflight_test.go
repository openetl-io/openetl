package activities

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

func TestTableName(t *testing.T) {
	for in, want := range map[string]string{"public.Orders": "public_orders", "ÜBER-x": "_ber_x", "": "_"} {
		if got := tableName(in, false); got != want {
			t.Errorf("tableName(%q) = %q, want %q", in, got, want)
		}
	}
	if got := tableName("Keep.Me", true); got != "Keep.Me" {
		t.Errorf("kept %q", got)
	}
}

func TestInterval(t *testing.T) {
	if got := interval(&apiv1.Schedule{Trigger: &apiv1.Schedule_Every{Every: durationpb.New(time.Hour)}, Jitter: durationpb.New(time.Minute)}); got != time.Hour+time.Minute {
		t.Errorf("every 1h with 1m jitter: %v", got)
	}
	if got := interval(&apiv1.Schedule{Trigger: &apiv1.Schedule_Cron{Cron: "0 6 * * 1-5"}}); got != 72*time.Hour {
		t.Errorf("weekdays at 6: %v, want the weekend's 72h", got)
	}
	if got := interval(nil); got != 0 {
		t.Errorf("no schedule: %v", got)
	}
}

func TestDestinationPreflight(t *testing.T) {
	job := runner.Job{Namespace: "cdc", Streams: []runner.Stream{
		{Name: "public.Orders", Mode: destination.Append, PrimaryKey: []string{"id"}},
		{Name: "users", Table: "people", Mode: destination.Merge},
	}}
	req := destinationPreflight("p1", job)
	if req.Pipeline != "p1" || req.Namespace != "cdc" || len(req.Streams) != 2 || req.Streams[1].Mode != destination.Merge {
		t.Fatalf("request %+v", req)
	}
	for stream, want := range map[string]string{"public.Orders": "public_orders", "users": "people"} {
		if got, _ := req.Schemas[stream].Metadata().GetValue(tableKey); got != want {
			t.Errorf("%s's table %q, want %q", stream, got, want)
		}
	}
}

type remotePreflight struct{}

func (remotePreflight) PreflightReport(_ context.Context, req source.PreflightRequest) check.Report {
	return check.FromError(errors.New("remote " + req.Pipeline))
}

type builtInPreflight struct{}

func (builtInPreflight) Preflight(req destination.PreflightRequest) []check.Check {
	if req.Pipeline == "panics" {
		panic("listing")
	}
	return []check.Check{{Name: "stream.a", Title: "Write a", Run: func(context.Context, *check.Probe) error { return nil }}}
}

// An external connector's preflight report is its own; a built-in one's
// checks run here, a panic listing them failing the report; and a connector
// without a preflight passes.
func TestRunPreflight(t *testing.T) {
	if r := runPreflight(t.Context(), remotePreflight{}, source.PreflightRequest{Pipeline: "p"}); r.Err() == nil || r.Err().Error() != "remote p" {
		t.Errorf("an external connector's: %v", r.Err())
	}
	if r := runPreflight(t.Context(), builtInPreflight{}, destination.PreflightRequest{Pipeline: "p"}); !r.OK() || len(r.Items) != 1 || r.Items[0].Status != check.Passed {
		t.Errorf("a built-in connector's: %+v, %v", r.Items, r.Err())
	}
	if r := runPreflight(t.Context(), builtInPreflight{}, destination.PreflightRequest{Pipeline: "panics"}); r.OK() || errs.CodeOf(r.Err()) != errs.Internal {
		t.Errorf("a panic listing a built-in connector's: %v", r.Err())
	}
	if r := runPreflight(t.Context(), builtInPreflight{}, source.PreflightRequest{}); !r.OK() || len(r.Items) != 0 {
		t.Errorf("a destination's for a source's request: %v", r.Err())
	}
}
