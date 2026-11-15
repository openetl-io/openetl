package activities

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/sdk/go/destination"
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
