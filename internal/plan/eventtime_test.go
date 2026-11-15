package plan

import (
	"slices"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
)

func TestOrderedReads(t *testing.T) {
	for _, c := range []struct {
		read   apiv1.SyncMode
		cursor string
		want   bool
	}{
		{apiv1.SyncMode_SYNC_MODE_CDC, "", true},
		{apiv1.SyncMode_SYNC_MODE_STREAM, "", true},
		{apiv1.SyncMode_SYNC_MODE_INCREMENTAL, "Created_At", true},
		{apiv1.SyncMode_SYNC_MODE_INCREMENTAL, "updated_at", false},
		{apiv1.SyncMode_SYNC_MODE_FULL_REFRESH, "created_at", false},
	} {
		if got := OrderedRead(c.read, c.cursor, "created_at"); got != c.want {
			t.Errorf("%v by %q: %v", c.read, c.cursor, got)
		}
	}
}

// The plan names each stream's event time, and says how its watermark
// goes: behind its latest by the delay, or held until each read ends.
func TestThePlanNamesEachStreamsEventTime(t *testing.T) {
	cat := &apiv1.Catalog{Streams: []*apiv1.CatalogStream{
		{Name: "orders", SyncModes: []apiv1.SyncMode{apiv1.SyncMode_SYNC_MODE_INCREMENTAL}, CursorField: "created_at"},
		{Name: "users", SyncModes: []apiv1.SyncMode{apiv1.SyncMode_SYNC_MODE_FULL_REFRESH}},
		{Name: "plain"},
	}}
	spec := &apiv1.PipelineSpec{
		Source: &apiv1.PipelineSource{Streams: []*apiv1.PipelineStream{
			{Name: "orders", EventTime: "created_at", Watermark: &apiv1.Watermark{Delay: durationpb.New(5 * time.Second)}},
			{Name: "users", EventTime: "joined_at"},
			{Name: "plain"},
		}},
		Transform: &apiv1.Transform{Engine: &apiv1.Transform_Sql{Sql: &apiv1.SqlTransform{}}},
	}
	want := []string{
		"stream orders: event time created_at; each split's watermark trails the latest it has read by 5s",
		"stream users: event time joined_at; read full_refresh, not in its order, so its watermark holds until each read ends",
	}
	if got := EventTimes(spec, cat); !slices.Equal(got, want) {
		t.Errorf("%q", got)
	}
	spec.Mode = apiv1.PipelineMode_PIPELINE_MODE_CONTINUOUS
	spec.Source.Streams[0].Sync = apiv1.SyncMode_SYNC_MODE_STREAM
	if got := EventTimes(spec, cat); got[0] != want[0]+", and moves on with the clock after 1m0s with nothing to read" {
		t.Errorf("%q", got[0])
	}
}
