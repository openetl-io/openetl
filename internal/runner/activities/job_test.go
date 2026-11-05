package activities

import (
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"google.golang.org/protobuf/types/known/structpb"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/connectorhost"
	"github.com/unsuitable001/openetl/internal/runner"
)

func TestBuildJobTakesTypeSettings(t *testing.T) {
	cfg, err := structpb.NewStruct(map[string]any{"count": 1})
	if err != nil {
		t.Fatal(err)
	}
	job, err := buildJob(t.Context(), &dataplanev1.GetSyncSpecResponse{
		PipelineId:  "p",
		Source:      &apiv1.ConnectorConfig{Connector: "faker", Config: cfg},
		Destination: &apiv1.ConnectorConfig{Connector: "stdout"},
		Pipeline: &apiv1.PipelineSpec{
			Source: &apiv1.PipelineSource{Streams: []*apiv1.PipelineStream{{
				Name:       "users",
				Timestamps: apiv1.TimestampUnit_TIMESTAMP_UNIT_NANOSECOND,
				Columns: map[string]*apiv1.PipelineColumn{
					"a": {Type: apiv1.ColumnType_COLUMN_TYPE_STRING},
					"b": {Type: apiv1.ColumnType_COLUMN_TYPE_FLOAT64},
				},
			}}},
			Types: &apiv1.Types{Infinity: apiv1.Infinity_INFINITY_QUARANTINE},
		},
		Releases: []*dataplanev1.HoldRelease{{Stream: "users", Column: "a", Since: &dataplanev1.CheckpointRef{Generation: 1, Epoch: 2}}},
	}, connectorhost.Env{})
	if err != nil {
		t.Fatal(err)
	}
	o := job.Streams[0].Overrides
	if len(o) != 2 || !arrow.TypeEqual(o["a"], arrow.BinaryTypes.String) || !arrow.TypeEqual(o["b"], arrow.PrimitiveTypes.Float64) {
		t.Errorf("overrides %v", o)
	}
	if !job.QuarantineInfinity {
		t.Error("infinities aren't quarantined")
	}
	if !job.Streams[0].NanosecondTimestamps {
		t.Error("timestamps aren't in nanoseconds")
	}
	if len(job.Releases) != 1 || job.Releases[0].Column != "a" {
		t.Errorf("releases %+v", job.Releases)
	}
}

func TestStreamHoldsFromTheSpec(t *testing.T) {
	h := streamHolds(&apiv1.PipelineStream{
		Quarantine: apiv1.QuarantineScope_QUARANTINE_SCOPE_CELL,
		Held:       &apiv1.Held{Mode: apiv1.HeldMode_HELD_MODE_VALUE, Value: "0"},
		Groups:     []*apiv1.ColumnGroup{{Columns: []string{"amount", "currency"}}},
		Columns: map[string]*apiv1.PipelineColumn{
			"note":  {Held: &apiv1.Held{Mode: apiv1.HeldMode_HELD_MODE_LAST}},
			"price": {Type: apiv1.ColumnType_COLUMN_TYPE_STRING},
		},
	})
	want := &runner.StreamHolds{
		Cells:   true,
		Held:    runner.Held{Mode: runner.HeldValue, Value: "0"},
		Columns: map[string]runner.Held{"note": {Mode: runner.HeldLast}},
		Groups:  [][]string{{"amount", "currency"}},
	}
	if !reflect.DeepEqual(h, want) {
		t.Errorf("holds %+v, want %+v", h, want)
	}
	if h := streamHolds(nil); h.Cells || h.Held != (runner.Held{}) {
		t.Errorf("a stream without a spec holds %+v", h)
	}
	r := releases([]*dataplanev1.HoldRelease{{Stream: "s", Column: "c", Since: &dataplanev1.CheckpointRef{Generation: 2, Epoch: 5}, Discard: true}})
	if len(r) != 1 || r[0] != (runner.Release{Stream: "s", Column: "c", Since: checkpoint.RecordRef{Generation: 2, Epoch: 5}, Discard: true}) {
		t.Errorf("releases %+v", r)
	}
}
