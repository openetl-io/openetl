package activities

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"google.golang.org/protobuf/types/known/structpb"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
)

func TestBuildJobTakesColumnTypes(t *testing.T) {
	cfg, err := structpb.NewStruct(map[string]any{"count": 1})
	if err != nil {
		t.Fatal(err)
	}
	job, err := buildJob(t.Context(), &dataplanev1.GetSyncSpecResponse{
		PipelineId:  "p",
		Source:      &apiv1.ConnectorConfig{Connector: "faker", Config: cfg},
		Destination: &apiv1.ConnectorConfig{Connector: "stdout"},
		Pipeline: &apiv1.PipelineSpec{Source: &apiv1.PipelineSource{Streams: []*apiv1.PipelineStream{{
			Name: "users",
			Columns: map[string]*apiv1.PipelineColumn{
				"a": {Type: apiv1.ColumnType_COLUMN_TYPE_STRING},
				"b": {Type: apiv1.ColumnType_COLUMN_TYPE_FLOAT64},
			},
		}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	o := job.Streams[0].Overrides
	if len(o) != 2 || !arrow.TypeEqual(o["a"], arrow.BinaryTypes.String) || !arrow.TypeEqual(o["b"], arrow.PrimitiveTypes.Float64) {
		t.Errorf("overrides %v", o)
	}
}
