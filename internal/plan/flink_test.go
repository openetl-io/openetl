package plan

import (
	"strings"
	"testing"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
)

func TestFlinkTransformInputs(t *testing.T) {
	cat := &apiv1.Catalog{Streams: []*apiv1.CatalogStream{
		{Name: "payments", PrimaryKey: []string{"id"}, Replay: apiv1.Replay_REPLAY_EXACT, SyncModes: []apiv1.SyncMode{apiv1.SyncMode_SYNC_MODE_FULL_REFRESH, apiv1.SyncMode_SYNC_MODE_CDC}},
		{Name: "hooks", Replay: apiv1.Replay_REPLAY_SUPERSET, SyncModes: []apiv1.SyncMode{apiv1.SyncMode_SYNC_MODE_STREAM}},
		{Name: "orders", CursorField: "at", Replay: apiv1.Replay_REPLAY_EXACT, SyncModes: []apiv1.SyncMode{apiv1.SyncMode_SYNC_MODE_INCREMENTAL}},
	}}
	flink := &apiv1.PipelineSpec{
		Mode:      apiv1.PipelineMode_PIPELINE_MODE_CONTINUOUS,
		Delivery:  &apiv1.Delivery{Require: apiv1.DeliveryLevel_DELIVERY_LEVEL_AT_LEAST_ONCE},
		Source:    &apiv1.PipelineSource{Streams: []*apiv1.PipelineStream{{Name: "payments"}}},
		Transform: &apiv1.Transform{Engine: &apiv1.Transform_Flink{Flink: &apiv1.FlinkTransform{Outputs: []*apiv1.TransformOutput{{Name: "scored", PrimaryKey: []string{"id"}}}}}},
	}
	plans, problems, _ := Pipeline(flink, cat, warehouse)
	if len(problems) != 0 || len(plans) != 1 || plans[0].GetName() != "scored" || plans[0].GetWrite() != apiv1.WriteMode_WRITE_MODE_APPEND {
		t.Errorf("plans = %v, problems = %v", plans, problems)
	}
	flink.Source.Streams = []*apiv1.PipelineStream{{Name: "hooks"}, {Name: "orders"}}
	_, problems, _ = Pipeline(flink, cat, warehouse)
	if len(problems) != 3 || !strings.Contains(problems[0].Error(), `stream "hooks" doesn't replay exactly`) || !strings.Contains(problems[1].Error(), `stream "orders" is read with incremental`) ||
		!strings.Contains(problems[2].Error(), `stream "orders" doesn't replay exactly`) {
		t.Errorf("problems = %v", problems)
	}
}
