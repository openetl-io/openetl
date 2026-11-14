package plan

import (
	"strings"
	"testing"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

func TestWorkers(t *testing.T) {
	lanes := destination.Capabilities{Class: destination.StageAndApply, WriterLanes: 8,
		WriteModes: []destination.WriteMode{destination.Append, destination.Merge, destination.Overwrite}}
	held := lanes
	held.Holds = destination.Holds{ByKey: true, Guarded: true}
	keyed := func(name string, splitsByKey bool, modes ...apiv1.SyncMode) *apiv1.CatalogStream {
		return &apiv1.CatalogStream{Name: name, PrimaryKey: []string{"id"}, SyncModes: modes, SplitsByKey: splitsByKey}
	}
	cat := &apiv1.Catalog{Streams: []*apiv1.CatalogStream{
		{Name: "logs", SyncModes: []apiv1.SyncMode{apiv1.SyncMode_SYNC_MODE_INCREMENTAL}},
		keyed("ranged", true, apiv1.SyncMode_SYNC_MODE_INCREMENTAL),
		keyed("hashed", false, apiv1.SyncMode_SYNC_MODE_INCREMENTAL),
		keyed("changes", true, apiv1.SyncMode_SYNC_MODE_CDC),
		{Name: "snapshot", SyncModes: []apiv1.SyncMode{apiv1.SyncMode_SYNC_MODE_FULL_REFRESH}},
	}}
	spec := func(workers uint32, extra func(*apiv1.PipelineSpec)) *apiv1.PipelineSpec {
		p := &apiv1.PipelineSpec{Resources: &apiv1.JobResources{Workers: workers}}
		if extra != nil {
			extra(p)
		}
		return p
	}
	for _, tc := range []struct {
		name      string
		spec      *apiv1.PipelineSpec
		cat       *apiv1.Catalog
		caps      destination.Capabilities
		engine    bool
		workers   int
		onePod    string
		oneWorker []string
	}{
		{name: "one asked", spec: spec(1, nil), cat: cat, caps: lanes},
		{name: "spread", spec: spec(4, nil), cat: cat, caps: lanes, workers: 4, oneWorker: []string{
			`hashed: each attempt reads it on one of its 4 workers: the source's splits don't partition it by key, and the rows of each key must meet on one writer lane`,
			`changes: each attempt reads it on one of its 4 workers: the changes to each key must reach one writer lane in order`,
			`snapshot: each attempt reads it on one of its 4 workers: an overwrite's read ends on one worker`,
		}},
		{name: "no catalog", spec: spec(4, nil), caps: lanes, workers: 4},
		{name: "one writer lane", spec: spec(2, nil), cat: cat, caps: destination.Capabilities{Class: destination.StageAndApply},
			onePod: "the destination declares one writer lane"},
		{name: "fewer writer lanes", spec: spec(16, nil), cat: cat, caps: lanes, onePod: "the destination takes 8 writer lanes, fewer than 16 workers"},
		{name: "transactional", spec: spec(2, nil), cat: cat, caps: destination.Capabilities{Class: destination.TransactionalStream, WriterLanes: 4},
			onePod: "a transactional destination's writer lanes commit on their own, which only one pod recovers"},
		{name: "staged", spec: spec(2, nil), cat: cat, caps: destination.Capabilities{Class: destination.StageAndApply, WriterLanes: 4, Delivery: destination.DeliveryStaged},
			onePod: "a destination that loads staged epochs runs on one pod"},
		{name: "engine written", spec: spec(2, nil), cat: cat, caps: lanes, engine: true, onePod: "a destination the engine writes runs on one pod"},
		{name: "holds", spec: spec(2, nil), cat: cat, caps: held, onePod: `stream "changes" holds columns, which runs on one pod`},
		{name: "sql", spec: spec(2, func(p *apiv1.PipelineSpec) {
			p.Transform = &apiv1.Transform{Engine: &apiv1.Transform_Sql{Sql: &apiv1.SqlTransform{Sql: "SELECT 1"}}}
		}), cat: cat, caps: lanes,
			onePod: "a SQL transform runs on one pod"},
		{name: "spark", spec: spec(2, func(p *apiv1.PipelineSpec) {
			p.Transform = &apiv1.Transform{Engine: &apiv1.Transform_Spark{Spark: &apiv1.SparkTransform{}}}
		}), cat: cat, caps: lanes,
			onePod: "a Spark or Flink transform runs on one pod"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			workers, onePod, oneWorker := Workers(tc.spec, tc.cat, tc.caps, tc.engine)
			var got []string
			for _, w := range oneWorker {
				got = append(got, w.GetStream()+": "+w.GetMessage())
			}
			if workers != tc.workers || onePod != tc.onePod || strings.Join(got, "\n") != strings.Join(tc.oneWorker, "\n") {
				t.Errorf("got %d workers, one pod %q and\n%s\nwant %d, %q and\n%s", workers, onePod, strings.Join(got, "\n"), tc.workers, tc.onePod, strings.Join(tc.oneWorker, "\n"))
			}
		})
	}
}
