package plan

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/internal/delivery"
	"github.com/unsuitable001/openetl/sdk/go/destination"
)

var (
	warehouse = destination.Capabilities{
		Class: destination.StageAndApply, DedupeAtApply: true, MinCommitInterval: time.Second,
		Scopes: []destination.Scope{destination.ScopePipeline, destination.ScopeStream},
	}
	parquet = destination.Capabilities{Class: destination.StageAndApply, Scopes: []destination.Scope{destination.ScopeStream}, NanosecondTimestamps: true}
)

func TestSettings(t *testing.T) {
	if s := Settings(nil, warehouse); s.Require != delivery.ExactlyOnce || s.Scope != destination.ScopePipeline || s.CommitInterval != 5*time.Minute {
		t.Errorf("defaults into a warehouse: %+v", s)
	}
	if s := Settings(nil, parquet); s.Scope != destination.ScopeStream {
		t.Errorf("defaults into parquet: %+v", s)
	}
	d := &apiv1.Delivery{Require: apiv1.DeliveryLevel_DELIVERY_LEVEL_AT_LEAST_ONCE, Commit: &apiv1.Commit{Interval: durationpb.New(time.Minute), Scope: apiv1.CommitScope_COMMIT_SCOPE_STREAM}}
	if s := Settings(d, warehouse); s.Require != delivery.AtLeastOnce || s.Scope != destination.ScopeStream || s.CommitInterval != time.Minute {
		t.Errorf("settings %+v", s)
	}
}

var catalog = &apiv1.Catalog{Streams: []*apiv1.CatalogStream{
	{Name: "orders", PrimaryKey: []string{"id"}, Replay: apiv1.Replay_REPLAY_EXACT, Columns: []*apiv1.CatalogColumn{
		{Name: "id", Type: "int64"}, {Name: "total", Type: "decimal(38, 9)"},
	}},
	{Name: "events", PrimaryKey: []string{"id"}, Replay: apiv1.Replay_REPLAY_SUPERSET},
	{Name: "logs", Replay: apiv1.Replay_REPLAY_SUPERSET},
}}

func TestPipeline(t *testing.T) {
	for _, tc := range []struct {
		name     string
		spec     string
		cat      *apiv1.Catalog
		caps     destination.Capabilities
		streams  string
		problems []string
	}{
		{name: "no catalog", caps: parquet, spec: `{"delivery": {"commit": {"scope": "COMMIT_SCOPE_PIPELINE"}}}`,
			problems: []string{"delivery.commit.scope: the destination doesn't support pipeline"}},
		{name: "every stream", cat: catalog, caps: warehouse,
			streams:  "orders exactly_once exact replay; events exactly_once+dedupe superset replay, deduped at apply; logs at_least_once superset replay, and no primary key to dedupe on",
			problems: []string{`delivery.require: stream "logs" gets at_least_once`}},
		{name: "named streams", cat: catalog, caps: warehouse, spec: `{"source": {"streams": [{"name": "orders"}, {"name": "gone"}]}}`,
			streams: "orders exactly_once exact replay", problems: []string{`the source has no stream "gone"`}},
		{name: "at least once", cat: catalog, caps: parquet, spec: `{"delivery": {"require": "DELIVERY_LEVEL_AT_LEAST_ONCE"}}`,
			streams: "orders exactly_once exact replay; events at_least_once superset replay, and the destination can't dedupe; logs at_least_once superset replay, and the destination can't dedupe"},
		{name: "overrides", cat: catalog, caps: warehouse,
			spec:    `{"source": {"streams": [{"name": "orders", "columns": {"total": {"type": "COLUMN_TYPE_FLOAT64"}, "id": {"type": "COLUMN_TYPE_STRING"}}}]}}`,
			streams: "orders exactly_once exact replay", problems: []string{`stream "orders": column "id" has a type override, but the source doesn't declare it a decimal`}},
		{name: "nanoseconds", cat: catalog, caps: warehouse, spec: `{"source": {"streams": [{"name": "orders", "timestamps": "TIMESTAMP_UNIT_NANOSECOND"}]}}`,
			streams: "orders exactly_once exact replay", problems: []string{`stream "orders" keeps nanosecond timestamps, but the destination keeps only microseconds`}},
		{name: "merge without a key", cat: catalog, caps: warehouse, spec: `{"destination": {"write": "WRITE_MODE_MERGE"}, "source": {"streams": [{"name": "logs"}]}}`,
			streams: "logs effectively_once superset replay, written with merge", problems: []string{`destination.write: merge needs a primary key, and stream "logs" has none`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := &apiv1.PipelineSpec{}
			if tc.spec != "" {
				if err := protojsonUnmarshal(tc.spec, spec); err != nil {
					t.Fatal(err)
				}
			}
			streams, problems := Pipeline(spec, tc.cat, tc.caps)
			var got []string
			for _, s := range streams {
				level := strings.ToLower(strings.TrimPrefix(s.GetLevel().String(), "DELIVERY_LEVEL_"))
				if s.GetDedupe() {
					level += "+dedupe"
				}
				got = append(got, s.GetName()+" "+level+" "+s.GetReason())
			}
			if g := strings.Join(got, "; "); g != tc.streams {
				t.Errorf("streams\n%s\nwant\n%s", g, tc.streams)
			}
			if len(problems) != len(tc.problems) {
				t.Fatalf("problems %v, want %v", problems, tc.problems)
			}
			for i, p := range problems {
				if !strings.HasPrefix(p.Error(), tc.problems[i]) {
					t.Errorf("problem %d: %q, want %q", i, p, tc.problems[i])
				}
			}
		})
	}
}

func TestDiff(t *testing.T) {
	before := Version{Source: "s1", Destination: "d1", DestinationConnector: "sha256:a", Dbt: map[string]string{"shop": "sha256:b1", "gone": "sha256:g"}, Spec: &apiv1.PipelineSpec{}}
	after := Version{Source: "s2", Destination: "d1", DestinationConnector: "sha256:a2", Dbt: map[string]string{"shop": "sha256:b2"}, Spec: &apiv1.PipelineSpec{}}
	if err := protojsonUnmarshal(`{"source": {"ref": "s", "streams": [{"name": "orders", "cursor": "updated_at"}, {"name": "old"}]}}`, before.Spec); err != nil {
		t.Fatal(err)
	}
	if err := protojsonUnmarshal(`{"source": {"ref": "s", "streams": [{"name": "orders", "cursor": "id"}]}, "delivery": {"require": "DELIVERY_LEVEL_AT_LEAST_ONCE"}}`, after.Spec); err != nil {
		t.Fatal(err)
	}
	changes, err := Diff(before, after)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, c := range changes {
		got = append(got, c.GetField()+": "+c.GetBefore()+" -> "+c.GetAfter())
	}
	want := []string{
		`dbt.gone: "sha256:g" -> `,
		`dbt.shop: "sha256:b1" -> "sha256:b2"`,
		`delivery.require:  -> "DELIVERY_LEVEL_AT_LEAST_ONCE"`,
		`destination.connector: "sha256:a" -> "sha256:a2"`,
		`source.streams[old].name: "old" -> `,
		`source.streams[orders].cursor: "updated_at" -> "id"`,
		`source.version: "s1" -> "s2"`,
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("changes\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if same, err := Diff(after, after); err != nil || len(same) != 0 {
		t.Errorf("no change: %v, %v", same, err)
	}
}

func protojsonUnmarshal(s string, m *apiv1.PipelineSpec) error {
	return protojson.Unmarshal([]byte(s), m)
}
