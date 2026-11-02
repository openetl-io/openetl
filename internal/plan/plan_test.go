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
		Scopes:     []destination.Scope{destination.ScopePipeline, destination.ScopeStream},
		WriteModes: []destination.WriteMode{destination.Append, destination.Overwrite, destination.Merge},
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
	if s := Settings(&apiv1.PipelineSpec{Delivery: d}, warehouse); s.Require != delivery.AtLeastOnce || s.Scope != destination.ScopeStream || s.CommitInterval != time.Minute {
		t.Errorf("settings %+v", s)
	}
	continuous := &apiv1.PipelineSpec{Mode: apiv1.PipelineMode_PIPELINE_MODE_CONTINUOUS}
	if s := Settings(continuous, warehouse); s.CommitInterval != 10*time.Second {
		t.Errorf("a continuous pipeline commits every %v by default", s.CommitInterval)
	}
	continuous.Delivery = d
	if s := Settings(continuous, warehouse); s.CommitInterval != time.Minute {
		t.Errorf("a continuous pipeline's own interval: %v", s.CommitInterval)
	}
}

var catalog = &apiv1.Catalog{Streams: []*apiv1.CatalogStream{
	{Name: "orders", PrimaryKey: []string{"id"}, Replay: apiv1.Replay_REPLAY_EXACT, Columns: []*apiv1.CatalogColumn{
		{Name: "id", Type: "int64"}, {Name: "total", Type: "decimal(38, 9)"},
	}, SyncModes: []apiv1.SyncMode{apiv1.SyncMode_SYNC_MODE_FULL_REFRESH}},
	{Name: "events", PrimaryKey: []string{"id"}, Replay: apiv1.Replay_REPLAY_SUPERSET, SyncModes: []apiv1.SyncMode{apiv1.SyncMode_SYNC_MODE_INCREMENTAL}},
	{Name: "logs", Replay: apiv1.Replay_REPLAY_SUPERSET},
}}

var withTopic = &apiv1.Catalog{Streams: append(catalog.GetStreams(),
	&apiv1.CatalogStream{Name: "topic", Replay: apiv1.Replay_REPLAY_EXACT, SyncModes: []apiv1.SyncMode{apiv1.SyncMode_SYNC_MODE_STREAM}})}

func TestPipeline(t *testing.T) {
	for _, tc := range []struct {
		name     string
		spec     string
		cat      *apiv1.Catalog
		caps     destination.Capabilities
		streams  string
		problems []string
		warnings []string
	}{
		{name: "no catalog", caps: parquet, spec: `{"delivery": {"commit": {"scope": "COMMIT_SCOPE_PIPELINE"}}}`,
			problems: []string{"delivery.commit.scope: the destination doesn't support pipeline"}},
		{name: "every stream", cat: catalog, caps: warehouse,
			streams:  "orders exactly_once exact replay; events exactly_once+dedupe superset replay, deduped at apply; logs exactly_once+dedupe+collapse superset replay, no primary key: collapsed within each epoch and against the target",
			warnings: []string{"logs: it has no primary key, so rows with the same content collapse into one, within each epoch and against the target"}},
		{name: "stream settings", cat: catalog, caps: warehouse,
			spec:     `{"source": {"streams": [{"name": "events", "require": "DELIVERY_LEVEL_AT_LEAST_ONCE", "collapse": true}, {"name": "logs", "require": "DELIVERY_LEVEL_AT_LEAST_ONCE", "collapse": true}]}}`,
			streams:  "events at_least_once superset replay, not deduped under at_least_once; logs at_least_once+dedupe+collapse superset replay, no primary key: collapsed within each epoch",
			warnings: []string{"events: it has a primary key, so collapse is ignored", "logs: it has no primary key, so rows with the same content collapse into one within each epoch only"}},
		{name: "no collapse", cat: catalog, caps: warehouse, spec: `{"source": {"streams": [{"name": "logs", "collapse": false}]}}`,
			streams:  "logs at_least_once superset replay, no primary key, and not collapsed",
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
		{name: "merge into parquet", cat: catalog, caps: parquet, spec: `{"destination": {"write": "WRITE_MODE_MERGE"}, "source": {"streams": [{"name": "orders"}]}}`,
			streams: "orders exactly_once exact replay", problems: []string{`destination.write: the destination doesn't support merge`}},
		{name: "merge into parquet, no catalog", caps: parquet, spec: `{"destination": {"write": "WRITE_MODE_MERGE"}}`,
			problems: []string{`destination.write: the destination doesn't support merge`}},
		{name: "overwrite an incremental stream", cat: catalog, caps: warehouse,
			spec:     `{"destination": {"write": "WRITE_MODE_OVERWRITE"}, "source": {"streams": [{"name": "orders", "sync": "SYNC_MODE_INCREMENTAL"}, {"name": "events", "sync": "SYNC_MODE_FULL_REFRESH"}]}}`,
			streams:  "orders exactly_once exact replay; events exactly_once+dedupe superset replay, deduped at apply",
			problems: []string{`destination.write: overwrite replaces each table with what one read wrote, so it needs full_refresh, and stream "orders" is read with incremental`}},
		{name: "overwrite a stream the source reads incrementally", cat: catalog, caps: warehouse,
			spec:     `{"destination": {"write": "WRITE_MODE_OVERWRITE"}, "source": {"streams": [{"name": "events"}]}}`,
			streams:  "events exactly_once+dedupe superset replay, deduped at apply",
			problems: []string{`destination.write: overwrite replaces each table with what one read wrote, so it needs full_refresh, and stream "events" is read with incremental`}},
		{name: "continuous", cat: withTopic, caps: warehouse,
			spec:    `{"mode": "PIPELINE_MODE_CONTINUOUS", "source": {"streams": [{"name": "topic"}, {"name": "orders", "sync": "SYNC_MODE_CDC"}]}}`,
			streams: "topic exactly_once exact replay; orders exactly_once exact replay"},
		{name: "continuous with reads that end", cat: catalog, caps: warehouse,
			spec:    `{"mode": "PIPELINE_MODE_CONTINUOUS", "source": {"streams": [{"name": "orders"}, {"name": "events"}, {"name": "logs"}]}}`,
			streams: "orders exactly_once exact replay; events exactly_once+dedupe superset replay, deduped at apply; logs exactly_once+dedupe+collapse superset replay, no primary key: collapsed within each epoch and against the target",
			problems: []string{
				`stream "orders" is read with full_refresh, whose reads end, and a continuous pipeline needs streams read with cdc or stream`,
				`stream "events" is read with incremental, whose reads end`,
				`stream "logs": the source doesn't say how it reads it`,
			},
			warnings: []string{"logs: it has no primary key"}},
		{name: "continuous, no catalog", caps: warehouse,
			spec:     `{"mode": "PIPELINE_MODE_CONTINUOUS", "source": {"streams": [{"name": "a", "sync": "SYNC_MODE_INCREMENTAL"}, {"name": "b"}]}}`,
			problems: []string{`stream "a" is read with incremental, whose reads end`}},
		{name: "deletes without merge", cat: catalog, caps: warehouse, spec: `{"destination": {"deletes": "DELETE_MODE_HARD"}, "source": {"streams": [{"name": "orders"}]}}`,
			streams: "orders exactly_once exact replay", warnings: []string{": destination.deletes applies only to merge"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := &apiv1.PipelineSpec{}
			if tc.spec != "" {
				if err := protojsonUnmarshal(tc.spec, spec); err != nil {
					t.Fatal(err)
				}
			}
			streams, problems, warnings := Pipeline(spec, tc.cat, tc.caps)
			var got []string
			for _, s := range streams {
				level := strings.ToLower(strings.TrimPrefix(s.GetLevel().String(), "DELIVERY_LEVEL_"))
				if s.GetDedupe() {
					level += "+dedupe"
				}
				if s.GetCollapse() {
					level += "+collapse"
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
			if len(warnings) != len(tc.warnings) {
				t.Fatalf("warnings %v, want %v", warnings, tc.warnings)
			}
			for i, w := range warnings {
				if got := w.GetStream() + ": " + w.GetMessage(); !strings.HasPrefix(got, tc.warnings[i]) {
					t.Errorf("warning %d: %q, want %q", i, got, tc.warnings[i])
				}
			}
		})
	}
}

// Without destination.write, a full refresh overwrites where the destination
// can, and everything else appends.
func TestStreamModes(t *testing.T) {
	for _, tc := range []struct {
		name, spec string
		caps       destination.Capabilities
		want       string
	}{
		{"default", `{}`, warehouse, "orders overwrite, events append, logs append"},
		{"sync in the spec", `{"source": {"streams": [{"name": "orders", "sync": "SYNC_MODE_INCREMENTAL"}, {"name": "logs", "sync": "SYNC_MODE_FULL_REFRESH"}]}}`, warehouse, "orders append, logs overwrite"},
		{"no overwrite", `{}`, parquet, "orders append, events append, logs append"},
		{"set", `{"destination": {"write": "WRITE_MODE_APPEND"}}`, warehouse, "orders append, events append, logs append"},
	} {
		spec := &apiv1.PipelineSpec{}
		if err := protojsonUnmarshal(tc.spec, spec); err != nil {
			t.Fatal(err)
		}
		streams, _, _ := Pipeline(spec, catalog, tc.caps)
		var got []string
		for _, s := range streams {
			got = append(got, s.GetName()+" "+strings.ToLower(strings.TrimPrefix(s.GetWrite().String(), "WRITE_MODE_")))
		}
		if g := strings.Join(got, ", "); g != tc.want {
			t.Errorf("%s: %s, want %s", tc.name, g, tc.want)
		}
	}
}

// A table read as a full refresh or with cdc, the way Postgres declares
// one with a replica identity, and one without a key.
var tables = &apiv1.Catalog{Streams: []*apiv1.CatalogStream{
	{Name: "users", PrimaryKey: []string{"id"}, Replay: apiv1.Replay_REPLAY_EXACT, SyncModes: []apiv1.SyncMode{apiv1.SyncMode_SYNC_MODE_FULL_REFRESH, apiv1.SyncMode_SYNC_MODE_CDC}},
	{Name: "audit", Replay: apiv1.Replay_REPLAY_EXACT, SyncModes: []apiv1.SyncMode{apiv1.SyncMode_SYNC_MODE_FULL_REFRESH, apiv1.SyncMode_SYNC_MODE_CDC}},
}}

func TestCDCStreams(t *testing.T) {
	for _, tc := range []struct {
		name, spec string
		caps       destination.Capabilities
		want       string
		warnings   int
	}{
		{"scheduled reads the first way", `{}`, warehouse, "users overwrite, audit overwrite", 0},
		{"continuous reads with cdc, and merges keyed tables", `{"mode": "PIPELINE_MODE_CONTINUOUS"}`, warehouse, "users merge, audit append", 0},
		{"cdc in a scheduled pipeline", `{"source": {"streams": [{"name": "users", "sync": "SYNC_MODE_CDC"}]}}`, warehouse, "users merge", 0},
		{"no merge", `{"mode": "PIPELINE_MODE_CONTINUOUS"}`, parquet, "users append, audit append", 0},
		{"deletes apply to the merge", `{"mode": "PIPELINE_MODE_CONTINUOUS", "destination": {"deletes": "DELETE_MODE_HARD"}}`, warehouse, "users merge, audit append", 0},
		{"deletes without a merge", `{"destination": {"deletes": "DELETE_MODE_HARD"}}`, warehouse, "users overwrite, audit overwrite", 1},
	} {
		spec := &apiv1.PipelineSpec{}
		if err := protojsonUnmarshal(tc.spec, spec); err != nil {
			t.Fatal(err)
		}
		streams, problems, warnings := Pipeline(spec, tables, tc.caps)
		if len(problems) > 0 {
			t.Errorf("%s: %v", tc.name, problems)
		}
		var got []string
		for _, s := range streams {
			got = append(got, s.GetName()+" "+strings.ToLower(strings.TrimPrefix(s.GetWrite().String(), "WRITE_MODE_")))
		}
		if g := strings.Join(got, ", "); g != tc.want {
			t.Errorf("%s: %s, want %s", tc.name, g, tc.want)
		}
		if len(warnings) != tc.warnings {
			t.Errorf("%s: warnings %v", tc.name, warnings)
		}
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
