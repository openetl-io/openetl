package plan

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/delivery"
	"github.com/openetl-io/openetl/sdk/go/destination"
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
	iceberg := warehouse
	iceberg.DefaultCommitInterval = 2 * time.Minute
	if s := Settings(continuous, iceberg); s.CommitInterval != 2*time.Minute {
		t.Errorf("a continuous pipeline commits every %v by default, not at its destination's default", s.CommitInterval)
	}
	if s := Settings(nil, iceberg); s.CommitInterval != 5*time.Minute {
		t.Errorf("a scheduled pipeline commits every %v by default, not every 5 min", s.CommitInterval)
	}
	continuous.Delivery = d
	if s := Settings(continuous, iceberg); s.CommitInterval != time.Minute {
		t.Errorf("a continuous pipeline's own interval: %v", s.CommitInterval)
	}
	if s := Settings(continuous, warehouse); s.CommitInterval != time.Minute {
		t.Errorf("a continuous pipeline's own interval: %v", s.CommitInterval)
	}
}

var catalog = &apiv1.Catalog{Streams: []*apiv1.CatalogStream{
	{Name: "orders", PrimaryKey: []string{"id"}, Replay: apiv1.Replay_REPLAY_EXACT, Columns: []*apiv1.CatalogColumn{
		{Name: "id", Type: "int64"}, {Name: "total", Type: "decimal(38, 9)"},
	}, SyncModes: []apiv1.SyncMode{apiv1.SyncMode_SYNC_MODE_FULL_REFRESH}},
	{Name: "events", PrimaryKey: []string{"id"}, CursorField: "at", Replay: apiv1.Replay_REPLAY_SUPERSET, SyncModes: []apiv1.SyncMode{apiv1.SyncMode_SYNC_MODE_INCREMENTAL}},
	{Name: "logs", Replay: apiv1.Replay_REPLAY_SUPERSET},
}}

var withTopic = &apiv1.Catalog{Streams: append(catalog.GetStreams(),
	&apiv1.CatalogStream{Name: "topic", Replay: apiv1.Replay_REPLAY_EXACT, SyncModes: []apiv1.SyncMode{apiv1.SyncMode_SYNC_MODE_STREAM}})}

var keyedTopic = &apiv1.Catalog{Streams: []*apiv1.CatalogStream{
	{Name: "topic", PrimaryKey: []string{"id"}, Replay: apiv1.Replay_REPLAY_EXACT, SyncModes: []apiv1.SyncMode{apiv1.SyncMode_SYNC_MODE_STREAM}},
}}

func TestCursorProblem(t *testing.T) {
	for _, tc := range []struct {
		read   apiv1.SyncMode
		keyed  bool
		cursor string
		fails  bool
	}{
		{read: apiv1.SyncMode_SYNC_MODE_INCREMENTAL, fails: true},
		{read: apiv1.SyncMode_SYNC_MODE_INCREMENTAL, cursor: "at"},
		{read: apiv1.SyncMode_SYNC_MODE_STREAM, keyed: true, fails: true},
		{read: apiv1.SyncMode_SYNC_MODE_STREAM, keyed: true, cursor: "offset"},
		{read: apiv1.SyncMode_SYNC_MODE_STREAM},
		// A changelog's batches carry their own sequence.
		{read: apiv1.SyncMode_SYNC_MODE_CDC, keyed: true},
		{read: apiv1.SyncMode_SYNC_MODE_FULL_REFRESH, keyed: true},
		{read: apiv1.SyncMode_SYNC_MODE_UNSPECIFIED, keyed: true},
	} {
		if err := CursorProblem("s", tc.read, tc.keyed, tc.cursor); (err != nil) != tc.fails {
			t.Errorf("%+v: %v", tc, err)
		}
	}
}

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
			spec:     `{"destination": {"write": "WRITE_MODE_OVERWRITE"}, "source": {"streams": [{"name": "orders", "sync": "SYNC_MODE_INCREMENTAL", "cursor": "id"}, {"name": "events", "sync": "SYNC_MODE_FULL_REFRESH"}]}}`,
			streams:  "orders exactly_once+dedupe superset replay, deduped at apply; events exactly_once+dedupe superset replay, deduped at apply",
			problems: []string{`destination.write: overwrite replaces each table with what one read wrote, so it needs full_refresh, and stream "orders" is read with incremental`}},
		{name: "overwrite a stream the source reads incrementally", cat: catalog, caps: warehouse,
			spec:     `{"destination": {"write": "WRITE_MODE_OVERWRITE"}, "source": {"streams": [{"name": "events"}]}}`,
			streams:  "events exactly_once+dedupe superset replay, deduped at apply",
			problems: []string{`destination.write: overwrite replaces each table with what one read wrote, so it needs full_refresh, and stream "events" is read with incremental`}},
		{name: "incremental without a cursor", cat: catalog, caps: warehouse, spec: `{"source": {"streams": [{"name": "orders", "sync": "SYNC_MODE_INCREMENTAL"}]}}`,
			streams: "orders exactly_once+dedupe superset replay, deduped at apply", problems: []string{`stream "orders" is read with incremental, which needs a cursor`}},
		{name: "incremental with the spec's cursor", cat: catalog, caps: parquet, spec: `{"source": {"streams": [{"name": "orders", "sync": "SYNC_MODE_INCREMENTAL", "cursor": "id"}]}}`,
			streams: "orders at_least_once superset replay, and the destination can't dedupe", problems: []string{`delivery.require: stream "orders" gets at_least_once`}},
		{name: "stream read, keyed, without a cursor", cat: keyedTopic, caps: warehouse,
			spec:     `{"mode": "PIPELINE_MODE_CONTINUOUS", "source": {"streams": [{"name": "topic"}]}}`,
			streams:  "topic exactly_once exact replay",
			problems: []string{`stream "topic" has a primary key and is read with stream, which needs a cursor`}},
		{name: "stream read, keyed, with the spec's cursor", cat: keyedTopic, caps: warehouse,
			spec:    `{"mode": "PIPELINE_MODE_CONTINUOUS", "source": {"streams": [{"name": "topic", "cursor": "offset"}]}}`,
			streams: "topic exactly_once exact replay"},
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
		{name: "type changes", cat: catalog, caps: warehouse,
			spec:     `{"destination": {"typeChanges": "TYPE_CHANGES_ALLOW"}, "source": {"streams": [{"name": "orders"}, {"name": "events", "typeChanges": "TYPE_CHANGES_REFUSE"}]}}`,
			streams:  "orders exactly_once+types exact replay; events exactly_once+dedupe superset replay, deduped at apply",
			warnings: []string{": typeChanges: allow lets the destination change a column's type itself as the source's widens: int16 to int32"}},
		{name: "type changes on a stream", cat: catalog, caps: warehouse, spec: `{"source": {"streams": [{"name": "orders", "typeChanges": "TYPE_CHANGES_ALLOW"}, {"name": "logs"}]}}`,
			streams:  "orders exactly_once+types exact replay; logs exactly_once+dedupe+collapse superset replay, no primary key: collapsed within each epoch and against the target",
			warnings: []string{": typeChanges: allow lets the destination", "logs: it has no primary key"}},
		{name: "type changes every stream refuses", cat: catalog, caps: warehouse,
			spec:    `{"destination": {"typeChanges": "TYPE_CHANGES_ALLOW"}, "source": {"streams": [{"name": "orders", "typeChanges": "TYPE_CHANGES_REFUSE"}]}}`,
			streams: "orders exactly_once exact replay"},
		{name: "type changes under block", cat: catalog, caps: warehouse,
			spec:     `{"schemaPolicy": "SCHEMA_POLICY_BLOCK", "destination": {"typeChanges": "TYPE_CHANGES_ALLOW"}, "source": {"streams": [{"name": "orders"}]}}`,
			streams:  "orders exactly_once+types exact replay",
			warnings: []string{": typeChanges: allow changes nothing, as schemaPolicy block holds every type change"}},
		{name: "type changes, no catalog", caps: warehouse, spec: `{"schemaPolicy": "SCHEMA_POLICY_IGNORE_NEW", "destination": {"typeChanges": "TYPE_CHANGES_ALLOW"}}`,
			warnings: []string{": typeChanges: allow changes nothing, as schemaPolicy ignore_new casts"}},
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
				if s.GetTypeChanges() {
					level += "+types"
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
	if err := protojsonUnmarshal(`{"source": {"ref": "s", "streams": [{"name": "orders", "cursor": "id", "sync": "SYNC_MODE_INCREMENTAL", "columns": {"amount": {"type": "COLUMN_TYPE_FLOAT64"}}}]}, "delivery": {"require": "DELIVERY_LEVEL_AT_LEAST_ONCE"}, "schedule": {"every": "60s"}}`, after.Spec); err != nil {
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
		`delivery.require:  -> "at_least_once"`,
		`destination.connector: "sha256:a" -> "sha256:a2"`,
		`schedule.every:  -> "60s"`,
		`source.streams[old].name: "old" -> `,
		`source.streams[orders].columns.amount.type:  -> "float64"`,
		`source.streams[orders].cursor: "updated_at" -> "id"`,
		`source.streams[orders].sync:  -> "incremental"`,
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

func TestTransformOutputs(t *testing.T) {
	cat := &apiv1.Catalog{Streams: []*apiv1.CatalogStream{
		{Name: "orders", PrimaryKey: []string{"id"}, Replay: apiv1.Replay_REPLAY_EXACT, SyncModes: []apiv1.SyncMode{apiv1.SyncMode_SYNC_MODE_FULL_REFRESH}},
		{Name: "events", CursorField: "at", Replay: apiv1.Replay_REPLAY_SUPERSET, SyncModes: []apiv1.SyncMode{apiv1.SyncMode_SYNC_MODE_INCREMENTAL}},
	}}
	spark := func(outputs ...*apiv1.TransformOutput) *apiv1.PipelineSpec {
		return &apiv1.PipelineSpec{
			Delivery:  &apiv1.Delivery{Require: apiv1.DeliveryLevel_DELIVERY_LEVEL_AT_LEAST_ONCE},
			Transform: &apiv1.Transform{Engine: &apiv1.Transform_Spark{Spark: &apiv1.SparkTransform{Outputs: outputs}}},
		}
	}
	// Unset: each input, with its settings; one input is superset and
	// incremental, so nothing is overwritten or exact.
	plans, problems, _ := Pipeline(spark(), cat, warehouse)
	if len(problems) != 0 || len(plans) != 2 || plans[0].GetName() != "orders" || plans[0].GetWrite() != apiv1.WriteMode_WRITE_MODE_APPEND ||
		plans[0].GetLevel() != apiv1.DeliveryLevel_DELIVERY_LEVEL_AT_LEAST_ONCE || plans[0].GetDedupe() || plans[1].GetName() != "events" {
		t.Errorf("plans = %v, problems = %v", plans, problems)
	}
	merged := spark(&apiv1.TransformOutput{Name: "enriched", PrimaryKey: []string{"order_id"}, Write: apiv1.WriteMode_WRITE_MODE_MERGE, Require: apiv1.DeliveryLevel_DELIVERY_LEVEL_EFFECTIVELY_ONCE})
	plans, problems, warnings := Pipeline(merged, cat, warehouse)
	if len(problems) != 0 || len(plans) != 1 || plans[0].GetLevel() != apiv1.DeliveryLevel_DELIVERY_LEVEL_EFFECTIVELY_ONCE || plans[0].GetTypeChanges() || len(warnings) != 0 {
		t.Errorf("plans = %v, problems = %v, warnings = %v", plans, problems, warnings)
	}
	// Outputs take the destination's typeChanges.
	merged.Destination = &apiv1.PipelineDestination{TypeChanges: apiv1.TypeChanges_TYPE_CHANGES_ALLOW}
	plans, _, warnings = Pipeline(merged, cat, warehouse)
	if !plans[0].GetTypeChanges() || len(warnings) != 1 || !strings.Contains(warnings[0].GetMessage(), "typeChanges: allow lets") {
		t.Errorf("plans = %v, warnings = %v", plans, warnings)
	}
	exactly := spark(&apiv1.TransformOutput{Name: "enriched", Require: apiv1.DeliveryLevel_DELIVERY_LEVEL_EXACTLY_ONCE})
	if _, problems, _ = Pipeline(exactly, cat, warehouse); len(problems) != 1 || !strings.Contains(problems[0].Error(), "transform.spark.outputs[enriched].require") {
		t.Errorf("problems = %v", problems)
	}
	over := spark(&apiv1.TransformOutput{Name: "orders", Write: apiv1.WriteMode_WRITE_MODE_OVERWRITE})
	if _, problems, _ = Pipeline(over, cat, warehouse); len(problems) != 1 || !strings.Contains(problems[0].Error(), `stream "events" is read with incremental`) {
		t.Errorf("problems = %v", problems)
	}
	unkeyed := spark(&apiv1.TransformOutput{Name: "totals", Write: apiv1.WriteMode_WRITE_MODE_MERGE})
	if _, problems, _ = Pipeline(unkeyed, cat, warehouse); len(problems) != 1 || !strings.Contains(problems[0].Error(), "transform.spark.outputs[totals].write") {
		t.Errorf("problems = %v", problems)
	}

	// Every input full_refresh and exact: outputs are overwritten, exactly once.
	refresh := &apiv1.Catalog{Streams: cat.GetStreams()[:1]}
	exactly.Delivery = nil
	plans, problems, _ = Pipeline(exactly, refresh, warehouse)
	if len(problems) != 0 || plans[0].GetWrite() != apiv1.WriteMode_WRITE_MODE_OVERWRITE || plans[0].GetLevel() != apiv1.DeliveryLevel_DELIVERY_LEVEL_EXACTLY_ONCE {
		t.Errorf("plans = %v, problems = %v", plans, problems)
	}
	cdc := &apiv1.Catalog{Streams: []*apiv1.CatalogStream{{Name: "orders", PrimaryKey: []string{"id"}, Replay: apiv1.Replay_REPLAY_EXACT, SyncModes: []apiv1.SyncMode{apiv1.SyncMode_SYNC_MODE_CDC}}}}
	if plans, _, _ = Pipeline(spark(), cdc, warehouse); plans[0].GetWrite() != apiv1.WriteMode_WRITE_MODE_MERGE {
		t.Errorf("a keyed output of a cdc input is merged: %v", plans)
	}
}

func TestHoldProblem(t *testing.T) {
	takes := destination.Capabilities{Holds: destination.Holds{ByKey: true, ByID: true, Guarded: true}}
	cell := &apiv1.PipelineStream{Quarantine: apiv1.QuarantineScope_QUARANTINE_SCOPE_CELL}
	last := &apiv1.PipelineStream{Columns: map[string]*apiv1.PipelineColumn{"c": {Held: &apiv1.Held{Mode: apiv1.HeldMode_HELD_MODE_LAST}}}}
	for name, c := range map[string]struct {
		caps  destination.Capabilities
		mode  destination.WriteMode
		keyed bool
		ps    *apiv1.PipelineStream
		want  string
	}{
		"nothing asked":           {destination.Capabilities{}, destination.Append, false, &apiv1.PipelineStream{}, ""},
		"cells":                   {takes, destination.Append, true, cell, ""},
		"unkeyed":                 {takes, destination.Append, false, cell, "only a keyed stream"},
		"no patches":              {destination.Capabilities{}, destination.Merge, true, cell, "doesn't patch merge streams"},
		"last without merge":      {takes, destination.Append, true, last, "needs merge"},
		"last without the caps":   {takes, destination.Merge, true, last, "needs merge"},
		"last into a keeper":      {destination.Capabilities{Holds: destination.Holds{ByKey: true, Last: true, Guarded: true}}, destination.Merge, true, last, ""},
		"overwrite isn't patched": {takes, destination.Overwrite, true, cell, "doesn't patch overwrite streams"},
	} {
		err := HoldProblem(c.caps, c.mode, c.keyed, "s", c.ps)
		if (err == nil) != (c.want == "") || err != nil && !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want %q", name, err, c.want)
		}
	}
	if !Holds(takes, destination.Merge, true) || Holds(takes, destination.Merge, false) || Holds(destination.Capabilities{}, destination.Merge, true) {
		t.Error("Holds")
	}
}
