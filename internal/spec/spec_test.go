package spec

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
)

const pipeline = `
apiVersion: openetl.io/v1alpha1
kind: Pipeline
metadata:
  name: faker-to-postgres
  workspace: default
spec:
  schedule: { every: 15m, jitter: 2m }
  source:
    ref: faker
    streams:
      - name: users
        sync: full_refresh
        timestamps: nanosecond
        columns:
          balance: { type: float64 }
      - name: events
        require: at_least_once
        collapse: false
      - name: logs
  destination:
    ref: warehouse
    namespace: raw_faker
    naming: source
    deletes: hard
  schemaPolicy: block
  delivery:
    require: exactly_once
    commit: { interval: 5s, maxBytes: 256MB, scope: stream }
    resumePointTimeout: none
  types:
    infinity: quarantine
  postSync:
    - dbt: { project: analytics, select: "tag:shop", schema: marts }
    - dbt: { project: checks, command: test }
  heal:
    mode: suggest
`

func TestParsePipeline(t *testing.T) {
	doc, err := Parse([]byte(pipeline))
	if err != nil {
		t.Fatal(err)
	}
	if doc.Kind != KindPipeline || doc.Name != "faker-to-postgres" || doc.Workspace != "default" {
		t.Fatalf("doc = %+v", doc)
	}
	p := doc.Pipeline
	switch {
	case p.GetMode() != apiv1.PipelineMode_PIPELINE_MODE_SCHEDULED:
		t.Error("mode doesn't default to scheduled")
	case p.GetSchedule().GetEvery().AsDuration() != 15*time.Minute || p.GetSchedule().GetJitter().AsDuration() != 2*time.Minute:
		t.Errorf("schedule = %v", p.GetSchedule())
	case p.GetSource().GetStreams()[0].GetSync() != apiv1.SyncMode_SYNC_MODE_FULL_REFRESH:
		t.Error("sync mode")
	case p.GetSource().GetStreams()[0].GetTimestamps() != apiv1.TimestampUnit_TIMESTAMP_UNIT_NANOSECOND:
		t.Error("timestamps")
	case p.GetSource().GetStreams()[0].GetColumns()["balance"].GetType() != apiv1.ColumnType_COLUMN_TYPE_FLOAT64:
		t.Errorf("columns = %v", p.GetSource().GetStreams()[0].GetColumns())
	case p.GetSource().GetStreams()[1].GetRequire() != apiv1.DeliveryLevel_DELIVERY_LEVEL_AT_LEAST_ONCE:
		t.Error("a stream's require")
	case p.GetSource().GetStreams()[1].Collapse == nil || p.GetSource().GetStreams()[1].GetCollapse():
		t.Error("collapse: false should be an explicit false")
	case p.GetSource().GetStreams()[2].Collapse != nil || p.GetSource().GetStreams()[2].GetRequire() != apiv1.DeliveryLevel_DELIVERY_LEVEL_UNSPECIFIED:
		t.Error("an unset require or collapse should stay unset")
	case p.GetDestination().GetWrite() != apiv1.WriteMode_WRITE_MODE_UNSPECIFIED:
		t.Error("an unset write should stay unset, for each stream's default")
	case p.GetDestination().GetNaming() != apiv1.Naming_NAMING_SOURCE:
		t.Error("naming")
	case p.GetDestination().GetDeletes() != apiv1.DeleteMode_DELETE_MODE_HARD:
		t.Error("deletes")
	case p.GetSchemaPolicy() != apiv1.SchemaPolicy_SCHEMA_POLICY_BLOCK:
		t.Error("schemaPolicy")
	case p.GetDelivery().GetRequire() != apiv1.DeliveryLevel_DELIVERY_LEVEL_EXACTLY_ONCE:
		t.Error("require")
	case p.GetDelivery().GetCommit().GetMaxBytes() != 256e6 || p.GetDelivery().GetCommit().GetScope() != apiv1.CommitScope_COMMIT_SCOPE_STREAM:
		t.Errorf("commit = %v", p.GetDelivery().GetCommit())
	case p.GetDelivery().GetResumePointTimeout() == nil || p.GetDelivery().GetResumePointTimeout().AsDuration() != 0:
		t.Error("resumePointTimeout: none should be an explicit zero")
	case p.GetTypes().GetInfinity() != apiv1.Infinity_INFINITY_QUARANTINE:
		t.Error("types.infinity")
	case p.GetHeal().GetMode() != apiv1.HealMode_HEAL_MODE_SUGGEST:
		t.Error("heal.mode")
	case len(p.GetPostSync()) != 2:
		t.Fatalf("postSync = %v", p.GetPostSync())
	}
	want := []*apiv1.DbtStep{
		{Project: "analytics", Command: apiv1.DbtCommand_DBT_COMMAND_BUILD, Select: "tag:shop", Schema: "marts"},
		{Project: "checks", Command: apiv1.DbtCommand_DBT_COMMAND_TEST},
	}
	for i, s := range p.GetPostSync() {
		if !proto.Equal(s.GetDbt(), want[i]) {
			t.Errorf("postSync[%d] = %v, want %v", i, s.GetDbt(), want[i])
		}
	}
}

func TestParseContinuousPipeline(t *testing.T) {
	doc, err := Parse([]byte("apiVersion: openetl.io/v1alpha1\nkind: Pipeline\nmetadata: {name: p}\nspec:\n  mode: continuous\n  source: {ref: s}\n  destination: {ref: d}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if p := doc.Pipeline; p.GetMode() != apiv1.PipelineMode_PIPELINE_MODE_CONTINUOUS || p.Schedule != nil {
		t.Errorf("mode %v, schedule %v", p.GetMode(), p.GetSchedule())
	}
}

func TestParseResources(t *testing.T) {
	header := "apiVersion: openetl.io/v1alpha1\nkind: Pipeline\nmetadata: {name: p}\nspec:\n  schedule: {every: 1m}\n  source: {ref: s}\n  destination: {ref: d}\n"
	doc, err := Parse([]byte(header + "  resources: {size: m, min: s, max: {cpu: 6, memory: 12Gi}, finishWithin: 1h, pool: batch}\n"))
	if err != nil {
		t.Fatal(err)
	}
	r := doc.Pipeline.GetResources()
	switch {
	case r.GetSize().GetPreset() != "m" || r.GetMin().GetPreset() != "s":
		t.Errorf("size %v, min %v", r.GetSize(), r.GetMin())
	case r.GetMax().GetCpuMillis() != 6000 || r.GetMax().GetMemoryBytes() != 12<<30:
		t.Errorf("max %v", r.GetMax())
	case r.GetFinishWithin().AsDuration() != time.Hour || r.GetPool() != "batch":
		t.Errorf("finishWithin %v, pool %q", r.GetFinishWithin(), r.GetPool())
	}
	doc, err = Parse([]byte(header + "  resources: {size: auto, min: xs}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if r := doc.Pipeline.GetResources(); r.GetSize() != nil || r.GetMin().GetPreset() != "xs" {
		t.Errorf("auto is an unset size: %v", r)
	}
	doc, err = Parse([]byte(header))
	if err != nil {
		t.Fatal(err)
	}
	if doc.Pipeline.Resources != nil {
		t.Errorf("resources %v with none in the document", doc.Pipeline.Resources)
	}
}

func TestParseDbtProject(t *testing.T) {
	doc, err := Parse([]byte("apiVersion: openetl.io/v1alpha1\nkind: DbtProject\nmetadata: {name: analytics}\nspec: {bundle: aGVsbG8=}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if doc.Kind != KindDbtProject || string(doc.Bundle) != "hello" {
		t.Errorf("doc = %+v", doc)
	}
	header := "apiVersion: openetl.io/v1alpha1\nkind: DbtProject\nmetadata: {name: analytics}\n"
	for name, spec := range map[string]string{
		"path":       "spec: {path: ./dbt}",
		"no bundle":  "spec: {bundle: ''}",
		"not base64": "spec: {bundle: '%%%'}",
	} {
		if _, err := Parse([]byte(header + spec)); err == nil {
			t.Errorf("%s: parsed", name)
		}
	}
	if _, err := Parse([]byte(strings.Replace(header, "{name: analytics}", "{name: analytics, version: 1.0.0}", 1) + "spec: {bundle: aGVsbG8=}")); err == nil {
		t.Error("a dbt project took a version")
	}
}

func TestParseWebhook(t *testing.T) {
	header := "apiVersion: openetl.io/v1alpha1\nkind: Webhook\nmetadata: {name: ops, workspace: analytics}\n"
	doc, err := Parse([]byte(header + `spec: {url: "https://hooks.example.com/openetl", events: [sync.failed, "schema.*"], pipelines: [shop], secrets: [ops-key, ops-key-old]}`))
	if err != nil {
		t.Fatal(err)
	}
	w := doc.Webhook
	if doc.Kind != KindWebhook || doc.Workspace != "analytics" || w.GetUrl() != "https://hooks.example.com/openetl" ||
		len(w.GetEvents()) != 2 || w.GetPipelines()[0] != "shop" || w.GetSecrets()[1] != "ops-key-old" {
		t.Errorf("doc = %+v", doc)
	}
	for name, spec := range map[string]string{
		"no url":         "spec: {events: ['*'], secrets: [k]}",
		"ftp":            "spec: {url: 'ftp://example.com', events: ['*'], secrets: [k]}",
		"no host":        "spec: {url: 'https:///x', events: ['*'], secrets: [k]}",
		"credentials":    "spec: {url: 'https://u:p@example.com', events: ['*'], secrets: [k]}",
		"no events":      "spec: {url: 'https://example.com', secrets: [k]}",
		"no secrets":     "spec: {url: 'https://example.com', events: ['*']}",
		"a secret twice": "spec: {url: 'https://example.com', events: ['*'], secrets: [k, k]}",
		"four secrets":   "spec: {url: 'https://example.com', events: ['*'], secrets: [a, b, c, d]}",
		"a bad name":     "spec: {url: 'https://example.com', events: ['*'], secrets: [K]}",
		"a bad pipeline": "spec: {url: 'https://example.com', events: ['*'], secrets: [k], pipelines: [a_b]}",
		"unknown field":  "spec: {url: 'https://example.com', events: ['*'], secrets: [k], headers: {a: b}}",
	} {
		if _, err := Parse([]byte(header + spec)); err == nil {
			t.Errorf("%s: parsed", name)
		}
	}
}

func TestParseConnector(t *testing.T) {
	doc, err := Parse([]byte(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Source", "metadata": {"name": "faker"},
		"spec": {"connector": "faker", "config": {"seed": 1, "count": 100, "nested": {"a": [1, "b"]}}}}`))
	if err != nil {
		t.Fatal(err)
	}
	got := doc.Connector.GetConfig().AsMap()
	want := map[string]any{"seed": 1.0, "count": 100.0, "nested": map[string]any{"a": []any{1.0, "b"}}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("config = %v, want %v", got, want)
	}
}

func TestParseErrors(t *testing.T) {
	header := "apiVersion: openetl.io/v1alpha1\nkind: Pipeline\nmetadata: {name: p}\n"
	base := "spec:\n  schedule: {every: 1m}\n  source: {ref: s}\n  destination: {ref: d}\n"
	for name, doc := range map[string]string{
		"apiVersion":                 strings.Replace(header, "v1alpha1", "v2", 1) + base,
		"bad name":                   strings.Replace(header, "name: p", "name: P_1", 1) + base,
		"unknown kind":               strings.Replace(header, "Pipeline", "Job", 1) + base,
		"unknown field":              header + base + "  heal: {mode: auto-safe}\n",
		"no schedule":                header + "spec:\n  source: {ref: s}\n  destination: {ref: d}\n",
		"two triggers":               header + "spec:\n  schedule: {every: 1m, cron: '* * * * *'}\n  source: {ref: s}\n  destination: {ref: d}\n",
		"continuous with a schedule": header + base + "  mode: continuous\n",
		"continuous postSync":        header + "spec:\n  mode: continuous\n  source: {ref: s}\n  destination: {ref: d}\n  postSync: [{dbt: {project: a}}]\n",
		"enum case":                  header + base + "  schemaPolicy: ALLOW_ALL\n",
		"unknown enum":               header + strings.Replace(base, "{ref: d}", "{ref: d, write: upsert}", 1),
		"naming":                     header + strings.Replace(base, "{ref: d}", "{ref: d, naming: exact}", 1),
		"deletes":                    header + strings.Replace(base, "{ref: d}", "{ref: d, deletes: purge}", 1),
		"column ops":                 header + strings.Replace(base, "{ref: s}", "{ref: s, streams: [{name: a, columns: {x: {op: hash}}}]}", 1),
		"column type":                header + strings.Replace(base, "{ref: s}", "{ref: s, streams: [{name: a, columns: {x: {type: int64}}}]}", 1),
		"no column type":             header + strings.Replace(base, "{ref: s}", "{ref: s, streams: [{name: a, columns: {x: {}}}]}", 1),
		"repeated":                   header + strings.Replace(base, "{ref: s}", "{ref: s, streams: [{name: a}, {name: a}]}", 1),
		"size":                       header + base + "  delivery: {commit: {maxBytes: lots}}\n",
		"zero timeout":               header + base + "  delivery: {resumePointTimeout: 0s}\n",
		"zero interval":              header + base + "  delivery: {commit: {interval: 0s}}\n",
		"infinity":                   header + base + "  types: {infinity: drop}\n",
		"heal mode":                  header + base + "  heal: {mode: auto-safe}\n",
		"timestamps":                 header + strings.Replace(base, "{ref: s}", "{ref: s, streams: [{name: a, timestamps: millisecond}]}", 1),
		"stream require":             header + strings.Replace(base, "{ref: s}", "{ref: s, streams: [{name: a, require: once}]}", 1),
		"collapse":                   header + strings.Replace(base, "{ref: s}", "{ref: s, streams: [{name: a, collapse: yes please}]}", 1),
		"two documents":              header + base + "---\n" + header + base,
		"missing source":             header + "spec:\n  schedule: {every: 1m}\n  destination: {ref: d}\n",
		"empty step":                 header + base + "  postSync: [{}]\n",
		"dbt command":                header + base + "  postSync: [{dbt: {project: a, command: seed}}]\n",
		"dbt project":                header + base + "  postSync: [{dbt: {project: A_1}}]\n",
		"preset":                     header + base + "  resources: {size: huge}\n",
		"min auto":                   header + base + "  resources: {min: auto}\n",
		"max auto":                   header + base + "  resources: {max: auto}\n",
		"size field":                 header + base + "  resources: {size: {cpu: 1, memory: 1Gi, disk: 1Gi}}\n",
		"no cpu":                     header + base + "  resources: {size: {memory: 1Gi}}\n",
		"no memory":                  header + base + "  resources: {size: {cpu: 1}}\n",
		"memory unit":                header + base + "  resources: {size: {cpu: 1, memory: 1GB}}\n",
		"min over max":               header + base + "  resources: {min: l, max: m}\n",
		"size under min":             header + base + "  resources: {size: s, min: m}\n",
		"size over max":              header + base + "  resources: {size: l, max: m}\n",
		"finishWithin":               header + base + "  resources: {finishWithin: soon}\n",
		"zero finishWithin":          header + base + "  resources: {finishWithin: 0s}\n",
		"continuous finishWithin":    header + "spec:\n  mode: continuous\n  source: {ref: s}\n  destination: {ref: d}\n  resources: {finishWithin: 1h}\n",
		"pool":                       header + base + "  resources: {pool: 'a pool'}\n",
	} {
		if _, err := Parse([]byte(doc)); err == nil {
			t.Errorf("%s: parsed", name)
		}
	}
}

func TestSize(t *testing.T) {
	for in, want := range map[string]int64{"256MB": 256e6, "64MiB": 64 << 20, "1024": 1024, "1B": 1} {
		if got, err := Size(in); err != nil || got != want {
			t.Errorf("Size(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "MB", "-1MB", "1.5GB", "0"} {
		if _, err := Size(in); err == nil {
			t.Errorf("Size(%q) succeeded", in)
		}
	}
}

func TestParseHolds(t *testing.T) {
	head := "apiVersion: openetl.io/v1alpha1\nkind: Pipeline\nmetadata: {name: p}\nspec:\n  schedule: {every: 1h}\n  destination: {ref: d}\n  source:\n    ref: s\n    streams:\n"
	doc, err := Parse([]byte(head + `      - name: orders
        quarantine: cell
        heldValue: 0
        groups: [[amount, currency]]
        columns:
          note: {held: last}
          code: {held: last, heldValue: "n/a"}
          amount: {type: string}
`))
	if err != nil {
		t.Fatal(err)
	}
	st := doc.Pipeline.GetSource().GetStreams()[0]
	switch {
	case st.GetQuarantine() != apiv1.QuarantineScope_QUARANTINE_SCOPE_CELL:
		t.Errorf("quarantine = %v", st.GetQuarantine())
	case st.GetHeld().GetMode() != apiv1.HeldMode_HELD_MODE_VALUE || st.GetHeld().GetValue() != "0":
		t.Errorf("held = %v: a value alone means value", st.GetHeld())
	case len(st.GetGroups()) != 1 || len(st.GetGroups()[0].GetColumns()) != 2:
		t.Errorf("groups = %v", st.GetGroups())
	case st.GetColumns()["note"].GetHeld().GetMode() != apiv1.HeldMode_HELD_MODE_LAST || st.GetColumns()["code"].GetHeld().GetValue() != `"n/a"`:
		t.Errorf("columns = %v", st.GetColumns())
	case st.GetColumns()["amount"].GetHeld() != nil:
		t.Error("a column without held got one")
	}

	for name, streams := range map[string]string{
		"value without a value": "      - {name: a, held: value}\n",
		"null with a value":     "      - {name: a, held: null, heldValue: 1}\n",
		"an unknown mode":       "      - {name: a, held: sometimes}\n",
		"a one-column group":    "      - {name: a, groups: [[x]]}\n",
		"an unknown scope":      "      - {name: a, quarantine: table}\n",
	} {
		if _, err := Parse([]byte(head + streams)); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	flink := strings.Replace(head, "  schedule: {every: 1h}\n", "  mode: continuous\n  transform: {flink: {cluster: c, code: {jar: 's3://j/a.jar', sha256: "+strings.Repeat("cd", 32)+"}}}\n", 1)
	if _, err := Parse([]byte(flink + "      - {name: a, quarantine: cell}\n")); err == nil || !strings.Contains(err.Error(), "transform") {
		t.Errorf("a transform's input holds: %v", err)
	}
}
