package spec

import (
	"reflect"
	"strings"
	"testing"
	"time"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
)

const pipeline = `
apiVersion: openetl.dev/v1alpha1
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
  destination:
    ref: warehouse
    namespace: raw_faker
    naming: source
  delivery:
    require: exactly_once
    commit: { interval: 5s, maxBytes: 256MB, scope: stream }
    resumePointTimeout: none
    dedupe: auto
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
	case p.GetDestination().GetWrite() != apiv1.WriteMode_WRITE_MODE_APPEND:
		t.Error("write doesn't default to append")
	case p.GetDestination().GetNaming() != apiv1.Naming_NAMING_SOURCE:
		t.Error("naming")
	case p.GetDelivery().GetRequire() != apiv1.DeliveryLevel_DELIVERY_LEVEL_EXACTLY_ONCE:
		t.Error("require")
	case p.GetDelivery().GetCommit().GetMaxBytes() != 256e6 || p.GetDelivery().GetCommit().GetScope() != apiv1.CommitScope_COMMIT_SCOPE_STREAM:
		t.Errorf("commit = %v", p.GetDelivery().GetCommit())
	case p.GetDelivery().GetResumePointTimeout() == nil || p.GetDelivery().GetResumePointTimeout().AsDuration() != 0:
		t.Error("resumePointTimeout: none should be an explicit zero")
	}
}

func TestParseConnector(t *testing.T) {
	doc, err := Parse([]byte(`{"apiVersion": "openetl.dev/v1alpha1", "kind": "Source", "metadata": {"name": "faker"},
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
	header := "apiVersion: openetl.dev/v1alpha1\nkind: Pipeline\nmetadata: {name: p}\n"
	base := "spec:\n  schedule: {every: 1m}\n  source: {ref: s}\n  destination: {ref: d}\n"
	for name, doc := range map[string]string{
		"apiVersion":     strings.Replace(header, "v1alpha1", "v2", 1) + base,
		"bad name":       strings.Replace(header, "name: p", "name: P_1", 1) + base,
		"unknown kind":   strings.Replace(header, "Pipeline", "Job", 1) + base,
		"unknown field":  header + base + "  heal: {mode: auto-safe}\n",
		"no schedule":    header + "spec:\n  source: {ref: s}\n  destination: {ref: d}\n",
		"two triggers":   header + "spec:\n  schedule: {every: 1m, cron: '* * * * *'}\n  source: {ref: s}\n  destination: {ref: d}\n",
		"continuous":     header + base + "  mode: continuous\n",
		"enum case":      header + base + "  schemaPolicy: ALLOW_ALL\n",
		"unknown enum":   header + strings.Replace(base, "{ref: d}", "{ref: d, write: upsert}", 1),
		"naming":         header + strings.Replace(base, "{ref: d}", "{ref: d, naming: exact}", 1),
		"column ops":     header + strings.Replace(base, "{ref: s}", "{ref: s, streams: [{name: a, columns: {x: {op: hash}}}]}", 1),
		"repeated":       header + strings.Replace(base, "{ref: s}", "{ref: s, streams: [{name: a}, {name: a}]}", 1),
		"size":           header + base + "  delivery: {commit: {maxBytes: lots}}\n",
		"zero timeout":   header + base + "  delivery: {resumePointTimeout: 0s}\n",
		"zero interval":  header + base + "  delivery: {commit: {interval: 0s}}\n",
		"two documents":  header + base + "---\n" + header + base,
		"missing source": header + "spec:\n  schedule: {every: 1m}\n  destination: {ref: d}\n",
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
