//go:build e2e

package e2e

import (
	"maps"
	"slices"
	"testing"
	"time"

	"connectrpc.com/connect"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/gen/openetl/api/v1/apiv1connect"
	"github.com/unsuitable001/openetl/internal/orchestration/contract"
	"github.com/unsuitable001/openetl/internal/platform/telemetry/telemetrytest"
)

// With only OTEL_* variables set, a collector gets a trace from the API
// through Temporal to the runner, one from a sync's workflow through the
// runner's attempt to the engine's job and the destination's calls, and the
// platform's and the job's metrics.
func TestTelemetry(t *testing.T) {
	startRunner(t)
	name := unique("telemetry")
	apply(t, pipelineDocs(name, `{"seed": 3, "count": 300}`, `{}`)...)
	if status, stats := waitSync(t, name, 60*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	id, err := query[string](t, "SELECT id::text FROM pipelines WHERE name = $1", name)
	if err != nil {
		t.Fatal(err)
	}

	sync := []string{
		"server RunWorkflow:" + contract.SyncWorkflow,
		"runner RunActivity:" + contract.RunSyncAttempt,
		"runner recover",
		"runner commit",
		"runner openetl.engine.v1.EngineService/StartJob",
		"runner openetl.connector.v1.DestinationService/Write",
		"engine job",
		"engine epoch",
		"engine batch",
	}
	var got []string
	eventually(t, 30*time.Second, "the sync's spans in one trace", func() bool {
		got = traceOf(env.collector.Spans(), func(s telemetrytest.Span) bool { return s.Name == "commit" && s.Attrs["openetl.pipeline"] == id })
		return containsAll(got, sync)
	})

	if _, err := apiv1connect.NewSourceServiceClient(env.http, env.apiURL).DiscoverSource(t.Context(), connect.NewRequest(&apiv1.DiscoverSourceRequest{
		Name: "workspaces/default/sources/" + name,
	})); err != nil {
		t.Fatal(err)
	}
	discover := []string{
		"server openetl.api.v1.SourceService/DiscoverSource",
		"server RunWorkflow:" + contract.ConnectorOpWorkflow,
		"runner RunActivity:" + contract.DiscoverSource,
	}
	eventually(t, 30*time.Second, "the discover's spans in one trace", func() bool {
		got = traceOf(env.collector.Spans(), func(s telemetrytest.Span) bool {
			return s.Name == "openetl.api.v1.SourceService/DiscoverSource" && s.Role == "server"
		})
		return containsAll(got, discover)
	})

	job := map[string]string{"openetl.pipeline": id}
	users := map[string]string{"openetl.pipeline": id, "openetl.stream": "users"}
	want := []struct {
		role, name string
		attrs      map[string]string
		ok         func(float64) bool
	}{
		{"runner", "openetl.job.rows.committed", users, func(v float64) bool { return v == 300 }},
		{"runner", "openetl.job.rows.in", users, func(v float64) bool { return v == 300 }},
		{"runner", "openetl.job.bytes.out", users, func(v float64) bool { return v > 0 }},
		{"runner", "openetl.job.epochs", job, func(v float64) bool { return v >= 1 }},
		{"runner", "openetl.job.commit.duration", job, func(v float64) bool { return v >= 1 }},
		{"runner", "openetl.job.attempts", map[string]string{"openetl.pipeline": id, "openetl.outcome": "succeeded", "openetl.retry": "false"}, func(v float64) bool { return v == 1 }},
		{"runner", "openetl.checkpoint.writes", map[string]string{"openetl.checkpoint.op": "commit", "openetl.outcome": "ok"}, func(v float64) bool { return v >= 1 }},
		{"runner", "openetl.objectstore.calls", map[string]string{"openetl.objectstore.op": "create", "openetl.outcome": "ok"}, func(v float64) bool { return v >= 1 }},
		{"runner", "openetl.engine.cpu.time", map[string]string{"openetl.cpu.mode": "user"}, func(v float64) bool { return v > 0 }},
		{"runner", "openetl.engine.edge.bytes", map[string]string{"openetl.edge.direction": "to_engine"}, func(v float64) bool { return v > 0 }},
		{"runner", "temporal_request", nil, func(v float64) bool { return v > 0 }},
		{"runner", "go.memory.used", nil, func(v float64) bool { return v > 0 }},
		{"server", "rpc.server.call.duration", map[string]string{"rpc.method": "openetl.api.v1.SourceService/DiscoverSource"}, func(v float64) bool { return v >= 1 }},
		{"server", "openetl.pipeline.freshness", job, func(v float64) bool { return v >= 0 }},
	}
	for _, w := range want {
		eventually(t, 30*time.Second, w.role+" metric "+w.name, func() bool {
			sum, found := 0.0, false
			for _, p := range env.collector.Points(w.name) {
				if p.Role == w.role && matches(p.Attrs, w.attrs) {
					sum, found = sum+p.Value, true
				}
			}
			return found && w.ok(sum)
		})
	}
}

// traceOf returns "role name" for each span in the trace of the first span
// that matches.
func traceOf(spans []telemetrytest.Span, match func(telemetrytest.Span) bool) []string {
	i := slices.IndexFunc(spans, match)
	if i < 0 {
		return nil
	}
	var out []string
	for _, s := range spans {
		if s.TraceID == spans[i].TraceID {
			out = append(out, s.Role+" "+s.Name)
		}
	}
	return out
}

func containsAll(have, want []string) bool {
	for _, w := range want {
		if !slices.Contains(have, w) {
			return false
		}
	}
	return true
}

func matches(attrs, want map[string]string) bool {
	for k := range maps.Keys(want) {
		if attrs[k] != want[k] {
			return false
		}
	}
	return true
}
