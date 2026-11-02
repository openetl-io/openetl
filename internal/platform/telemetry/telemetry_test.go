package telemetry

import (
	"context"
	"slices"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/unsuitable001/openetl/internal/platform/telemetry/telemetrytest"
)

func TestConfigured(t *testing.T) {
	for _, k := range []string{"OTEL_TRACES_EXPORTER", "OTEL_METRICS_EXPORTER", "OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"} {
		t.Setenv(k, "")
	}
	if configured("TRACES") || configured("METRICS") {
		t.Fatal("configured without any OTEL_* setting")
	}
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://localhost:4317")
	if !configured("TRACES") || configured("METRICS") {
		t.Error("a traces endpoint configures traces only")
	}
	t.Setenv("OTEL_METRICS_EXPORTER", "prometheus")
	if !configured("METRICS") {
		t.Error("an exporter configures its signal")
	}
}

func TestExportsOverOTLP(t *testing.T) {
	c, err := telemetrytest.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for k, v := range c.Env() {
		t.Setenv(k, v)
	}
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment.name=test")
	stop, err := Start(t.Context(), Options{Service: "openetld", Attributes: []attribute.KeyValue{attribute.String("openetl.role", "runner")}})
	if err != nil {
		t.Fatal(err)
	}
	_, span := otel.Tracer("test").Start(t.Context(), "work")
	span.End()
	counter, err := otel.Meter("test").Int64Counter("openetl.test.things")
	if err != nil {
		t.Fatal(err)
	}
	counter.Add(t.Context(), 3)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := stop(ctx); err != nil {
		t.Fatal(err)
	}
	spans := c.Spans()
	if !slices.ContainsFunc(spans, func(s telemetrytest.Span) bool { return s.Name == "work" && s.Service == "openetld" }) {
		t.Errorf("spans %+v", spans)
	}
	points := c.Points("openetl.test.things")
	if len(points) != 1 || points[0].Value != 3 || points[0].Service != "openetld" {
		t.Errorf("points %+v", points)
	}
	if len(c.Points("go.memory.used")) == 0 {
		t.Error("no Go runtime metrics")
	}
}
