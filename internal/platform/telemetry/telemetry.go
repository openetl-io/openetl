// Package telemetry sets up OpenTelemetry traces and metrics from the
// standard OTEL_* variables. A signal is exported only once its exporter
// (OTEL_TRACES_EXPORTER, OTEL_METRICS_EXPORTER) or an OTLP endpoint is set;
// OTEL_METRICS_EXPORTER=prometheus serves a Prometheus endpoint instead.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"strings"

	"go.opentelemetry.io/contrib/exporters/autoexport"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

type Options struct {
	// The service's name, unless OTEL_SERVICE_NAME sets one.
	Service string
	// Resource attributes, such as openetl.role.
	Attributes []attribute.KeyValue
}

// Start sets the global tracer and meter providers and the W3C propagators.
// The function it returns flushes what's left and stops the exporters.
func Start(ctx context.Context, o Options) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	if strings.EqualFold(os.Getenv("OTEL_SDK_DISABLED"), "true") || !configured("TRACES") && !configured("METRICS") {
		return func(context.Context) error { return nil }, nil
	}
	attrs := append([]attribute.KeyValue{semconv.ServiceName(o.Service), semconv.ServiceVersion(Version())}, o.Attributes...)
	res, err := resource.New(ctx,
		resource.WithAttributes(attrs...),
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		resource.WithProcessPID(),
	)
	if err != nil {
		return nil, fmt.Errorf("telemetry: resource: %w", err)
	}
	var stops []func(context.Context) error
	stop := func(ctx context.Context) error {
		var errs []error
		for _, s := range stops {
			errs = append(errs, s(ctx))
		}
		return errors.Join(errs...)
	}
	if configured("TRACES") {
		exp, err := autoexport.NewSpanExporter(ctx)
		if err != nil {
			return nil, fmt.Errorf("telemetry: traces: %w", err)
		}
		if !autoexport.IsNoneSpanExporter(exp) {
			tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp), sdktrace.WithResource(res))
			otel.SetTracerProvider(tp)
			stops = append(stops, tp.Shutdown)
		}
	}
	if configured("METRICS") {
		r, err := autoexport.NewMetricReader(ctx)
		if err != nil {
			_ = stop(ctx)
			return nil, fmt.Errorf("telemetry: metrics: %w", err)
		}
		if !autoexport.IsNoneMetricReader(r) {
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(r), sdkmetric.WithResource(res), sdkmetric.WithView(views...))
			otel.SetMeterProvider(mp)
			stops = append(stops, mp.Shutdown)
			if err := runtime.Start(runtime.WithMeterProvider(mp)); err != nil {
				_ = stop(ctx)
				return nil, fmt.Errorf("telemetry: runtime metrics: %w", err)
			}
		}
	}
	return stop, nil
}

// configured says whether signal (TRACES or METRICS) has an exporter or an
// OTLP endpoint set.
func configured(signal string) bool {
	for _, k := range []string{"OTEL_" + signal + "_EXPORTER", "OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_" + signal + "_ENDPOINT"} {
		if os.Getenv(k) != "" {
			return true
		}
	}
	return false
}

// Version is the main module's version, or "(devel)".
func Version() string {
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" {
		return bi.Main.Version
	}
	return "(devel)"
}

// Durations of epochs and commits run from milliseconds to many minutes.
var views = []sdkmetric.View{
	sdkmetric.NewView(
		sdkmetric.Instrument{Name: "openetl.*.duration", Kind: sdkmetric.InstrumentKindHistogram},
		sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
			Boundaries: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800},
		}},
	),
}
