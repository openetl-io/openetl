// Package telemetrytest is an OTLP collector over gRPC that keeps what it
// receives, for tests.
package telemetrytest

import (
	"context"
	"encoding/hex"
	"fmt"
	"maps"
	"net"
	"slices"
	"strings"
	"sync"

	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricpb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
)

type Span struct {
	// The resource's service.name and openetl.role.
	Service, Role             string
	Name                      string
	TraceID, SpanID, ParentID string
	Attrs                     map[string]string
}

// Point is a metric's latest data point in one series: a sum's or gauge's
// value, or a histogram's count.
type Point struct {
	Service string
	Role    string
	Name    string
	Attrs   map[string]string
	Value   float64
}

type Collector struct {
	collectortrace.UnimplementedTraceServiceServer
	srv  *grpc.Server
	addr string

	mu     sync.Mutex
	spans  []Span
	points map[string]Point
}

// Start serves on a loopback port until Close.
func Start() (*Collector, error) {
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	c := &Collector{srv: grpc.NewServer(), addr: ln.Addr().String(), points: map[string]Point{}}
	collectortrace.RegisterTraceServiceServer(c.srv, c)
	collectormetrics.RegisterMetricsServiceServer(c.srv, metricsServer{c: c})
	go func() { _ = c.srv.Serve(ln) }()
	return c, nil
}

// Env is the OTEL_* settings that export to the collector, quickly.
func (c *Collector) Env() map[string]string {
	return map[string]string{
		"OTEL_EXPORTER_OTLP_ENDPOINT": "http://" + c.addr,
		"OTEL_EXPORTER_OTLP_PROTOCOL": "grpc",
		"OTEL_METRIC_EXPORT_INTERVAL": "500",
		"OTEL_BSP_SCHEDULE_DELAY":     "200",
	}
}

func (c *Collector) Close() { c.srv.Stop() }

func (c *Collector) Export(_ context.Context, req *collectortrace.ExportTraceServiceRequest) (*collectortrace.ExportTraceServiceResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, rs := range req.GetResourceSpans() {
		svc, role := service(rs.GetResource())
		for _, ss := range rs.GetScopeSpans() {
			for _, s := range ss.GetSpans() {
				c.spans = append(c.spans, Span{
					Service: svc, Role: role, Name: s.GetName(), Attrs: attrs(s.GetAttributes()),
					TraceID: hex.EncodeToString(s.GetTraceId()), SpanID: hex.EncodeToString(s.GetSpanId()), ParentID: hex.EncodeToString(s.GetParentSpanId()),
				})
			}
		}
	}
	return &collectortrace.ExportTraceServiceResponse{}, nil
}

func (c *Collector) exportMetrics(req *collectormetrics.ExportMetricsServiceRequest) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, rm := range req.GetResourceMetrics() {
		svc, role := service(rm.GetResource())
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				add := func(kvs []*commonpb.KeyValue, v float64) {
					p := Point{Service: svc, Role: role, Name: m.GetName(), Attrs: attrs(kvs), Value: v}
					c.points[key(p)] = p
				}
				switch d := m.GetData().(type) {
				case *metricpb.Metric_Sum:
					for _, dp := range d.Sum.GetDataPoints() {
						add(dp.GetAttributes(), number(dp))
					}
				case *metricpb.Metric_Gauge:
					for _, dp := range d.Gauge.GetDataPoints() {
						add(dp.GetAttributes(), number(dp))
					}
				case *metricpb.Metric_Histogram:
					for _, dp := range d.Histogram.GetDataPoints() {
						add(dp.GetAttributes(), float64(dp.GetCount()))
					}
				}
			}
		}
	}
}

// metricsServer is the collector's metrics service; its method is also
// called Export.
type metricsServer struct {
	collectormetrics.UnimplementedMetricsServiceServer
	c *Collector
}

func (m metricsServer) Export(_ context.Context, req *collectormetrics.ExportMetricsServiceRequest) (*collectormetrics.ExportMetricsServiceResponse, error) {
	m.c.exportMetrics(req)
	return &collectormetrics.ExportMetricsServiceResponse{}, nil
}

// Spans returns what was received so far.
func (c *Collector) Spans() []Span {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.spans)
}

// Points returns each series' latest point of the metric.
func (c *Collector) Points(name string) []Point {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []Point
	for _, p := range c.points {
		if p.Name == name {
			out = append(out, p)
		}
	}
	slices.SortFunc(out, func(a, b Point) int { return strings.Compare(key(a), key(b)) })
	return out
}

func service(r *resourcepb.Resource) (name, role string) {
	a := attrs(r.GetAttributes())
	return a["service.name"], a["openetl.role"]
}

func attrs(kvs []*commonpb.KeyValue) map[string]string {
	out := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		v := kv.GetValue()
		switch x := v.GetValue().(type) {
		case *commonpb.AnyValue_StringValue:
			out[kv.GetKey()] = x.StringValue
		case *commonpb.AnyValue_IntValue:
			out[kv.GetKey()] = fmt.Sprint(x.IntValue)
		case *commonpb.AnyValue_BoolValue:
			out[kv.GetKey()] = fmt.Sprint(x.BoolValue)
		case *commonpb.AnyValue_DoubleValue:
			out[kv.GetKey()] = fmt.Sprint(x.DoubleValue)
		}
	}
	return out
}

func number(dp *metricpb.NumberDataPoint) float64 {
	if v, ok := dp.GetValue().(*metricpb.NumberDataPoint_AsInt); ok {
		return float64(v.AsInt)
	}
	return dp.GetAsDouble()
}

func key(p Point) string {
	var b strings.Builder
	b.WriteString(p.Service + "|" + p.Role + "|" + p.Name)
	for _, k := range slices.Sorted(maps.Keys(p.Attrs)) {
		b.WriteString("|" + k + "=" + p.Attrs[k])
	}
	return b.String()
}
