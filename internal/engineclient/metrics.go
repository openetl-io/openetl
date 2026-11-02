package engineclient

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	enginev1 "github.com/unsuitable001/openetl/gen/openetl/engine/v1"
)

var (
	meter           = otel.Meter("github.com/unsuitable001/openetl/internal/engineclient")
	engines, _      = meter.Int64UpDownCounter("openetl.engine.processes", metric.WithUnit("{process}"), metric.WithDescription("Engine processes running"))
	engineCPU, _    = meter.Float64Counter("openetl.engine.cpu.time", metric.WithUnit("s"), metric.WithDescription("CPU time of engines that exited, by mode"))
	enginePeak, _   = meter.Int64Histogram("openetl.engine.memory.peak", metric.WithUnit("By"), metric.WithDescription("Peak resident memory of engines that exited, where it can be read"))
	engineMemory, _ = meter.Int64Gauge("openetl.engine.memory", metric.WithUnit("By"), metric.WithDescription("A running engine's resident memory, as it last reported"))
	edgeBytes, _    = meter.Int64Counter("openetl.engine.edge.bytes", metric.WithUnit("By"), metric.WithDescription("Frames' bytes across the runner/engine boundary, by direction"))
	rowsIn, _       = meter.Int64Counter("openetl.job.rows.in", metric.WithUnit("{row}"), metric.WithDescription("Rows the engine received from the source"))
	bytesIn, _      = meter.Int64Counter("openetl.job.bytes.in", metric.WithUnit("By"), metric.WithDescription("Bytes of batches the engine received from the source"))
	rowsOut, _      = meter.Int64Counter("openetl.job.rows.out", metric.WithUnit("{row}"), metric.WithDescription("Rows the engine passed to the sink"))
	bytesOut, _     = meter.Int64Counter("openetl.job.bytes.out", metric.WithUnit("By"), metric.WithDescription("Bytes of batches the engine passed to the sink"))
)

var (
	toEngine   = metric.WithAttributes(attribute.String("openetl.edge.direction", "to_engine"))
	fromEngine = metric.WithAttributes(attribute.String("openetl.edge.direction", "from_engine"))
	userCPU    = metric.WithAttributes(attribute.String("openetl.cpu.mode", "user"))
	systemCPU  = metric.WithAttributes(attribute.String("openetl.cpu.mode", "system"))
)

// jobCounters turns the engine's cumulative counts into job metrics.
type jobCounters struct {
	attrs []attribute.KeyValue
	last  map[string]*enginev1.StreamMetrics
	opts  map[string]metric.MeasurementOption
}

func newJobCounters(org, pipeline string) *jobCounters {
	return &jobCounters{
		attrs: []attribute.KeyValue{attribute.String("openetl.org", org), attribute.String("openetl.pipeline", pipeline)},
		last:  map[string]*enginev1.StreamMetrics{},
		opts:  map[string]metric.MeasurementOption{},
	}
}

func (j *jobCounters) record(ctx context.Context, m *enginev1.Metrics) {
	if m.GetMemoryBytes() > 0 {
		engineMemory.Record(ctx, int64(min(m.GetMemoryBytes(), 1<<63-1)), metric.WithAttributes(j.attrs...))
	}
	for _, s := range m.GetStreams() {
		opt, ok := j.opts[s.GetStream()]
		if !ok {
			opt = metric.WithAttributes(append(j.attrs[:len(j.attrs):len(j.attrs)], attribute.String("openetl.stream", s.GetStream()))...)
			j.opts[s.GetStream()] = opt
		}
		prev := j.last[s.GetStream()]
		add := func(c metric.Int64Counter, cur, was uint64) {
			if cur > was {
				c.Add(ctx, int64(cur-was), opt) //nolint:gosec // deltas are small
			}
		}
		add(rowsIn, s.GetRowsIn(), prev.GetRowsIn())
		add(bytesIn, s.GetBytesIn(), prev.GetBytesIn())
		add(rowsOut, s.GetRowsOut(), prev.GetRowsOut())
		add(bytesOut, s.GetBytesOut(), prev.GetBytesOut())
		j.last[s.GetStream()] = s
	}
}
