package engineclient

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	enginev1 "github.com/openetl-io/openetl/gen/openetl/engine/v1"
	"github.com/openetl-io/openetl/internal/platform/resources"
)

var (
	meter           = otel.Meter("github.com/openetl-io/openetl/internal/engineclient")
	engines, _      = meter.Int64UpDownCounter("openetl.engine.processes", metric.WithUnit("{process}"), metric.WithDescription("Engine processes running"))
	engineCPU, _    = meter.Float64Counter("openetl.engine.cpu.time", metric.WithUnit("s"), metric.WithDescription("CPU time of engines that exited, by mode"))
	enginePeak, _   = meter.Int64Histogram("openetl.engine.memory.peak", metric.WithUnit("By"), metric.WithDescription("Peak resident memory of engines that exited, where it can be read"))
	engineMemory, _ = meter.Int64Gauge("openetl.engine.memory", metric.WithUnit("By"), metric.WithDescription("What a running engine's allocations hold, as it last reported"))
	edgeBytes, _    = meter.Int64Counter("openetl.engine.edge.bytes", metric.WithUnit("By"), metric.WithDescription("Frames' bytes across the runner/engine boundary, by direction"))
	rowsIn, _       = meter.Int64Counter("openetl.job.rows.in", metric.WithUnit("{row}"), metric.WithDescription("Rows the engine received from the source"))
	bytesIn, _      = meter.Int64Counter("openetl.job.bytes.in", metric.WithUnit("By"), metric.WithDescription("Bytes of batches the engine received from the source"))
	rowsOut, _      = meter.Int64Counter("openetl.job.rows.out", metric.WithUnit("{row}"), metric.WithDescription("Rows the engine passed to the sink"))
	bytesOut, _     = meter.Int64Counter("openetl.job.bytes.out", metric.WithUnit("By"), metric.WithDescription("Bytes of batches the engine passed to the sink"))
	laneTime, _     = meter.Float64Counter("openetl.lane.time", metric.WithUnit("s"), metric.WithDescription("Where a job's lane spent its time: busy, backpressured or idle"))
	laneRows, _     = meter.Int64Counter("openetl.lane.rows", metric.WithUnit("{row}"), metric.WithDescription("Rows a job's lane took in"))
)

var (
	toEngine   = metric.WithAttributes(attribute.String("openetl.edge.direction", "to_engine"))
	fromEngine = metric.WithAttributes(attribute.String("openetl.edge.direction", "from_engine"))
	userCPU    = metric.WithAttributes(attribute.String("openetl.cpu.mode", "user"))
	systemCPU  = metric.WithAttributes(attribute.String("openetl.cpu.mode", "system"))
)

// jobCounters turns the engine's cumulative counts into job metrics, and
// adds its lanes' to the attempt's meter.
type jobCounters struct {
	attrs []attribute.KeyValue
	last  map[string]*enginev1.StreamMetrics
	opts  map[string]metric.MeasurementOption
	meter *resources.Meter
	lanes []*enginev1.LaneMetrics
}

func newJobCounters(org, pipeline string, m *resources.Meter) *jobCounters {
	return &jobCounters{
		attrs: []attribute.KeyValue{attribute.String("openetl.org", org), attribute.String("openetl.pipeline", pipeline)},
		last:  map[string]*enginev1.StreamMetrics{},
		opts:  map[string]metric.MeasurementOption{},
		meter: m,
	}
}

// delta is what a cumulative count grew by.
func delta(cur, was uint64) int64 {
	if cur <= was {
		return 0
	}
	return int64(min(cur-was, 1<<63-1))
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
			if d := delta(cur, was); d > 0 {
				c.Add(ctx, d, opt)
			}
		}
		add(rowsIn, s.GetRowsIn(), prev.GetRowsIn())
		add(bytesIn, s.GetBytesIn(), prev.GetBytesIn())
		add(rowsOut, s.GetRowsOut(), prev.GetRowsOut())
		add(bytesOut, s.GetBytesOut(), prev.GetBytesOut())
		j.last[s.GetStream()] = s
	}
	j.lanes = j.recordLanes(ctx, m.GetLanes())
}

// recordLanes records what each lane did since the engine's last report.
func (j *jobCounters) recordLanes(ctx context.Context, lanes []*enginev1.LaneMetrics) []*enginev1.LaneMetrics {
	if len(lanes) == 0 {
		return j.lanes
	}
	var busy, backpressured, idle, rows int64
	for i, l := range lanes {
		var was *enginev1.LaneMetrics
		if i < len(j.lanes) {
			was = j.lanes[i]
		}
		b, p, d := delta(l.GetBusyNanos(), was.GetBusyNanos()), delta(l.GetBackpressuredNanos(), was.GetBackpressuredNanos()), delta(l.GetIdleNanos(), was.GetIdleNanos())
		r := delta(l.GetRowsIn(), was.GetRowsIn())
		busy, backpressured, idle, rows = busy+b, backpressured+p, idle+d, rows+r
		lane := attribute.Int("openetl.lane", i)
		for state, n := range map[string]int64{"busy": b, "backpressured": p, "idle": d} {
			if n > 0 {
				laneTime.Add(ctx, time.Duration(n).Seconds(), metric.WithAttributes(append(j.attrs[:len(j.attrs):len(j.attrs)], lane, attribute.String("openetl.lane.state", state))...))
			}
		}
		if r > 0 {
			laneRows.Add(ctx, r, metric.WithAttributes(append(j.attrs[:len(j.attrs):len(j.attrs)], lane)...))
		}
	}
	j.meter.AddLanes(time.Duration(busy), time.Duration(backpressured), time.Duration(idle), rows)
	return lanes
}
