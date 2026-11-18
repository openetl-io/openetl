package runner

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/platform/ints"
)

var (
	operatorRowsIn, _       = meter.Int64Counter("openetl.operator.rows.in", metric.WithUnit("{row}"), metric.WithDescription("Rows a stateful operator took in, in committed epochs"))
	operatorRowsWritten, _  = meter.Int64Counter("openetl.operator.rows.written", metric.WithUnit("{row}"), metric.WithDescription("Rows a stateful operator wrote to its output in committed epochs, by op"))
	operatorRowsLate, _     = meter.Int64Counter("openetl.operator.rows.late", metric.WithUnit("{row}"), metric.WithDescription("Rows a stateful operator quarantined as late in committed epochs"))
	operatorRowsRepeats, _  = meter.Int64Counter("openetl.operator.rows.repeats", metric.WithUnit("{row}"), metric.WithDescription("Rows a dedupe dropped in committed epochs: repeats of a key it keeps, or rows no later than a key's latest"))
	operatorWindows, _      = meter.Int64Gauge("openetl.operator.windows", metric.WithUnit("{window}"), metric.WithDescription("Windows whose state a stateful operator kept at its last commit"))
	operatorWatermark, _    = meter.Float64Gauge("openetl.operator.watermark", metric.WithUnit("s"), metric.WithDescription("A stateful operator's watermark at its last commit, in seconds since the Unix epoch"))
	operatorWatermarkLag, _ = meter.Float64Gauge("openetl.operator.watermark.lag", metric.WithUnit("s"), metric.WithDescription("How far a stateful operator's watermark was behind its last commit"))
	operatorStateMemory, _  = meter.Int64Gauge("openetl.operator.state.memory", metric.WithUnit("By"), metric.WithDescription("What a stateful operator's state held in memory at its last commit"))
	operatorStateStored, _  = meter.Int64Gauge("openetl.operator.state.stored", metric.WithUnit("By"), metric.WithDescription("The bytes on object storage of the state that a stateful operator's last checkpoint names"))
	operatorStateCarried, _ = meter.Int64Gauge("openetl.operator.state.checkpoint", metric.WithUnit("By"), metric.WithDescription("The bytes of a stateful operator's state that its last checkpoint record carries itself"))
	operatorRequests, _     = meter.Int64Counter("openetl.operator.state.requests", metric.WithUnit("{request}"), metric.WithDescription("A stateful operator's requests to object storage in committed epochs, by kind"))
	operatorRequestBytes, _ = meter.Int64Counter("openetl.operator.state.request.bytes", metric.WithUnit("By"), metric.WithDescription("The bytes of a stateful operator's requests to object storage in committed epochs, by kind"))
	operatorDiskReads, _    = meter.Int64Counter("openetl.operator.state.disk.reads", metric.WithUnit("{read}"), metric.WithDescription("A stateful operator's reads of state that the runner's disk cache answered, in place of object storage, in committed epochs"))
	operatorDiskBytes, _    = meter.Int64Counter("openetl.operator.state.disk.read.bytes", metric.WithUnit("By"), metric.WithDescription("The bytes of a stateful operator's reads of state that the runner's disk cache answered in committed epochs"))
)

// measureOperator records what o did in the epoch of a record made at now.
func measureOperator(ctx context.Context, org, pipeline string, o checkpoint.Operator, now time.Time) {
	kv := []attribute.KeyValue{attribute.String("openetl.org", org), attribute.String("openetl.pipeline", pipeline), attribute.String("openetl.operator", o.Operator)}
	attrs := metric.WithAttributes(kv...)
	with := func(k, v string) metric.MeasurementOption {
		return metric.WithAttributes(append(kv[:len(kv):len(kv)], attribute.String(k, v))...)
	}
	add := func(c metric.Int64Counter, n uint64, opt metric.MeasurementOption) {
		if n > 0 {
			c.Add(ctx, ints.Int64Clamped(n), opt)
		}
	}
	add(operatorRowsIn, o.RowsIn, attrs)
	add(operatorRowsWritten, o.Inserts, with("openetl.op", "insert"))
	add(operatorRowsWritten, o.Updates, with("openetl.op", "update"))
	add(operatorRowsWritten, o.Deletes, with("openetl.op", "delete"))
	add(operatorRowsLate, o.Late, attrs)
	add(operatorRowsRepeats, o.Repeats, attrs)
	kinds := map[string]checkpoint.Requests{"barrier": o.Barrier, "lookup": o.Lookup, "restore": o.Restore, "merge_write": o.MergeWrite, "merge_read": o.MergeRead}
	for kind, r := range kinds {
		add(operatorRequests, r.Count, with("openetl.request.kind", kind))
		add(operatorRequestBytes, r.Bytes, with("openetl.request.kind", kind))
	}
	add(operatorDiskReads, o.Disk.Count, attrs)
	add(operatorDiskBytes, o.Disk.Bytes, attrs)
	operatorWindows.Record(ctx, o.Windows, attrs)
	operatorStateMemory.Record(ctx, ints.Int64Clamped(o.MemoryBytes), attrs)
	operatorStateStored.Record(ctx, ints.Int64Clamped(o.StoredBytes), attrs)
	operatorStateCarried.Record(ctx, ints.Int64Clamped(o.CheckpointBytes), attrs)
	// The end of input has no time.
	if o.Watermark != nil && *o.Watermark != EndOfInput {
		operatorWatermark.Record(ctx, float64(*o.Watermark)/1e6, attrs)
		operatorWatermarkLag.Record(ctx, now.Sub(time.UnixMicro(*o.Watermark)).Seconds(), attrs)
	}
}
