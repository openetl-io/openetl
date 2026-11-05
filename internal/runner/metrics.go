package runner

import (
	"context"
	"slices"
	"strings"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/unsuitable001/openetl/sdk/go/source"
)

var (
	meter              = otel.Meter("github.com/unsuitable001/openetl/internal/runner")
	tracer             = otel.Tracer("github.com/unsuitable001/openetl/internal/runner")
	rowsCommitted, _   = meter.Int64Counter("openetl.job.rows.committed", metric.WithUnit("{row}"), metric.WithDescription("Rows in committed epochs, quarantined ones included"))
	rowsQuarantined, _ = meter.Int64Counter("openetl.job.rows.quarantined", metric.WithUnit("{row}"), metric.WithDescription("Rows quarantined in committed epochs"))
	rowsCollapsed, _   = meter.Int64Counter("openetl.job.rows.collapsed", metric.WithUnit("{row}"), metric.WithDescription("Rows the destination's apply didn't insert because of dedupe"))
	epochs, _          = meter.Int64Counter("openetl.job.epochs", metric.WithUnit("{epoch}"), metric.WithDescription("Epochs committed"))
	epochLength, _     = meter.Float64Histogram("openetl.job.epoch.duration", metric.WithUnit("s"), metric.WithDescription("From an epoch's barrier request until it committed"))
	commitLength, _    = meter.Float64Histogram("openetl.job.commit.duration", metric.WithUnit("s"), metric.WithDescription("How long an epoch's commit took: its checkpoint record, schemas and the destination's commit"))
	lagBytes, _        = meter.Int64ObservableGauge("openetl.job.lag.bytes", metric.WithUnit("By"), metric.WithDescription("What the source holds that the read hasn't acknowledged, as the source last reported"))
	lagRecords, _      = meter.Int64ObservableGauge("openetl.job.lag.records", metric.WithUnit("{record}"), metric.WithDescription("Records the source holds that the read hasn't acknowledged, as the source last reported"))
	lagTime, _         = meter.Float64ObservableGauge("openetl.job.lag", metric.WithUnit("s"), metric.WithDescription("How far behind its source in time the read is, as the source last reported"))
	retentionBytes, _  = meter.Int64ObservableGauge("openetl.job.retention_left.bytes", metric.WithUnit("By"), metric.WithDescription("How much more the source can take before it discards what the read hasn't acknowledged"))
	retentionTime, _   = meter.Float64ObservableGauge("openetl.job.retention_left", metric.WithUnit("s"), metric.WithDescription("How long the source waits before it discards what the read hasn't acknowledged"))
	cellsHeld, _       = meter.Int64Counter("openetl.job.cells.held", metric.WithUnit("{cell}"), metric.WithDescription("Cells held back in committed epochs, by stream and column, and a held column's values"))
	heldBytes, _       = meter.Int64Counter("openetl.job.held.bytes", metric.WithUnit("By"), metric.WithDescription("Held values written in committed epochs"))
	holdAge, _         = meter.Float64Gauge("openetl.job.hold.age", metric.WithUnit("s"), metric.WithDescription("How long each open hold has been held, by stream and column, at each commit"))
)

func endSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

// Running jobs' progress, for the gauges.
var live sync.Map // *Progress → struct{}

func init() {
	_, _ = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		live.Range(func(k, _ any) bool {
			p := k.(*Progress)
			for _, r := range p.Latest() {
				attrs := p.attrs
				if r.Stream != "" {
					attrs = append(slices.Clip(attrs), attribute.String("openetl.stream", r.Stream))
				}
				opt := metric.WithAttributes(attrs...)
				if r.LagBytes != nil {
					o.ObserveInt64(lagBytes, int64(min(*r.LagBytes, 1<<63-1)), opt)
				}
				if r.LagRecords != nil {
					o.ObserveInt64(lagRecords, int64(min(*r.LagRecords, 1<<63-1)), opt)
				}
				if r.Lag != nil {
					o.ObserveFloat64(lagTime, r.Lag.Seconds(), opt)
				}
				if r.RetentionLeftBytes != nil {
					o.ObserveInt64(retentionBytes, int64(min(*r.RetentionLeftBytes, 1<<63-1)), opt)
				}
				if r.RetentionLeft != nil {
					o.ObserveFloat64(retentionTime, r.RetentionLeft.Seconds(), opt)
				}
			}
			return true
		})
		return nil
	}, lagBytes, lagRecords, lagTime, retentionBytes, retentionTime)
}

// Progress keeps what a job's read last reported of its progress, by
// stream. Its methods may be called concurrently.
type Progress struct {
	attrs  []attribute.KeyValue
	mu     sync.Mutex
	latest map[string]source.Progress
}

// NewProgress makes a job's progress, whose gauges carry attrs while the job
// runs.
func NewProgress(attrs ...attribute.KeyValue) *Progress {
	return &Progress{attrs: attrs, latest: map[string]source.Progress{}}
}

func (p *Progress) set(r source.Progress) {
	p.mu.Lock()
	p.latest[r.Stream] = r
	p.mu.Unlock()
}

// Latest returns each stream's latest report, by stream.
func (p *Progress) Latest() []source.Progress {
	p.mu.Lock()
	out := make([]source.Progress, 0, len(p.latest))
	for _, r := range p.latest {
		out = append(out, r)
	}
	p.mu.Unlock()
	slices.SortFunc(out, func(a, b source.Progress) int { return strings.Compare(a.Stream, b.Stream) })
	return out
}
