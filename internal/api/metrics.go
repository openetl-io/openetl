package api

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/openetl-io/openetl/internal/store/db"
)

// freshnessEvery is how often the freshness gauge reads the store, however
// often it's collected.
const freshnessEvery = 15 * time.Second

// RegisterMetrics adds the server's gauges: openetl.pipeline.freshness, the
// time since each pipeline's latest commit that the checkpoint mirror holds.
// Unregister it when the server stops.
func (s *Server) RegisterMetrics() (metric.Registration, error) {
	meter := otel.Meter("github.com/openetl-io/openetl/internal/api")
	fresh, err := meter.Float64ObservableGauge("openetl.pipeline.freshness", metric.WithUnit("s"),
		metric.WithDescription("Time since the pipeline's latest commit; pipelines without one in the last 30 days are left out"))
	if err != nil {
		return nil, err
	}
	var (
		mu   sync.Mutex
		read time.Time
		rows []db.LastCommitsRow
	)
	return meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		mu.Lock()
		defer mu.Unlock()
		if time.Since(read) >= freshnessEvery {
			qctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			r, err := s.Store.LastCommits(qctx, s.Tenant.Org)
			cancel()
			if err != nil {
				s.log().Warn("reading pipelines' last commits for metrics failed", "err", err)
			} else {
				rows, read = r, time.Now()
			}
		}
		org := attribute.String("openetl.org", s.Tenant.Org.String())
		for _, r := range rows {
			o.ObserveFloat64(fresh, time.Since(r.CommitTime).Seconds(), metric.WithAttributes(org,
				attribute.String("openetl.workspace", r.WorkspaceID.String()), attribute.String("openetl.pipeline", r.PipelineID.String())))
		}
		return nil
	}, fresh)
}
