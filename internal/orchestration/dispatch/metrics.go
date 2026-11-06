package dispatch

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/unsuitable001/openetl/internal/platform/resources"
	"github.com/unsuitable001/openetl/internal/store/db"
)

// metricsEvery is how often the placer's gauges read the store, however often
// they're collected.
const metricsEvery = 10 * time.Second

// RegisterMetrics adds the placer's gauges, by data plane and pool, and for
// demand by size, which autoscalers scale on (ADR-0027 §7). Unregister it when
// the orchestrator stops.
func (d *Dispatcher) RegisterMetrics() (metric.Registration, error) {
	meter := otel.Meter("github.com/unsuitable001/openetl/internal/orchestration/dispatch")
	gauge := func(name, unit, description string) (metric.Float64ObservableGauge, error) {
		return meter.Float64ObservableGauge(name, metric.WithUnit(unit), metric.WithDescription(description))
	}
	cpuCapacity, err := gauge("openetl.placement.cpu.capacity", "{vCPU}", "CPU runners reporting now can place")
	if err != nil {
		return nil, err
	}
	cpuReserved, err := gauge("openetl.placement.cpu.reserved", "{vCPU}", "CPU of attempts placed or running")
	if err != nil {
		return nil, err
	}
	cpuPending, err := gauge("openetl.placement.cpu.pending", "{vCPU}", "CPU of attempts waiting for room")
	if err != nil {
		return nil, err
	}
	memCapacity, err := gauge("openetl.placement.memory.capacity", "By", "Memory runners reporting now can place")
	if err != nil {
		return nil, err
	}
	memReserved, err := gauge("openetl.placement.memory.reserved", "By", "Memory of attempts placed or running")
	if err != nil {
		return nil, err
	}
	memPending, err := gauge("openetl.placement.memory.pending", "By", "Memory of attempts waiting for room")
	if err != nil {
		return nil, err
	}
	age, err := gauge("openetl.placement.pending.age", "s", "How long the oldest attempt waiting for room has waited")
	if err != nil {
		return nil, err
	}
	var (
		mu       sync.Mutex
		read     time.Time
		capacity []db.PlacementCapacityRow
		demand   []db.PlacementDemandRow
	)
	return meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		mu.Lock()
		defer mu.Unlock()
		if time.Since(read) >= metricsEvery {
			qctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			c, de, err := d.readMetrics(qctx)
			if err != nil {
				d.log().Warn("reading placement for metrics failed", "err", err)
			} else {
				capacity, demand, read = c, de, time.Now()
			}
		}
		for _, r := range capacity {
			attrs := metric.WithAttributes(attribute.String("data_plane", r.DataPlaneID), attribute.String("pool", r.Pool))
			o.ObserveFloat64(cpuCapacity, float64(r.Cpu)/1000, attrs)
			o.ObserveFloat64(memCapacity, float64(r.Memory), attrs)
		}
		for _, r := range demand {
			size := resources.Size{CPU: r.SizeCpu, Memory: r.SizeMemory}
			attrs := metric.WithAttributes(attribute.String("data_plane", r.DataPlaneID), attribute.String("pool", r.Pool), attribute.String("size", size.Name()))
			cpu, mem := float64(r.Attempts*r.SizeCpu)/1000, float64(r.Attempts*r.SizeMemory)
			if r.State == "pending" {
				o.ObserveFloat64(cpuPending, cpu, attrs)
				o.ObserveFloat64(memPending, mem, attrs)
				o.ObserveFloat64(age, r.OldestSeconds, attrs)
			} else {
				o.ObserveFloat64(cpuReserved, cpu, attrs)
				o.ObserveFloat64(memReserved, mem, attrs)
			}
		}
		return nil
	}, cpuCapacity, cpuReserved, cpuPending, memCapacity, memReserved, memPending, age)
}

func (d *Dispatcher) readMetrics(ctx context.Context) ([]db.PlacementCapacityRow, []db.PlacementDemandRow, error) {
	orgs, err := d.Store.ListOrgs(ctx)
	if err != nil {
		return nil, nil, err
	}
	var capacity []db.PlacementCapacityRow
	var demand []db.PlacementDemandRow
	for _, org := range orgs {
		c, err := d.Store.PlacementCapacity(ctx, db.PlacementCapacityParams{OrgID: org, LapseMs: d.lapse().Milliseconds()})
		if err != nil {
			return nil, nil, err
		}
		de, err := d.Store.PlacementDemand(ctx, org)
		if err != nil {
			return nil, nil, err
		}
		capacity, demand = append(capacity, c...), append(demand, de...)
	}
	return capacity, demand, nil
}
