package dispatch

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/openetl-io/openetl/internal/platform/resources"
	"github.com/openetl-io/openetl/internal/store/db"
)

// metricsEvery is how often the placer's gauges read the store, however often
// they're collected.
const metricsEvery = 10 * time.Second

// RegisterMetrics adds the placer's gauges, by data plane and pool, and for
// demand by size and whether runners or pods run it, which autoscalers scale
// on. Unregister it when the orchestrator stops.
func (d *Dispatcher) RegisterMetrics() (metric.Registration, error) {
	meter := otel.Meter("github.com/openetl-io/openetl/internal/orchestration/dispatch")
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
		demands  []demand
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
				capacity, demands, read = c, de, time.Now()
			}
		}
		for _, r := range capacity {
			attrs := metric.WithAttributes(attribute.String("data_plane", r.DataPlaneID), attribute.String("pool", r.Pool))
			o.ObserveFloat64(cpuCapacity, float64(r.Cpu)/1000, attrs)
			o.ObserveFloat64(memCapacity, float64(r.Memory), attrs)
		}
		for _, de := range demands {
			attrs := metric.WithAttributes(attribute.String("data_plane", de.dataPlane), attribute.String("pool", de.pool),
				attribute.String("size", de.size.Name()), attribute.String("runs_on", de.runsOn()))
			cpu, mem := float64(de.held.CPU)/1000, float64(de.held.Memory)
			if de.pending {
				o.ObserveFloat64(cpuPending, cpu, attrs)
				o.ObserveFloat64(memPending, mem, attrs)
				o.ObserveFloat64(age, de.oldest, attrs)
			} else {
				o.ObserveFloat64(cpuReserved, cpu, attrs)
				o.ObserveFloat64(memReserved, mem, attrs)
			}
		}
		return nil
	}, cpuCapacity, cpuReserved, cpuPending, memCapacity, memReserved, memPending, age)
}

func (d *Dispatcher) readMetrics(ctx context.Context) ([]db.PlacementCapacityRow, []demand, error) {
	orgs, err := d.Store.ListOrgs(ctx)
	if err != nil {
		return nil, nil, err
	}
	var capacity []db.PlacementCapacityRow
	var demands []demand
	for _, org := range orgs {
		c, err := d.Store.PlacementCapacity(ctx, db.PlacementCapacityParams{OrgID: org, LapseMs: d.lapse().Milliseconds()})
		if err != nil {
			return nil, nil, err
		}
		rows, err := d.Store.PlacementDemand(ctx, org)
		if err != nil {
			return nil, nil, err
		}
		de, err := sumDemand(rows, func(dataPlane string) ([]Runner, error) {
			rows, err := d.Store.PlacementRunners(ctx, db.PlacementRunnersParams{OrgID: org, DataPlaneID: dataPlane, LapseMs: d.lapse().Milliseconds()})
			runners, _ := runnersOf(rows)
			return runners, err
		})
		if err != nil {
			return nil, nil, err
		}
		capacity, demands = append(capacity, c...), append(demands, de...)
	}
	return capacity, demands, nil
}

// demandKey is a gauge's series: its attributes. An attempt on workers counts
// under its workers' size.
type demandKey struct {
	dataPlane, pool string
	size            resources.Size
	pending, pod    bool
}

// demand is the attempts of a data plane, pool and size that are placed or
// running, or that wait for room, on runners or in pods.
type demand struct {
	demandKey
	attempts int64
	// What they hold between them: an attempt on workers, its workers' and
	// its coordinator's.
	held resources.Size
	// How long the oldest has waited, if they wait.
	oldest float64
}

func (d demand) runsOn() string {
	if d.pod {
		return "pod"
	}
	return "runner"
}

// sumDemand adds rows up by where their attempts run, so an autoscaler of
// runners sees their demand alone. An attempt waiting for room goes where the
// placer would send it among the data plane's runners now: one on workers
// goes to pods.
func sumDemand(rows []db.PlacementDemandRow, runners func(dataPlane string) ([]Runner, error)) ([]demand, error) {
	planes := map[string][]Runner{}
	at := map[demandKey]int{}
	var out []demand
	for _, r := range rows {
		k := demandKey{dataPlane: r.DataPlaneID, pool: r.Pool, size: resources.Size{CPU: r.SizeCpu, Memory: r.SizeMemory}, pending: r.State == "pending", pod: r.Pod}
		each := k.size
		if r.Workers > 0 {
			each = k.size.Times(int(r.Workers)).Add(resources.Size{CPU: r.CoordinatorCpu, Memory: r.CoordinatorMemory})
		}
		if k.pending && r.Workers > 0 {
			k.pod = true
		} else if k.pending {
			rs, ok := planes[k.dataPlane]
			if !ok {
				var err error
				if rs, err = runners(k.dataPlane); err != nil {
					return nil, err
				}
				planes[k.dataPlane] = rs
			}
			k.pod = ToPods(rs, k.pool, k.size)
		}
		i, ok := at[k]
		if !ok {
			i, at[k] = len(out), len(out)
			out = append(out, demand{demandKey: k})
		}
		out[i].attempts += r.Attempts
		out[i].held = out[i].held.Add(each.Times(int(r.Attempts)))
		if k.pending {
			out[i].oldest = max(out[i].oldest, r.OldestSeconds)
		}
	}
	return out, nil
}
