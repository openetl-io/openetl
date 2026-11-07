package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/jackc/pgx/v5"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/engineclient"
	"github.com/openetl-io/openetl/internal/enginesink"
	"github.com/openetl-io/openetl/internal/platform/resources"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// A lane's memory, as the presets give a vCPU.
const laneMemory = 2 << 30

// The gate for vertical scaling: a share of linear throughput at each count.
const linearGate = 0.7

// rawRows emits the same JSON rows over and over on as many splits as it has,
// so what's measured is the engine's raw lane.
type rawRows struct {
	splits int
	chunk  []json.RawMessage
	size   int64
	total  int64
	sent   atomic.Int64
}

func newRawRows(splits int, total int64) *rawRows {
	r := &rawRows{splits: splits, total: total}
	for i := range 2000 {
		b := fmt.Appendf(nil, `{"id": %d, "customer": %d, "sku": "SKU-%08d", "description": "%032x%032x", "quantity": %d, "price": %d.%02d, "discount": 0.%d, "active": %t, "created_at": "2026-01-01T00:%02d:%02dZ"}`,
			i, i*7919%1000000, i, i*31, i*131, i%50+1, i%1000, i%100, i%1000, i%3 != 0, i%60, i%60)
		r.chunk = append(r.chunk, b)
		r.size += int64(len(b))
	}
	return r
}

func (r *rawRows) Discover(context.Context) (source.Catalog, error) { return source.Catalog{}, nil }

func (r *rawRows) Splits(context.Context, source.ConfiguredStream, []source.Split) ([]source.Split, error) {
	out := make([]source.Split, r.splits)
	for i := range out {
		out[i] = source.Split{ID: strconv.Itoa(i)}
	}
	return out, nil
}

func (r *rawRows) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	return source.ReadSplits(ctx, req, out, func(_ context.Context, _ source.ConfiguredStream, _ source.Split, e source.SplitEmitter) error {
		for r.sent.Add(r.size) <= r.total {
			if err := e.Rows(r.chunk); err != nil {
				return err
			}
			if err := e.ResumePoint([]byte("x")); err != nil {
				return err
			}
		}
		return e.Complete(nil)
	})
}

// countedSplits counts what a source that splits streams emits.
type countedSplits struct {
	counted
	splitter source.Splitter
}

func (c *countedSplits) Splits(ctx context.Context, s source.ConfiguredStream, ended []source.Split) ([]source.Split, error) {
	return c.splitter.Splits(ctx, s, ended)
}

// lanesAttempt runs one attempt at this many lanes, and returns the bytes it
// read.
func lanesAttempt(ctx context.Context, name string, src source.Source, streams []runner.Stream, dst destination.Destination, flow runner.Dataflow, ns string, lanes int) (int64, error) {
	c := &countedSplits{counted: counted{Reader: src}}
	var read source.Reader = &c.counted
	if s, ok := src.(source.Splitter); ok {
		c.splitter = s
		read = c
	}
	settings := settings(dst.Capabilities())
	_, err := runner.RunAttempt(ctx, runner.Job{
		Owner:       rand.Text(),
		Pipeline:    name,
		Log:         checkpoint.NewLog(checkpoint.NewMemStore(), "bench", name),
		Source:      read,
		Streams:     streams,
		Destination: dst,
		Namespace:   ns,
		Dataflow:    flow,
		Settings:    settings,
		Lanes:       lanes,
	})
	return c.bytes.Load(), err
}

// manyWriters is the engine's discard sink, with a writer lane per lane.
type manyWriters struct{ engineDiscard }

func (manyWriters) Capabilities() destination.Capabilities {
	c := engineDiscard{}.Capabilities()
	c.WriterLanes = 64
	return c
}

// measureLanes runs the raw lane, and Postgres into Parquet and Iceberg, at
// each count of lanes.
func measureLanes(ctx context.Context, conn *pgx.Conn, bin string, counts []int) error {
	// Splits come from the key's histogram.
	if _, err := conn.Exec(ctx, "ANALYZE "+pgx.Identifier{schema, table}.Sanitize()); err != nil {
		return err
	}
	b := &bench{runs: *runsFlag}
	type line struct {
		name  string
		lanes int
		p     *phase
	}
	var lines []line
	add := func(name string, lanes int, run func(context.Context) (int64, error)) error {
		p, err := b.add(ctx, fmt.Sprintf("%s, %d lanes", name, lanes), step{run: run})
		if err != nil {
			return err
		}
		lines = append(lines, line{name, lanes, p})
		return nil
	}
	for _, n := range counts {
		size := resources.Size{CPU: int64(n) * 1000, Memory: int64(n) * laneMemory}
		flow := &engineclient.Dataflow{Options: engineclient.Options{Binary: bin, PeakMemory: b.enginePeak, Size: size}}
		err := add("raw rows, discarded", n, func(ctx context.Context) (int64, error) {
			streams := []runner.Stream{{Name: table, Replay: source.Exact, Mode: destination.Append}}
			return lanesAttempt(ctx, "raw", newRawRows(n, int64(n)*(*laneBytes)), streams, manyWriters{}, flow, "", n)
		})
		if err != nil {
			return err
		}
		env := connectorhost.Env{Connectors: *connFlag, Size: size}
		config := fmt.Appendf(nil, `{"dsn": %q, "schema": %q, "tables": [%q], "maxConnections": %d}`, *dsnFlag, schema, table, n)
		fromPostgres := func(ctx context.Context, name string, dst destination.Destination, ns string) (int64, error) {
			src, err := connectorhost.NewSource("postgres", config, env)
			if err != nil {
				return 0, err
			}
			defer connectorhost.Close(src)
			if err := connectorhost.Start(ctx, src); err != nil {
				return 0, err
			}
			cat, err := src.Discover(ctx)
			if err != nil {
				return 0, err
			}
			var streams []runner.Stream
			for _, s := range cat.Streams {
				streams = append(streams, runner.Stream{Name: s.Name, Replay: s.Replay, Mode: destination.Append, PrimaryKey: s.PrimaryKey, SplitsByKey: s.SplitsByKey, Schema: s.Schema})
			}
			return lanesAttempt(ctx, name, src, streams, dst, flow, ns, n)
		}
		err = add("postgres into parquet", n, func(ctx context.Context) (int64, error) {
			dir, err := os.MkdirTemp("", "bench-lanes-")
			if err != nil {
				return 0, err
			}
			defer func() { _ = os.RemoveAll(dir) }()
			dst, err := enginesink.NewParquet(fmt.Appendf(nil, `{"url": %q}`, "file://"+filepath.ToSlash(dir)))
			if err != nil {
				return 0, err
			}
			return fromPostgres(ctx, "parquet", dst, "")
		})
		if err != nil {
			return err
		}
		if *icebergFlag == "" {
			continue
		}
		err = add("postgres into iceberg", n, func(ctx context.Context) (int64, error) {
			dst, err := connectorhost.NewDestination("iceberg", []byte(*icebergFlag), env)
			if err != nil {
				return 0, err
			}
			defer connectorhost.Close(dst)
			if err := connectorhost.Start(ctx, dst); err != nil {
				return 0, err
			}
			return fromPostgres(ctx, "iceberg", dst, "bench_"+strings.ToLower(rand.Text()[:10]))
		})
		if err != nil {
			return err
		}
	}
	b.print()
	fmt.Printf("\nthroughput against one lane (gate: %.1f of linear)\n", linearGate)
	base := map[string]float64{}
	for _, l := range lines {
		mbps := l.p.median(sample.mbps)
		if _, ok := base[l.name]; !ok {
			base[l.name] = mbps / float64(l.lanes)
		}
		share := mbps / (base[l.name] * float64(l.lanes))
		fmt.Printf("  %-26s %2d lanes  %6.0f MB/s  %.2f of linear\t%s\n", l.name, l.lanes, mbps, share, verdict(share >= linearGate))
	}
	return nil
}

func parseLanes(s string) ([]int, error) {
	var out []int
	for _, f := range strings.Split(s, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil || n < 1 {
			return nil, fmt.Errorf("-lanes %q: want counts such as 1,2,4,8", s)
		}
		out = append(out, n)
	}
	return out, nil
}
