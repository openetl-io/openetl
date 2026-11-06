package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/engineclient"
	"github.com/openetl-io/openetl/internal/enginesink"
	"github.com/openetl-io/openetl/internal/platform/resources"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

const (
	smallTable      = "small_items"
	smallRows       = 2_000_000
	neighbourRounds = 5
	// P1's exit: a large snapshot slows small syncs by at most this.
	neighbourGate = 0.20
)

// measureNeighbours times n small syncs at xs, alone and next to a large
// snapshot at l, in one process as on one runner.
func measureNeighbours(ctx context.Context, conn *pgx.Conn, bin string, n int) error {
	items, small := pgx.Identifier{schema, table}.Sanitize(), pgx.Identifier{schema, smallTable}.Sanitize()
	for _, q := range []string{
		"DROP TABLE IF EXISTS " + small,
		"CREATE TABLE " + small + " (LIKE " + items + " INCLUDING ALL)",
		"INSERT INTO " + small + " SELECT * FROM " + items + " ORDER BY id LIMIT " + strconv.Itoa(smallRows),
		"VACUUM (ANALYZE) " + small,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			return fmt.Errorf("the small table: %w", err)
		}
	}
	round := func(ctx context.Context) (time.Duration, error) {
		start := time.Now()
		errc := make(chan error, n)
		var wg sync.WaitGroup
		for range n {
			wg.Go(func() { errc <- syncToParquet(ctx, bin, smallTable, resources.XS) })
		}
		wg.Wait()
		close(errc)
		var all []error
		for err := range errc {
			all = append(all, err)
		}
		return time.Since(start), errors.Join(all...)
	}
	median := func(ctx context.Context) (time.Duration, error) {
		var took []time.Duration
		for range neighbourRounds {
			d, err := round(ctx)
			if err != nil {
				return 0, err
			}
			took = append(took, d)
		}
		slices.Sort(took)
		return took[len(took)/2], nil
	}

	alone, err := median(ctx)
	if err != nil {
		return err
	}
	largeCtx, stop := context.WithCancel(ctx)
	defer stop()
	var large sync.WaitGroup
	var largeErr error
	var snapshots int
	large.Go(func() {
		for largeCtx.Err() == nil {
			if err := syncToParquet(largeCtx, bin, table, resources.L); err != nil {
				if largeCtx.Err() == nil {
					largeErr = err
				}
				return
			}
			snapshots++
		}
	})
	time.Sleep(2 * time.Second)
	next, err := median(ctx)
	stop()
	large.Wait()
	if err != nil {
		return err
	}
	if largeErr != nil {
		return fmt.Errorf("the large snapshot: %w", largeErr)
	}
	slowdown := next.Seconds()/alone.Seconds() - 1
	fmt.Printf("neighbours: %d syncs of %d rows at xs at once, %d rounds, medians, limits off: %v\n", n, smallRows, neighbourRounds, *unlimited)
	fmt.Printf("  alone                    %.2f s\n", alone.Seconds())
	fmt.Printf("  next to snapshots at l   %.2f s (%d finished meanwhile)\n", next.Seconds(), snapshots)
	fmt.Printf("  slowdown                 %.0f%%\t≤ %.0f%%\t%s\n", 100*slowdown, 100*neighbourGate, verdict(slowdown <= neighbourGate))
	return nil
}

// syncToParquet runs one attempt of source-postgres → engine → Parquet at this
// size.
func syncToParquet(ctx context.Context, bin, from string, size resources.Size) error {
	if *unlimited {
		size = resources.Size{}
	}
	env := connectorhost.Env{Connectors: *connFlag, Size: size}
	src, err := connectorhost.NewSource("postgres", fmt.Appendf(nil, `{"dsn": %q, "schema": %q, "tables": [%q]}`, *dsnFlag, schema, from), env)
	if err != nil {
		return err
	}
	defer connectorhost.Close(src)
	if err := connectorhost.Start(ctx, src); err != nil {
		return err
	}
	cat, err := src.Discover(ctx)
	if err != nil {
		return err
	}
	var streams []runner.Stream
	for _, s := range cat.Streams {
		streams = append(streams, runner.Stream{Name: s.Name, Replay: s.Replay, Mode: destination.Append, PrimaryKey: s.PrimaryKey, Schema: s.Schema})
	}
	dir, err := os.MkdirTemp("", "bench-neighbours-")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	dst, err := enginesink.NewParquet(fmt.Appendf(nil, `{"url": %q}`, "file://"+filepath.ToSlash(dir)))
	if err != nil {
		return err
	}
	flow := &engineclient.Dataflow{Options: engineclient.Options{Binary: bin, Size: size}}
	_, err = attempt(ctx, "neighbours", src, streams, dst, flow, "")
	return err
}
