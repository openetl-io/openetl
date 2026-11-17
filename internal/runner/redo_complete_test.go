package runner_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/runner"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/host"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// rangesEnv makes this test binary serve servedRanges, as a connector's
// executable.
const rangesEnv = "OPENETL_TEST_SERVE_RANGES"

func TestMain(m *testing.M) {
	if os.Getenv(rangesEnv) != "" {
		sdk.Serve(servedRanges)
		return
	}
	os.Exit(m.Run())
}

// rangesConfig is servedRanges' config: a rangeSource's settings.
type rangesConfig struct {
	Splits     int  `json:"splits"`
	Rows       int  `json:"rows"`
	Page       int  `json:"page"`
	Grown      int  `json:"grown"`
	CompleteAt bool `json:"completeAt"`
	Flushes    bool `json:"flushes"`
}

// servedRanges is rangeSource as an external connector, which the runner
// reaches through the connector protocol.
var servedRanges = sdk.Connector{
	Name:         "ranges",
	ConfigSchema: []byte(`{"type": "object"}`),
	Egress:       sdk.Egress{Tier: sdk.EgressRaw},
	Splits:       true,
	Source: func(config []byte, _ host.Host) (source.Source, error) {
		var c rangesConfig
		if err := json.Unmarshal(config, &c); err != nil {
			return nil, err
		}
		src := flushing(c.Splits, c.Rows, c.Page, c.Grown, c.CompleteAt)()
		src.flushes = c.Flushes
		return discovered{src}, nil
	},
}

// flushing are rangeSources whose splits' last pages go with their
// completions, so that an epoch holds both, grown and stopping at a
// CompleteAt as asked.
func flushing(splits, rows, page, grown int, completeAt bool) func() *rangeSource {
	return func() *rangeSource {
		src := ranges(splits, rows, page)()
		src.grown, src.completeAt, src.flushes = grown, completeAt, true
		return src
	}
}

// discovered is a rangeSource with its catalog.
type discovered struct{ *rangeSource }

func (discovered) Discover(context.Context) (source.Catalog, error) {
	return source.Catalog{Streams: []source.CatalogStream{{Name: "s"}}}, nil
}

// completing is a commit hook of p's store that fails the commits, by
// writers of generation 1, of the lanes named of the epoch in which the
// last split completed, if those lanes hold rows of a split that completed
// in it, and runs first once before the first.
func completing(p *pool, lanes []int, first func()) func(uint64, laneMark) error {
	var once sync.Once
	return func(gen uint64, m laneMark) error {
		if gen != 1 || m.Gen != 1 || !slices.Contains(lanes, m.Lane) {
			return nil
		}
		rec, err := p.log.Latest(context.Background())
		if err != nil || rec == nil || rec.Epoch != m.N || !rec.Streams["s"].Complete || m.Lane >= len(rec.Coverage) {
			return nil
		}
		splits := rec.Splits("s")
		completed := slices.ContainsFunc(rec.Coverage[m.Lane].Splits, func(c checkpoint.CoveredSplit) bool {
			i := slices.IndexFunc(splits, func(sp checkpoint.SplitState) bool { return sp.ID == c.ID })
			return i >= 0 && splits[i].Complete
		})
		if !completed {
			return nil
		}
		once.Do(first)
		return fmt.Errorf("lane %d's commit of epoch %d failed", m.Lane, m.N)
	}
}

// completedIn are the splits that lanes of head hold rows of, and that
// completed in its epoch, as "split@state" at their completion.
func completedIn(t *testing.T, head *checkpoint.Record, lanes []int) []string {
	t.Helper()
	splits := head.Splits("s")
	var out []string
	for _, l := range lanes {
		for _, c := range head.Coverage[l].Splits {
			i := slices.IndexFunc(splits, func(sp checkpoint.SplitState) bool { return sp.ID == c.ID })
			if i >= 0 && splits[i].Complete && !slices.Contains(out, c.ID+"@"+string(splits[i].State)) {
				out = append(out, c.ID+"@"+string(splits[i].State))
			}
		}
	}
	if len(out) == 0 {
		t.Fatalf("lanes %v of epoch %d hold no split that completed in it", lanes, head.Epoch)
	}
	return out
}

// The worker lost between the sync's last record and its lanes' commits,
// where the record has the splits it read complete, after which the source
// moved on: the next attempt reads those splits again, asking the source to
// complete each where the record has it complete. A source that does
// writes the lanes' parts again as they were. One that reads on to the
// source's end completes elsewhere, and the attempt fails with
// POSITION_LOST, saying what to do, and commits nothing of the lanes. If
// the source hasn't moved on, one that reads to its end completes where
// the record has it, as before.
func TestARedoOfASplitThatCompleted(t *testing.T) {
	for name, c := range map[string]struct {
		grown      int
		completeAt bool
		lost       bool
	}{
		"the source stops":            {grown: 7, completeAt: true},
		"the source reads on":         {grown: 7, lost: true},
		"the source hasn't moved on":  {},
		"the source stops, unchanged": {completeAt: true},
	} {
		t.Run(name, func(t *testing.T) {
			s := newTxnStore(t)
			p := txnPool(t, s, flushing(6, 20, 5, 0, false))
			lanes := []int{0, 1}
			head := p.loseEpoch(t, s, 1, completing(p, lanes, func() { p.kill("a1", 0) }))
			want := completedIn(t, head, lanes)
			p.source = flushing(6, 20, 5, c.grown, c.completeAt)
			if !c.lost {
				p.settle(t, s, "a2", 2, head)
				s.check(t, 120)
				heldRows(t, s, head, lanes)
				p.mu.Lock()
				var asked []string
				for _, src := range p.sources["a2"] {
					src.mu.Lock()
					asked = append(asked, src.completions...)
					src.mu.Unlock()
				}
				p.mu.Unlock()
				for _, w := range want {
					if !slices.Contains(asked, w) {
						t.Errorf("split %s wasn't asked to complete there: %v", w, asked)
					}
				}
				return
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			_, err := runner.RunAttempt(ctx, p.workers("a2", 2))
			p.wait("a2")
			if errs.CodeOf(err) != errs.PositionLost || !strings.Contains(fmt.Sprint(err), "use a version of the source connector that completes a split where OpenETL asks") {
				t.Fatalf("the attempt ended with %v (%s)", err, errs.CodeOf(err))
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			for _, l := range lanes {
				if s.marked[laneMark{head.Generation, head.Epoch, l}] {
					t.Errorf("lane %d was committed", l)
				}
			}
		})
	}
}

// The redo of a split that completed reaches a source through the
// connector host and the connector protocol, as the runner's binary starts
// an external one: the source stops at the completion it's asked for, and
// the lanes' parts are written again as they were.
func TestARedoAsksAnExternalSourceToCompleteWhereItDid(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	script := fmt.Sprintf("#!/bin/sh\n%s=1 exec %q -test.run='^$'\n", rangesEnv, exe)
	if err := os.WriteFile(filepath.Join(dir, "source-ranges"), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var started []source.Source
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		for _, src := range started {
			if c, ok := src.(io.Closer); ok {
				_ = c.Close()
			}
		}
	})
	external := func(grown int) func() source.Reader {
		return func() source.Reader {
			cfg, _ := json.Marshal(rangesConfig{Splits: 6, Rows: 20, Page: 5, Grown: grown, CompleteAt: true, Flushes: true})
			src, err := connectorhost.NewSource("ranges", cfg, connectorhost.Env{Connectors: dir})
			if err != nil {
				t.Error(err)
				return noSource{t}
			}
			mu.Lock()
			started = append(started, src)
			mu.Unlock()
			return src
		}
	}
	s := newTxnStore(t)
	p := txnPool(t, s, ranges(6, 20, 5))
	p.reader = external(0)
	lanes := []int{0, 1}
	head := p.loseEpoch(t, s, 1, completing(p, lanes, func() { p.kill("a1", 0) }))
	completedIn(t, head, lanes)
	p.reader = external(7)
	p.settle(t, s, "a2", 2, head)
	s.check(t, 120)
	heldRows(t, s, head, lanes)
}

// heldRows fails unless a lane of head's epoch that was lost held rows,
// which the store compared with those its redo wrote.
func heldRows(t *testing.T, s *txnStore, head *checkpoint.Record, lanes []int) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	compared := 0
	for _, l := range lanes {
		m := laneMark{head.Generation, head.Epoch, l}
		if s.redone[m] && len(s.aborted[m]) > 0 {
			compared++
		}
	}
	if compared == 0 {
		t.Errorf("no lane of %v that held rows of epoch %d was written again", lanes, head.Epoch)
	}
}
