//go:build e2e && chaos

package e2e

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

// TestChaosWorkers runs a continuous pipeline from Kafka's partitions on
// workers in pods through each kill of an attempt on workers, one a run: a
// worker killed mid-epoch, and between a record and its commit or during
// the commit; the coordinator likewise. The runs alternate between Postgres
// and Iceberg. Every row lands once.
func TestChaosWorkers(t *testing.T) {
	s := newChaosSuite(t, 1, 1)
	faults := []string{
		faultWorker, "crash " + crashWorkerHead, "crash " + crashWorkerCommit,
		faultKill, "crash " + crashHead, "crash " + crashCommit,
	}
	sem := make(chan struct{}, *chaosParallel)
	for i, f := range faults {
		c := chaosConfig{
			i: i, seed: 1, source: "kafka", continuous: true, write: "append", dest: []string{"postgres", "iceberg"}[i%2], scope: "stream", interval: "1s",
			size: "m", pods: true, workers: 2, faults: []string{f}, writeFor: 30 * time.Second, strict: true,
		}
		name := fmt.Sprintf("%s-%s", c.name(), strings.ReplaceAll(f, " ", "-"))
		if *chaosOnly != "" && !slices.ContainsFunc(strings.Split(*chaosOnly, ","), func(s string) bool { return strings.Contains(name, s) }) {
			continue
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			sem <- struct{}{}
			defer func() { <-sem }()
			s.run(t, c)
		})
	}
}
