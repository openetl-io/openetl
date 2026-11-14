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
// and Iceberg. Then keyed CDC streams on workers: merged into Iceberg, with
// held columns into Postgres, and through a SQL transform, through a
// worker's kill and the coordinator's. Every row lands once.
func TestChaosWorkers(t *testing.T) {
	s := newChaosSuite(t, 1, 1)
	faults := []string{
		faultWorker, "crash " + crashWorkerHead, "crash " + crashWorkerCommit,
		faultKill, "crash " + crashHead, "crash " + crashCommit,
	}
	var configs []chaosConfig
	for i, f := range faults {
		configs = append(configs, chaosConfig{
			i: i, seed: 1, source: "kafka", continuous: true, write: "append", dest: []string{"postgres", "iceberg"}[i%2], scope: "stream", interval: "1s",
			size: "m", pods: true, workers: 2, faults: []string{f}, writeFor: 30 * time.Second, strict: true,
		})
	}
	for _, k := range []struct {
		dest, write, transform string
		holds                  bool
		fault                  string
	}{
		{"iceberg", "merge", "", false, faultWorker},
		{"postgres", "merge", "", true, faultKill},
		{"postgres", "merge", "sql", false, faultWorker},
	} {
		configs = append(configs, chaosConfig{
			i: len(configs), seed: 1, source: "cdc", continuous: true, write: k.write, dest: k.dest, transform: k.transform, holds: k.holds,
			scope: "stream", interval: "1s", size: "m", pods: true, workers: 2, faults: []string{k.fault}, writeFor: 30 * time.Second, strict: true,
		})
	}
	sem := make(chan struct{}, *chaosParallel)
	for _, c := range configs {
		f := c.faults[0]
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
