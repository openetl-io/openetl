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
// worker's kill and the coordinator's; one worker reads the changes, and
// the exchange between the workers' engines sends each row to the writer
// lane owning its key group, so on three workers too, through a worker's
// kill and one at its record. Then keyed CDC streams into Kafka, whose
// writer lanes commit on their own, through a SQL transform on two workers
// and as they are on three. Every row lands once.
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
		workers                int
	}{
		{"iceberg", "merge", "", false, faultWorker, 2},
		{"postgres", "merge", "", true, faultKill, 2},
		{"postgres", "merge", "sql", false, faultWorker, 2},
		{"postgres", "merge", "", false, faultWorker, 3},
		{"iceberg", "merge", "", false, "crash " + crashWorkerHead, 3},
		{"kafka", "append", "sql", false, faultWorker, 2},
		{"kafka", "append", "", false, "crash " + crashWorkerCommit, 3},
	} {
		// Kafka commits a pipeline's streams together.
		scope := "stream"
		if k.dest == "kafka" {
			scope = "pipeline"
		}
		configs = append(configs, chaosConfig{
			i: len(configs), seed: 1, source: "cdc", continuous: true, write: k.write, dest: k.dest, transform: k.transform, holds: k.holds,
			scope: scope, interval: "1s", size: "m", pods: true, workers: k.workers, faults: []string{k.fault}, writeFor: 30 * time.Second, strict: true,
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
