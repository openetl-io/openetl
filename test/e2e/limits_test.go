//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
)

// sizedDocs is a scheduled sync of faker into Parquet, on the data plane, with
// these spec.resources.
func sizedDocs(t *testing.T, name, plane, faker, res string) []string {
	t.Helper()
	ds := docs(name, fmt.Sprintf(`{"connector": "faker", "config": %s}`, faker), parquetDest(t.TempDir()), `{}`)
	ds[2] = strings.Replace(ds[2], `"source":`, fmt.Sprintf(`"dataPlane": %q, "resources": %s, "source":`, plane, res), 1)
	t.Cleanup(func() {
		_, _ = pipelines().DeletePipeline(context.Background(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: "workspaces/default/pipelines/" + name}))
	})
	return ds
}

// A sync over its size's memory fails alone, and runs again one size up.
func TestJobOverItsMemoryRetriesOneSizeUp(t *testing.T) {
	plane, token := placementPlane(t)
	startPlaneRunner(t, plane, token, "4,8Gi")
	neighbour := unique("neighbour")
	sizedStreamOn(t, neighbour, plane, `{"size": "xs"}`)
	eventually(t, 60*time.Second, "the neighbour's first commit", func() bool { return committedEpochs(t, schema(neighbour)) > 0 })

	name := unique("over-memory")
	faker := `{"seed": 3, "count": 400000, "batchSize": 100000}`
	apply(t, sizedDocs(t, name, plane, faker, `{"size": {"cpu": 0.5, "memory": "32Mi"}, "max": "xs"}`)...)
	if status, stats := waitSync(t, name, 3*time.Minute); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	got := latestAttempts(t, name)
	if len(got) != 2 || got[0].code != "RESOURCE_EXHAUSTED" || got[1].why != "out_of_memory" || got[1].code != "" {
		t.Errorf("attempts %+v, want one out of memory, and one a size up", got)
	}

	if got := latestAttempts(t, neighbour); len(got) != 1 || got[0].code != "" {
		t.Errorf("the neighbour's attempts: %+v, want the one it started with", got)
	}
	before := committedEpochs(t, schema(neighbour))
	eventually(t, 30*time.Second, "the neighbour to keep committing", func() bool { return committedEpochs(t, schema(neighbour)) > before })
}

// Small syncs keep their pace next to a large snapshot. A laptop's timings only
// bear a loose check; the benchmark's -neighbours measures it on the reference
// node.
func TestSmallSyncsKeepTheirPace(t *testing.T) {
	plane, token := placementPlane(t)
	startPlaneRunner(t, plane, token, "8,16Gi")

	faker := `{"seed": 3, "count": 50000, "batchSize": 2000}`
	round := func() time.Duration {
		t.Helper()
		names := make([]string, 3)
		for i := range names {
			names[i] = unique("small")
			apply(t, sizedDocs(t, names[i], plane, faker, `{"size": "xs"}`)...)
		}
		var slowest time.Duration
		for _, name := range names {
			if status, stats := waitSync(t, name, 3*time.Minute); status != "succeeded" {
				t.Fatalf("sync %s: %s", status, stats)
			}
			secs := mustQuery[float64](t, `SELECT extract(epoch FROM s.end_time - s.start_time)::float8 FROM syncs s JOIN pipelines p ON p.id = s.pipeline_id
				WHERE p.name = $1 ORDER BY s.start_time DESC LIMIT 1`, name)
			slowest = max(slowest, time.Duration(secs*float64(time.Second)))
		}
		return slowest
	}
	alone := round()

	large := unique("large")
	apply(t, sizedDocs(t, large, plane, `{"seed": 3, "count": 200000000, "batchSize": 10000}`, `{"size": "l"}`)...)
	running := func() bool {
		n, err := query[int](t, "SELECT count(*) FROM dispatches WHERE data_plane_id = $1 AND kind = 'RunSyncAttempt' AND state = 'running'", plane)
		return err == nil && n == 1
	}
	eventually(t, 60*time.Second, "the large snapshot to start", running)
	time.Sleep(5 * time.Second)
	together := round()
	if !running() {
		t.Errorf("the large snapshot wasn't running to the end, so it didn't slow anything")
	}
	t.Logf("three small syncs took %v alone, and %v next to a large snapshot", alone, together)
	if together > 2*alone+10*time.Second {
		t.Errorf("small syncs took %v next to a large snapshot, and %v alone", together, alone)
	}
}
