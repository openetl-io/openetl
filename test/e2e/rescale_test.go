//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
)

// A continuous job rescaled through the API stops at a checkpoint and starts
// again at the new size, on more lanes, and back once the spec has its say
// again. Every change lands once throughout.
func TestRescaleAContinuousJob(t *testing.T) {
	startRunner(t)
	name := unique("rescale")
	in := schema(name)
	run(t, "CREATE SCHEMA "+pgx.Identifier{in}.Sanitize())
	for _, table := range []string{"a", "b"} {
		tbl := pgx.Identifier{in, table}.Sanitize()
		run(t, "CREATE TABLE "+tbl+" (id bigint PRIMARY KEY, v int)",
			"INSERT INTO "+tbl+" SELECT i, 0 FROM generate_series(1, 1000) i")
	}
	src := fmt.Sprintf(`{"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}, "schema": %q, "tables": ["a", "b"], "slot": %q, "publication": %q}}`, in, "s_"+in, "p_"+in)
	dst := `{"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}}}`
	d := docs(name, src, dst, `{}`)
	d[2] = fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
		"spec": {"mode": "continuous", "source": {"ref": %q, "streams": [{"name": "a", "sync": "cdc"}, {"name": "b", "sync": "cdc"}]},
		"destination": {"ref": %q, "namespace": %q, "write": "merge"}, "delivery": {"commit": {"interval": "1s"}}}}`, name, name, name, in+"_out")
	apply(t, d...)
	full := "workspaces/default/pipelines/" + name
	t.Cleanup(func() {
		_, _ = pipelines().DeletePipeline(context.Background(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: full}))
	})
	sum := func() string {
		got, err := query[string](t, fmt.Sprintf("SELECT (SELECT count(*) || ' ' || sum(v) FROM %s) || ' ' || (SELECT count(*) || ' ' || sum(v) FROM %s)",
			pgx.Identifier{in + "_out", "a"}.Sanitize(), pgx.Identifier{in + "_out", "b"}.Sanitize()))
		if err != nil {
			return ""
		}
		return got
	}
	eventually(t, 2*time.Minute, "the snapshot", func() bool { return sum() == "1000 0 1000 0" })
	// The job's attempt with this reason, once its runner reports its lanes.
	running := func(reason apiv1.JobSizeReason) *apiv1.SyncAttempt {
		attempts := latestSync(t, name).GetAttempts()
		if len(attempts) == 0 {
			return nil
		}
		a := attempts[len(attempts)-1]
		if a.GetSizeReason() != reason || a.GetLanes() == nil {
			return nil
		}
		return a
	}

	if _, err := pipelines().RescalePipeline(t.Context(), connect.NewRequest(&apiv1.RescalePipelineRequest{Name: full, Size: &apiv1.JobSize{CpuMillis: 1 << 30, MemoryBytes: 1 << 50}})); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Errorf("a size no runner can take: %v", err)
	}
	res, err := pipelines().RescalePipeline(t.Context(), connect.NewRequest(&apiv1.RescalePipelineRequest{Name: full, Size: &apiv1.JobSize{Preset: "m"}}))
	if err != nil {
		t.Fatal(err)
	}
	if s := res.Msg.GetPipeline().GetStatus().GetScaling(); s.GetSize().GetPreset() != "m" || s.GetRescaleTime() == nil {
		t.Errorf("scaling %v", s)
	}
	for v := 1; v <= 3; v++ {
		run(t, fmt.Sprintf("UPDATE %s SET v = v + 1", pgx.Identifier{in, "a"}.Sanitize()),
			fmt.Sprintf("UPDATE %s SET v = v + 1", pgx.Identifier{in, "b"}.Sanitize()))
	}
	var rescaled *apiv1.SyncAttempt
	eventually(t, 2*time.Minute, "an attempt at m", func() bool {
		rescaled = running(apiv1.JobSizeReason_JOB_SIZE_REASON_RESCALE)
		return rescaled != nil
	})
	if rescaled.GetSize().GetPreset() != "m" || rescaled.GetLanes().GetLanes() != 2 {
		t.Errorf("the rescaled attempt %v", rescaled)
	}
	attempts := latestSync(t, name).GetAttempts()
	if before := attempts[len(attempts)-2]; before.GetCode() != "DRAINING" {
		t.Errorf("the attempt before the rescale ended with %q, not DRAINING", before.GetCode())
	}
	eventually(t, time.Minute, "the updates", func() bool { return sum() == "1000 3000 1000 3000" })

	res, err = pipelines().RescalePipeline(t.Context(), connect.NewRequest(&apiv1.RescalePipelineRequest{Name: full}))
	if err != nil {
		t.Fatal(err)
	}
	if s := res.Msg.GetPipeline().GetStatus().GetScaling(); s.GetSize() != nil {
		t.Errorf("scaling after auto %v", s)
	}
	run(t, fmt.Sprintf("UPDATE %s SET v = v + 1", pgx.Identifier{in, "a"}.Sanitize()))
	eventually(t, 2*time.Minute, "an attempt sized automatically", func() bool {
		a := latestSync(t, name).GetAttempts()
		last := a[len(a)-1]
		return len(a) > len(attempts) && last.GetSizeReason() != apiv1.JobSizeReason_JOB_SIZE_REASON_RESCALE && last.GetLanes() != nil
	})
	eventually(t, time.Minute, "the last update", func() bool { return sum() == "1000 4000 1000 3000" })
}
