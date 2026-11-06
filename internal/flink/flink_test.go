package flink

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/internal/spark"
)

func openArea(t *testing.T, location string) (*spark.Area, error) {
	a, err := spark.OpenDir(t.Context(), location, "org", "pay", Dir, "Flink")
	if err == nil {
		t.Cleanup(func() { _ = a.Close() })
	}
	return a, err
}

func TestJobIDs(t *testing.T) {
	if a := JobID("pay", 1); a != JobID("pay", 1) || a == JobID("pay", 2) || JobID("pay", 1) == JobID("pa", 1) || len(JobID("pay", 1)) != 32 {
		t.Error("job IDs aren't one per pipeline and generation")
	}
	if n := deploymentName(JobID("0a1b2c3d-4e5f-6789-abcd-ef0123456789", 12345)); len(n) > 45 {
		t.Errorf("deployment name %q is longer than Flink's cluster IDs may be", n)
	}
}

func touch(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointsAndSavepoints(t *testing.T) {
	_, srv := newFakeREST(t)
	c, store := newTestCluster(t, srv.URL, nil)
	area := filepath.Join(store, "org/pay/_flink")
	base := "file://" + filepath.ToSlash(area) + "/"
	job, old := JobID("pay", 2), JobID("pay", 1)
	for _, f := range []string{"chk-9/_metadata", "chk-10/_metadata", "chk-11/x", "shared/a", "taskowned/b"} {
		touch(t, filepath.Join(area, "checkpoints", job, f))
	}
	touch(t, filepath.Join(area, "checkpoints", old, "chk-3/_metadata"))
	got, err := c.Completed(t.Context(), job)
	if err != nil {
		t.Fatal(err)
	}
	if want := []runner.FlinkCheckpoint{{ID: 9, Path: base + "checkpoints/" + job + "/chk-9"}, {ID: 10, Path: base + "checkpoints/" + job + "/chk-10"}}; !slices.Equal(got, want) {
		t.Errorf("completed = %v, want %v", got, want)
	}
	if got, err := c.Completed(t.Context(), "none"); err != nil || len(got) != 0 {
		t.Errorf("a job without checkpoints: %v, %v", got, err)
	}

	dir := c.SavepointDir("g2-stop")
	if dir != base+"savepoints/g2-stop" {
		t.Errorf("savepoint dir = %s", dir)
	}
	if sp, err := c.Savepoint(t.Context(), dir); err != nil || sp != "" {
		t.Errorf("no savepoint yet: %q, %v", sp, err)
	}
	touch(t, filepath.Join(area, "savepoints/g2-stop/savepoint-abc123-0123456789ab/_metadata"))
	touch(t, filepath.Join(area, "savepoints/g1-stop/savepoint-def456-0123456789ab/_metadata"))
	if sp, err := c.Savepoint(t.Context(), dir); err != nil || sp != dir+"/savepoint-abc123-0123456789ab" {
		t.Errorf("savepoint = %q, %v", sp, err)
	}

	if err := c.Prune(t.Context(), []string{job, dir}); err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]bool{"checkpoints/" + job: true, "checkpoints/" + old: false, "savepoints/g2-stop": true, "savepoints/g1-stop": false} {
		if _, err := os.Stat(filepath.Join(area, path)); (err == nil) != want {
			t.Errorf("%s kept: %v, want %v", path, err == nil, want)
		}
	}
}

func TestAccessKeys(t *testing.T) {
	c := &Cluster{s: Settings{Cluster: &apiv1.FlinkClusterSpec{}, Transform: &apiv1.FlinkTransform{}, AccessKeyID: "AK", SecretAccessKey: "SK"}}
	var err error
	if c.area, err = openArea(t, "s3://lake/base?region=us-east-1&endpoint=http://127.0.0.1:1"); err != nil {
		t.Fatal(err)
	}
	conf := c.conf(&job{name: "n", id: "i"}, 0)
	if conf["fs.s3a.bucket.lake.access.key"] != "AK" || conf["fs.s3a.bucket.lake.secret.key"] != "SK" || conf["execution.checkpointing.dir"] != "s3a://lake/base/org/pay/_flink/checkpoints" ||
		conf["execution.checkpointing.interval"] != "1000 ms" {
		t.Errorf("conf = %v", conf)
	}
	c.s.AccessKeyID = ""
	if conf := c.conf(&job{}, 0); conf["execution.checkpointing.dir"] != "s3://lake/base/org/pay/_flink/checkpoints" || conf["fs.s3a.bucket.lake.access.key"] != "" {
		t.Errorf("conf = %v", conf)
	}
}

func TestResolveSecrets(t *testing.T) {
	ref := func(name string) *structpb.Value {
		return structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{"secret": structpb.NewStringValue(name)}})
	}
	p := &dataplanev1.PinnedFlinkCluster{
		Spec: &apiv1.FlinkClusterSpec{
			Target: &apiv1.FlinkClusterSpec_Kubernetes{Kubernetes: &apiv1.FlinkKubernetes{Kubeconfig: ref("kube")}},
			Conf:   &structpb.Struct{Fields: map[string]*structpb.Value{"s3.secret-key": ref("s3"), "a.b": structpb.NewStringValue("c")}},
		},
		Secrets:       []*dataplanev1.ExternalSecret{{Field: "s3.secret-key", Secret: "s3"}},
		TargetSecrets: []*dataplanev1.ExternalSecret{{Field: "kubeconfig", Secret: "kube"}},
	}
	read := func(_ context.Context, cfg *apiv1.ConnectorConfig, ext []*dataplanev1.ExternalSecret) error {
		for _, e := range ext {
			cfg.Config.Fields[e.GetField()] = structpb.NewStringValue("value of " + e.GetSecret())
		}
		return nil
	}
	if err := ResolveSecrets(t.Context(), p, read); err != nil {
		t.Fatal(err)
	}
	if p.GetSpec().GetKubernetes().GetKubeconfig().GetStringValue() != "value of kube" || p.GetSpec().GetConf().GetFields()["s3.secret-key"].GetStringValue() != "value of s3" {
		t.Errorf("spec = %v", p.GetSpec())
	}
}
