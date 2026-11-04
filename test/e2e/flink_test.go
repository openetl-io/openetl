//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"connectrpc.com/connect"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/gen/openetl/api/v1/apiv1connect"
)

// A Flink cluster is pinned by the continuous pipelines whose transforms
// name it, repins them when it changes, and can't be deleted while one
// uses it.
func TestFlinkClusterPinning(t *testing.T) {
	name := unique("flink")
	cluster := func(url string) string {
		return fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "FlinkCluster", "metadata": {"name": %q},
			"spec": {"rest": {"url": %q}, "conf": {"taskmanager.memory.process.size": "1g"}}}`, name, url)
	}
	docs := continuousDocs(name, endlessFaker)
	docs[2] = strings.Replace(docs[2], `"delivery": {"commit": {"interval": "1s"}}`, fmt.Sprintf(`"delivery": {"commit": {"interval": "1s"}},
		"transform": {"flink": {"cluster": %q, "code": {"jar": "https://jobs.example/fraud.jar", "sha256": %q, "class": "com.acme.Fraud"},
			"parallelism": 2, "outputs": [{"name": "scored"}]}}`, name, strings.Repeat("ab", 32)), 1)
	res := apply(t, append([]string{cluster("http://127.0.0.1:8091")}, docs...)...)
	pipelineName, clusterName := "workspaces/default/pipelines/"+name, "workspaces/default/flinkClusters/"+name
	t.Cleanup(func() {
		_, _ = pipelines().DeletePipeline(context.Background(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: pipelineName}))
	})
	if res[0].GetKind() != "FlinkCluster" || res[0].GetAction() != apiv1.ApplyAction_APPLY_ACTION_CREATED || res[3].GetAction() != apiv1.ApplyAction_APPLY_ACTION_CREATED {
		t.Fatalf("applied %v", res)
	}
	clusters := apiv1connect.NewFlinkClusterServiceClient(env.http, env.apiURL)
	c, err := clusters.GetFlinkCluster(t.Context(), connect.NewRequest(&apiv1.GetFlinkClusterRequest{Name: clusterName}))
	if err != nil {
		t.Fatal(err)
	}
	if u := c.Msg.GetFlinkCluster().GetUsedBy(); len(u) != 1 || u[0] != pipelineName || c.Msg.GetFlinkCluster().GetSpec().GetRest().GetUrl() != "http://127.0.0.1:8091" {
		t.Errorf("got %v", c.Msg.GetFlinkCluster())
	}
	version := func() *apiv1.PipelineVersion {
		t.Helper()
		v, err := pipelines().ListPipelineVersions(t.Context(), connect.NewRequest(&apiv1.ListPipelineVersionsRequest{Parent: pipelineName}))
		if err != nil {
			t.Fatal(err)
		}
		return v.Msg.GetPipelineVersions()[0]
	}
	first := version()
	if first.GetFlinkClusters()[name] != c.Msg.GetFlinkCluster().GetVersion() || first.GetSpec().GetTransform().GetFlink().GetParallelism() != 2 {
		t.Errorf("version %v pins %v", first.GetName(), first.GetFlinkClusters())
	}

	// Changing the cluster gives the pipeline a new version that pins it.
	changed := applyAll(t, false, cluster("http://127.0.0.1:8092"))
	if d := changed.GetDependents(); len(d) != 1 || d[0].GetName() != pipelineName || d[0].GetAction() != apiv1.ApplyAction_APPLY_ACTION_UPDATED {
		t.Errorf("dependents = %v", d)
	}
	if v := version(); v.GetName() == first.GetName() || v.GetFlinkClusters()[name] == first.GetFlinkClusters()[name] {
		t.Errorf("the pipeline wasn't repinned: %v", v.GetFlinkClusters())
	}

	if _, err := clusters.DeleteFlinkCluster(t.Context(), connect.NewRequest(&apiv1.DeleteFlinkClusterRequest{Name: clusterName})); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Errorf("deleting a cluster in use: %v", err)
	}
	if _, err := pipelines().DeletePipeline(t.Context(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: pipelineName})); err != nil {
		t.Fatal(err)
	}
	if _, err := clusters.DeleteFlinkCluster(t.Context(), connect.NewRequest(&apiv1.DeleteFlinkClusterRequest{Name: clusterName})); err != nil {
		t.Errorf("deleting a cluster no pipeline uses: %v", err)
	}
}
