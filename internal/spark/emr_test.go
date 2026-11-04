package spark

import (
	"bytes"
	"compress/gzip"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/emr"
	"github.com/aws/aws-sdk-go-v2/service/emr/types"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/sdk/go/arrowipc"
)

func orderSchema(t *testing.T) []byte {
	t.Helper()
	md := arrow.NewMetadata([]string{"openetl.table"}, []string{"shop_orders"})
	b, err := arrowipc.EncodeSchema(arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, &md))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// fakeEMR keeps steps in memory, newest first, with two pages of
// clusters.
type fakeEMR struct {
	mu       sync.Mutex
	clusters []types.ClusterSummary
	steps    []types.StepSummary
	added    []types.StepConfig
	state    types.StepState
	failure  *types.FailureDetails
	logs     map[string][]byte
	canceled []string
}

func (f *fakeEMR) AddJobFlowSteps(_ context.Context, in *emr.AddJobFlowStepsInput, _ ...func(*emr.Options)) (*emr.AddJobFlowStepsOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.added = append(f.added, in.Steps...)
	id := "s-" + strings.Repeat("A", len(f.added))
	f.steps = append([]types.StepSummary{{Id: aws.String(id), Name: in.Steps[0].Name}}, f.steps...)
	return &emr.AddJobFlowStepsOutput{StepIds: []string{id}}, nil
}

func (f *fakeEMR) ListSteps(_ context.Context, in *emr.ListStepsInput, _ ...func(*emr.Options)) (*emr.ListStepsOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// One step a page.
	i := 0
	if in.Marker != nil {
		i = len(*in.Marker)
	}
	if i >= len(f.steps) {
		return &emr.ListStepsOutput{}, nil
	}
	out := &emr.ListStepsOutput{Steps: f.steps[i : i+1]}
	if i+1 < len(f.steps) {
		out.Marker = aws.String(strings.Repeat("m", i+1))
	}
	return out, nil
}

func (f *fakeEMR) DescribeStep(_ context.Context, in *emr.DescribeStepInput, _ ...func(*emr.Options)) (*emr.DescribeStepOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &emr.DescribeStepOutput{Step: &types.Step{Id: in.StepId, Status: &types.StepStatus{State: f.state, FailureDetails: f.failure}}}, nil
}

func (f *fakeEMR) CancelSteps(_ context.Context, in *emr.CancelStepsInput, _ ...func(*emr.Options)) (*emr.CancelStepsOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if in.StepCancellationOption != types.StepCancellationOptionTerminateProcess {
		return nil, errs.New(errs.Internal, "a running step needs TERMINATE_PROCESS")
	}
	f.canceled = append(f.canceled, in.StepIds...)
	return &emr.CancelStepsOutput{}, nil
}

func (f *fakeEMR) ListClusters(_ context.Context, in *emr.ListClustersInput, _ ...func(*emr.Options)) (*emr.ListClustersOutput, error) {
	if in.Marker == nil {
		return &emr.ListClustersOutput{Clusters: f.clusters[:1], Marker: aws.String("2")}, nil
	}
	return &emr.ListClustersOutput{Clusters: f.clusters[1:]}, nil
}

func (f *fakeEMR) DescribeCluster(_ context.Context, in *emr.DescribeClusterInput, _ ...func(*emr.Options)) (*emr.DescribeClusterOutput, error) {
	return &emr.DescribeClusterOutput{Cluster: &types.Cluster{Id: in.ClusterId, LogUri: aws.String("s3n://logs/emr/"), StepConcurrencyLevel: aws.Int32(1)}}, nil
}

func (f *fakeEMR) Log(_ context.Context, uri string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if b, ok := f.logs[uri]; ok {
		return b, nil
	}
	return nil, ErrNoLog
}

func TestEMR(t *testing.T) {
	ctx := context.Background()
	fake := &fakeEMR{
		clusters: []types.ClusterSummary{{Id: aws.String("j-OTHER"), Name: aws.String("adhoc")}, {Id: aws.String("j-NIGHTLY"), Name: aws.String("nightly")}},
		state:    types.StepStateRunning,
	}
	open := func(_ context.Context, region string) (EMR, error) {
		if region != "eu-west-1" {
			t.Errorf("region %q", region)
		}
		return fake, nil
	}
	spec := &apiv1.SparkClusterSpec{Target: &apiv1.SparkClusterSpec_Emr{Emr: &apiv1.SparkEmr{ClusterName: "nightly", Region: "eu-west-1"}}}
	sub, err := New(ctx, spec, nil, Options{EMR: open})
	if err != nil {
		t.Fatal(err)
	}
	sub.(*emrSteps).logWait = 0
	if id, err := sub.Find(ctx, app.Name); err != nil || id != "" {
		t.Fatalf("found %q, %v before submitting", id, err)
	}
	id, err := sub.Submit(ctx, app)
	if err != nil || id != "j-NIGHTLY:s-A" {
		t.Fatalf("submitted %q, %v", id, err)
	}
	step := fake.added[0]
	if step.ActionOnFailure != types.ActionOnFailureContinue || aws.ToString(step.HadoopJarStep.Jar) != "command-runner.jar" ||
		strings.Join(step.HadoopJarStep.Args, " ") != "spark-submit --deploy-mode cluster --name openetl-s1-2 --conf spark.executor.memory=2g --class com.acme.Models file:///data/org/p/_spark/code/x/a.jar --full" {
		t.Errorf("added %+v, args %v", step, step.HadoopJarStep.Args)
	}
	other := app
	other.Name = "openetl-s2-1"
	if _, err := sub.Submit(ctx, other); err != nil {
		t.Fatal(err)
	}
	// Steps list newest first, a page each, so the first is on page two.
	if found, err := sub.Find(ctx, app.Name); err != nil || found != id {
		t.Errorf("found %q, %v", found, err)
	}
	if st, err := sub.Status(ctx, id); err != nil || st.Done || st.State != "RUNNING" || !strings.Contains(st.URL, "j-NIGHTLY") {
		t.Errorf("running: %+v, %v", st, err)
	}
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	_, _ = zw.Write([]byte("Exception in thread \"main\" java.lang.RuntimeException: boom\n"))
	_ = zw.Close()
	fake.logs = map[string][]byte{"s3://logs/emr/j-NIGHTLY/steps/s-A/stderr.gz": gz.Bytes()}
	fake.state, fake.failure = types.StepStateFailed, &types.FailureDetails{Reason: aws.String("Unknown Error."), Message: aws.String("java.lang.RuntimeException: boom")}
	start := time.Now()
	st, err := sub.Status(ctx, id)
	if err != nil || !st.Failed || st.Message != "EMR step s-A ended FAILED: Unknown Error.: java.lang.RuntimeException: boom" || time.Since(start) > 10*time.Second {
		t.Errorf("failed: %+v, %v", st, err)
	}
	if err := sub.Kill(ctx, id); err != nil || len(fake.canceled) != 1 || fake.canceled[0] != "s-A" {
		t.Errorf("kill: %v, canceled %v", err, fake.canceled)
	}
	fake.clusters = append(fake.clusters, types.ClusterSummary{Id: aws.String("j-NIGHTLY2"), Name: aws.String("nightly")})
	if _, err := sub.Submit(ctx, app); errs.CodeOf(err) != errs.ConfigInvalid || !strings.Contains(err.Error(), "j-NIGHTLY, j-NIGHTLY2") {
		t.Errorf("two clusters of the name: %v", err)
	}
}
