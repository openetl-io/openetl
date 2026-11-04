package flink

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/emr"
	"github.com/aws/aws-sdk-go-v2/service/emr/types"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/runner"
	"github.com/unsuitable001/openetl/internal/spark"
)

// fakeEMR keeps steps in memory. Steps it adds run until their state is
// set, but kill and stop steps complete at once.
type fakeEMR struct {
	mu          sync.Mutex
	concurrency int32
	added       []types.StepConfig
	roles       []string
	steps       map[string]*types.Step
	order       []string
	canceled    []string
	logs        map[string][]byte
}

func newFakeEMR() *fakeEMR {
	return &fakeEMR{concurrency: 10, steps: map[string]*types.Step{}, logs: map[string][]byte{}}
}

func (f *fakeEMR) AddJobFlowSteps(_ context.Context, in *emr.AddJobFlowStepsInput, _ ...func(*emr.Options)) (*emr.AddJobFlowStepsOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.added = append(f.added, in.Steps...)
	f.roles = append(f.roles, aws.ToString(in.ExecutionRoleArn))
	id := "s-" + strings.Repeat("A", len(f.added))
	state := types.StepStateRunning
	if name := aws.ToString(in.Steps[0].Name); strings.HasPrefix(name, "openetl-flink-") {
		state = types.StepStateCompleted
	}
	f.steps[id] = &types.Step{Id: aws.String(id), Name: in.Steps[0].Name, Status: &types.StepStatus{State: state}}
	f.order = append([]string{id}, f.order...)
	return &emr.AddJobFlowStepsOutput{StepIds: []string{id}}, nil
}

func (f *fakeEMR) ListSteps(_ context.Context, in *emr.ListStepsInput, _ ...func(*emr.Options)) (*emr.ListStepsOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := &emr.ListStepsOutput{}
	for _, id := range f.order {
		s := f.steps[id]
		if len(in.StepStates) == 0 || slices.Contains(in.StepStates, s.Status.State) {
			out.Steps = append(out.Steps, types.StepSummary{Id: s.Id, Name: s.Name})
		}
	}
	return out, nil
}

func (f *fakeEMR) DescribeStep(_ context.Context, in *emr.DescribeStepInput, _ ...func(*emr.Options)) (*emr.DescribeStepOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := *f.steps[aws.ToString(in.StepId)]
	st := *s.Status
	s.Status = &st
	return &emr.DescribeStepOutput{Step: &s}, nil
}

func (f *fakeEMR) CancelSteps(_ context.Context, in *emr.CancelStepsInput, _ ...func(*emr.Options)) (*emr.CancelStepsOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.canceled = append(f.canceled, in.StepIds...)
	for _, id := range in.StepIds {
		f.steps[id].Status.State = types.StepStateCancelled
	}
	return &emr.CancelStepsOutput{}, nil
}

func (f *fakeEMR) ListClusters(context.Context, *emr.ListClustersInput, ...func(*emr.Options)) (*emr.ListClustersOutput, error) {
	return &emr.ListClustersOutput{Clusters: []types.ClusterSummary{{Id: aws.String("j-1"), Name: aws.String("streams")}}}, nil
}

func (f *fakeEMR) DescribeCluster(context.Context, *emr.DescribeClusterInput, ...func(*emr.Options)) (*emr.DescribeClusterOutput, error) {
	return &emr.DescribeClusterOutput{Cluster: &types.Cluster{StepConcurrencyLevel: aws.Int32(f.concurrency), LogUri: aws.String("s3n://logs/emr/")}}, nil
}

func (f *fakeEMR) Log(_ context.Context, uri string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if b, ok := f.logs[uri]; ok {
		return b, nil
	}
	return nil, spark.ErrNoLog
}

func (f *fakeEMR) set(step string, state types.StepState) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.steps[step].Status.State = state
}

func newEMRCluster(t *testing.T) (*Cluster, *fakeEMR) {
	t.Helper()
	f := newFakeEMR()
	c, _ := newTestCluster(t, "http://unused", nil)
	c.s.EMRRole = "arn:aws:iam::123456789012:role/flink"
	target, err := newEMR(t.Context(), &apiv1.FlinkEmr{ClusterName: "streams", Region: "eu-west-1"}, c, func(context.Context, string) (EMR, error) { return f, nil })
	if err != nil {
		t.Fatal(err)
	}
	target.logWait = 0
	c.target = target
	return c, f
}

func TestEMRDeploy(t *testing.T) {
	c, f := newEMRCluster(t)
	j, err := c.Deploy(t.Context(), runner.FlinkDeploy{Generation: 5, RestoreFrom: "s3://x/chk-2", Bridge: "runner:7443", Fingerprint: "ab", Token: "tok"})
	if err != nil {
		t.Fatal(err)
	}
	step := f.added[0]
	args := step.HadoopJarStep.Args
	sep := slices.Index(args, "--")
	if aws.ToString(step.Name) != "openetl-pay-g5" || aws.ToString(step.HadoopJarStep.Jar) != "command-runner.jar" || args[0] != "bash" || args[2] != runScript ||
		!strings.HasSuffix(args[4], "/fraud.jar") || f.roles[0] != c.s.EMRRole || sep < 0 ||
		!slices.Equal(args[sep+1:], []string{"--openetl.bridge", "runner:7443", "--openetl.fingerprint", "ab", "--openetl.token", "tok"}) {
		t.Errorf("step = %v, args = %q", aws.ToString(step.Name), args)
	}
	opts := args[5:sep]
	for _, want := range []string{"-t", "yarn-per-job", "-D$internal.pipeline.job-id=" + j.ID(), "-Dyarn.application.name=openetl-pay-g5", "-Dexecution.shutdown-on-attached-exit=true",
		"-Dexecution.state-recovery.claim-mode=NO_CLAIM", "-s", "s3://x/chk-2", "-c", "com.acme.Fraud", "-p", "2"} {
		if !slices.Contains(opts, want) {
			t.Errorf("flink run's options %q lack %q", opts, want)
		}
	}

	f.logs["s3://logs/emr/j-1/steps/s-A/stderr.gz"] = gzipped(t, "loading\nThe program finished with the following exception:\n\njava.lang.RuntimeException: bad record\n\tat Fraud.map")
	f.set("s-A", types.StepStateFailed)
	var failure *runner.FlinkFailed
	if err := j.Wait(t.Context()); !errors.As(err, &failure) || !strings.Contains(failure.Message, "bad record") || strings.Contains(failure.Message, "loading") {
		t.Errorf("err = %v", err)
	}

	f.concurrency = 1
	if _, err := c.Deploy(t.Context(), runner.FlinkDeploy{Generation: 6}); errs.CodeOf(err) != errs.ConfigInvalid {
		t.Errorf("a cluster that runs one step at a time: %v", err)
	}
}

func TestEMRStopAndCancel(t *testing.T) {
	c, f := newEMRCluster(t)
	j, err := c.Deploy(t.Context(), runner.FlinkDeploy{Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	f.set("s-A", types.StepStateCompleted)
	dir := c.SavepointDir("s1")
	if err := j.Stop(t.Context(), dir); err != nil {
		t.Fatal(err)
	}
	stop := f.added[1].HadoopJarStep.Args
	if stop[2] != stopScript || !slices.Equal(stop[4:], []string{"openetl-pay-g1", j.ID(), dir}) {
		t.Errorf("stop step = %q", stop)
	}
	if err := j.Wait(t.Context()); err != nil {
		t.Errorf("a stopped job's wait: %v", err)
	}

	j2, err := c.Deploy(t.Context(), runner.FlinkDeploy{Generation: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.CancelAll(t.Context()); err != nil {
		t.Fatal(err)
	}
	kill := f.added[len(f.added)-1]
	if !slices.Equal(f.canceled, []string{"s-AAA"}) || kill.HadoopJarStep.Args[2] != killScript || kill.HadoopJarStep.Args[4] != "openetl-pay-g" {
		t.Errorf("canceled = %v, kill = %q", f.canceled, kill.HadoopJarStep.Args)
	}
	if err := j2.Wait(t.Context()); errs.CodeOf(err) != errs.Transient {
		t.Errorf("a cancelled job's wait: %v", err)
	}
}

func gzipped(t *testing.T, s string) []byte {
	t.Helper()
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	if _, err := w.Write([]byte(s)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}
