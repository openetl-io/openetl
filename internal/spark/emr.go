package spark

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/emr"
	"github.com/aws/aws-sdk-go-v2/service/emr/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/internal/platform/errs"
)

// EMR is the part of EMR's API the runner uses, and reading a step's log.
type EMR interface {
	AddJobFlowSteps(ctx context.Context, in *emr.AddJobFlowStepsInput, opts ...func(*emr.Options)) (*emr.AddJobFlowStepsOutput, error)
	ListSteps(ctx context.Context, in *emr.ListStepsInput, opts ...func(*emr.Options)) (*emr.ListStepsOutput, error)
	DescribeStep(ctx context.Context, in *emr.DescribeStepInput, opts ...func(*emr.Options)) (*emr.DescribeStepOutput, error)
	CancelSteps(ctx context.Context, in *emr.CancelStepsInput, opts ...func(*emr.Options)) (*emr.CancelStepsOutput, error)
	ListClusters(ctx context.Context, in *emr.ListClustersInput, opts ...func(*emr.Options)) (*emr.ListClustersOutput, error)
	DescribeCluster(ctx context.Context, in *emr.DescribeClusterInput, opts ...func(*emr.Options)) (*emr.DescribeClusterOutput, error)
	// Log reads an object of the cluster's log URI; ErrNoLog if it's not
	// there yet.
	Log(ctx context.Context, uri string) ([]byte, error)
}

var ErrNoLog = errors.New("spark: no such log")

// emrSteps adds steps to a cluster of EMR on EC2. Its submission IDs are
// {cluster}:{step}, so a cluster found by name is watched where it was
// submitted.
type emrSteps struct {
	api     EMR
	spec    *apiv1.SparkEmr
	log     *slog.Logger
	logWait time.Duration
}

// How long a failed step's stderr may take to reach the log URI: EMR
// uploads logs every five minutes.
const emrLogWait = 5 * time.Minute

func newEMR(ctx context.Context, spec *apiv1.SparkEmr, o Options) (Submitter, error) {
	open := o.EMR
	if open == nil {
		open = defaultEMR
	}
	api, err := open(ctx, spec.GetRegion())
	if err != nil {
		return nil, err
	}
	return &emrSteps{api: api, spec: spec, log: o.Logger, logWait: emrLogWait}, nil
}

type awsEMR struct {
	*emr.Client
	s3 *s3.Client
}

func defaultEMR(ctx context.Context, region string) (EMR, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, errs.Wrap(errs.ConfigInvalid, err, "spark: AWS credentials for EMR")
	}
	return awsEMR{emr.NewFromConfig(cfg), s3.NewFromConfig(cfg)}, nil
}

func (a awsEMR) Log(ctx context.Context, uri string) ([]byte, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	out, err := a.s3.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(u.Host), Key: aws.String(strings.TrimPrefix(u.Path, "/"))})
	if err != nil {
		if strings.Contains(err.Error(), "NoSuchKey") {
			return nil, ErrNoLog
		}
		return nil, err
	}
	defer func() { _ = out.Body.Close() }()
	return io.ReadAll(io.LimitReader(out.Body, 64<<20))
}

func (e *emrSteps) Kind() string { return "emr" }

// cluster is the cluster's ID, found by name if the spec gives one: the one
// active cluster of that name.
func (e *emrSteps) cluster(ctx context.Context) (string, error) {
	if id := e.spec.GetClusterId(); id != "" {
		return id, nil
	}
	in := &emr.ListClustersInput{ClusterStates: []types.ClusterState{types.ClusterStateStarting, types.ClusterStateBootstrapping, types.ClusterStateRunning, types.ClusterStateWaiting}}
	var found []string
	for {
		out, err := e.api.ListClusters(ctx, in)
		if err != nil {
			return "", errs.Wrap(errs.Transient, err, "spark: list EMR clusters")
		}
		for _, c := range out.Clusters {
			if aws.ToString(c.Name) == e.spec.GetClusterName() {
				found = append(found, aws.ToString(c.Id))
			}
		}
		if in.Marker = out.Marker; in.Marker == nil {
			break
		}
	}
	switch len(found) {
	case 0:
		return "", errs.New(errs.Transient, "spark: no active EMR cluster is named %q in %s", e.spec.GetClusterName(), e.spec.GetRegion())
	case 1:
		return found[0], nil
	}
	return "", errs.New(errs.ConfigInvalid, "spark: %d active EMR clusters are named %q in %s (%s); name one by clusterId", len(found), e.spec.GetClusterName(), e.spec.GetRegion(), strings.Join(found, ", "))
}

func (e *emrSteps) Submit(ctx context.Context, app Application) (string, error) {
	cluster, err := e.cluster(ctx)
	if err != nil {
		return "", err
	}
	args := append([]string{"spark-submit"}, submitArgs("", app, nil)...)
	out, err := e.api.AddJobFlowSteps(ctx, &emr.AddJobFlowStepsInput{
		JobFlowId: aws.String(cluster),
		Steps: []types.StepConfig{{
			Name:            aws.String(app.Name),
			ActionOnFailure: types.ActionOnFailureContinue,
			HadoopJarStep:   &types.HadoopJarStepConfig{Jar: aws.String("command-runner.jar"), Args: args},
		}},
	})
	if err != nil {
		return "", errs.Wrap(errs.Transient, err, "spark: add a step to EMR cluster "+cluster)
	}
	if len(out.StepIds) != 1 {
		return "", errs.New(errs.Transient, "spark: EMR added %d steps", len(out.StepIds))
	}
	id := cluster + ":" + out.StepIds[0]
	if c, err := e.api.DescribeCluster(ctx, &emr.DescribeClusterInput{ClusterId: aws.String(cluster)}); err == nil && c.Cluster != nil && c.Cluster.StepConcurrencyLevel != nil && *c.Cluster.StepConcurrencyLevel == 1 {
		e.log.Warn("EMR cluster "+cluster+" runs one step at a time, so this step waits for those before it", "cluster", cluster)
	}
	return id, nil
}

func (e *emrSteps) Find(ctx context.Context, name string) (string, error) {
	cluster, err := e.cluster(ctx)
	if err != nil {
		return "", err
	}
	in := &emr.ListStepsInput{ClusterId: aws.String(cluster)}
	for {
		out, err := e.api.ListSteps(ctx, in)
		if err != nil {
			return "", errs.Wrap(errs.Transient, err, "spark: list EMR cluster "+cluster+"'s steps")
		}
		// Newest first.
		for _, s := range out.Steps {
			if aws.ToString(s.Name) == name {
				return cluster + ":" + aws.ToString(s.Id), nil
			}
		}
		if in.Marker = out.Marker; in.Marker == nil {
			return "", nil
		}
	}
}

func splitStep(id string) (cluster, step string, err error) {
	cluster, step, ok := strings.Cut(id, ":")
	if !ok {
		return "", "", errs.New(errs.Internal, "spark: %q isn't an EMR step's ID", id)
	}
	return cluster, step, nil
}

func (e *emrSteps) Status(ctx context.Context, id string) (State, error) {
	cluster, step, err := splitStep(id)
	if err != nil {
		return State{}, err
	}
	out, err := e.api.DescribeStep(ctx, &emr.DescribeStepInput{ClusterId: aws.String(cluster), StepId: aws.String(step)})
	if err != nil {
		return State{}, errs.Wrap(errs.Transient, err, "spark: EMR step "+step)
	}
	st := State{URL: fmt.Sprintf("https://%[1]s.console.aws.amazon.com/emr/home?region=%[1]s#/clusterDetails/%[2]s", e.spec.GetRegion(), cluster)}
	if out.Step == nil || out.Step.Status == nil {
		return st, nil
	}
	s := out.Step.Status
	st.State = string(s.State)
	switch s.State {
	case types.StepStateCompleted:
		st.Done = true
	case types.StepStateCancelled, types.StepStateFailed, types.StepStateInterrupted:
		st.Done, st.Failed = true, true
		st.Message = "EMR step " + step + " ended " + st.State
		if f := s.FailureDetails; f != nil {
			for _, p := range []*string{f.Reason, f.Message} {
				if v := aws.ToString(p); v != "" {
					st.Message += ": " + v
				}
			}
		}
		e.copyStderr(ctx, cluster, step)
	}
	return st, nil
}

// copyStderr logs a failed step's stderr from the cluster's log URI,
// waiting for EMR to upload it.
func (e *emrSteps) copyStderr(ctx context.Context, cluster, step string) {
	c, err := e.api.DescribeCluster(ctx, &emr.DescribeClusterInput{ClusterId: aws.String(cluster)})
	if err != nil || c.Cluster == nil || aws.ToString(c.Cluster.LogUri) == "" {
		return
	}
	uri := strings.TrimSuffix(strings.Replace(aws.ToString(c.Cluster.LogUri), "s3n://", "s3://", 1), "/") + "/" + cluster + "/steps/" + step + "/stderr.gz"
	deadline := time.Now().Add(e.logWait)
	for {
		b, err := e.api.Log(ctx, uri)
		if err == nil {
			if zr, zerr := gzip.NewReader(bytes.NewReader(b)); zerr == nil {
				if plain, rerr := io.ReadAll(io.LimitReader(zr, 256<<20)); rerr == nil {
					b = plain
				}
			}
			lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
			for _, l := range lines[max(0, len(lines)-200):] {
				e.log.Info(l, "component", "emr-step")
			}
			return
		}
		if !errors.Is(err, ErrNoLog) || time.Now().After(deadline) {
			e.log.Info("the step's stderr isn't in the cluster's log URI", "uri", uri, "err", err)
			return
		}
		select {
		case <-time.After(15 * time.Second):
		case <-ctx.Done():
			return
		}
	}
}

func (e *emrSteps) Kill(ctx context.Context, id string) error {
	cluster, step, err := splitStep(id)
	if err != nil {
		return err
	}
	_, err = e.api.CancelSteps(ctx, &emr.CancelStepsInput{ClusterId: aws.String(cluster), StepIds: []string{step}, StepCancellationOption: types.StepCancellationOptionTerminateProcess})
	return err
}
