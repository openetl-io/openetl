package flink

import (
	"bytes"
	"cmp"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/emr"
	"github.com/aws/aws-sdk-go-v2/service/emr/types"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/runner"
	"github.com/unsuitable001/openetl/internal/spark"
)

// EMR is the part of EMR's API the runner uses, and reading a step's log.
type EMR = spark.EMR

// emrTarget runs each generation's job on YARN in per-job mode, attached to
// a step that lasts as long as the job, so the step's state is the job's.
// Stops and kills are steps of their own, so the cluster must run more than
// one step at a time.
type emrTarget struct {
	c       *Cluster
	spec    *apiv1.FlinkEmr
	api     EMR
	logWait time.Duration
}

// How long a failed step's stderr may take to reach the log URI: EMR
// uploads logs every five minutes.
const emrLogWait = 5 * time.Minute

func newEMR(ctx context.Context, spec *apiv1.FlinkEmr, c *Cluster, open func(context.Context, string) (EMR, error)) (*emrTarget, error) {
	if open == nil {
		open = spark.OpenEMR
	}
	api, err := open(ctx, spec.GetRegion())
	if err != nil {
		return nil, err
	}
	return &emrTarget{c: c, spec: spec, api: api, logWait: emrLogWait}, nil
}

func (e *emrTarget) cluster(ctx context.Context) (string, error) {
	return spark.FindEMRCluster(ctx, e.api, e.spec.GetClusterId(), e.spec.GetClusterName(), e.spec.GetRegion())
}

// runScript copies the jar from the store, then runs the job attached, so
// the step ends with it. $1 is the jar's URI, flink run's options follow
// until --, and the job's arguments after it.
const runScript = `set -e
jar=$(mktemp --suffix=.jar)
hadoop fs -copyToLocal -f "$1" "$jar"
shift
opts=()
while [ "$#" -gt 0 ] && [ "$1" != "--" ]; do opts+=("$1"); shift; done
shift
exec flink run "${opts[@]}" "$jar" "$@"`

// appsScript lists the YARN applications whose names start with $1.
const appsScript = `yarn application -list -appStates NEW,NEW_SAVING,SUBMITTED,ACCEPTED,RUNNING 2>/dev/null | awk -F'\t' -v p="$1" 'index($2, p) == 1 { gsub(/ /, "", $1); print $1 }'`

// killScript kills the YARN applications whose names start with $1.
const killScript = `for app in $(` + appsScript + `); do yarn application -kill "$app"; done`

// stopScript stops job $2 of the application named $1 with a savepoint into
// $3.
const stopScript = `set -e
app=$(` + appsScript + ` | head -n 1)
test -n "$app"
flink stop -t yarn-per-job -Dyarn.application.id="$app" --savepointPath "$3" "$2"`

func (e *emrTarget) addStep(ctx context.Context, cluster, name string, args []string) (string, error) {
	in := &emr.AddJobFlowStepsInput{
		JobFlowId: aws.String(cluster),
		Steps: []types.StepConfig{{
			Name:            aws.String(name),
			ActionOnFailure: types.ActionOnFailureContinue,
			HadoopJarStep:   &types.HadoopJarStepConfig{Jar: aws.String("command-runner.jar"), Args: args},
		}},
	}
	if r := e.c.s.EMRRole; r != "" {
		in.ExecutionRoleArn = aws.String(r)
	}
	out, err := e.api.AddJobFlowSteps(ctx, in)
	if err != nil {
		return "", errs.Wrap(errs.Transient, err, "flink: add a step to EMR cluster "+cluster)
	}
	if len(out.StepIds) != 1 {
		return "", errs.New(errs.Transient, "flink: EMR added %d steps", len(out.StepIds))
	}
	return out.StepIds[0], nil
}

func (e *emrTarget) deploy(ctx context.Context, j *job) (runner.FlinkJob, error) {
	cluster, err := e.cluster(ctx)
	if err != nil {
		return nil, err
	}
	c, err := e.api.DescribeCluster(ctx, &emr.DescribeClusterInput{ClusterId: aws.String(cluster)})
	if err != nil {
		return nil, errs.Wrap(errs.Transient, err, "flink: describe EMR cluster "+cluster)
	}
	if c.Cluster != nil && (c.Cluster.StepConcurrencyLevel == nil || *c.Cluster.StepConcurrencyLevel < 2) {
		return nil, errs.New(errs.ConfigInvalid, "flink: EMR cluster %s runs one step at a time, and a Flink job's step runs as long as the job, so its stop and cancel steps would never run; raise its StepConcurrencyLevel", cluster)
	}
	args := []string{"bash", "-c", runScript, "openetl-flink", e.c.objectURI(j.jar), "-t", "yarn-per-job"}
	for _, k := range slices.Sorted(maps.Keys(j.conf)) {
		if k == "execution.state-recovery.path" {
			continue
		}
		args = append(args, "-D"+k+"="+j.conf[k])
	}
	args = append(args, "-Dyarn.application.name="+j.name, "-Dexecution.shutdown-on-attached-exit=true")
	if j.restore != "" {
		args = append(args, "-s", j.restore)
	}
	if j.class != "" {
		args = append(args, "-c", j.class)
	}
	if j.parallelism > 0 {
		args = append(args, "-p", strconv.Itoa(int(j.parallelism)))
	}
	args = append(append(args, "--"), j.args...)
	step, err := e.addStep(ctx, cluster, j.name, args)
	if err != nil {
		return nil, err
	}
	return &emrJob{e: e, id: j.id, name: j.name, cluster: cluster, step: step}, nil
}

func (e *emrTarget) cancelAll(ctx context.Context, prefix string) error {
	cluster, err := e.cluster(ctx)
	if err != nil {
		return err
	}
	in := &emr.ListStepsInput{ClusterId: aws.String(cluster), StepStates: []types.StepState{types.StepStatePending, types.StepStateRunning}}
	var steps []string
	for {
		out, err := e.api.ListSteps(ctx, in)
		if err != nil {
			return errs.Wrap(errs.Transient, err, "flink: list EMR cluster "+cluster+"'s steps")
		}
		for _, s := range out.Steps {
			if strings.HasPrefix(aws.ToString(s.Name), prefix) {
				steps = append(steps, aws.ToString(s.Id))
			}
		}
		if in.Marker = out.Marker; in.Marker == nil {
			break
		}
	}
	kill, err := e.addStep(ctx, cluster, "openetl-flink-kill-"+strings.TrimSuffix(prefix, "-g"), []string{"bash", "-c", killScript, "openetl-flink", prefix})
	if err != nil {
		return err
	}
	if len(steps) > 0 {
		_, err := e.api.CancelSteps(ctx, &emr.CancelStepsInput{ClusterId: aws.String(cluster), StepIds: steps, StepCancellationOption: types.StepCancellationOptionSendInterrupt})
		if err != nil {
			return errs.Wrap(errs.Transient, err, "flink: cancel EMR steps")
		}
	}
	if _, err := e.waitStep(ctx, cluster, kill); err != nil {
		return err
	}
	for _, s := range steps {
		if _, err := e.waitStep(ctx, cluster, s); err != nil {
			return err
		}
	}
	return nil
}

// waitStep waits until a step is over, and returns how it ended.
func (e *emrTarget) waitStep(ctx context.Context, cluster, step string) (*types.Step, error) {
	for {
		out, err := e.api.DescribeStep(ctx, &emr.DescribeStepInput{ClusterId: aws.String(cluster), StepId: aws.String(step)})
		if err != nil {
			return nil, errs.Wrap(errs.Transient, err, "flink: EMR step "+step)
		}
		if s := out.Step; s != nil && s.Status != nil {
			switch s.Status.State {
			case types.StepStateCompleted, types.StepStateFailed, types.StepStateCancelled, types.StepStateInterrupted:
				return s, nil
			}
		}
		if err := wait(ctx, e.c.poll); err != nil {
			return nil, err
		}
	}
}

// stderr is the tail of a step's stderr from the cluster's log URI, from
// the job's exception on if it has one, waiting for EMR to upload it.
func (e *emrTarget) stderr(ctx context.Context, cluster, step string) string {
	c, err := e.api.DescribeCluster(ctx, &emr.DescribeClusterInput{ClusterId: aws.String(cluster)})
	if err != nil || c.Cluster == nil || aws.ToString(c.Cluster.LogUri) == "" {
		return ""
	}
	uri := strings.TrimSuffix(strings.Replace(aws.ToString(c.Cluster.LogUri), "s3n://", "s3://", 1), "/") + "/" + cluster + "/steps/" + step + "/stderr.gz"
	deadline := time.Now().Add(e.logWait)
	for {
		b, err := e.api.Log(ctx, uri)
		if err == nil {
			if zr, zerr := gzip.NewReader(bytes.NewReader(b)); zerr == nil {
				if plain, rerr := io.ReadAll(io.LimitReader(zr, 64<<20)); rerr == nil {
					b = plain
				}
			}
			s := string(b)
			if i := strings.LastIndex(s, "The program finished with the following exception:"); i >= 0 {
				return strings.TrimSpace(s[i:])
			}
			lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
			return strings.Join(lines[max(0, len(lines)-20):], "\n")
		}
		if !errors.Is(err, spark.ErrNoLog) || time.Now().After(deadline) {
			return ""
		}
		if err := wait(ctx, 15*time.Second); err != nil {
			return ""
		}
	}
}

type emrJob struct {
	e                       *emrTarget
	id, name, cluster, step string
}

func (j *emrJob) ID() string { return j.id }

func (j *emrJob) UI() string {
	return "https://" + j.e.spec.GetRegion() + ".console.aws.amazon.com/emr/home?region=" + j.e.spec.GetRegion() + "#/clusterDetails/" + j.cluster
}

func (j *emrJob) Wait(ctx context.Context) error {
	s, err := j.e.waitStep(ctx, j.cluster, j.step)
	if err != nil {
		return err
	}
	switch s.Status.State {
	case types.StepStateCompleted:
		return nil
	case types.StepStateFailed:
		msg := j.e.stderr(ctx, j.cluster, j.step)
		if f := s.Status.FailureDetails; msg == "" && f != nil {
			msg = aws.ToString(f.Message)
		}
		return failed(cmp.Or(msg, "EMR step "+j.step+" failed"))
	}
	return errs.New(errs.Transient, "flink: EMR step %s, which ran job %s, ended %s", j.step, j.id, s.Status.State)
}

func (j *emrJob) Stop(ctx context.Context, dir string) error {
	stop, err := j.e.addStep(ctx, j.cluster, "openetl-flink-stop-"+j.id, []string{"bash", "-c", stopScript, "openetl-flink", j.name, j.id, dir})
	if err != nil {
		return err
	}
	s, err := j.e.waitStep(ctx, j.cluster, stop)
	if err != nil {
		return err
	}
	if s.Status.State != types.StepStateCompleted {
		return errs.New(errs.Transient, "flink: stopping job %s with a savepoint failed: EMR step %s ended %s: %s", j.id, stop, s.Status.State, j.e.stderr(ctx, j.cluster, stop))
	}
	_, err = j.e.waitStep(ctx, j.cluster, j.step)
	return err
}

func (j *emrJob) Cancel(ctx context.Context) error {
	kill, err := j.e.addStep(ctx, j.cluster, "openetl-flink-kill-"+j.id, []string{"bash", "-c", killScript, "openetl-flink", j.name})
	if err != nil {
		return err
	}
	_, err = j.e.api.CancelSteps(ctx, &emr.CancelStepsInput{ClusterId: aws.String(j.cluster), StepIds: []string{j.step}, StepCancellationOption: types.StepCancellationOptionSendInterrupt})
	if err != nil {
		return errs.Wrap(errs.Transient, err, "flink: cancel EMR step "+j.step)
	}
	if _, err := j.e.waitStep(ctx, j.cluster, kill); err != nil {
		return err
	}
	_, err = j.e.waitStep(ctx, j.cluster, j.step)
	return err
}
