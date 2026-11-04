// Package flink runs a continuous pipeline's Flink job on the user's
// cluster (ADR-0022): the Flink Kubernetes Operator, a standalone session
// cluster's REST API, or EMR on EC2. Everything a cluster reads or writes of
// the data plane's store is below the pipeline's {org}/{pipeline}/_flink/:
// the checked copies of its code under code/{sha256}/, each job's
// checkpoints under checkpoints/{job}/, stops' savepoints under
// savepoints/{name}/, and Kubernetes HA's data under ha/.
package flink

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/runner"
	"github.com/unsuitable001/openetl/internal/spark"
)

// Dir holds what a pipeline's Flink jobs read and write, next to its
// checkpoints.
const Dir = "_flink"

// Settings is what a pipeline's job runs with. The cluster's and access's
// secrets must be values by now: see ResolveSecrets.
type Settings struct {
	Org, Pipeline string
	// The data plane's store, as objectstore.Open takes it: S3, or a
	// directory the cluster mounts at the same path.
	Store     string
	Cluster   *apiv1.FlinkClusterSpec
	Transform *apiv1.FlinkTransform
	// From the pipeline's flinkAccess; zero for the cluster's own identity.
	AccessKeyID, SecretAccessKey string
	EMRRole                      string
}

type Options struct {
	Logger *slog.Logger
	// Fetches https:// code, and calls a REST cluster. Nil: a default
	// client.
	HTTP *http.Client
	// Opens EMR's API for a region; nil: the AWS SDK's default chain.
	EMR func(ctx context.Context, region string) (EMR, error)
	// Opens the Kubernetes API with a kubeconfig, or nil for the runner's
	// own service account; nil: client-go.
	Kube func(ctx context.Context, kubeconfig []byte, namespace string) (Kube, error)
	// How often job states are polled. Zero: 2 s.
	Poll time.Duration
}

// Cluster is runner.Flink for one pipeline on one FlinkCluster.
type Cluster struct {
	s      Settings
	area   *spark.Area
	target target
	log    *slog.Logger
	http   *http.Client
	poll   time.Duration
}

var _ runner.Flink = (*Cluster)(nil)

// target is how one kind of cluster deploys and finds jobs.
type target interface {
	deploy(ctx context.Context, j *job) (runner.FlinkJob, error)
	// cancelAll cancels every job named with prefix, or among ids, the jobs
	// deployed and not yet seen terminal, and returns once each is.
	cancelAll(ctx context.Context, prefix string, ids []string) error
}

// job is one generation's deployment.
type job struct {
	id, name string
	gen      uint64
	// The code copy's key in the area.
	jar         string
	class       string
	args        []string
	parallelism int32
	restore     string
	conf        map[string]string
}

// New prepares the pipeline's Flink cluster. Close it when the attempt
// ends.
func New(ctx context.Context, s Settings, o Options) (*Cluster, error) {
	if s.Cluster == nil || s.Transform == nil {
		return nil, errs.New(errs.Internal, "flink: no cluster or transform")
	}
	for k, v := range s.Cluster.GetConf().GetFields() {
		if _, ok := v.GetKind().(*structpb.Value_StringValue); !ok {
			return nil, errs.New(errs.Internal, "flink: conf.%s's secret wasn't read", k)
		}
	}
	area, err := spark.OpenDir(ctx, s.Store, s.Org, s.Pipeline, Dir, "Flink")
	if err != nil {
		return nil, err
	}
	c := &Cluster{s: s, area: area, log: o.Logger, http: o.HTTP, poll: o.Poll}
	if c.log == nil {
		c.log = slog.New(slog.DiscardHandler)
	}
	if c.http == nil {
		c.http = &http.Client{Timeout: time.Minute}
	}
	if c.poll == 0 {
		c.poll = 2 * time.Second
	}
	switch {
	case s.Cluster.GetRest() != nil:
		c.target, err = newREST(s.Cluster.GetRest(), c)
	case s.Cluster.GetKubernetes() != nil:
		c.target, err = newKubernetes(ctx, s.Cluster.GetKubernetes(), c, o.Kube)
	case s.Cluster.GetEmr() != nil:
		c.target, err = newEMR(ctx, s.Cluster.GetEmr(), c, o.EMR)
	default:
		err = errs.New(errs.ConfigInvalid, "flink: the cluster has no target")
	}
	if err != nil {
		_ = area.Close()
		return nil, err
	}
	return c, nil
}

func (c *Cluster) Close() error { return c.area.Close() }

// ResolveSecrets reads p's secrets that are in cloud secret stores with
// read, which fills each field ext names, so that p's spec holds only values.
func ResolveSecrets(ctx context.Context, p *dataplanev1.PinnedFlinkCluster, read func(context.Context, *apiv1.ConnectorConfig, []*dataplanev1.ExternalSecret) error) error {
	if conf := p.GetSpec().GetConf(); conf != nil {
		if err := read(ctx, &apiv1.ConnectorConfig{Config: conf}, p.GetSecrets()); err != nil {
			return err
		}
	}
	var field string
	var v *structpb.Value
	if k := p.GetSpec().GetKubernetes(); k.GetKubeconfig() != nil {
		field, v = "kubeconfig", k.GetKubeconfig()
	} else if r := p.GetSpec().GetRest(); r.GetAuth() != nil {
		field, v = "auth", r.GetAuth()
	} else {
		return nil
	}
	cfg := &apiv1.ConnectorConfig{Config: &structpb.Struct{Fields: map[string]*structpb.Value{field: v}}}
	if err := read(ctx, cfg, p.GetTargetSecrets()); err != nil {
		return err
	}
	v = cfg.GetConfig().GetFields()[field]
	if _, ok := v.GetKind().(*structpb.Value_StringValue); !ok {
		return errs.New(errs.Internal, "flink: the cluster's %s secret wasn't read", field)
	}
	if k := p.GetSpec().GetKubernetes(); k != nil {
		k.Kubeconfig = v
	} else {
		p.GetSpec().GetRest().Auth = v
	}
	return nil
}

// JobID is a generation's Flink job ID: 32 hex digits, the same for every
// attempt of that generation.
func JobID(pipeline string, gen uint64) string {
	sum := sha256.Sum256([]byte(pipeline + "/" + strconv.FormatUint(gen, 10)))
	return hex.EncodeToString(sum[:16])
}

// JobName is a generation's Flink job name; every generation's starts with
// namePrefix(pipeline).
func JobName(pipeline string, gen uint64) string {
	return namePrefix(pipeline) + strconv.FormatUint(gen, 10)
}

func namePrefix(pipeline string) string { return "openetl-" + strings.ToLower(pipeline) + "-g" }

// CancelAll finds the pipeline's jobs by name on Kubernetes and EMR, where
// the runner names them, and on a session cluster by the IDs deployments
// noted in the area, since a job's main can name it.
func (c *Cluster) CancelAll(ctx context.Context) error {
	ids, err := c.area.Dirs(ctx, jobsDir)
	if err != nil {
		return errs.Wrap(errs.Transient, err, "flink: list the pipeline's jobs")
	}
	if err := c.target.cancelAll(ctx, namePrefix(c.s.Pipeline), ids); err != nil {
		return err
	}
	for _, id := range ids {
		if err := c.area.Drop(ctx, jobsDir+id+"/"); err != nil {
			c.log.Warn("dropping a cancelled job's note failed", "job", id, "err", err)
		}
	}
	return nil
}

const (
	checkpointsDir = "checkpoints/"
	savepointsDir  = "savepoints/"
	// Notes of the jobs deployed and not yet seen cancelled.
	jobsDir = "jobs/"
)

func (c *Cluster) Completed(ctx context.Context, jobID string) ([]runner.FlinkCheckpoint, error) {
	dir := checkpointsDir + jobID + "/"
	names, err := c.area.Dirs(ctx, dir)
	if err != nil {
		return nil, errs.Wrap(errs.Transient, err, "flink: list job "+jobID+"'s checkpoints")
	}
	var out []runner.FlinkCheckpoint
	for _, n := range names {
		id, err := strconv.ParseInt(strings.TrimPrefix(n, "chk-"), 10, 64)
		if err != nil || !strings.HasPrefix(n, "chk-") {
			continue
		}
		key := dir + n + "/"
		switch ok, err := c.area.Exists(ctx, key+"_metadata"); {
		case err != nil:
			return nil, errs.Wrap(errs.Transient, err, "flink: read job "+jobID+"'s checkpoint "+n)
		case ok:
			out = append(out, runner.FlinkCheckpoint{ID: id, Path: c.objectURI(strings.TrimSuffix(key, "/"))})
		}
	}
	slices.SortFunc(out, func(a, b runner.FlinkCheckpoint) int { return int(a.ID - b.ID) })
	return out, nil
}

func (c *Cluster) SavepointDir(name string) string {
	return c.objectURI(savepointsDir + name)
}

// key is the area's key of a URI the cluster names, with a trailing slash.
func (c *Cluster) key(uri string) (string, bool) {
	base := c.objectURI("")
	if !strings.HasPrefix(uri, base) {
		return "", false
	}
	return strings.TrimSuffix(strings.TrimPrefix(uri, base), "/") + "/", true
}

func (c *Cluster) Savepoint(ctx context.Context, dir string) (string, error) {
	key, ok := c.key(dir)
	if !ok || !strings.HasPrefix(key, savepointsDir) {
		return "", errs.New(errs.Internal, "flink: %s isn't one of the pipeline's savepoint directories", dir)
	}
	names, err := c.area.Dirs(ctx, key)
	if err != nil {
		return "", errs.Wrap(errs.Transient, err, "flink: list "+dir)
	}
	for _, n := range names {
		if !strings.HasPrefix(n, "savepoint-") {
			continue
		}
		switch ok, err := c.area.Exists(ctx, key+n+"/_metadata"); {
		case err != nil:
			return "", errs.Wrap(errs.Transient, err, "flink: read "+dir)
		case ok:
			return c.objectURI(key + n), nil
		}
	}
	return "", nil
}

func (c *Cluster) Prune(ctx context.Context, keep []string) error {
	jobs, err := c.area.Dirs(ctx, checkpointsDir)
	if err != nil {
		return errs.Wrap(errs.Transient, err, "flink: list the pipeline's checkpoints")
	}
	savepoints, err := c.area.Dirs(ctx, savepointsDir)
	if err != nil {
		return errs.Wrap(errs.Transient, err, "flink: list the pipeline's savepoints")
	}
	kept := map[string]bool{}
	for _, k := range keep {
		if key, ok := c.key(k); ok {
			kept[key] = true
		} else {
			kept[checkpointsDir+k+"/"] = true
		}
	}
	for _, j := range jobs {
		if key := checkpointsDir + j + "/"; !kept[key] {
			if err := c.area.Drop(ctx, key); err != nil {
				c.log.Warn("dropping a Flink job's checkpoints failed", "job", j, "err", err)
			}
		}
	}
	for _, s := range savepoints {
		if key := savepointsDir + s + "/"; !kept[key] {
			if err := c.area.Drop(ctx, key); err != nil {
				c.log.Warn("dropping a Flink savepoint failed", "savepoint", s, "err", err)
			}
		}
	}
	return nil
}

func (c *Cluster) Deploy(ctx context.Context, d runner.FlinkDeploy) (runner.FlinkJob, error) {
	t := c.s.Transform
	key, err := c.area.StageCode(ctx, t.GetCode().GetJar(), t.GetCode().GetSha256(), c.http)
	if err != nil {
		return nil, err
	}
	if err := c.area.DropCode(ctx, map[string]bool{key: true}); err != nil {
		c.log.Warn("dropping old copies of the pipeline's Flink code failed; the next deploy tries again", "err", err)
	}
	j := &job{
		id: JobID(c.s.Pipeline, d.Generation), name: JobName(c.s.Pipeline, d.Generation), gen: d.Generation,
		jar: key, class: t.GetCode().GetMainClass(), parallelism: t.GetParallelism(), restore: d.RestoreFrom,
		args: []string{"--openetl.bridge", d.Bridge, "--openetl.fingerprint", d.Fingerprint, "--openetl.token", d.Token},
	}
	j.conf = c.conf(j, d.CheckpointInterval)
	if err := c.area.Write(ctx, jobsDir+j.id+"/_deployed", nil); err != nil {
		return nil, errs.Wrap(errs.Transient, err, "flink: note the job")
	}
	c.log.Info("deploying the Flink job", "job", j.id, "name", j.name, "restore", j.restore)
	return c.target.deploy(ctx, j)
}

// Restarts allowed within restartInterval before the job fails for good,
// so a record that keeps crashing the user's code ends it.
const (
	maxRestarts       = 3
	restartInterval   = 10 * time.Minute
	restartDelay      = 10 * time.Second
	checkpointTimeout = 10 * time.Minute
)

// conf is the job's Flink configuration: the cluster's, the transform's,
// then what the runner sets.
func (c *Cluster) conf(j *job, interval time.Duration) map[string]string {
	out := map[string]string{}
	for k, v := range c.s.Cluster.GetConf().GetFields() {
		out[k] = v.GetStringValue()
	}
	maps.Copy(out, c.s.Transform.GetConf())
	maps.Copy(out, map[string]string{
		"execution.runtime-mode":                                    "STREAMING",
		"pipeline.name":                                             j.name,
		"$internal.pipeline.job-id":                                 j.id,
		"execution.checkpointing.interval":                          strconv.FormatInt(max(interval, time.Second).Milliseconds(), 10) + " ms",
		"execution.checkpointing.mode":                              "EXACTLY_ONCE",
		"execution.checkpointing.max-concurrent-checkpoints":        "1",
		"execution.checkpointing.tolerable-failed-checkpoints":      "0",
		"execution.checkpointing.timeout":                           strconv.FormatInt(checkpointTimeout.Milliseconds(), 10) + " ms",
		"execution.checkpointing.externalized-checkpoint-retention": "RETAIN_ON_CANCELLATION",
		"execution.checkpointing.num-retained":                      "2",
		"execution.checkpointing.dir":                               c.objectURI(strings.TrimSuffix(checkpointsDir, "/")),
		"execution.checkpointing.savepoint-dir":                     c.objectURI(strings.TrimSuffix(savepointsDir, "/")),
		"jobmanager.execution.failover-strategy":                    "full",
		"restart-strategy.type":                                     "failure-rate",
		"restart-strategy.failure-rate.max-failures-per-interval":   strconv.Itoa(maxRestarts),
		"restart-strategy.failure-rate.failure-rate-interval":       strconv.FormatInt(restartInterval.Milliseconds(), 10) + " ms",
		"restart-strategy.failure-rate.delay":                       strconv.FormatInt(restartDelay.Milliseconds(), 10) + " ms",
	})
	if j.parallelism > 0 {
		out["parallelism.default"] = strconv.Itoa(int(j.parallelism))
	}
	if j.restore != "" {
		out["execution.state-recovery.path"] = j.restore
		out["execution.state-recovery.claim-mode"] = "NO_CLAIM"
	}
	if c.s.AccessKeyID != "" {
		if u := c.area.Base(); u.Scheme == "s3" {
			p := "fs.s3a.bucket." + u.Host + "."
			out[p+"aws.credentials.provider"] = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
			out[p+"access.key"] = c.s.AccessKeyID
			out[p+"secret.key"] = c.s.SecretAccessKey
		}
	}
	return out
}

// objectURI is how Flink names key of the area: s3:// or, with keys for
// Hadoop's S3 filesystem alone, s3a://; or file://.
func (c *Cluster) objectURI(key string) string {
	u := c.area.Base()
	u.Path = path.Join(u.Path, key)
	if strings.HasSuffix(key, "/") || key == "" {
		u.Path += "/"
	}
	if u.Scheme == "s3" && c.s.AccessKeyID != "" {
		u.Scheme = "s3a"
	}
	return u.String()
}

// failed is the job's failure, with its message cut to its first lines.
func failed(msg string) error {
	lines := strings.Split(strings.TrimSpace(msg), "\n")
	return &runner.FlinkFailed{Message: strings.Join(lines[:min(len(lines), 20)], "\n")}
}

func wait(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func redactURL(u *url.URL) string {
	v := *u
	v.User, v.RawQuery = nil, ""
	return v.String()
}
