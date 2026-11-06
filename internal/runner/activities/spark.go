package activities

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
	"github.com/unsuitable001/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
	"github.com/unsuitable001/openetl/internal/attemptlog"
	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/connectorhost"
	"github.com/unsuitable001/openetl/internal/orchestration/contract"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/runner/worker"
	"github.com/unsuitable001/openetl/internal/secretstore"
	"github.com/unsuitable001/openetl/internal/spark"
)

// Spark runs pipelines' Spark steps on their clusters (ADR-0022).
type Spark struct {
	API dataplanev1connect.DataPlaneServiceClient
	// Nil: secrets in cloud secret stores can't be read.
	Secrets *secretstore.Reader
	// The data plane's store, and where it is, as Sync has them. Steps read
	// the log, and put what the cluster reads below the pipeline's _spark/.
	Checkpoints checkpoint.Store
	Storage     string
	// Fetches https:// code and reaches YARN's ResourceManager.
	HTTP *http.Client
	// Empty: spark-submit on PATH.
	SparkSubmit string
	// Nil: EMR through the AWS SDK's default chain.
	EMR    func(ctx context.Context, region string) (spark.EMR, error)
	Logger *slog.Logger
	// Where steps' logs and submissions go. Nil: steps keep neither.
	Logs checkpoint.Store
	// How often a running application is checked. Zero: 15 s.
	Poll time.Duration
}

// RunSpark runs a Spark step's application to its end. One that fails is a
// non-retryable SPARK_FAILED error. A retried activity finds the
// application its earlier attempt submitted, and waits for it.
func (a *Spark) RunSpark(ctx context.Context, in contract.SparkInput) (contract.SparkResult, error) {
	log := a.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	attempt := int(in.Attempt)
	log = log.With("pipeline", in.PipelineID, "sync", in.SyncID, "step", in.StepNumber, "cluster", in.Step.Cluster, "attempt", attempt)
	log, done := attemptLog(ctx, a.Logs, log, in.OrgID, in.PipelineID, in.SyncID, in.StepNumber, attempt)
	defer done()
	res, err := a.run(ctx, in, log)
	var f *spark.Failed
	switch {
	case errors.As(err, &f):
		log.Warn("the Spark application failed", "err", f)
		return res, &contract.Failure{Code: contract.SparkFailed, Message: f.Error()}
	case err != nil:
		log.Warn("Spark step failed", "code", errs.CodeOf(err).String(), "err", err)
	default:
		log.Info("the Spark application finished", "submission", res.Submission, "seconds", res.Seconds)
	}
	return res, err
}

func (a *Spark) run(ctx context.Context, in contract.SparkInput, log *slog.Logger) (contract.SparkResult, error) {
	step := in.Step
	res := contract.SparkResult{Cluster: step.Cluster}
	if a.Storage == "" {
		return res, errs.New(errs.ConfigInvalid, "a Spark step needs the data plane's object store, and this runner has none")
	}
	spec, err := syncSpec(ctx, a.API, in.OrgID, in.PipelineID, in.Version)
	if err != nil {
		return res, err
	}
	pc := spec.GetSparkClusters()[step.Cluster]
	if pc == nil || pc.GetVersion() != step.ClusterVersion {
		return res, errs.New(errs.ConfigInvalid, "pipeline version %s pins Spark cluster %s's version %s, which is gone", in.Version, step.Cluster, step.ClusterVersion)
	}
	conf, err := a.clusterConf(ctx, pc)
	if err != nil {
		return res, err
	}
	area, err := spark.Open(ctx, a.Storage, in.OrgID, in.PipelineID)
	if err != nil {
		return res, err
	}
	defer func() { _ = area.Close() }()
	code, err := a.stageCode(ctx, area, stepCode(step), spec.GetPipeline(), log)
	if err != nil {
		return res, err
	}
	dst := spec.GetDestination()
	name, _ := connectorhost.SplitRef(dst.GetConnector())
	c, err := spark.BuildContext(ctx, checkpoint.NewLog(a.Checkpoints, in.OrgID, in.PipelineID), in.PipelineID, in.SyncID, in.RunKey,
		spark.Destination{Connector: name, Namespace: spec.GetPipeline().GetDestination().GetNamespace()})
	if err != nil {
		return res, errs.Wrap(errs.Transient, err, "spark: read the checkpoint log")
	}
	doc, err := c.JSON()
	if err != nil {
		return res, err
	}
	ctxKey, err := area.WriteContext(ctx, in.SyncID, in.StepNumber, doc)
	if err != nil {
		return res, err
	}

	emr := pc.GetSpec().GetEmr() != nil
	app := spark.Application{
		Name: spark.Name(in.SyncID, in.StepNumber), Code: area.URI(code, emr), Python: step.Python != "",
		MainClass: step.MainClass, Args: step.Args, Conf: maps.Clone(conf),
	}
	if app.Conf == nil {
		app.Conf = map[string]string{}
	}
	maps.Copy(app.Conf, step.Conf)
	app.Conf["spark.openetl.context"] = area.URI(ctxKey, emr)
	if err := a.grant(ctx, spec, emr, area, &app); err != nil {
		return res, err
	}
	sub, err := spark.New(ctx, pc.GetSpec(), conf, spark.Options{SparkSubmit: a.SparkSubmit, HTTP: a.HTTP, Logger: log, EMR: a.EMR})
	if err != nil {
		return res, err
	}
	start := time.Now()
	id, err := a.submission(ctx, sub, app, in, log)
	if err != nil {
		return res, err
	}
	res.Submission = id
	st, err := a.watch(ctx, sub, id, log)
	res.State, res.URL, res.Seconds = st.State, st.URL, time.Since(start).Seconds()
	if err == nil && st.Failed {
		err = &spark.Failed{Message: st.Message}
	}
	return res, err
}

func stepCode(step contract.SparkStep) *apiv1.SparkCode {
	if step.Python != "" {
		return &apiv1.SparkCode{File: &apiv1.SparkCode_Python{Python: step.Python}, Sha256: step.SHA256}
	}
	return &apiv1.SparkCode{File: &apiv1.SparkCode_Jar{Jar: step.Jar}, Sha256: step.SHA256}
}

// stageCode copies code into the pipeline's area, checked, and drops the
// copies its version no longer pins. It returns the copy's key.
func (a *Spark) stageCode(ctx context.Context, area *spark.Area, code *apiv1.SparkCode, p *apiv1.PipelineSpec, log *slog.Logger) (string, error) {
	key, err := area.StageCode(ctx, code.GetJar()+code.GetPython(), code.GetSha256(), code.GetPython() != "", a.HTTP)
	if err != nil {
		return "", err
	}
	if err := area.DropCode(ctx, pinnedCode(p)); err != nil {
		log.Warn("dropping old copies of the pipeline's Spark code failed; the next run tries again", "err", err)
	}
	return key, nil
}

// transform prepares a sync attempt's Spark transform: its code checked and
// copied, and the cluster it's submitted to. Close the area when the
// attempt ends.
func (a *Spark) transform(ctx context.Context, spec *dataplanev1.GetSyncSpecResponse, outputs []string, log *slog.Logger) (*spark.Transform, *spark.Area, error) {
	t := spec.GetPipeline().GetTransform().GetSpark()
	if a.Storage == "" {
		return nil, nil, errs.New(errs.ConfigInvalid, "a Spark transform needs the data plane's object store, and this runner has none")
	}
	pc := spec.GetSparkClusters()[t.GetCluster()]
	if pc == nil {
		return nil, nil, errs.New(errs.ConfigInvalid, "the pipeline version pins no version of Spark cluster %s", t.GetCluster())
	}
	conf, err := a.clusterConf(ctx, pc)
	if err != nil {
		return nil, nil, err
	}
	area, err := spark.Open(ctx, a.Storage, spec.GetOrgId(), spec.GetPipelineId())
	if err != nil {
		return nil, nil, err
	}
	tr, err := func() (*spark.Transform, error) {
		key, err := a.stageCode(ctx, area, t.GetCode(), spec.GetPipeline(), log)
		if err != nil {
			return nil, err
		}
		sub, err := spark.New(ctx, pc.GetSpec(), conf, spark.Options{SparkSubmit: a.SparkSubmit, HTTP: a.HTTP, Logger: log, EMR: a.EMR})
		if err != nil {
			return nil, err
		}
		emr := pc.GetSpec().GetEmr() != nil
		app := spark.Application{
			Code: area.URI(key, emr), Python: t.GetCode().GetPython() != "",
			MainClass: t.GetCode().GetMainClass(), Conf: maps.Clone(conf),
		}
		if app.Conf == nil {
			app.Conf = map[string]string{}
		}
		maps.Copy(app.Conf, t.GetConf())
		if err := a.grant(ctx, spec, emr, area, &app); err != nil {
			return nil, err
		}
		return spark.NewTransform(area, a.Storage, sub, app, outputs, log.With("component", "spark"))
	}()
	if err != nil {
		_ = area.Close()
		return nil, nil, err
	}
	return tr, area, nil
}

// grant gives app the pipeline's grant on its _spark/ prefix, if its
// sparkAccess has one for the cluster: on EMR, the step's runtime role;
// elsewhere, S3A keys for the data plane's bucket alone.
func (a *Spark) grant(ctx context.Context, spec *dataplanev1.GetSyncSpecResponse, emr bool, area *spark.Area, app *spark.Application) error {
	access := spec.GetPipeline().GetSparkAccess()
	if emr {
		app.Role = access.GetEmrRole()
		return nil
	}
	keys := access.GetKeys()
	if keys == nil {
		return nil
	}
	if err := readSecrets(ctx, a.Secrets, &apiv1.ConnectorConfig{Config: keys}, spec.GetSparkAccessSecrets()); err != nil {
		return fmt.Errorf("sparkAccess.keys: %w", err)
	}
	id, secret := keys.GetFields()["accessKeyId"].GetStringValue(), keys.GetFields()["secretAccessKey"].GetStringValue()
	if id == "" || secret == "" {
		return errs.New(errs.Internal, "spark: sparkAccess.keys' secrets weren't read")
	}
	conf, err := area.S3AKeys(id, secret)
	if err != nil {
		return err
	}
	maps.Copy(app.Conf, conf)
	return nil
}

// clusterConf is the cluster's properties as text, with the secrets in
// cloud secret stores read.
func (a *Spark) clusterConf(ctx context.Context, pc *dataplanev1.PinnedSparkCluster) (map[string]string, error) {
	sub := pc.GetSpec().GetSubmit()
	if sub == nil {
		return nil, nil
	}
	if err := readSecrets(ctx, a.Secrets, &apiv1.ConnectorConfig{Config: sub.GetConf()}, pc.GetSecrets()); err != nil {
		return nil, err
	}
	conf := map[string]string{}
	for k, v := range sub.GetConf().GetFields() {
		s, ok := v.GetKind().(*structpb.Value_StringValue)
		if !ok {
			return nil, errs.New(errs.Internal, "spark: property %s's secret wasn't read", k)
		}
		conf[k] = s.StringValue
	}
	return conf, nil
}

// pinnedCode is the code copies a pipeline's Spark transform and steps run.
func pinnedCode(p *apiv1.PipelineSpec) map[string]bool {
	keep := map[string]bool{}
	if c := p.GetTransform().GetSpark().GetCode(); c != nil {
		keep[spark.CodeKey(c.GetSha256(), c.GetJar()+c.GetPython(), c.GetPython() != "")] = true
	}
	for _, s := range p.GetPostSync() {
		if c := s.GetSpark().GetCode(); c != nil {
			keep[spark.CodeKey(c.GetSha256(), c.GetJar()+c.GetPython(), c.GetPython() != "")] = true
		}
	}
	return keep
}

type submission struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

// submission is the application's ID: the one an earlier attempt wrote
// beside the step's log, the one the cluster finds by name, or a new
// submission's, which it writes there at once.
func (a *Spark) submission(ctx context.Context, sub spark.Submitter, app spark.Application, in contract.SparkInput, log *slog.Logger) (string, error) {
	key := attemptlog.SyncPrefix(in.OrgID, in.PipelineID, in.SyncID) + fmt.Sprintf("spark-%d.json", in.StepNumber)
	if a.Logs != nil {
		b, _, err := a.Logs.Get(ctx, key)
		var s submission
		switch {
		case err == nil && json.Unmarshal(b, &s) == nil && s.Kind == sub.Kind() && s.ID != "":
			log.Info("watching the application an earlier attempt submitted", "submission", s.ID)
			return s.ID, nil
		case err != nil && !errors.Is(err, checkpoint.ErrNotFound):
			return "", errs.Wrap(errs.Transient, err, "spark: read the step's submission")
		}
	}
	id, err := sub.Find(ctx, app.Name)
	if err != nil {
		return "", err
	}
	if id != "" {
		log.Info("found the application an earlier attempt submitted", "submission", id)
	} else {
		log.Info("submitting the Spark application", "name", app.Name, "cluster_kind", sub.Kind(), "code", app.Code)
		if id, err = sub.Submit(ctx, app); err != nil {
			return "", err
		}
		log.Info("submitted the Spark application", "submission", id)
	}
	if a.Logs != nil {
		b, _ := json.Marshal(submission{Kind: sub.Kind(), ID: id})
		if _, err := a.Logs.Create(context.WithoutCancel(ctx), key, b); err != nil && !errors.Is(err, checkpoint.ErrPrecondition) {
			log.Warn("recording the submission failed, so a retry finds it by name if the cluster can", "submission", id, "err", err)
		}
	}
	return id, nil
}

// statusFailures is how many checks in a row may fail before the step
// fails with the last one's error, to be retried.
const statusFailures = 10

// watch checks the application until it ends. Cancelled, it kills the
// application, unless the runner is stopping, when the next attempt
// watches it instead.
func (a *Spark) watch(ctx context.Context, sub spark.Submitter, id string, log *slog.Logger) (spark.State, error) {
	poll := a.Poll
	if poll == 0 {
		poll = 15 * time.Second
	}
	var last spark.State
	failures := 0
	for {
		st, err := sub.Status(ctx, id)
		switch {
		case ctx.Err() != nil:
		case err != nil:
			if failures++; failures >= statusFailures {
				return last, err
			}
			log.Warn("checking the Spark application failed", "submission", id, "err", err)
		default:
			failures = 0
			if st.State != last.State {
				log.Info("the Spark application is "+st.State, "submission", id, "url", st.URL)
			}
			last = st
			if st.Done {
				return st, nil
			}
		}
		select {
		case <-time.After(poll):
		case <-ctx.Done():
			return last, a.cancelled(ctx, sub, id, log)
		}
	}
}

func (a *Spark) cancelled(ctx context.Context, sub spark.Submitter, id string, log *slog.Logger) error {
	select {
	case <-worker.From(ctx).Draining():
		return errs.New(errs.Transient, "the runner is stopping; the next attempt watches application %s", id)
	default:
	}
	kctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
	defer cancel()
	if err := sub.Kill(kctx, id); err != nil {
		log.Warn("killing the Spark application failed", "submission", id, "err", err)
	} else {
		log.Info("killed the Spark application", "submission", id, "why", ctx.Err())
	}
	return ctx.Err()
}
