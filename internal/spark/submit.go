package spark

import (
	"context"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/platform/errs"
)

// Application is one submission of the user's code.
type Application struct {
	// Unique to the sync and step, so a retried activity can find it.
	Name string
	// The cluster's URI of the checked copy.
	Code      string
	Python    bool
	MainClass string
	Args      []string
	// The cluster's properties, then the step's, then the runner's.
	Conf map[string]string
	// On EMR, the runtime role the step runs as. Empty: the cluster's.
	Role string
}

// Name is the submission's name for a sync's post-sync step: lowercase, and
// a valid Kubernetes name.
func Name(sync string, step int) string {
	return "openetl-" + strings.ToLower(sync) + "-" + strconv.Itoa(step)
}

// State is how a submission stands.
type State struct {
	Done, Failed bool
	// As the cluster reports it, such as RUNNING or FINISHED.
	State string
	// Why it failed, where the cluster says.
	Message string
	// Its UI, where the cluster has one.
	URL string
}

// Submitter submits applications to one cluster, and finds, watches and
// kills them by the ID it returned.
type Submitter interface {
	Submit(ctx context.Context, app Application) (id string, err error)
	// Find returns the submission named name, or "" if there's none or the
	// cluster can't search by name.
	Find(ctx context.Context, name string) (id string, err error)
	Status(ctx context.Context, id string) (State, error)
	Kill(ctx context.Context, id string) error
	// What the cluster is, for the sync's log: standalone, yarn,
	// kubernetes or emr.
	Kind() string
}

type Options struct {
	// The spark-submit to run. Empty: spark-submit on PATH.
	SparkSubmit string
	// For a standalone master's UI and a YARN ResourceManager's REST API.
	HTTP *http.Client
	// Gets spark-submit's output.
	Logger *slog.Logger
	// Opens EMR in a region. Nil: the AWS SDK's default chain.
	EMR func(ctx context.Context, region string) (EMR, error)
}

// New returns the submitter for a cluster. conf is the cluster's properties,
// with its secrets read.
func New(ctx context.Context, spec *apiv1.SparkClusterSpec, conf map[string]string, o Options) (Submitter, error) {
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	if o.SparkSubmit == "" {
		o.SparkSubmit = "spark-submit"
	}
	if e := spec.GetEmr(); e != nil {
		return newEMR(ctx, e, o)
	}
	master := spec.GetSubmit().GetMaster()
	sub := &submit{bin: o.SparkSubmit, master: master, log: o.Logger}
	switch {
	case strings.HasPrefix(master, "spark://"):
		return &standalone{submit: sub, ui: standaloneUI(master, spec.GetSubmit().GetUi()), http: o.HTTP}, nil
	case strings.HasPrefix(master, "k8s://"):
		ns := conf["spark.kubernetes.namespace"]
		if ns == "" {
			ns = "default"
		}
		return &kubernetes{submit: sub, namespace: ns}, nil
	case master == "yarn":
		rm, err := resourceManager()
		if err != nil {
			return nil, err
		}
		return &yarn{submit: sub, rm: rm, http: o.HTTP}, nil
	}
	return nil, errs.New(errs.ConfigInvalid, "spark: master %q isn't spark://, yarn or k8s://", master)
}

// submitArgs are spark-submit's arguments for app, in cluster deploy mode,
// with extra properties.
func submitArgs(master string, app Application, extra map[string]string) []string {
	args := []string{"--deploy-mode", "cluster", "--name", app.Name}
	if master != "" {
		args = append([]string{"--master", master}, args...)
	}
	conf := maps.Clone(app.Conf)
	if conf == nil {
		conf = map[string]string{}
	}
	maps.Copy(conf, extra)
	for _, k := range slices.Sorted(maps.Keys(conf)) {
		args = append(args, "--conf", k+"="+conf[k])
	}
	if app.MainClass != "" {
		args = append(args, "--class", app.MainClass)
	}
	return append(append(args, app.Code), app.Args...)
}
