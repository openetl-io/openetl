package spec

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strings"

	"go.yaml.in/yaml/v3"
	"google.golang.org/protobuf/types/known/structpb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
)

type flinkResourcesYAML struct {
	CPU    float64 `yaml:"cpu"`
	Memory string  `yaml:"memory"`
}

type flinkClusterYAML struct {
	Kubernetes *struct {
		Namespace      string              `yaml:"namespace"`
		Image          string              `yaml:"image"`
		ServiceAccount string              `yaml:"serviceAccount"`
		Kubeconfig     yaml.Node           `yaml:"kubeconfig"`
		JobManager     *flinkResourcesYAML `yaml:"jobManager"`
		TaskManager    *flinkResourcesYAML `yaml:"taskManager"`
	} `yaml:"kubernetes"`
	Rest *struct {
		URL  string    `yaml:"url"`
		Auth yaml.Node `yaml:"auth"`
	} `yaml:"rest"`
	Emr *struct {
		ClusterID   string `yaml:"clusterId"`
		ClusterName string `yaml:"clusterName"`
		Region      string `yaml:"region"`
	} `yaml:"emr"`
	Conf map[string]yaml.Node `yaml:"conf"`
}

var quantity = regexp.MustCompile(`^[0-9]+(\.[0-9]+)?([kKmMgGtT]i?|[kKmMgGtT])?$`)

func parseFlinkCluster(b []byte) (*apiv1.FlinkClusterSpec, error) {
	var y flinkClusterYAML
	if err := strict(b, &y); err != nil {
		return nil, err
	}
	targets := 0
	for _, set := range []bool{y.Kubernetes != nil, y.Rest != nil, y.Emr != nil} {
		if set {
			targets++
		}
	}
	if targets != 1 {
		return nil, errors.New("needs one of kubernetes, rest and emr")
	}
	c := &apiv1.FlinkClusterSpec{}
	switch {
	case y.Kubernetes != nil:
		k := y.Kubernetes
		if !ValidName(k.Namespace) {
			return nil, fmt.Errorf("kubernetes.namespace %q isn't a Kubernetes namespace", k.Namespace)
		}
		if k.ServiceAccount != "" && !ValidName(k.ServiceAccount) {
			return nil, fmt.Errorf("kubernetes.serviceAccount %q isn't a Kubernetes name", k.ServiceAccount)
		}
		spec := &apiv1.FlinkKubernetes{Namespace: k.Namespace, Image: k.Image, ServiceAccount: k.ServiceAccount}
		if k.Kubeconfig.Kind != 0 {
			v, err := secretOnly("kubernetes.kubeconfig", &k.Kubeconfig)
			if err != nil {
				return nil, err
			}
			spec.Kubeconfig = v
		}
		var err error
		if spec.JobManager, err = flinkResources("kubernetes.jobManager", k.JobManager); err != nil {
			return nil, err
		}
		if spec.TaskManager, err = flinkResources("kubernetes.taskManager", k.TaskManager); err != nil {
			return nil, err
		}
		c.Target = &apiv1.FlinkClusterSpec_Kubernetes{Kubernetes: spec}
	case y.Rest != nil:
		u, err := url.Parse(y.Rest.URL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.RawQuery != "" {
			return nil, fmt.Errorf("rest.url %q isn't an http:// or https:// URL", y.Rest.URL)
		}
		spec := &apiv1.FlinkRest{Url: strings.TrimSuffix(y.Rest.URL, "/")}
		if y.Rest.Auth.Kind != 0 {
			if spec.Auth, err = secretOnly("rest.auth", &y.Rest.Auth); err != nil {
				return nil, err
			}
		}
		c.Target = &apiv1.FlinkClusterSpec_Rest{Rest: spec}
	default:
		e := y.Emr
		switch {
		case (e.ClusterID == "") == (e.ClusterName == ""):
			return nil, errors.New("emr needs one of clusterId and clusterName")
		case e.Region == "":
			return nil, errors.New("emr.region is missing")
		}
		c.Target = &apiv1.FlinkClusterSpec_Emr{Emr: &apiv1.FlinkEmr{ClusterId: e.ClusterID, ClusterName: e.ClusterName, Region: e.Region}}
	}
	if len(y.Conf) > 0 {
		c.Conf = &structpb.Struct{Fields: map[string]*structpb.Value{}}
	}
	for k, n := range y.Conf {
		field := "conf." + k
		if err := flinkOption(field, k); err != nil {
			return nil, err
		}
		v, err := sparkConfValue(field, &n)
		if err != nil {
			return nil, err
		}
		c.Conf.Fields[k] = v
	}
	return c, nil
}

func flinkResources(field string, y *flinkResourcesYAML) (*apiv1.FlinkResources, error) {
	if y == nil {
		return nil, nil
	}
	switch {
	case y.CPU < 0:
		return nil, fmt.Errorf("%s.cpu must be positive", field)
	case y.Memory != "" && !quantity.MatchString(y.Memory):
		return nil, fmt.Errorf("%s.memory %q isn't a quantity, such as 2048m or 2g", field, y.Memory)
	}
	return &apiv1.FlinkResources{Cpu: y.CPU, Memory: y.Memory}, nil
}

// secretOnly is a field that must be a secret reference.
func secretOnly(field string, n *yaml.Node) (*structpb.Value, error) {
	v, err := sparkConfValue(field, n)
	if err != nil || v.GetStructValue() == nil {
		return nil, fmt.Errorf("%s: a secret reference, such as {secret: name}", field)
	}
	return v, nil
}

// runnerFlinkPrefixes are the options the runner sets on every job, by
// prefix, or by name when they end without a dot.
var runnerFlinkPrefixes = []string{
	"execution.checkpointing.", "state.checkpoints.", "state.savepoints.", "execution.state-recovery.", "execution.savepoint",
	"restart-strategy", "jobmanager.execution.failover-strategy", "high-availability", "$internal.", "openetl.",
	"parallelism.default", "pipeline.name", "execution.runtime-mode", "execution.target", "kubernetes.operator.",
	"kubernetes.cluster-id", "kubernetes.namespace", "yarn.application.name", "yarn.tags",
}

func flinkOption(field, k string) error {
	switch {
	case !strings.Contains(k, "."):
		return fmt.Errorf("%s: %q isn't a Flink option", field, k)
	case slices.ContainsFunc(runnerFlinkPrefixes, func(p string) bool { return strings.HasPrefix(k, p) }):
		return fmt.Errorf("%s: the runner sets it", field)
	}
	return nil
}

type flinkTransformYAML struct {
	Cluster string `yaml:"cluster"`
	Code    struct {
		Jar    string `yaml:"jar"`
		SHA256 string `yaml:"sha256"`
		Class  string `yaml:"class"`
	} `yaml:"code"`
	Parallelism int32             `yaml:"parallelism"`
	Conf        map[string]string `yaml:"conf"`
	Outputs     []outputYAML      `yaml:"outputs"`
}

func parseFlinkTransform(y *flinkTransformYAML) (*apiv1.FlinkTransform, error) {
	const field = "transform.flink"
	c := y.Code
	switch {
	case !ValidName(y.Cluster):
		return nil, fmt.Errorf("%s.cluster: %q isn't a FlinkCluster name", field, y.Cluster)
	case c.Jar == "":
		return nil, fmt.Errorf("%s.code.jar is missing", field)
	case !strings.HasPrefix(c.Jar, "s3://") && !strings.HasPrefix(c.Jar, "s3a://") && !strings.HasPrefix(c.Jar, "https://"):
		return nil, fmt.Errorf("%s.code: %q isn't an s3:// or https:// URI, which the runner can read and check", field, c.Jar)
	case !sha256Pattern.MatchString(c.SHA256):
		return nil, fmt.Errorf("%s.code.sha256: the file's SHA-256, as 64 lowercase hex digits", field)
	case y.Parallelism < 0:
		return nil, fmt.Errorf("%s.parallelism must be positive", field)
	}
	for k := range y.Conf {
		if err := flinkOption(field+".conf."+k, k); err != nil {
			return nil, err
		}
	}
	outputs, err := parseOutputs(field, y.Outputs)
	if err != nil {
		return nil, err
	}
	return &apiv1.FlinkTransform{
		Cluster: y.Cluster, Code: &apiv1.FlinkCode{Jar: c.Jar, Sha256: c.SHA256, MainClass: c.Class},
		Parallelism: y.Parallelism, Conf: y.Conf, Outputs: outputs,
	}, nil
}

func parseFlinkAccess(y *sparkAccessYAML) (*apiv1.FlinkAccess, error) {
	a, err := parseAccess("flinkAccess", y)
	if err != nil {
		return nil, err
	}
	return &apiv1.FlinkAccess{EmrRole: a.GetEmrRole(), Keys: a.GetKeys()}, nil
}
