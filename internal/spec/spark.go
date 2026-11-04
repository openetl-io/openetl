package spec

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"go.yaml.in/yaml/v3"
	"google.golang.org/protobuf/types/known/structpb"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
)

type sparkClusterYAML struct {
	Submit *struct {
		Master string               `yaml:"master"`
		Conf   map[string]yaml.Node `yaml:"conf"`
		UI     string               `yaml:"ui"`
	} `yaml:"submit"`
	Emr *struct {
		ClusterID   string `yaml:"clusterId"`
		ClusterName string `yaml:"clusterName"`
		Region      string `yaml:"region"`
	} `yaml:"emr"`
}

func parseSparkCluster(b []byte) (*apiv1.SparkClusterSpec, error) {
	var y sparkClusterYAML
	if err := strict(b, &y); err != nil {
		return nil, err
	}
	switch {
	case (y.Submit == nil) == (y.Emr == nil):
		return nil, errors.New("needs one of submit and emr")
	case y.Emr != nil:
		e := y.Emr
		switch {
		case (e.ClusterID == "") == (e.ClusterName == ""):
			return nil, errors.New("emr needs one of clusterId and clusterName")
		case e.Region == "":
			return nil, errors.New("emr.region is missing")
		}
		return &apiv1.SparkClusterSpec{Target: &apiv1.SparkClusterSpec_Emr{Emr: &apiv1.SparkEmr{
			ClusterId: e.ClusterID, ClusterName: e.ClusterName, Region: e.Region,
		}}}, nil
	}
	master := y.Submit.Master
	if master != "yarn" && !strings.HasPrefix(master, "spark://") && !strings.HasPrefix(master, "k8s://") {
		return nil, fmt.Errorf("submit.master %q isn't spark://host:port, yarn or k8s://https://host:port", master)
	}
	switch ui := y.Submit.UI; {
	case ui != "" && !strings.HasPrefix(master, "spark://"):
		return nil, errors.New("submit.ui is a standalone master's web UI, and this master isn't one")
	case ui != "" && !strings.HasPrefix(ui, "http://") && !strings.HasPrefix(ui, "https://"):
		return nil, fmt.Errorf("submit.ui %q isn't an http:// or https:// URL", ui)
	}
	conf := &structpb.Struct{Fields: map[string]*structpb.Value{}}
	for k, n := range y.Submit.Conf {
		field := "submit.conf." + k
		if err := sparkProperty(field, k); err != nil {
			return nil, err
		}
		v, err := sparkConfValue(field, &n)
		if err != nil {
			return nil, err
		}
		conf.Fields[k] = v
	}
	return &apiv1.SparkClusterSpec{Target: &apiv1.SparkClusterSpec_Submit{Submit: &apiv1.SparkSubmit{Master: master, Conf: conf, Ui: y.Submit.UI}}}, nil
}

// runnerProperties are the Spark properties the runner sets on every
// application.
var runnerProperties = []string{"spark.master", "spark.submit.deployMode", "spark.app.name", "spark.yarn.tags", "spark.kubernetes.driver.pod.name"}

func sparkProperty(field, k string) error {
	switch {
	case !strings.HasPrefix(k, "spark."):
		return fmt.Errorf("%s: a Spark property's name starts with \"spark.\"", field)
	case slices.Contains(runnerProperties, k) || strings.HasPrefix(k, "spark.openetl."):
		return fmt.Errorf("%s: the runner sets it", field)
	}
	return nil
}

// sparkConfValue is a property's value as text, or a secret reference.
func sparkConfValue(field string, n *yaml.Node) (*structpb.Value, error) {
	switch n.Kind {
	case yaml.ScalarNode:
		return structpb.NewStringValue(n.Value), nil
	case yaml.MappingNode:
		var ref struct {
			Secret string `yaml:"secret"`
		}
		if err := n.Decode(&ref); err == nil && ValidName(ref.Secret) && len(n.Content) == 2 {
			return structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{"secret": structpb.NewStringValue(ref.Secret)}}), nil
		}
	}
	return nil, fmt.Errorf("%s: a value, or a secret reference such as {secret: name}", field)
}

type sparkCodeYAML struct {
	Jar    string `yaml:"jar"`
	Python string `yaml:"python"`
	SHA256 string `yaml:"sha256"`
	Class  string `yaml:"class"`
}

type sparkStepYAML struct {
	Cluster string            `yaml:"cluster"`
	Code    sparkCodeYAML     `yaml:"code"`
	Args    []string          `yaml:"args"`
	Conf    map[string]string `yaml:"conf"`
}

var sha256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

func parseSparkStep(field string, y *sparkStepYAML) (*apiv1.SparkStep, error) {
	if !ValidName(y.Cluster) {
		return nil, fmt.Errorf("%s.cluster: %q isn't a SparkCluster name", field, y.Cluster)
	}
	code, err := parseSparkCode(field+".code", y.Code, "only a jar has a main class")
	if err != nil {
		return nil, err
	}
	if err := sparkConf(field+".conf", y.Conf); err != nil {
		return nil, err
	}
	return &apiv1.SparkStep{Cluster: y.Cluster, Code: code, Args: y.Args, Conf: y.Conf}, nil
}

// parseSparkCode checks a code block. pythonClass says why a Python file
// takes no class.
func parseSparkCode(field string, c sparkCodeYAML, pythonClass string) (*apiv1.SparkCode, error) {
	switch {
	case (c.Jar == "") == (c.Python == ""):
		return nil, fmt.Errorf("%s needs one of jar and python", field)
	case !sha256Pattern.MatchString(c.SHA256):
		return nil, fmt.Errorf("%s.sha256: the file's SHA-256, as 64 lowercase hex digits", field)
	case c.Python != "" && c.Class != "":
		return nil, fmt.Errorf("%s.class: %s", field, pythonClass)
	}
	code := &apiv1.SparkCode{Sha256: c.SHA256, MainClass: c.Class}
	uri := c.Jar
	if c.Jar != "" {
		code.File = &apiv1.SparkCode_Jar{Jar: c.Jar}
	} else {
		uri = c.Python
		code.File = &apiv1.SparkCode_Python{Python: c.Python}
	}
	if !strings.HasPrefix(uri, "s3://") && !strings.HasPrefix(uri, "s3a://") && !strings.HasPrefix(uri, "https://") {
		return nil, fmt.Errorf("%s: %q isn't an s3:// or https:// URI, which the runner can read and check", field, uri)
	}
	return code, nil
}

func sparkConf(field string, conf map[string]string) error {
	for k := range conf {
		if err := sparkProperty(field+"."+k, k); err != nil {
			return err
		}
	}
	return nil
}

type sparkTransformYAML struct {
	Cluster string            `yaml:"cluster"`
	Code    sparkCodeYAML     `yaml:"code"`
	Conf    map[string]string `yaml:"conf"`
	Outputs []outputYAML      `yaml:"outputs"`
}

type outputYAML struct {
	Name       string   `yaml:"name"`
	PrimaryKey []string `yaml:"primaryKey"`
	Write      string   `yaml:"write"`
	Require    string   `yaml:"require"`
}

func parseSparkTransform(y *sparkTransformYAML) (*apiv1.SparkTransform, error) {
	const field = "transform.spark"
	if !ValidName(y.Cluster) {
		return nil, fmt.Errorf("%s.cluster: %q isn't a SparkCluster name", field, y.Cluster)
	}
	if y.Code.Jar != "" && y.Code.Class == "" {
		return nil, fmt.Errorf("%s.code.class: the class in the jar that implements openetl-spark's Transform", field)
	}
	code, err := parseSparkCode(field+".code", y.Code, "a Python file calls openetl_spark.run with its transform")
	if err != nil {
		return nil, err
	}
	if err := sparkConf(field+".conf", y.Conf); err != nil {
		return nil, err
	}
	outputs, err := parseOutputs(field, y.Outputs)
	if err != nil {
		return nil, err
	}
	return &apiv1.SparkTransform{Cluster: y.Cluster, Code: code, Conf: y.Conf, Outputs: outputs}, nil
}

// parseOutputs checks a transform's outputs.
func parseOutputs(field string, outputs []outputYAML) ([]*apiv1.TransformOutput, error) {
	var out []*apiv1.TransformOutput
	seen := map[string]bool{}
	var err error
	for i, o := range outputs {
		f := fmt.Sprintf("%s.outputs[%d]", field, i)
		if o.Name == "" || seen[o.Name] {
			return nil, fmt.Errorf("%s: missing or repeated name %q", f, o.Name)
		}
		seen[o.Name] = true
		keys := map[string]bool{}
		for _, k := range o.PrimaryKey {
			if k == "" || keys[k] {
				return nil, fmt.Errorf("%s.primaryKey: missing or repeated column %q", f, k)
			}
			keys[k] = true
		}
		t := &apiv1.TransformOutput{Name: o.Name, PrimaryKey: o.PrimaryKey}
		if t.Write, err = enum(apiv1.WriteMode_value, "WRITE_MODE_", f+".write", o.Write, apiv1.WriteMode_WRITE_MODE_UNSPECIFIED); err != nil {
			return nil, err
		}
		if t.Require, err = enum(apiv1.DeliveryLevel_value, "DELIVERY_LEVEL_", f+".require", o.Require, apiv1.DeliveryLevel_DELIVERY_LEVEL_UNSPECIFIED); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, nil
}

type sparkAccessYAML struct {
	EmrRole string `yaml:"emrRole"`
	Keys    *struct {
		AccessKeyID     yaml.Node `yaml:"accessKeyId"`
		SecretAccessKey yaml.Node `yaml:"secretAccessKey"`
	} `yaml:"keys"`
}

var iamRole = regexp.MustCompile(`^arn:aws[a-z-]*:iam::\d{12}:role/\S+$`)

func parseSparkAccess(y *sparkAccessYAML) (*apiv1.SparkAccess, error) {
	return parseAccess("sparkAccess", y)
}

// parseAccess checks a cluster's access to the data plane's store.
func parseAccess(field string, y *sparkAccessYAML) (*apiv1.SparkAccess, error) {
	switch {
	case y.EmrRole == "" && y.Keys == nil:
		return nil, fmt.Errorf("%s needs emrRole, keys, or both", field)
	case y.EmrRole != "" && !iamRole.MatchString(y.EmrRole):
		return nil, fmt.Errorf("%s.emrRole: %q isn't an IAM role's ARN", field, y.EmrRole)
	}
	a := &apiv1.SparkAccess{EmrRole: y.EmrRole}
	if y.Keys == nil {
		return a, nil
	}
	a.Keys = &structpb.Struct{Fields: map[string]*structpb.Value{}}
	for _, k := range []struct {
		name string
		n    *yaml.Node
	}{{"accessKeyId", &y.Keys.AccessKeyID}, {"secretAccessKey", &y.Keys.SecretAccessKey}} {
		f := field + ".keys." + k.name
		n := k.n
		if n.Kind == 0 {
			return nil, fmt.Errorf("%s is missing", f)
		}
		v, err := sparkConfValue(f, n)
		if err != nil {
			return nil, err
		}
		if k.name == "secretAccessKey" && v.GetStructValue() == nil {
			return nil, fmt.Errorf("%s: a secret reference, such as {secret: name}, so the key isn't kept in the pipeline's spec", f)
		}
		a.Keys.Fields[k.name] = v
	}
	return a, nil
}
