package spec

import (
	"strings"
	"testing"
)

func TestParseSparkCluster(t *testing.T) {
	header := "apiVersion: openetl.io/v1alpha1\nkind: SparkCluster\nmetadata: {name: lab}\n"
	doc, err := Parse([]byte(header + "spec:\n  submit:\n    master: spark://spark:7077\n    conf:\n      spark.executor.memory: 2g\n      spark.executor.cores: 2\n      spark.hadoop.fs.s3a.secret.key: {secret: s3a-key}\n"))
	if err != nil {
		t.Fatal(err)
	}
	s := doc.SparkCluster.GetSubmit()
	conf := s.GetConf().GetFields()
	if doc.Kind != KindSparkCluster || s.GetMaster() != "spark://spark:7077" || conf["spark.executor.memory"].GetStringValue() != "2g" ||
		conf["spark.executor.cores"].GetStringValue() != "2" || conf["spark.hadoop.fs.s3a.secret.key"].GetStructValue().GetFields()["secret"].GetStringValue() != "s3a-key" {
		t.Errorf("doc = %+v", doc)
	}
	doc, err = Parse([]byte(header + "spec: {emr: {clusterName: nightly, region: eu-west-1}}"))
	if err != nil {
		t.Fatal(err)
	}
	if e := doc.SparkCluster.GetEmr(); e.GetClusterName() != "nightly" || e.GetRegion() != "eu-west-1" {
		t.Errorf("emr = %+v", e)
	}
	for name, spec := range map[string]string{
		"neither":         "spec: {}",
		"both":            "spec: {submit: {master: yarn}, emr: {clusterId: j-1, region: us-east-1}}",
		"local master":    "spec: {submit: {master: 'local[2]'}}",
		"not spark.":      "spec: {submit: {master: yarn, conf: {executor.memory: 2g}}}",
		"runner's":        "spec: {submit: {master: yarn, conf: {spark.submit.deployMode: client}}}",
		"ours":            "spec: {submit: {master: yarn, conf: {spark.openetl.context: x}}}",
		"list":            "spec: {submit: {master: yarn, conf: {spark.jars: [a, b]}}}",
		"not a secret":    "spec: {submit: {master: yarn, conf: {spark.x: {value: b}}}}",
		"secret and more": "spec: {submit: {master: yarn, conf: {spark.x: {secret: s, field: a}}}}",
		"id and name":     "spec: {emr: {clusterId: j-1, clusterName: n, region: us-east-1}}",
		"no region":       "spec: {emr: {clusterId: j-1}}",
	} {
		if _, err := Parse([]byte(header + spec)); err == nil {
			t.Errorf("%s: parsed", name)
		}
	}
}

func TestParseSparkStep(t *testing.T) {
	header := "apiVersion: openetl.io/v1alpha1\nkind: Pipeline\nmetadata: {name: p}\n"
	base := "spec:\n  schedule: {every: 1m}\n  source: {ref: s}\n  destination: {ref: d}\n"
	digest := strings.Repeat("ab", 32)
	doc, err := Parse([]byte(header + base + "  postSync:\n    - dbt: {project: a}\n    - spark:\n        cluster: lab\n        code: {jar: 's3://jobs/models.jar', sha256: " + digest + ", class: com.acme.Models}\n        args: [--full]\n        conf: {spark.executor.memory: 8g}\n"))
	if err != nil {
		t.Fatal(err)
	}
	steps := doc.Pipeline.GetPostSync()
	s := steps[1].GetSpark()
	if len(steps) != 2 || steps[0].GetDbt() == nil || s.GetCluster() != "lab" || s.GetCode().GetJar() != "s3://jobs/models.jar" ||
		s.GetCode().GetSha256() != digest || s.GetCode().GetMainClass() != "com.acme.Models" || s.GetArgs()[0] != "--full" ||
		s.GetConf()["spark.executor.memory"] != "8g" {
		t.Errorf("steps = %v", steps)
	}
	step := func(spark string) string { return header + base + "  postSync: [{spark: " + spark + "}]\n" }
	for name, doc := range map[string]string{
		"both":         header + base + "  postSync: [{dbt: {project: a}, spark: {cluster: lab, code: {jar: 's3://j/a.jar', sha256: " + digest + "}}}]\n",
		"cluster":      step("{cluster: Lab, code: {jar: 's3://j/a.jar', sha256: " + digest + "}}"),
		"no code":      step("{cluster: lab}"),
		"jar and py":   step("{cluster: lab, code: {jar: 's3://j/a.jar', python: 's3://j/a.py', sha256: " + digest + "}}"),
		"short digest": step("{cluster: lab, code: {jar: 's3://j/a.jar', sha256: abc}}"),
		"upper digest": step("{cluster: lab, code: {jar: 's3://j/a.jar', sha256: " + strings.ToUpper(digest) + "}}"),
		"python class": step("{cluster: lab, code: {python: 's3://j/a.py', sha256: " + digest + ", class: Main}}"),
		"hdfs":         step("{cluster: lab, code: {jar: 'hdfs:///j/a.jar', sha256: " + digest + "}}"),
		"local":        step("{cluster: lab, code: {jar: 'local:///opt/a.jar', sha256: " + digest + "}}"),
		"http":         step("{cluster: lab, code: {jar: 'http://j/a.jar', sha256: " + digest + "}}"),
		"app name":     step("{cluster: lab, code: {jar: 's3://j/a.jar', sha256: " + digest + "}, conf: {spark.app.name: x}}"),
	} {
		if _, err := Parse([]byte(doc)); err == nil {
			t.Errorf("%s: parsed", name)
		}
	}
}
