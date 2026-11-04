package spec

import (
	"strings"
	"testing"
)

func TestParseFlinkCluster(t *testing.T) {
	header := "apiVersion: openetl.io/v1alpha1\nkind: FlinkCluster\nmetadata: {name: fl}\n"
	doc, err := Parse([]byte(header + "spec:\n  kubernetes:\n    namespace: flink\n    image: acme/flink:1.20\n    kubeconfig: {secret: kube}\n    taskManager: {cpu: 2, memory: 4g}\n  conf:\n    state.backend.type: rocksdb\n    s3.secret-key: {secret: s3}\n"))
	if err != nil {
		t.Fatal(err)
	}
	k := doc.FlinkCluster.GetKubernetes()
	conf := doc.FlinkCluster.GetConf().GetFields()
	if doc.Kind != KindFlinkCluster || k.GetNamespace() != "flink" || k.GetImage() != "acme/flink:1.20" ||
		k.GetKubeconfig().GetStructValue().GetFields()["secret"].GetStringValue() != "kube" || k.GetTaskManager().GetCpu() != 2 ||
		k.GetTaskManager().GetMemory() != "4g" || k.GetJobManager() != nil || conf["state.backend.type"].GetStringValue() != "rocksdb" ||
		conf["s3.secret-key"].GetStructValue().GetFields()["secret"].GetStringValue() != "s3" {
		t.Errorf("doc = %+v", doc.FlinkCluster)
	}
	doc, err = Parse([]byte(header + "spec: {rest: {url: 'http://flink:8081/', auth: {secret: token}}}"))
	if err != nil {
		t.Fatal(err)
	}
	if r := doc.FlinkCluster.GetRest(); r.GetUrl() != "http://flink:8081" || r.GetAuth().GetStructValue() == nil {
		t.Errorf("rest = %+v", r)
	}
	doc, err = Parse([]byte(header + "spec: {emr: {clusterId: j-1, region: eu-west-1}}"))
	if err != nil {
		t.Fatal(err)
	}
	if e := doc.FlinkCluster.GetEmr(); e.GetClusterId() != "j-1" || e.GetRegion() != "eu-west-1" {
		t.Errorf("emr = %+v", e)
	}
	for name, spec := range map[string]string{
		"neither":         "spec: {conf: {a.b: c}}",
		"two":             "spec: {rest: {url: 'http://f:8081'}, emr: {clusterId: j-1, region: us-east-1}}",
		"namespace":       "spec: {kubernetes: {namespace: Flink}}",
		"inline config":   "spec: {kubernetes: {namespace: f, kubeconfig: 'apiVersion: v1'}}",
		"memory":          "spec: {kubernetes: {namespace: f, jobManager: {memory: lots}}}",
		"cpu":             "spec: {kubernetes: {namespace: f, jobManager: {cpu: -1}}}",
		"url":             "spec: {rest: {url: 'flink:8081'}}",
		"inline token":    "spec: {rest: {url: 'http://f:8081', auth: abc}}",
		"id and name":     "spec: {emr: {clusterId: j-1, clusterName: n, region: us-east-1}}",
		"no region":       "spec: {emr: {clusterId: j-1}}",
		"runner's":        "spec: {rest: {url: 'http://f:8081'}, conf: {execution.checkpointing.interval: 1s}}",
		"restart":         "spec: {rest: {url: 'http://f:8081'}, conf: {restart-strategy.type: none}}",
		"ha":              "spec: {rest: {url: 'http://f:8081'}, conf: {high-availability.type: zookeeper}}",
		"not an option":   "spec: {rest: {url: 'http://f:8081'}, conf: {parallelism: 2}}",
		"secret and more": "spec: {rest: {url: 'http://f:8081'}, conf: {a.b: {secret: s, field: a}}}",
	} {
		if _, err := Parse([]byte(header + spec)); err == nil {
			t.Errorf("%s: parsed", name)
		}
	}
}

func TestParseFlinkTransform(t *testing.T) {
	header := "apiVersion: openetl.io/v1alpha1\nkind: Pipeline\nmetadata: {name: p}\n"
	base := "spec:\n  mode: continuous\n  source: {ref: s, streams: [{name: payments}]}\n  destination: {ref: d}\n"
	digest := strings.Repeat("cd", 32)
	transform := func(flink string) string { return header + base + "  transform:\n    flink: " + flink + "\n" }
	doc, err := Parse([]byte(transform("{cluster: fl, code: {jar: 's3://jobs/fraud.jar', sha256: "+digest+", class: com.acme.Fraud}, parallelism: 4, conf: {state.backend.type: rocksdb}, outputs: [{name: scored, primaryKey: [id], write: merge}]}") +
		"  flinkAccess: {keys: {accessKeyId: AK, secretAccessKey: {secret: sk}}}\n"))
	if err != nil {
		t.Fatal(err)
	}
	f := doc.Pipeline.GetTransform().GetFlink()
	if f.GetCluster() != "fl" || f.GetCode().GetJar() != "s3://jobs/fraud.jar" || f.GetCode().GetSha256() != digest || f.GetCode().GetMainClass() != "com.acme.Fraud" ||
		f.GetParallelism() != 4 || f.GetConf()["state.backend.type"] != "rocksdb" || len(f.GetOutputs()) != 1 || f.GetOutputs()[0].GetPrimaryKey()[0] != "id" ||
		doc.Pipeline.GetFlinkAccess().GetKeys().GetFields()["accessKeyId"].GetStringValue() != "AK" {
		t.Errorf("transform = %+v, access = %+v", f, doc.Pipeline.GetFlinkAccess())
	}
	if _, err := Parse([]byte(transform("{cluster: fl, code: {jar: 'https://jobs/f.jar', sha256: " + digest + "}}"))); err != nil {
		t.Errorf("without a class: %v", err)
	}
	scheduled := header + "spec:\n  schedule: {every: 1m}\n  source: {ref: s}\n  destination: {ref: d}\n"
	for name, doc := range map[string]string{
		"scheduled":    scheduled + "  transform: {flink: {cluster: fl, code: {jar: 's3://j/a.jar', sha256: " + digest + "}}}\n",
		"both":         transform("{cluster: fl, code: {jar: 's3://j/a.jar', sha256: "+digest+"}}") + "    spark: {cluster: lab, code: {jar: 's3://j/a.jar', sha256: " + digest + ", class: A}}\n",
		"spark":        header + base + "  transform: {spark: {cluster: lab, code: {jar: 's3://j/a.jar', sha256: " + digest + ", class: A}}}\n",
		"cluster":      transform("{cluster: Fl, code: {jar: 's3://j/a.jar', sha256: " + digest + "}}"),
		"no jar":       transform("{cluster: fl, code: {sha256: " + digest + "}}"),
		"python":       transform("{cluster: fl, code: {python: 's3://j/a.py', sha256: " + digest + "}}"),
		"local":        transform("{cluster: fl, code: {jar: 'local:///opt/a.jar', sha256: " + digest + "}}"),
		"digest":       transform("{cluster: fl, code: {jar: 's3://j/a.jar', sha256: abc}}"),
		"parallelism":  transform("{cluster: fl, code: {jar: 's3://j/a.jar', sha256: " + digest + "}, parallelism: -1}"),
		"runner's":     transform("{cluster: fl, code: {jar: 's3://j/a.jar', sha256: " + digest + "}, conf: {execution.checkpointing.timeout: 1m}}"),
		"output twice": transform("{cluster: fl, code: {jar: 's3://j/a.jar', sha256: " + digest + "}, outputs: [{name: a}, {name: a}]}"),
		"collapse": header + "spec:\n  mode: continuous\n  source: {ref: s, streams: [{name: payments, collapse: true}]}\n  destination: {ref: d}\n" +
			"  transform: {flink: {cluster: fl, code: {jar: 's3://j/a.jar', sha256: " + digest + "}}}\n",
		"spark access": transform("{cluster: fl, code: {jar: 's3://j/a.jar', sha256: "+digest+"}}") + "  sparkAccess: {emrRole: 'arn:aws:iam::123456789012:role/r'}\n",
		"no transform": header + base + "  flinkAccess: {emrRole: 'arn:aws:iam::123456789012:role/r'}\n",
		"inline key":   transform("{cluster: fl, code: {jar: 's3://j/a.jar', sha256: "+digest+"}}") + "  flinkAccess: {keys: {accessKeyId: AK, secretAccessKey: SK}}\n",
	} {
		if _, err := Parse([]byte(doc)); err == nil {
			t.Errorf("%s: parsed", name)
		}
	}
}
