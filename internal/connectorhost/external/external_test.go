package external

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/util"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/platform/resources"
	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

var connectors string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "oc-test-")
	if err != nil {
		panic(err)
	}
	bin := filepath.Join(dir, "testconnector")
	if out, err := exec.Command("go", "build", "-o", bin, "./testconnector").CombinedOutput(); err != nil {
		panic(string(out))
	}
	for _, name := range []string{"source-faker", "source-typed", "source-crash", "source-hang", "source-leaky", "source-bulky", "source-env", "source-future", "source-supports", "source-echo", "source-prepared", "source-unprepared", "destination-files", "source-both"} {
		if err := os.Link(bin, filepath.Join(dir, name)); err != nil {
			panic(err)
		}
	}
	connectors = dir
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func exe(name string) string { return filepath.Join(connectors, name) }

// collect is an Emitter that keeps what it gets.
type collect struct {
	mu       sync.Mutex
	rows     int
	raw      []json.RawMessage
	batches  []arrow.RecordBatch
	resumes  []string
	complete []string
}

func (c *collect) Rows(_ string, rows []json.RawMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rows += len(rows)
	c.raw = append(c.raw, rows...)
	return nil
}

func (c *collect) Records(_ string, b arrow.RecordBatch) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.batches = append(c.batches, b)
	return nil
}

func (c *collect) ResumePoint(stream string, state []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resumes = append(c.resumes, stream+"="+string(state))
	return nil
}

func (c *collect) Split(stream, id string) source.SplitEmitter {
	return source.WholeSplit(c, stream, id)
}

func (c *collect) Complete(stream string, _ []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.complete = append(c.complete, stream)
	return nil
}

func TestSourceRawRows(t *testing.T) {
	src := NewSource(exe("source-faker"), []byte(`{"seed": 1, "count": 250, "batchSize": 100}`), Options{})
	defer func() { _ = src.Close() }()
	cat, err := src.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var streams []source.ConfiguredStream
	for _, s := range cat.Streams {
		streams = append(streams, source.ConfiguredStream{Name: s.Name})
	}
	if len(streams) != 2 || cat.Streams[0].Replay != source.Exact {
		t.Fatalf("catalog = %+v", cat)
	}
	var out collect
	if err := src.Read(t.Context(), source.ReadRequest{Streams: streams}, &out); err != nil {
		t.Fatal(err)
	}
	if out.rows != 500 || len(out.complete) != 2 || len(out.resumes) != 6 {
		t.Fatalf("rows %d, resume points %v, complete %v", out.rows, out.resumes, out.complete)
	}
	// Faker doesn't acknowledge, so this is a no-op.
	if err := src.Committed(t.Context(), map[string][]byte{"users": []byte("1")}, nil); err != nil {
		t.Fatal(err)
	}
}

func TestSourceArrow(t *testing.T) {
	src := NewSource(exe("source-typed"), nil, Options{})
	defer func() { _ = src.Close() }()
	cat, err := src.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if got := cat.Streams[0].Schema.Field(0).Type; !arrow.TypeEqual(got, extensions.NewUUIDType()) {
		t.Fatalf("declared id type = %v", got)
	}
	var out collect
	if err := src.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "things"}}}, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.batches) != 1 {
		t.Fatalf("%d batches", len(out.batches))
	}
	b := out.batches[0]
	defer b.Release()
	if b.NumRows() != 3 || !arrow.TypeEqual(b.Column(0).DataType(), extensions.NewUUIDType()) || b.Column(1).(*array.Int64).Value(2) != 2 {
		t.Fatalf("batch = %v", b)
	}
	if strings.Join(out.resumes, ",") != "things=3" || len(out.complete) != 1 {
		t.Fatalf("resume points %v, complete %v", out.resumes, out.complete)
	}
}

// forward is a Forwarder that keeps what it gets.
type forward struct {
	collect
	encoded []arrowipc.Encoded
}

func (f *forward) Forward(_ string, b arrowipc.Encoded) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.encoded = append(f.encoded, b)
	return nil
}

func (f *forward) Split(stream, id string) source.SplitEmitter {
	return source.WholeSplit(f, stream, id)
}

func TestSourceForwardsArrow(t *testing.T) {
	src := NewSource(exe("source-typed"), nil, Options{})
	defer func() { _ = src.Close() }()
	var out forward
	if err := src.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "things"}}}, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.batches) != 0 || len(out.encoded) != 1 {
		t.Fatalf("%d batches decoded, %d forwarded", len(out.batches), len(out.encoded))
	}
	enc := out.encoded[0]
	b, err := arrowipc.DecodeBatch(enc.Bytes, memory.DefaultAllocator)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Release()
	if enc.Rows != 3 || enc.Size != util.TotalRecordSize(b) || !arrow.TypeEqual(b.Column(0).DataType(), extensions.NewUUIDType()) {
		t.Fatalf("forwarded %d rows, %d bytes: %v", enc.Rows, enc.Size, b)
	}
}

func TestSourceCrash(t *testing.T) {
	src := NewSource(exe("source-crash"), nil, Options{})
	defer func() { _ = src.Close() }()
	err := src.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "s"}}}, &collect{})
	if errs.CodeOf(err) != errs.Internal || !strings.Contains(err.Error(), "exited unexpectedly") {
		t.Fatalf("err = %v", err)
	}
}

func TestSourceKilled(t *testing.T) {
	src := NewSource(exe("source-faker"), []byte(`{"count": 100000, "batchSize": 10, "delay": "10ms"}`), Options{})
	defer func() { _ = src.Close() }()
	if err := src.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	out := &killer{kill: func() { _ = src.p.cmd.Process.Kill() }}
	err := src.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "users"}}}, out)
	if errs.CodeOf(err) != errs.ResourceExhausted {
		t.Fatalf("err = %v, want RESOURCE_EXHAUSTED for a SIGKILL nobody asked for", err)
	}
}

// killer kills the connector at its first resume point.
type killer struct {
	collect
	kill func()
	once sync.Once
}

func (k *killer) ResumePoint(string, []byte) error {
	k.once.Do(k.kill)
	return nil
}

func (k *killer) Split(stream, id string) source.SplitEmitter {
	return source.WholeSplit(k, stream, id)
}

// The runner's environment holds its own credentials.
func TestConnectorsDontInheritTheEnvironment(t *testing.T) {
	t.Setenv("AWS_SECRET_ACCESS_KEY", "runner's")
	src := NewSource(exe("source-env"), nil, Options{})
	defer func() { _ = src.Close() }()
	var out collect
	if err := src.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "env"}}}, &out); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, r := range out.raw {
		var v struct{ Name string }
		if err := json.Unmarshal(r, &v); err != nil {
			t.Fatal(err)
		}
		names = append(names, v.Name)
	}
	if slices.Contains(names, "AWS_SECRET_ACCESS_KEY") || !slices.Contains(names, "OPENETL_CONNECTOR_ADDR") || !slices.Contains(names, "OPENETL_CONNECTOR_ROLE") {
		t.Fatalf("the connector's environment: %v", names)
	}
}

// What the AWS SDK's default chain finds in a connector's process: nothing
// of the runner's unless it inherits, and a role's alone when it has one.
func TestConnectorsAWSCredentials(t *testing.T) {
	home := t.TempDir()
	if err := os.MkdirAll(filepath.Join(home, ".aws"), 0o700); err != nil {
		t.Fatal(err)
	}
	profile := "[default]\naws_access_key_id = PROFILEKEY\naws_secret_access_key = secret\n"
	if err := os.WriteFile(filepath.Join(home, ".aws", "credentials"), []byte(profile), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	role := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "lease-token" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		_, _ = fmt.Fprintf(w, `{"AccessKeyId": "ROLEKEY", "SecretAccessKey": "s", "Token": "t", "Expiration": %q}`, time.Now().Add(time.Hour).UTC().Format(time.RFC3339))
	}))
	defer role.Close()
	found := func(a AWS) string {
		t.Helper()
		src := NewSource(exe("source-env"), nil, Options{AWS: a})
		defer func() { _ = src.Close() }()
		var out collect
		if err := src.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "env"}}}, &out); err != nil {
			t.Fatal(err)
		}
		for _, r := range out.raw {
			var v struct{ Name, Value string }
			if err := json.Unmarshal(r, &v); err != nil {
				t.Fatal(err)
			}
			if v.Name == "aws-default-chain" {
				return v.Value
			}
		}
		t.Fatal("the connector didn't say what the chain found")
		return ""
	}

	if got := found(AWS{}); !strings.HasPrefix(got, "error: ") {
		t.Errorf("none, with a profile in HOME: the chain found %s", got)
	}
	t.Setenv("AWS_ACCESS_KEY_ID", "RUNNERKEY")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	if got := found(AWS{}); !strings.HasPrefix(got, "error: ") {
		t.Errorf("none, with keys in the runner's environment: the chain found %s", got)
	}
	if got := found(AWS{Inherit: true}); got != "RUNNERKEY" {
		t.Errorf("inherit: the chain found %s, want the runner's keys", got)
	}
	if got := found(AWS{Inherit: true, URL: role.URL, Token: "lease-token"}); got != "ROLEKEY" {
		t.Errorf("a role, where the runner's would be inherited: the chain found %s, want the role's", got)
	}
	if got := found(AWS{URL: role.URL, Token: "another"}); !strings.HasPrefix(got, "error: ") {
		t.Errorf("a role, with the wrong token: the chain found %s", got)
	}
}

// AWS_EC2_METADATA_DISABLED is what keeps the chain off EC2's instance
// metadata, which a connector can reach wherever the runner can.
func TestNoAWSSkipsInstanceMetadata(t *testing.T) {
	imds := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			w.Header().Set("X-Aws-Ec2-Metadata-Token-Ttl-Seconds", "21600")
			_, _ = io.WriteString(w, "token")
		case strings.HasSuffix(r.URL.Path, "/security-credentials/"):
			_, _ = io.WriteString(w, "instance-role")
		default:
			_, _ = fmt.Fprintf(w, `{"Code": "Success", "AccessKeyId": "IMDSKEY", "SecretAccessKey": "s", "Token": "t", "Expiration": %q}`, time.Now().Add(time.Hour).UTC().Format(time.RFC3339))
		}
	}))
	defer imds.Close()
	for _, kv := range os.Environ() {
		if name, _, _ := strings.Cut(kv, "="); strings.HasPrefix(name, "AWS_") {
			t.Setenv(name, "")
		}
	}
	t.Setenv("AWS_EC2_METADATA_SERVICE_ENDPOINT", imds.URL)
	retrieve := func() string {
		t.Helper()
		cfg, err := awsconfig.LoadDefaultConfig(t.Context(), awsconfig.WithRegion("us-east-1"))
		if err != nil {
			t.Fatal(err)
		}
		creds, err := cfg.Credentials.Retrieve(t.Context())
		if err != nil {
			return "error: " + err.Error()
		}
		return creds.AccessKeyID
	}
	if got := retrieve(); got != "IMDSKEY" {
		t.Fatalf("the fake instance metadata gave %s", got)
	}
	for _, kv := range (AWS{}).env(t.TempDir()) {
		name, value, _ := strings.Cut(kv, "=")
		t.Setenv(name, value)
	}
	if got := retrieve(); !strings.HasPrefix(got, "error: ") {
		t.Errorf("none: the chain found %s in instance metadata", got)
	}
}

func TestSizeSetsAGoConnectorsLimits(t *testing.T) {
	envOf := func(opts Options) map[string]bool {
		src := NewSource(exe("source-env"), nil, opts)
		defer func() { _ = src.Close() }()
		var out collect
		if err := src.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "env"}}}, &out); err != nil {
			t.Fatal(err)
		}
		got := map[string]bool{}
		for _, r := range out.raw {
			var v struct{ Name, Value string }
			if err := json.Unmarshal(r, &v); err != nil {
				t.Fatal(err)
			}
			got[v.Name+"="+v.Value] = true
		}
		return got
	}
	if got := envOf(Options{Size: resources.M}); !got["GOMAXPROCS=2"] || !got["GOMEMLIMIT=2147483648"] {
		t.Errorf("a job's connector's environment: %v", got)
	}
	for kv := range envOf(Options{}) {
		if strings.HasPrefix(kv, "GOM") {
			t.Errorf("a connector outside a job got %s", kv)
		}
	}
}

// An executable with both roles serves the one the runner starts.
func TestRolesShareAnExecutable(t *testing.T) {
	for _, kind := range []protocolv1.ConnectorKind{protocolv1.ConnectorKind_CONNECTOR_KIND_SOURCE, protocolv1.ConnectorKind_CONNECTOR_KIND_DESTINATION} {
		s, err := ReadSpec(t.Context(), exe("source-both"), kind, Options{})
		if err != nil || s.GetKind() != kind {
			t.Errorf("as a %s: %v, %v", kind, s.GetKind(), err)
		}
	}
	if _, err := ReadSpec(t.Context(), exe("source-both"), protocolv1.ConnectorKind_CONNECTOR_KIND_UNSPECIFIED, Options{}); err == nil {
		t.Error("started without a role")
	}
	src := NewSource(exe("source-both"), []byte(`{"seed": 1, "count": 3}`), Options{})
	defer func() { _ = src.Close() }()
	if _, err := src.Discover(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// The runner tells a connector its features in Spec, and refuses one that
// needs a feature it hasn't.
func TestHandshake(t *testing.T) {
	src := NewSource(exe("source-future"), nil, Options{})
	defer func() { _ = src.Close() }()
	if err := src.Start(t.Context()); errs.CodeOf(err) != errs.ConfigInvalid || !strings.Contains(err.Error(), "from-the-future") {
		t.Errorf("a connector needing a feature the runner hasn't: %v", err)
	}
	Features = []string{"testing"}
	defer func() { Features = nil }()
	sup := NewSource(exe("source-supports"), nil, Options{})
	defer func() { _ = sup.Close() }()
	var out collect
	if err := sup.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "supports"}}}, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.raw) != 1 || string(out.raw[0]) != `{"other":false,"testing":true}` {
		t.Errorf("the connector saw %s", out.raw)
	}
}

// A stream's read mode, cursor and state reach the connector.
func TestConfiguredStreams(t *testing.T) {
	src := NewSource(exe("source-echo"), nil, Options{})
	defer func() { _ = src.Close() }()
	var out collect
	req := source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "echo", SyncMode: source.Incremental, Cursor: "updated_at", State: []byte(`{"since":"5"}`), Columns: []string{"id", "updated_at"}}}}
	if err := src.Read(t.Context(), req, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.raw) != 1 || string(out.raw[0]) != `{"columns":["id","updated_at"],"cursor":"updated_at","mode":2,"state":"{\"since\":\"5\"}"}` {
		t.Errorf("the connector saw %s", out.raw)
	}
}

// Where a read of a whole stream or of a split is to end reaches the
// connector.
func TestReadsUntil(t *testing.T) {
	src := NewSource(exe("source-echo"), nil, Options{})
	defer func() { _ = src.Close() }()
	var out collect
	req := source.ReadRequest{Streams: []source.ConfiguredStream{
		{Name: "echo", State: []byte("1"), Until: []byte("9")},
		{Name: "echo", Splits: []source.Split{{ID: "p0", State: []byte("2"), Until: []byte("8")}, {ID: "p1"}}},
	}}
	if err := src.Read(t.Context(), req, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.raw) != 2 || !strings.Contains(string(out.raw[0]), `"until":"9"`) || !strings.Contains(string(out.raw[1]), `"splits":["p0@2..8","p1@.."]`) {
		t.Errorf("the connector saw %s", out.raw)
	}
}

// Setup and Teardown reach a connector whose spec sets setup, and only one.
func TestSetupAndTeardown(t *testing.T) {
	for name, calls := range map[string]bool{"source-prepared": true, "source-unprepared": false} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			cfg, _ := json.Marshal(map[string]string{"dir": dir})
			src := NewSource(exe(name), cfg, Options{})
			defer func() { _ = src.Close() }()
			if err := src.Setup(t.Context(), "p1"); err != nil {
				t.Fatal(err)
			}
			f := filepath.Join(dir, "p1")
			if _, err := os.Stat(f); (err == nil) != calls {
				t.Errorf("after Setup: %v", err)
			}
			if err := os.WriteFile(f, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := src.Teardown(t.Context(), "p1"); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(f); (err != nil) != calls {
				t.Errorf("after Teardown: %v", err)
			}
		})
	}
}

func TestUnknownExecutable(t *testing.T) {
	src := NewSource(filepath.Join(t.TempDir(), "source-none"), nil, Options{})
	if err := src.Start(t.Context()); errs.CodeOf(err) != errs.ConfigInvalid {
		t.Fatalf("err = %v", err)
	}
	if _, err := NewSource(exe("destination-files"), nil, Options{}).Discover(t.Context()); err == nil || !strings.Contains(err.Error(), "not a CONNECTOR_KIND_SOURCE") {
		t.Fatalf("a destination started as a source: %v", err)
	}
}

func filesConfig(dir, token string) []byte {
	b, _ := json.Marshal(map[string]string{"dir": dir, "token": token})
	return b
}

func TestDestinationSecrets(t *testing.T) {
	dir := t.TempDir()
	d := NewDestination(exe("destination-files"), filesConfig(dir, "s3cr3t"), Options{})
	defer func() { _ = d.Close() }()
	if err := d.Check(t.Context()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(d.p.config), "s3cr3t") {
		t.Fatalf("the config sent to the connector holds the secret: %s", d.p.config)
	}
	if !slices.Contains(d.p.secrets, "s3cr3t") {
		t.Errorf("the host doesn't know the secret it gave: %q", d.p.secrets)
	}
	if c := d.Capabilities(); c.Class != destination.StageAndApply || c.MaxIdentifierLength != 63 || !c.SupportsScope(destination.ScopePipeline) {
		t.Fatalf("capabilities = %+v", c)
	}

	bad := NewDestination(exe("destination-files"), filesConfig(dir, "nope"), Options{})
	defer func() { _ = bad.Close() }()
	if err := bad.Check(t.Context()); errs.CodeOf(err) != errs.AuthInvalid || err.Error() != "Sign in: The token of 4 bytes isn't the directory's." {
		t.Fatalf("check with a wrong token: %v", err)
	}
}

// A connector's checks and preflight come back item by item, with their
// templates and params apart.
func TestDestinationCheckAndPreflight(t *testing.T) {
	dir := t.TempDir()
	d := NewDestination(exe("destination-files"), filesConfig(dir, "s3cr3t"), Options{})
	defer func() { _ = d.Close() }()
	if r, err := d.CheckReport(t.Context()); err != nil || !r.OK() || statuses(r) != "token=passed dir=passed replicated=warning" || r.Items[1].Rendered().Message != dir+" is there" {
		t.Errorf("a good check: %s, %v, %v", statuses(r), r.Err(), err)
	}

	bad := NewDestination(exe("destination-files"), filesConfig(dir, "nope"), Options{})
	defer func() { _ = bad.Close() }()
	r, err := bad.CheckReport(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if statuses(r) != "token=failed dir=skipped replicated=warning" || errs.CodeOf(r.Err()) != errs.AuthInvalid {
		t.Fatalf("a check with a wrong token: %s, %v", statuses(r), r.Err())
	}
	token := r.Items[0]
	if token.Message != tokenRejected || token.Field != "token" || token.Kind != check.Authentication || token.Params["length"] != "4" {
		t.Errorf("the token's item, as it came: %+v", token)
	}
	if it := token.Rendered(); it.Message != "The token of 4 bytes isn't the directory's." || it.Failure.Fix != "Set token to the token you were given." ||
		it.Failure.Reason != "token_rejected" || it.Failure.Code != protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID {
		t.Errorf("the token's item %+v, %+v", it, it.Failure)
	}
	if it := r.Items[1]; it.Message != "needs token, which failed" {
		t.Errorf("the directory's item %+v", it)
	}
	if f := r.Items[2].Failure; f == nil || f.Detail != "a directory is on one disk" {
		t.Errorf("the warning %+v", r.Items[2])
	}

	r, err = d.PreflightReport(t.Context(), destination.PreflightRequest{Pipeline: "p", Streams: []destination.Stream{{Name: "s", Mode: destination.Append}, {Name: "m", Mode: destination.Merge}}})
	if err != nil {
		t.Fatal(err)
	}
	if statuses(r) != "stream.s=passed stream.m=failed" || errs.CodeOf(r.Err()) != errs.ConfigInvalid || r.Items[1].Failure.Reason != "setting_mismatch" {
		t.Errorf("a preflight: %s, %v", statuses(r), r.Err())
	}
}

const tokenRejected = "The token of {length} bytes isn't the directory's."

func statuses(r check.Report) string {
	var s []string
	for _, it := range r.Items {
		s = append(s, it.Name+"="+it.Status.String())
	}
	return strings.Join(s, " ")
}

// An answer is read failing closed, a connector without a preflight passes
// it, and one that doesn't answer is an error, not a report.
func TestPreflightReport(t *testing.T) {
	p := &process{secrets: []string{"s3cr3t"}}
	if r, err := preflightReport(p, nil, status.Error(codes.Unimplemented, "unknown method")); err != nil || !r.OK() || len(r.Items) != 0 {
		t.Errorf("Unimplemented: %s, %v, %v", statuses(r), r.Err(), err)
	}
	r, err := preflightReport(p, &protocolv1.PreflightSourceResponse{Items: []*protocolv1.CheckItem{{Name: "a", Title: "A", Status: 9}}}, nil)
	if err != nil || r.OK() || statuses(r) != "a=failed" {
		t.Errorf("an unknown status: %s, %v, %v", statuses(r), r.Err(), err)
	}
	r, err = preflightReport(p, nil, status.Error(codes.Unavailable, "dial s3cr3t:\x1b[2J"))
	if len(r.Items) != 0 || errs.CodeOf(err) != errs.Internal || err.Error() != "INTERNAL: dial [redacted]: [2J" {
		t.Errorf("no answer: %s, %v", statuses(r), err)
	}
}

// A check that dies or doesn't answer in time is the check's error, not a
// report.
func TestCheckUnanswered(t *testing.T) {
	src := NewSource(exe("source-crash"), nil, Options{})
	defer func() { _ = src.Close() }()
	if r, err := src.CheckReport(t.Context()); len(r.Items) != 0 || errs.CodeOf(err) != errs.Internal || !strings.Contains(err.Error(), "exited unexpectedly") {
		t.Errorf("a check that dies: %s, %v", statuses(r), err)
	}

	hang := NewSource(exe("source-hang"), nil, Options{})
	defer func() { _ = hang.p.cmd.Process.Kill(); _ = hang.Close() }()
	if err := hang.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	if r, err := hang.CheckReport(ctx); len(r.Items) != 0 || !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("a check that doesn't answer: %s, %v", statuses(r), err)
	}
}

// What a connector answers is kept without the secrets the host gave it,
// without control characters, and within its bounds.
func TestCheckAnswerKept(t *testing.T) {
	src := NewSource(exe("source-leaky"), nil, Options{AWS: AWS{URL: "http://127.0.0.1:1/creds", Token: "lease-t0ken/+"}})
	defer func() { _ = src.Close() }()
	r, err := src.CheckReport(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Items) != maxItems || r.OK() || r.Err().Error() != "the connector answered with 2101 items, and an answer has at most 2000: the first 2000 are kept" {
		t.Fatalf("%d items, %v", len(r.Items), r.Err())
	}
	it := r.Items[0]
	if b, _ := json.Marshal(it); strings.Contains(string(b), "t0ken") {
		t.Errorf("the item shows the token: %s", b)
	}
	if it.Status != check.Passed || it.Title != "Use [redacted] ]52;c;ZXZpbA== " || it.Field != "aws.[redacted]" || it.Message != "found  [redacted]" {
		t.Errorf("the item %+v", it)
	}
	if len(it.Description) != maxText-1 || !strings.HasSuffix(it.Description, "é…") || len(it.Params["long"]) != maxParamValue || it.Params["token"] != "[redacted]" {
		t.Errorf("the item's description has %d bytes, its params %v", len(it.Description), it.Params)
	}
}

// A connector that inherits the runner's AWS variables has their secrets
// taken out of its answer.
func TestCheckAnswerKeptInherited(t *testing.T) {
	secrets := map[string]string{
		"AWS_SECRET_ACCESS_KEY":             "runner-secret-key",
		"AWS_SESSION_TOKEN":                 "runner-session-token",
		"AWS_CONTAINER_AUTHORIZATION_TOKEN": "runner-container-token",
	}
	for name, value := range secrets {
		t.Setenv(name, value)
	}
	src := NewSource(exe("source-leaky"), nil, Options{AWS: AWS{Inherit: true}})
	defer func() { _ = src.Close() }()
	r, err := src.CheckReport(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	it := r.Items[0]
	b, _ := json.Marshal(it)
	for name, value := range secrets {
		if strings.Contains(string(b), value) {
			t.Errorf("the item shows %s", name)
		}
	}
	for _, p := range []string{"token", "secret_key", "session_token"} {
		if it.Params[p] != "[redacted]" {
			t.Errorf("the param %s is %q", p, it.Params[p])
		}
	}
}

// answer takes out secrets as written and escaped, makes texts valid and
// cuts them, and keeps each item's status.
func TestAnswer(t *testing.T) {
	long := strings.Repeat("x", 5000)
	params := map[string]string{"pw": "p@ss word"}
	for i := range 60 {
		params[fmt.Sprintf("p%02d", i)] = long
	}
	r := answer([]string{"p@ss word"}, &protocolv1.CheckResponse{Items: []*protocolv1.CheckItem{
		{Name: "a", Title: "Reach p%40ss+word", Status: protocolv1.CheckStatus_CHECK_STATUS_FAILED, Detail: "line 1\n\tline 2\x00\u202e\xff" + long,
			Fix: "dsn postgres://u:p%40ss%20word@h " + long, Error: &protocolv1.Error{Code: protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID, Message: "p@ss word\nrejected"}},
		{Name: "b", Title: "B" + long, Status: protocolv1.CheckStatus_CHECK_STATUS_PASSED, Params: params},
	}})
	a, b := r.Items[0], r.Items[1]
	if a.Status != check.Failed || b.Status != check.Passed || errs.CodeOf(r.Err()) != errs.AuthInvalid || !strings.HasPrefix(r.Err().Error(), "Reach [redacted]: [redacted] rejected: line 1") {
		t.Errorf("%s, %v", statuses(r), r.Err())
	}
	if d := a.Failure.Detail; len(d) != maxDetail || !strings.HasPrefix(d, "line 1\n\tline 2  \uFFFDxxx") || !strings.HasSuffix(d, "x…") {
		t.Errorf("a's detail has %d bytes: %q", len(d), d[:30])
	}
	if f := a.Failure.Fix; len(f) != maxText || !strings.HasPrefix(f, "dsn postgres://u:[redacted]@h x") {
		t.Errorf("a's fix has %d bytes: %q", len(f), f[:40])
	}
	if _, ok := b.Params["pw"]; len(b.Title) != maxTitle || len(b.Params) != maxParams || ok || len(b.Params["p00"]) != maxParamValue {
		t.Errorf("b's title has %d bytes, and it has %d params", len(b.Title), len(b.Params))
	}
}

// tidy drops Unicode's bidirectional marks and tag characters, and takes a
// line or paragraph separator as a line break: a newline in a detail, a
// space elsewhere.
func TestTidy(t *testing.T) {
	s := "a\u200eb\u200fc\u061cd\U000E0001\U000E0041\U000E007Fe\u2028f\u2029g\nh\ti\x1bj\u202ek"
	if got, want := tidy(s, 100, false), "abcde f g h i j k"; got != want {
		t.Errorf("a text: %q, want %q", got, want)
	}
	if got, want := tidy(s, 100, true), "abcde\nf\ng\nh\ti j k"; got != want {
		t.Errorf("a detail: %q, want %q", got, want)
	}
}

// fit keeps what fitting a text whole keeps, or less, though of a long text
// it redacts only the start: wherever the cut splits a form of a secret, and
// however much redacting and cleaning shrink what comes before.
func TestFit(t *testing.T) {
	secret, other := "pa55 w0rd/ü+x", "an0ther-s3cret-l0nger-than-redacted"
	r := newRedactor([]string{secret, other})
	const limit = 60
	n := 4 * (limit + r.margin)
	for _, c := range []struct {
		name, before string
		// Whether fit keeps what fitting the text whole keeps; or else only
		// "…".
		whole bool
	}{
		{"text", strings.Repeat("x", n), true},
		{"another secret", strings.Repeat(other, n/len(other)+1), true},
		{"tag characters", strings.Repeat("\U000E0041", n/4+1), false},
		{"invalid bytes", strings.Repeat("\xff", n), false},
	} {
		for _, form := range []string{secret, url.QueryEscape(secret), url.PathEscape(secret), strings.TrimPrefix(url.UserPassword("", secret).String(), ":")} {
			for at := n - len(form); at <= n; at++ {
				s := c.before[:at] + form + "yyyy"
				got, whole := r.fit(s, limit, false), clean(r.redact(s), false)
				if !r.cuts(s, limit) || len(got) > limit || !strings.HasPrefix(whole, strings.TrimSuffix(got, "…")) ||
					c.whole && got != tidy(whole, limit, false) || !c.whole && got != "…" {
					t.Fatalf("%s, then %q at %d: %q", c.name, form, at, got)
				}
			}
		}
	}

	// answer fits each long text on its own.
	big := strings.Repeat(secret+" ", 100000)
	ans := answer([]string{secret, other}, &protocolv1.CheckResponse{Items: []*protocolv1.CheckItem{
		{Name: "a", Title: "A", Status: protocolv1.CheckStatus_CHECK_STATUS_FAILED, Detail: big, Params: map[string]string{"big": big}},
	}})
	if it := ans.Items[0]; !r.cuts(big, maxDetail) || it.Failure == nil || it.Failure.Detail != tidy(r.redact(big), maxDetail, true) ||
		it.Params["big"] != tidy(r.redact(big), maxParamValue, false) {
		t.Errorf("an answer's long texts: %+v", it)
	}

	// A secret that's part of what redacting puts in: no cut.
	r = newRedactor([]string{"act"})
	if s := strings.Repeat("act", 10000); r.cuts(s, limit) || r.fit(s, limit, false) != tidy(r.redact(s), limit, false) {
		t.Errorf("a secret within %q: %d", redacted, r.margin)
	}
}

// answer keeps the items that fit in maxAnswerBytes, and fails an answer
// with more, whatever the items it drops say.
func TestAnswerSize(t *testing.T) {
	// Items of each bytes, 19 of which fit.
	const each = len("i000") + len("T") + maxDetail + maxParams*(len("p00")+maxParamValue)
	items := func(n, detail int) []*protocolv1.CheckItem {
		var out []*protocolv1.CheckItem
		for i := range n {
			it := &protocolv1.CheckItem{Name: fmt.Sprintf("i%03d", i), Title: "T", Status: protocolv1.CheckStatus_CHECK_STATUS_PASSED, Detail: strings.Repeat("d", detail)}
			if detail == maxDetail {
				it.Params = map[string]string{}
				for j := range maxParams {
					it.Params[fmt.Sprintf("p%02d", j)] = strings.Repeat("v", maxParamValue)
				}
			}
			out = append(out, it)
		}
		return out
	}
	if r := answer(nil, &protocolv1.CheckResponse{Items: items(19, maxDetail)}); len(r.Items) != 19 || !r.OK() {
		t.Errorf("under the limit: %d items, %v", len(r.Items), r.Err())
	}
	over := fmt.Sprintf("the connector answered with %d bytes of items, and an answer has at most 1048576: the first 19 items are kept", 20*each)
	for _, failed := range []bool{false, true} {
		its := items(20, maxDetail)
		if failed {
			its[19].Status, its[19].Error = protocolv1.CheckStatus_CHECK_STATUS_FAILED, &protocolv1.Error{Code: protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID}
		}
		r := answer(nil, &protocolv1.CheckResponse{Items: its})
		if len(r.Items) != 19 || r.OK() || errs.CodeOf(r.Err()) != errs.Internal || r.Err().Error() != over {
			t.Errorf("over the limit, the last item failed %v: %d items, %v", failed, len(r.Items), r.Err())
		}
	}
	// 2001 items of 605 bytes, from i1000 on 606: 1731 fit.
	if r := answer(nil, &protocolv1.CheckResponse{Items: items(2001, 600)}); len(r.Items) != 1731 || r.Err().Error() !=
		"the connector answered with 2001 items and more than 1048576 bytes of them, and an answer has at most 2000 items and 1048576 bytes: the first 1731 are kept" {
		t.Errorf("over both limits: %d items, %v", len(r.Items), r.Err())
	}
	if r := answer(nil, &protocolv1.CheckResponse{Items: items(2001, 0)}); len(r.Items) != maxItems || r.Err().Error() !=
		"the connector answered with 2001 items, and an answer has at most 2000: the first 2000 are kept" {
		t.Errorf("over the count: %d items, %v", len(r.Items), r.Err())
	}
}

// An answer's items past maxAnswerBytes are dropped, and fail it.
func TestCheckAnswerTooBig(t *testing.T) {
	src := NewSource(exe("source-bulky"), nil, Options{})
	defer func() { _ = src.Close() }()
	r, err := src.CheckReport(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if e := r.Err(); len(r.Items) != 20 || errs.CodeOf(e) != errs.Internal || !strings.HasPrefix(e.Error(), "the connector answered with ") ||
		!strings.HasSuffix(e.Error(), " bytes of items, and an answer has at most 1048576: the first 20 items are kept") {
		t.Errorf("%d items, %v", len(r.Items), e)
	}
}

func batch(n int) arrow.RecordBatch {
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	for i := range n {
		b.Append(int64(i))
	}
	a := b.NewArray()
	defer a.Release()
	return array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil), []arrow.Array{a}, int64(n))
}

func open(t *testing.T, dir string, gen uint64) (*Destination, destination.Writer) {
	t.Helper()
	d := NewDestination(exe("destination-files"), filesConfig(dir, "s3cr3t"), Options{})
	t.Cleanup(func() { _ = d.Close() })
	w, err := d.Open(t.Context(), destination.OpenConfig{Pipeline: "p", Generation: gen, Scope: destination.ScopePipeline, CommitInterval: 3 * time.Second,
		Streams: []destination.Stream{{Name: "s", Mode: destination.Append}}})
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(dir, "interval")); string(got) != "3s" {
		t.Fatalf("the connector got a commit interval of %q", got)
	}
	t.Cleanup(func() { _ = w.Close(context.Background()) })
	return d, w
}

func rows(t *testing.T, dir string) int64 {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "rows.json"))
	if err != nil {
		return 0
	}
	var m map[string]int64
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m["s"]
}

func TestDestinationCommitProtocol(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	d, w := open(t, dir, 1)
	if r, err := w.Recover(ctx, 1, destination.Epoch{}, nil); err != nil || r != destination.Applied {
		t.Fatalf("recover = %v, %v", r, err)
	}
	sa, ok := w.(destination.SchemaApplier)
	if !ok {
		t.Fatal("the writer doesn't apply schemas")
	}
	b := batch(5)
	defer b.Release()
	withRefused := arrow.NewSchema(append(b.Schema().Fields(), arrow.Field{Name: "refused", Type: arrow.BinaryTypes.String, Nullable: true}), nil)
	applied, err := sa.ApplySchema(ctx, []destination.StreamSchema{{Name: "s", Schema: withRefused}})
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(dir, "schemas.json")); string(got) != `{"s":["n:int64"]}` {
		t.Fatalf("schemas = %s", got)
	}
	want := destination.SchemaApplied{
		Refused: []destination.RefusedColumn{{Stream: "s", Column: "refused", Reason: "it refuses columns by name", ToType: "text", Fix: "RENAME refused"}},
		Limits:  map[string]destination.TableLimits{"s": {Columns: 1, MaxColumns: 1600}},
	}
	if !reflect.DeepEqual(applied, want) {
		t.Fatalf("applied = %+v, want %+v", applied, want)
	}
	if h := d.Capabilities().Holds; h != (destination.Holds{ByID: true, Guarded: true}) {
		t.Fatalf("holds = %+v", h)
	}

	rt, ok := w.(destination.ReadTracker)
	if !ok {
		t.Fatal("the writer doesn't track reads")
	}
	if err := rt.StartReads(ctx, map[string]time.Time{"s": time.UnixMicro(42)}); err != nil {
		t.Fatal(err)
	}
	e1 := destination.Epoch{Generation: 1, N: 1}
	if err := w.Write(ctx, e1, "s", b); err != nil {
		t.Fatal(err)
	}
	if err := rt.EndRead(ctx, e1, "s"); err != nil {
		t.Fatal(err)
	}
	st1, err := w.PreCommit(ctx, e1)
	if err != nil {
		t.Fatal(err)
	}
	// They go unanswered, in order with the pre-commit.
	if got, _ := os.ReadFile(filepath.Join(dir, "reads.log")); string(got) != "start s 42\nend s 1-1\n" {
		t.Fatalf("reads.log = %q", got)
	}
	if err := w.(destination.Patcher).Patch(ctx, destination.Epoch{Generation: 1, N: 2}, "s", b); err != nil {
		t.Fatal(err)
	}
	// Epoch 2's batches arrive while epoch 1 commits.
	e2 := destination.Epoch{Generation: 1, N: 2}
	var wg sync.WaitGroup
	wg.Go(func() {
		if _, err := w.Commit(ctx, e1, []destination.Staged{st1}); err != nil {
			t.Error(err)
		}
	})
	for range 3 {
		if err := w.Write(ctx, e2, "s", b); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	st2, err := w.PreCommit(ctx, e2)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(dir, "reads.log")); !strings.HasSuffix(string(got), "patch s 5\n") {
		t.Fatalf("reads.log = %q, without the patch", got)
	}
	if got := rows(t, dir); got != 5 {
		t.Fatalf("rows after epoch 1 = %d", got)
	}

	// A newer generation recovers epoch 2, staged but not applied, and
	// fences the old writer.
	_, w2 := open(t, dir, 2)
	if r, err := w2.Recover(ctx, 2, e2, []destination.Staged{st2}); err != nil || r != destination.Redone {
		t.Fatalf("recover epoch 2 = %v, %v", r, err)
	}
	if got := rows(t, dir); got != 20 {
		t.Fatalf("rows after redoing epoch 2 = %d", got)
	}
	_, err = w.Commit(ctx, e2, []destination.Staged{st2})
	if !errors.Is(err, destination.ErrFenced) || errs.CodeOf(err) != errs.Fenced {
		t.Fatalf("the fenced writer's commit = %v", err)
	}
	if r, err := w2.Recover(ctx, 2, destination.Epoch{Generation: 1, N: 3}, []destination.Staged{destination.Staged("1-3")}); err != nil || r != destination.Lost {
		t.Fatalf("recover an epoch never staged = %v, %v", r, err)
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestDestinationStreamFailure(t *testing.T) {
	dir := t.TempDir()
	_, w := open(t, dir, 3)
	if _, err := w.Recover(t.Context(), 3, destination.Epoch{}, nil); err != nil {
		t.Fatal(err)
	}
	// Staged for an epoch nobody pre-committed: the commit fails and the
	// stream stays open.
	if _, err := w.Commit(t.Context(), destination.Epoch{Generation: 3, N: 9}, []destination.Staged{destination.Staged("3-9")}); err == nil {
		t.Fatal("committing an epoch that wasn't staged succeeded")
	}
	b := batch(1)
	defer b.Release()
	if err := w.Write(t.Context(), destination.Epoch{Generation: 3, N: 1}, "s", b); err != nil {
		t.Fatal(err)
	}
	if _, err := w.PreCommit(t.Context(), destination.Epoch{Generation: 3, N: 1}); err != nil {
		t.Fatal(err)
	}
}

func TestDescriptorSpec(t *testing.T) {
	files := NewDestination(exe("destination-files"), []byte(`{"dir": "x"}`), Options{})
	if err := files.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	spec := files.p.spec
	_ = files.Close()
	if modes := Capabilities(spec).WriteModes; !slices.Equal(modes, []destination.WriteMode{destination.Append, destination.Overwrite}) {
		t.Errorf("write modes = %v", modes)
	}

	same := NewDestination(exe("destination-files"), []byte(`{"dir": "x"}`), Options{Spec: spec})
	if err := same.Start(t.Context()); err != nil {
		t.Fatalf("the connector's own spec: %v", err)
	}
	_ = same.Close()

	host := proto.Clone(spec).(*protocolv1.SpecResponse)
	host.Egress.Tier = protocolv1.EgressTier_EGRESS_TIER_HOST
	other := NewDestination(exe("destination-files"), []byte(`{"dir": "x"}`), Options{Spec: host})
	defer func() { _ = other.Close() }()
	err := other.Start(t.Context())
	if errs.CodeOf(err) != errs.ConfigInvalid || !strings.Contains(err.Error(), "descriptor") || !strings.Contains(err.Error(), `"tier":"raw"`) {
		t.Fatalf("a descriptor on another tier: %v", err)
	}
}

func TestValidateConfig(t *testing.T) {
	schema := []byte(`{"type": "object", "required": ["dir"], "properties": {"dir": {"type": "string"}, "token": {"type": "string", "x-secret": true, "pattern": "^tok_"}}}`)
	if err := ValidateConfig("files", schema, []byte(`{"dir": "x", "token": "tok_1"}`)); err != nil {
		t.Fatal(err)
	}
	if err := ValidateConfig("files", schema, []byte(`{"token": "tok_1"}`)); errs.CodeOf(err) != errs.ConfigInvalid {
		t.Errorf("missing dir: %v", err)
	}
	err := ValidateConfig("files", schema, []byte(`{"dir": "x", "token": "hunter2"}`))
	if err == nil || strings.Contains(err.Error(), "hunter2") {
		t.Errorf("a secret that fails its pattern: %v", err)
	}
	if err := ValidateConfig("files", []byte(`{"type": 3}`), []byte(`{}`)); errs.CodeOf(err) != errs.ConfigInvalid {
		t.Errorf("a bad schema: %v", err)
	}
	if _, err := CompileSchema([]byte(`{"$ref": "http://169.254.169.254/schema.json"}`)); err == nil {
		t.Error("compiled a schema that refers to a URL")
	}
}
