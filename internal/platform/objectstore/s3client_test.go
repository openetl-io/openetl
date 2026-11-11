package objectstore_test

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/checkpoint/openstore"
	"github.com/openetl-io/openetl/internal/platform/objectstore"
)

// fakeS3 answers path-style requests as S3 does for buckets in other
// regions, keeping the Authorization header and path of each.
type fakeS3 struct {
	mu       sync.Mutex
	requests []*http.Request
	// The region each bucket is in; a bucket not here doesn't exist.
	regions map[string]string
	// The region the requests are sent to.
	region string
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requests = append(f.requests, r.Clone(r.Context()))
	f.mu.Unlock()
	bucket, _, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	region, ok := f.regions[bucket]
	switch {
	case !ok:
		w.WriteHeader(http.StatusNotFound)
		return
	case region != f.region:
		w.Header().Set("X-Amz-Bucket-Region", region)
		w.WriteHeader(http.StatusMovedPermanently)
		return
	case r.Header.Get("Authorization") == "":
		w.Header().Set("X-Amz-Bucket-Region", region)
		w.WriteHeader(http.StatusForbidden)
		return
	}
	w.Header().Set("X-Amz-Bucket-Region", region)
	_, _ = io.WriteString(w, "hello")
}

func (f *fakeS3) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests)
}

func (f *fakeS3) last() *http.Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests[len(f.requests)-1]
}

func newFakeS3(t *testing.T, region string) (*fakeS3, *httptest.Server) {
	t.Helper()
	f := &fakeS3{region: region, regions: map[string]string{"far": "ap-southeast-2", "near": "us-east-1"}}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return f, srv
}

func mustURL(t *testing.T, s string) *url.URL {
	t.Helper()
	u, err := url.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestResolve(t *testing.T) {
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	f, srv := newFakeS3(t, "us-east-1")
	objectstore.AskRegionAt(t, srv.URL)
	for in, want := range map[string]string{
		"s3://far/base":                         "s3://far/base?region=ap-southeast-2",
		"s3://near":                             "s3://near?region=us-east-1",
		"s3://far/base?anonymous=true":          "s3://far/base?anonymous=true&region=ap-southeast-2",
		"s3://far?region=eu-west-1":             "s3://far?region=eu-west-1",
		"s3://far?endpoint=http://127.0.0.1:1":  "s3://far?endpoint=http://127.0.0.1:1",
		"s3://gone":                             "s3://gone",
		"gs://far/base":                         "gs://far/base",
		"/var/lib/openetl":                      "/var/lib/openetl",
		"azblob://far?account=a&endpoint=x://y": "azblob://far?account=a&endpoint=x://y",
	} {
		if got := objectstore.Resolve(t.Context(), in); got != want {
			t.Errorf("Resolve(%s) = %s, want %s", in, got, want)
		}
	}
	n := f.count()
	objectstore.Resolve(t.Context(), "s3://far/other")
	if f.count() != n {
		t.Error("asked again for a bucket's region it knew")
	}
}

func TestS3ClientRegion(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_REGION", "eu-central-1")
	_, srv := newFakeS3(t, "eu-central-1")
	objectstore.AskRegionAt(t, srv.URL)
	for in, want := range map[string]string{
		"s3://far":                  "ap-southeast-2",
		"s3://far?region=eu-west-1": "eu-west-1",
		"s3://far?anonymous=true&endpoint=http://x": "eu-central-1",
		"s3://gone": "eu-central-1",
	} {
		c, err := objectstore.S3Client(t.Context(), mustURL(t, in))
		if err != nil {
			t.Fatal(err)
		}
		if got := c.Options().Region; got != want {
			t.Errorf("S3Client(%s)'s region = %s, want %s", in, got, want)
		}
	}
}

func TestS3ClientSigns(t *testing.T) {
	t.Setenv("AWS_REGION", "us-east-1")
	f, srv := newFakeS3(t, "us-east-1")
	get := func(loc string) error {
		t.Helper()
		c, err := objectstore.S3Client(t.Context(), mustURL(t, loc))
		if err != nil {
			t.Fatal(err)
		}
		out, err := c.GetObject(t.Context(), &s3.GetObjectInput{Bucket: new("near"), Key: new("k")})
		if err == nil {
			_ = out.Body.Close()
		}
		return err
	}
	loc := "s3://near?endpoint=" + url.QueryEscape(srv.URL)

	t.Setenv("AWS_ACCESS_KEY_ID", "AKID")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	if err := get(loc); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(f.last().Header.Get("Authorization"), "Credential=AKID/") {
		t.Errorf("keys in the environment: Authorization = %q", f.last().Header.Get("Authorization"))
	}

	if err := get(loc + "&anonymous=true"); err == nil {
		t.Error("an unsigned request got what only signed ones do")
	}
	if a := f.last().Header.Get("Authorization"); a != "" {
		t.Errorf("anonymous=true: Authorization = %q", a)
	}

	noAWSCredentials(t)
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	objectstore.ReloadAWSConfig(t)
	n := f.count()
	err := get(loc)
	if err == nil || !strings.Contains(err.Error(), "anonymous=true") || !strings.Contains(err.Error(), "hop limit of 2") {
		t.Errorf("no credentials: err = %v, want one that says what to set, and the hop limit a container on EC2 needs", err)
	}
	if f.count() != n {
		t.Error("sent a request without the credentials it needed")
	}

	if _, err := objectstore.S3Client(t.Context(), mustURL(t, loc+"&anonymous=maybe")); err == nil {
		t.Error("took anonymous=maybe")
	}
}

// noAWSCredentials leaves the default chain nothing to find, but EC2's
// instance metadata.
func noAWSCredentials(t *testing.T) {
	t.Helper()
	for _, k := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_ROLE_ARN", "AWS_WEB_IDENTITY_TOKEN_FILE",
		"AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_CONTAINER_AUTHORIZATION_TOKEN", "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
		"AWS_EC2_METADATA_DISABLED", "AWS_EC2_METADATA_SERVICE_ENDPOINT", "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_STS"} {
		t.Setenv(k, "")
	}
	t.Setenv("AWS_CONFIG_FILE", t.TempDir()+"/config")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", t.TempDir()+"/credentials")
}

// Each source the default chain can pick says what failed when it gives no
// credentials.
func TestS3ClientCredentialsErrors(t *testing.T) {
	t.Setenv("AWS_REGION", "us-east-1")
	_, s3srv := newFakeS3(t, "us-east-1")
	answer := func(status int, body string) string {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
			_, _ = io.WriteString(w, body)
		}))
		t.Cleanup(srv.Close)
		return srv.URL
	}
	sts := answer(http.StatusBadRequest, `<ErrorResponse><Error><Type>Sender</Type><Code>AccessDenied</Code>`+
		`<Message>Not authorized to perform sts:AssumeRoleWithWebIdentity</Message></Error><RequestId>r</RequestId></ErrorResponse>`)
	token := t.TempDir() + "/token"
	if err := os.WriteFile(token, []byte("eyJ.token"), 0o600); err != nil {
		t.Fatal(err)
	}
	imds := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && r.URL.Path == "/latest/api/token" {
			w.Header().Set("X-Aws-Ec2-Metadata-Token-Ttl-Seconds", "21600")
			_, _ = io.WriteString(w, "imds-token")
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(imds.Close)

	for _, tc := range []struct {
		name string
		env  map[string]string
		want []string
	}{
		{"STS refuses the web identity token", map[string]string{"AWS_ROLE_ARN": "arn:aws:iam::123456789012:role/runner", "AWS_WEB_IDENTITY_TOKEN_FILE": token, "AWS_ENDPOINT_URL_STS": sts},
			[]string{"objectstore: STS refused the web identity token for the role: on EKS, check that IAM has an OIDC provider", "AccessDenied"}},
		{"no web identity token", map[string]string{"AWS_ROLE_ARN": "arn:aws:iam::123456789012:role/runner", "AWS_WEB_IDENTITY_TOKEN_FILE": t.TempDir() + "/gone", "AWS_ENDPOINT_URL_STS": sts},
			[]string{"objectstore: no credentials for the web identity role: ", "unable to read file"}},
		{"Pod Identity gives none", map[string]string{"AWS_CONTAINER_CREDENTIALS_FULL_URI": answer(http.StatusBadRequest, `{"code":"AccessDeniedException","message":"no association"}`)},
			[]string{"objectstore: the container credentials endpoint gave no credentials: on EKS, check the service account's Pod Identity association", "no association"}},
		{"an EC2 instance without a role", map[string]string{"AWS_EC2_METADATA_SERVICE_ENDPOINT": imds.URL},
			[]string{"objectstore: no AWS credentials, and the EC2 instance has no IAM role: set AWS_ACCESS_KEY_ID", "StatusCode: 404"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			noAWSCredentials(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			objectstore.ReloadAWSConfig(t)
			// What a runner checks as it starts.
			store, err := openstore.Open(t.Context(), "s3://near?endpoint="+url.QueryEscape(s3srv.URL))
			if err != nil {
				t.Fatal(err)
			}
			err = checkpoint.Probe(t.Context(), store, "")
			if err == nil {
				t.Fatal("the probe passed without credentials")
			}
			if _, ok := errors.AsType[*objectstore.CredentialsError](err); !ok {
				t.Errorf("err = %v, not a CredentialsError", err)
			}
			for _, w := range tc.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("err = %v\nwant it to say %q", err, w)
				}
			}
		})
	}
}

func TestS3WrongRegion(t *testing.T) {
	t.Setenv("AWS_REGION", "")
	_, srv := newFakeS3(t, "us-east-1")
	read := func(bucket string) error {
		t.Helper()
		b, err := objectstore.Open(t.Context(), "s3://"+bucket+"?anonymous=true&region=us-east-1&endpoint="+url.QueryEscape(srv.URL))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = b.Close() }()
		_, err = b.NewReader(t.Context(), "k", nil)
		return err
	}
	err := read("far")
	if region, ok := objectstore.S3WrongRegion(err); !ok || region != "ap-southeast-2" {
		t.Errorf("S3WrongRegion(%v) = %q, %v; want ap-southeast-2", err, region, ok)
	}
	if err := read("near"); err == nil {
		t.Error("read without access")
	} else if region, ok := objectstore.S3WrongRegion(err); ok {
		t.Errorf("S3WrongRegion of a refusal = %q", region)
	}
}
