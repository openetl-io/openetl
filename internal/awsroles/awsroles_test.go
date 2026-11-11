package awsroles

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/credentials/endpointcreds"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/platform/errs"
)

// fakeSTS answers AssumeRole for every role but one named denied, and
// GetCallerIdentity, keeping each AssumeRole's form.
type fakeSTS struct {
	mu      sync.Mutex
	assumed []url.Values
}

func (f *fakeSTS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "text/xml")
	switch r.Form.Get("Action") {
	case "GetCallerIdentity":
		_, _ = fmt.Fprint(w, `<GetCallerIdentityResponse><GetCallerIdentityResult><Arn>arn:aws:sts::111111111111:assumed-role/openetl-runner/i-1</Arn>`+
			`<UserId>AROA:i-1</UserId><Account>111111111111</Account></GetCallerIdentityResult><ResponseMetadata><RequestId>r</RequestId></ResponseMetadata></GetCallerIdentityResponse>`)
	case "AssumeRole":
		f.mu.Lock()
		f.assumed = append(f.assumed, r.Form)
		f.mu.Unlock()
		if strings.HasSuffix(r.Form.Get("RoleArn"), "/denied") {
			w.WriteHeader(http.StatusForbidden)
			_, _ = fmt.Fprint(w, `<ErrorResponse><Error><Type>Sender</Type><Code>AccessDenied</Code><Message>not authorized to perform sts:AssumeRole</Message></Error><RequestId>r</RequestId></ErrorResponse>`)
			return
		}
		_, _ = fmt.Fprintf(w, `<AssumeRoleResponse><AssumeRoleResult><Credentials><AccessKeyId>ROLEKEY</AccessKeyId><SecretAccessKey>secret</SecretAccessKey>`+
			`<SessionToken>session</SessionToken><Expiration>%s</Expiration></Credentials><AssumedRoleUser><Arn>arn:aws:sts::222222222222:assumed-role/x/y</Arn>`+
			`<AssumedRoleId>AROA:y</AssumedRoleId></AssumedRoleUser></AssumeRoleResult><ResponseMetadata><RequestId>r</RequestId></ResponseMetadata></AssumeRoleResponse>`,
			time.Now().Add(time.Hour).UTC().Format(time.RFC3339))
	default:
		w.WriteHeader(http.StatusBadRequest)
	}
}

func (f *fakeSTS) last() url.Values {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.assumed[len(f.assumed)-1]
}

// withFakeSTS gives the runner keys and sends its STS calls to a fake.
func withFakeSTS(t *testing.T) *fakeSTS {
	t.Helper()
	f := &fakeSTS{}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	none := t.TempDir() + "/none"
	for k, v := range map[string]string{
		"AWS_ACCESS_KEY_ID": "RUNNERKEY", "AWS_SECRET_ACCESS_KEY": "secret", "AWS_SESSION_TOKEN": "", "AWS_REGION": "ap-southeast-2", "AWS_PROFILE": "",
		"AWS_ENDPOINT_URL_STS": srv.URL, "AWS_CONFIG_FILE": none, "AWS_SHARED_CREDENTIALS_FILE": none, "AWS_EC2_METADATA_DISABLED": "true",
		"AWS_WEB_IDENTITY_TOKEN_FILE": "", "AWS_CONTAINER_CREDENTIALS_FULL_URI": "", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI": "",
	} {
		t.Setenv(k, v)
	}
	return f
}

func TestAllows(t *testing.T) {
	b, err := New(" arn:aws:iam::111111111111:role/openetl-*, arn:aws:iam::222222222222:role/lake,arn:aws:iam::444444444444:role/a.b")
	if err != nil {
		t.Fatal(err)
	}
	for role, want := range map[string]bool{
		"arn:aws:iam::111111111111:role/openetl-lake": true,
		"arn:aws:iam::111111111111:role/openetl-a/b":  true,
		"arn:aws:iam::222222222222:role/lake":         true,
		"arn:aws:iam::222222222222:role/lake-admin":   false,
		"arn:aws:iam::333333333333:role/openetl-lake": false,
		"arn:aws:iam::444444444444:role/a.b":          true,
		"arn:aws:iam::444444444444:role/aXb":          false,
	} {
		if got := b.Allows(role); got != want {
			t.Errorf("Allows(%s) = %v, want %v", role, got, want)
		}
	}
	if _, err := New("lake"); err == nil {
		t.Error("took a pattern that isn't an ARN")
	}
	empty, err := New("")
	if err != nil || empty.Allows("arn:aws:iam::111111111111:role/openetl-lake") {
		t.Errorf("an empty setting allows a role, or fails: %v", err)
	}
}

func TestSessionName(t *testing.T) {
	if got := (Session{Kind: "destination", Name: "lake"}).SessionName(); got != "openetl-destination-lake" {
		t.Errorf("SessionName = %s", got)
	}
	if got := (Session{Kind: "source", Name: strings.Repeat("a", 80) + " b"}).SessionName(); len(got) != 64 || strings.Contains(got, " ") {
		t.Errorf("a long name's session name = %q", got)
	}
}

// A lease assumes the role with the session's name, tags and external ID,
// and serves its credentials to the AWS SDK's container credentials
// provider, with the lease's token only, until it's closed.
func TestLease(t *testing.T) {
	sts := withFakeSTS(t)
	b, err := New("arn:aws:iam::222222222222:role/lake-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()
	l, err := b.Lease(t.Context(), &apiv1.AWSAccess{Role: "arn:aws:iam::222222222222:role/lake-writer", ExternalId: "ext-1"},
		Session{Workspace: "default", Kind: "destination", Name: "lake", Pipeline: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	got := sts.last()
	for k, want := range map[string]string{
		"RoleArn": "arn:aws:iam::222222222222:role/lake-writer", "RoleSessionName": "openetl-destination-lake", "ExternalId": "ext-1", "DurationSeconds": "3600",
		"Tags.member.1.Key": "openetl:workspace", "Tags.member.1.Value": "default",
		"Tags.member.2.Key": "openetl:destination", "Tags.member.2.Value": "lake",
		"Tags.member.3.Key": "openetl:pipeline", "Tags.member.3.Value": "orders",
	} {
		if got.Get(k) != want {
			t.Errorf("AssumeRole's %s = %q, want %q", k, got.Get(k), want)
		}
	}
	if !strings.HasPrefix(l.URL, "http://127.0.0.1:") {
		t.Errorf("the lease is served at %s, not on loopback", l.URL)
	}

	read := func(token string) (string, error) {
		creds, err := endpointcreds.New(l.URL, func(o *endpointcreds.Options) { o.AuthorizationToken = token }).Retrieve(t.Context())
		return creds.AccessKeyID + "/" + creds.SessionToken, err
	}
	if got, err := read(l.Token); err != nil || got != "ROLEKEY/session" {
		t.Errorf("the SDK read %s, %v; want the role's credentials", got, err)
	}
	if _, err := read("not-the-token"); err == nil {
		t.Error("served a lease's credentials without its token")
	}
	l.Close()
	if _, err := read(l.Token); err == nil {
		t.Error("served a closed lease")
	}
}

func TestLeaseRefused(t *testing.T) {
	withFakeSTS(t)
	lease := func(allow, role string) error {
		t.Helper()
		b, err := New(allow)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = b.Close() }()
		_, err = b.Lease(t.Context(), &apiv1.AWSAccess{Role: role}, Session{Workspace: "default", Kind: "source", Name: "events"})
		return err
	}
	for _, tc := range []struct {
		name, allow, role string
		code              errs.Code
		says              []string
	}{
		{"no roles", "", "arn:aws:iam::222222222222:role/lake", errs.ConfigInvalid, []string{"source events: aws.role names arn:aws:iam::222222222222:role/lake, and this runner may assume no roles: list it in OPENETL_AWS_ASSUME_ROLES"}},
		{"not listed", "arn:aws:iam::222222222222:role/lake", "arn:aws:iam::222222222222:role/other", errs.ConfigInvalid, []string{"which OPENETL_AWS_ASSUME_ROLES (arn:aws:iam::222222222222:role/lake) doesn't list"}},
		{"refused by STS", "arn:aws:iam::222222222222:role/*", "arn:aws:iam::222222222222:role/denied", errs.PermissionDenied,
			[]string{"STS refused it to arn:aws:sts::111111111111:assumed-role/openetl-runner/i-1", "sts:TagSession", "AccessDenied"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := lease(tc.allow, tc.role)
			if errs.CodeOf(err) != tc.code {
				t.Fatalf("err = %v (%s), want %s", err, errs.CodeOf(err), tc.code)
			}
			for _, s := range tc.says {
				if !strings.Contains(err.Error(), s) {
					t.Errorf("err = %v\nwant it to say %q", err, s)
				}
			}
		})
	}
}
