package secretstore

import (
	"context"
	"encoding/json"
	"hash/crc32"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
	"github.com/aws/smithy-go"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/secretstore/secretstoretest"
)

const hunter2 = `{"user": "shop", "password": "hunter2", "port": 5432, "none": null}`

func TestField(t *testing.T) {
	for _, tc := range []struct{ value, field, want, err string }{
		{"plain", "", "plain", ""},
		{"plain\n", "", "plain", ""},
		{"plain\r\n", "", "plain", ""},
		{"two lines\n\n", "", "two lines\n", ""},
		{"-----BEGIN KEY-----\nabc\n-----END KEY-----\n", "", "-----BEGIN KEY-----\nabc\n-----END KEY-----", ""},
		{hunter2, "", hunter2, ""},
		{`{"dsn": "postgres://h\n"}`, "dsn", "postgres://h\n", ""},
		{hunter2, "password", "hunter2", ""},
		{hunter2, "port", "5432", ""},
		{hunter2, "nope", "", `has no field "nope"`},
		{hunter2, "none", "", `field "none" is null`},
		{"hunter2", "password", "", "isn't a JSON object"},
		{"\xff\xfe", "", "", "isn't UTF-8"},
	} {
		got, err := field([]byte(tc.value), tc.field)
		if got != tc.want || (err == nil) != (tc.err == "") || err != nil && !strings.Contains(err.Error(), tc.err) {
			t.Errorf("field(%q, %q) = %q, %v; want %q, %q", tc.value, tc.field, got, err, tc.want, tc.err)
		}
		if err != nil && strings.Contains(err.Error(), "hunter2") {
			t.Errorf("field(%q, %q)'s error holds the value: %v", tc.value, tc.field, err)
		}
	}
}

func TestReadAWS(t *testing.T) {
	fake := secretstoretest.NewAWS()
	defer fake.Close()
	fake.Set("prod/db", hunter2)
	fake.Setenv(t)
	ctx := t.Context()

	var r Reader
	if _, err := r.Read(ctx, "aws-sm://prod/db"); errs.CodeOf(err) != errs.ConfigInvalid || !strings.Contains(err.Error(), "no AWS region") {
		t.Errorf("no region: %v", err)
	}
	v, err := r.Read(ctx, "aws-sm://prod/db?region=eu-west-1&field=password&stage=AWSPREVIOUS")
	if err != nil || v != "hunter2" {
		t.Errorf("Read = %q, %v", v, err)
	}
	if v, err := r.Read(ctx, "aws-sm://arn:aws:secretsmanager:eu-west-1:1:secret:prod/db?version=v7"); err == nil || v != "" {
		t.Errorf("an ARN the fake doesn't have: %q, %v", v, err)
	}
	reqs := fake.Requests()
	if len(reqs) != 2 || reqs[0] != (secretstoretest.Request{SecretID: "prod/db", VersionStage: "AWSPREVIOUS", AccessKey: secretstoretest.AccessKey}) || reqs[1].VersionID != "v7" {
		t.Errorf("requests: %+v", reqs)
	}
	_, err = r.Read(ctx, "aws-sm://prod/other?region=eu-west-1")
	if errs.CodeOf(err) != errs.ConfigInvalid || !strings.Contains(err.Error(), "read aws-sm://prod/other") || !strings.Contains(err.Error(), "ResourceNotFoundException") {
		t.Errorf("a secret that doesn't exist: %v", err)
	}
	if _, err := r.Read(ctx, "aws-sm://prod/db?region=eu-west-1&field=pasword"); errs.CodeOf(err) != errs.ConfigInvalid || strings.Contains(err.Error(), "hunter2") {
		t.Errorf("a field that doesn't exist: %v", err)
	}
	if _, err := r.Read(ctx, "aws-sm://prod/db?regoin=eu-west-1"); errs.CodeOf(err) != errs.ConfigInvalid {
		t.Errorf("a bad reference: %v", err)
	}
	for code, want := range map[string]errs.Code{
		"AccessDeniedException":       errs.PermissionDenied,
		"UnrecognizedClientException": errs.AuthInvalid,
		"ThrottlingException":         errs.RateLimited,
		"InternalServiceError":        errs.Transient,
	} {
		if got := awsCode(&apiError{code}); got != want {
			t.Errorf("awsCode(%s) = %v; want %v", code, got, want)
		}
	}
}

type apiError struct{ code string }

func (e *apiError) Error() string                 { return e.code }
func (e *apiError) ErrorCode() string             { return e.code }
func (e *apiError) ErrorMessage() string          { return e.code }
func (e *apiError) ErrorFault() smithy.ErrorFault { return smithy.FaultUnknown }

type fakeGCP struct {
	secretmanagerpb.UnimplementedSecretManagerServiceServer
	values map[string]string
	names  []string
}

func (f *fakeGCP) AccessSecretVersion(_ context.Context, req *secretmanagerpb.AccessSecretVersionRequest) (*secretmanagerpb.AccessSecretVersionResponse, error) {
	f.names = append(f.names, req.GetName())
	switch name, _, _ := strings.Cut(req.GetName(), "/versions/"); name {
	case "projects/shop/secrets/denied":
		return nil, status.Error(codes.PermissionDenied, "denied")
	case "projects/shop/secrets/corrupt":
		sum := int64(1)
		return &secretmanagerpb.AccessSecretVersionResponse{Payload: &secretmanagerpb.SecretPayload{Data: []byte("x"), DataCrc32C: &sum}}, nil
	}
	v, ok := f.values[req.GetName()]
	if !ok {
		return nil, status.Error(codes.NotFound, "not found")
	}
	sum := int64(crc32.Checksum([]byte(v), castagnoli))
	return &secretmanagerpb.AccessSecretVersionResponse{Name: req.GetName(), Payload: &secretmanagerpb.SecretPayload{Data: []byte(v), DataCrc32C: &sum}}, nil
}

func TestReadGCP(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	fake := &fakeGCP{values: map[string]string{
		"projects/shop/secrets/db/versions/latest": hunter2,
		"projects/shop/secrets/db/versions/3":      "old",
	}}
	secretmanagerpb.RegisterSecretManagerServiceServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	r := Reader{gcpOptions: []option.ClientOption{
		option.WithEndpoint(lis.Addr().String()), option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	}}
	defer func() { _ = r.Close() }()
	ctx := t.Context()
	for ref, want := range map[string]string{
		"gcp-sm://projects/shop/secrets/db?field=password": "hunter2",
		"gcp-sm://projects/shop/secrets/db/versions/3":     "old",
		"gcp-sm://projects/shop/secrets/db?version=3":      "old",
	} {
		if v, err := r.Read(ctx, ref); err != nil || v != want {
			t.Errorf("Read(%s) = %q, %v", ref, v, err)
		}
	}
	for ref, want := range map[string]errs.Code{
		"gcp-sm://projects/shop/secrets/missing": errs.ConfigInvalid,
		"gcp-sm://projects/shop/secrets/denied":  errs.PermissionDenied,
		"gcp-sm://projects/shop/secrets/corrupt": errs.Transient,
	} {
		if _, err := r.Read(ctx, ref); errs.CodeOf(err) != want || !strings.Contains(err.Error(), ref) {
			t.Errorf("Read(%s) = %v; want %v", ref, err, want)
		}
	}
}

// fakeVault answers Key Vault's challenge, then GetSecret with a bearer
// token.
func fakeVault(values map[string]string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer vault-token" {
			w.Header().Set("WWW-Authenticate", `Bearer authorization="https://login.microsoftonline.com/tenant", resource="https://vault.azure.net"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		name, version, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/secrets/"), "/")
		v, ok := values[name+"/"+version]
		switch {
		case name == "denied":
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error": {"code": "Forbidden", "message": "denied"}}`))
		case !ok:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error": {"code": "SecretNotFound", "message": "not found"}}`))
		default:
			_ = json.NewEncoder(w).Encode(map[string]string{"value": v, "id": "https://" + r.Host + r.URL.Path})
		}
	})
}

type fakeCredential struct{ scopes []string }

func (f *fakeCredential) GetToken(_ context.Context, o policy.TokenRequestOptions) (azcore.AccessToken, error) {
	f.scopes = append(f.scopes, o.Scopes...)
	return azcore.AccessToken{Token: "vault-token", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

func TestReadAzure(t *testing.T) {
	srv := httptest.NewTLSServer(fakeVault(map[string]string{"db/": hunter2, "db/abc": "old"}))
	defer srv.Close()
	cred := &fakeCredential{}
	r := Reader{azureVault: srv.URL, azureCred: cred, azureOptions: &azsecrets.ClientOptions{
		ClientOptions:                        azcore.ClientOptions{Transport: srv.Client()},
		DisableChallengeResourceVerification: true,
	}}
	ctx := t.Context()
	for ref, want := range map[string]string{
		"azure-kv://shop/db?field=user":  "shop",
		"azure-kv://shop/db?version=abc": "old",
	} {
		if v, err := r.Read(ctx, ref); err != nil || v != want {
			t.Errorf("Read(%s) = %q, %v", ref, v, err)
		}
	}
	for ref, want := range map[string]errs.Code{
		"azure-kv://shop/missing": errs.ConfigInvalid,
		"azure-kv://shop/denied":  errs.PermissionDenied,
	} {
		if _, err := r.Read(ctx, ref); errs.CodeOf(err) != want || !strings.Contains(err.Error(), ref) {
			t.Errorf("Read(%s) = %v; want %v", ref, err, want)
		}
	}
	if len(cred.scopes) == 0 || cred.scopes[0] != "https://vault.azure.net/.default" {
		t.Errorf("token scopes: %v", cred.scopes)
	}
}
