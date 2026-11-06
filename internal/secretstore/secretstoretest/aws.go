// Package secretstoretest fakes AWS Secrets Manager's GetSecretValue for
// tests. Point the AWS SDK at it with AWS_ENDPOINT_URL_SECRETS_MANAGER.
package secretstoretest

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// AccessKey is the access key Setenv gives the AWS SDK.
const AccessKey = "AKIDRUNNER"

type AWS struct {
	*httptest.Server

	mu       sync.Mutex
	secrets  map[string]string
	requests []Request
}

// Request is a GetSecretValue the fake answered.
type Request struct {
	SecretID     string
	VersionID    string
	VersionStage string
	// The access key that signed it, or empty if it wasn't signed.
	AccessKey string
}

func NewAWS() *AWS {
	a := &AWS{secrets: map[string]string{}}
	a.Server = httptest.NewServer(http.HandlerFunc(a.serve))
	return a
}

// Setenv points the AWS SDK at the fake for the rest of t, with static
// credentials and nothing from the machine's AWS config, and no region.
// Processes started meanwhile inherit it.
func (a *AWS) Setenv(t testing.TB) {
	t.Setenv("AWS_ENDPOINT_URL_SECRETS_MANAGER", a.URL)
	t.Setenv("AWS_ACCESS_KEY_ID", AccessKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", "runner-secret")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(t.TempDir(), "none"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "none"))
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
}

// Set gives the secret a value; empty deletes it.
func (a *AWS) Set(name, value string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if value == "" {
		delete(a.secrets, name)
		return
	}
	a.secrets[name] = value
}

func (a *AWS) Requests() []Request {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]Request(nil), a.requests...)
}

func (a *AWS) serve(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("X-Amz-Target") != "secretsmanager.GetSecretValue" {
		fail(w, http.StatusBadRequest, "InvalidAction", "only GetSecretValue")
		return
	}
	var in struct{ SecretId, VersionId, VersionStage string } //nolint:revive // the API's names
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		fail(w, http.StatusBadRequest, "SerializationException", err.Error())
		return
	}
	req := Request{SecretID: in.SecretId, VersionID: in.VersionId, VersionStage: in.VersionStage}
	// AWS4-HMAC-SHA256 Credential={key}/{date}/{region}/secretsmanager/aws4_request, …
	if _, cred, ok := strings.Cut(r.Header.Get("Authorization"), "Credential="); ok {
		req.AccessKey, _, _ = strings.Cut(cred, "/")
	}
	a.mu.Lock()
	a.requests = append(a.requests, req)
	v, ok := a.secrets[in.SecretId]
	a.mu.Unlock()
	switch {
	case req.AccessKey == "":
		fail(w, http.StatusBadRequest, "UnrecognizedClientException", "the request isn't signed")
	case !ok:
		fail(w, http.StatusBadRequest, "ResourceNotFoundException", "Secrets Manager can't find the specified secret.")
	default:
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		_ = json.NewEncoder(w).Encode(map[string]string{"Name": in.SecretId, "SecretString": v, "VersionId": "v1"})
	}
}

func fail(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"__type": code, "message": msg})
}
