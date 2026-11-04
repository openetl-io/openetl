package connectorhost

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/unsuitable001/openetl/internal/declarative"
	"github.com/unsuitable001/openetl/internal/netguard"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

const acme = `
apiVersion: openetl.io/v1alpha1
kind: DeclarativeSource
metadata: { name: acme, version: 0.1.0 }
spec:
  config:
    type: object
    required: [base_url, api_key]
    properties:
      base_url: { type: string }
      api_key: { type: string, x-secret: true }
  http:
    base_url: "{config.base_url}"
    auth: { type: bearer, token: { secret: api_key } }
  streams:
    - name: contacts
      request: { path: /contacts }
`

type rows struct{ n int }

func (r *rows) Rows(_ string, rs []json.RawMessage) error { r.n += len(rs); return nil }
func (r *rows) Records(string, arrow.RecordBatch) error   { return nil }
func (r *rows) ResumePoint(string, []byte) error          { return nil }
func (r *rows) Complete(string, []byte) error             { return nil }

// Each declarative instance gets host services with its own secrets, and
// reaches only what the runner's egress policy allows.
func TestDeclarativeSources(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer k1" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`[{"id": 1}, {"id": 2}]`))
	}))
	defer srv.Close()
	m, err := declarative.Load([]byte(acme))
	if err != nil {
		t.Fatal(err)
	}
	cfg := func(key string) []byte {
		b, _ := json.Marshal(map[string]string{"base_url": srv.URL, "api_key": key})
		return b
	}
	env := Env{
		Egress:    netguard.Policy{Allow: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}},
		Manifests: map[string]*declarative.Connector{"openetl/acme": m},
	}
	read := func(env Env, key string) (int, error) {
		s, err := NewSource("acme", cfg(key), env)
		if err != nil {
			t.Fatal(err)
		}
		var r rows
		err = s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "contacts"}}}, &r)
		return r.n, err
	}
	if n, err := read(env, "k1"); err != nil || n != 2 {
		t.Errorf("read: %d rows, %v", n, err)
	}
	if _, err := read(env, "k2"); errs.CodeOf(err) != errs.AuthInvalid {
		t.Errorf("read with another instance's key: %v, want AUTH_INVALID", err)
	}
	if _, err := read(Env{Manifests: env.Manifests}, "k1"); errs.CodeOf(err) != errs.ConfigInvalid || !strings.Contains(err.Error(), "not a public address") {
		t.Errorf("read without loopback in the egress policy: %v, want it blocked", err)
	}

	if _, err := NewSource("acme", []byte(`{"base_url": "https://x.example"}`), env); errs.CodeOf(err) != errs.ConfigInvalid {
		t.Errorf("a config without its secret: %v, want CONFIG_INVALID", err)
	}
	if _, err := NewSource("nope", nil, env); err == nil || !strings.Contains(err.Error(), "acme") {
		t.Errorf("an unknown connector: %v, want the known ones listed, acme among them", err)
	}
}
