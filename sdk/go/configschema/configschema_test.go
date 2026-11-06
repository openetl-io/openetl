package configschema

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

func TestSecretPaths(t *testing.T) {
	schema := `{"type": "object", "properties": {
		"dsn": {"type": "string", "x-secret": true},
		"region": {"type": "string"},
		"auth": {"type": "object", "oneOf": [
			{"properties": {"password": {"type": "string", "x-secret": true}}},
			{"properties": {"oauth": {"type": "object", "properties": {"client_secret": {"type": "string", "x-secret": true}, "client_id": {"type": "string"}}}}}
		]},
		"tls": {"anyOf": [{"properties": {"key": {"x-secret": true}}}]},
		"tags": {"type": "array", "items": {"type": "string"}}
	}}`
	got, err := SecretPaths([]byte(schema))
	if want := []string{"auth.oauth.client_secret", "auth.password", "dsn", "tls.key"}; err != nil || !slices.Equal(got, want) {
		t.Errorf("SecretPaths = %v, %v; want %v", got, err, want)
	}
	for name, bad := range map[string]string{
		"in an array":     `{"properties": {"hosts": {"type": "array", "items": {"properties": {"password": {"x-secret": true}}}}}}`,
		"in a tuple":      `{"properties": {"pair": {"prefixItems": [{"x-secret": true}]}}}`,
		"the whole thing": `{"x-secret": true}`,
		"a dotted name":   `{"properties": {"a.b": {"properties": {"c": {"x-secret": true}}}}}`,
		"not JSON":        `{`,
	} {
		if _, err := SecretPaths([]byte(bad)); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	if got, err := SecretPaths(nil); err != nil || got != nil {
		t.Errorf("no schema: %v, %v", got, err)
	}
}

func TestLookupAndReplace(t *testing.T) {
	var m map[string]any
	if err := json.Unmarshal([]byte(`{"dsn": "x", "auth": {"oauth": {"client_secret": "s"}}, "n": 1}`), &m); err != nil {
		t.Fatal(err)
	}
	if v, ok := Lookup(m, "auth.oauth.client_secret"); !ok || v != "s" {
		t.Errorf("lookup = %v, %v", v, ok)
	}
	for _, p := range []string{"auth.password", "n.x", "missing.y"} {
		if _, ok := Lookup(m, p); ok {
			t.Errorf("%s found", p)
		}
	}
	if !Replace(m, "auth.oauth.client_secret", "auth.oauth.client_secret") || Replace(m, "auth.password", "x") {
		t.Error("replace")
	}
	b, _ := json.Marshal(m)
	if !strings.Contains(string(b), `"client_secret":"auth.oauth.client_secret"`) {
		t.Errorf("replaced: %s", b)
	}
}
