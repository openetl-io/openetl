package external

import (
	"context"
	"strings"
	"testing"
)

func TestNestedSecrets(t *testing.T) {
	schema := []byte(`{"type": "object", "properties": {"region": {"type": "string"},
		"auth": {"oneOf": [{"properties": {"client_secret": {"type": "string", "x-secret": true}}}, {"properties": {"password": {"type": "string", "x-secret": true}}}]}}}`)
	cfg, secrets, err := splitSecrets(schema, []byte(`{"region": "eu", "port": 5432, "auth": {"client_id": "id", "client_secret": "s3cr3t"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := string(cfg); got != `{"auth":{"client_id":"id","client_secret":"auth.client_secret"},"port":5432,"region":"eu"}` {
		t.Errorf("config %s", got)
	}
	if v, err := secrets.Secret(context.Background(), "auth.client_secret"); err != nil || v.Reveal() != "s3cr3t" || len(secrets) != 1 {
		t.Errorf("secrets %v, %v", secrets, err)
	}
	if _, _, err := splitSecrets(schema, []byte(`{"auth": {"client_secret": 1}}`)); err == nil || !strings.Contains(err.Error(), "auth.client_secret is a secret") {
		t.Errorf("a secret that isn't a string: %v", err)
	}
	if _, err := SecretProperties([]byte(`{"properties": {"hosts": {"items": {"properties": {"pw": {"x-secret": true}}}}}}`)); err == nil {
		t.Error("a secret inside an array")
	}
}
