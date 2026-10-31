package sdk_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	sdk "github.com/unsuitable001/openetl/sdk/go"
)

type secretConfig struct {
	Key  sdk.Secret `json:"key"`
	Host string     `json:"host"`
}

type hidden struct {
	key sdk.Secret
}

func TestSecretNeverPrints(t *testing.T) {
	var cfg secretConfig
	if err := json.Unmarshal([]byte(`{"key":"hunter2","host":"db"}`), &cfg); err != nil {
		t.Fatal(err)
	}
	if got := cfg.Key.Reveal(); got != "hunter2" {
		t.Fatalf("Reveal() = %q", got)
	}

	var out []string
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%d"} {
		out = append(out, fmt.Sprintf(verb, cfg), fmt.Sprintf(verb, cfg.Key), fmt.Sprintf(verb, hidden{key: cfg.Key}))
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	out = append(out, string(b))
	var logged bytes.Buffer
	slog.New(slog.NewJSONHandler(&logged, nil)).Info("x", "cfg", cfg, "key", cfg.Key)
	out = append(out, logged.String())

	for _, s := range out {
		if strings.Contains(s, "hunter2") || strings.Contains(s, fmt.Sprintf("%x", "hunter2")) {
			t.Errorf("leaked: %s", s)
		}
	}
	if !strings.Contains(string(b), `"key":"[redacted]"`) {
		t.Errorf("json.Marshal = %s", b)
	}
}

func TestSecretZero(t *testing.T) {
	var cfg secretConfig
	if err := json.Unmarshal([]byte(`{"key":null}`), &cfg); err != nil {
		t.Fatal(err)
	}
	if !cfg.Key.IsZero() || cfg.Key.Reveal() != "" {
		t.Fatal("a null secret should be zero")
	}
	if err := json.Unmarshal([]byte(`{"key":42}`), &cfg); err == nil {
		t.Fatal("a number decoded as a secret")
	}
}
