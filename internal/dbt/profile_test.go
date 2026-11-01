package dbt

import (
	"maps"
	"reflect"
	"slices"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

func TestParseDSN(t *testing.T) {
	for dsn, want := range map[string]map[string]string{
		"postgres://u:p%40ss@db.example.com:6543/shop?sslmode=require": {
			"user": "u", "password": "p@ss", "host": "db.example.com", "port": "6543", "dbname": "shop", "sslmode": "require",
		},
		"postgresql://u@[::1]/shop": {"user": "u", "password": "", "host": "::1", "port": "", "dbname": "shop"},
		"postgres://a:1,b:2/shop":   {"host": "a:1,b:2", "dbname": "shop"},
		`host=db port=5432 user=u password='it\'s s\\ecret' dbname = shop`: {
			"host": "db", "port": "5432", "user": "u", "password": `it's s\ecret`, "dbname": "shop",
		},
		"  host=db\tuser=u\npassword=a\\ b  ": {"host": "db", "user": "u", "password": "a b"},
	} {
		got, err := parseDSN(dsn)
		if err != nil {
			t.Errorf("%q: %v", dsn, err)
			continue
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%q = %v, want %v", dsn, got, want)
		}
	}
	for _, dsn := range []string{"host", "host=db user", "=x", "password='open"} {
		if _, err := parseDSN(dsn); err == nil {
			t.Errorf("%q parsed", dsn)
		}
	}
}

func TestPostgresTarget(t *testing.T) {
	cfg := []byte(`{"dsn": "postgres://u:%7B%7B%20p%20%7D%7D@db:6543/shop?sslmode=verify-full&sslrootcert=/ca.pem&application_name=x", "schema": "landing"}`)
	tg, err := DestinationTarget("postgres", cfg, "", "")
	if err != nil {
		t.Fatal(err)
	}
	env := map[string]string{}
	for _, kv := range tg.Env {
		k, v, _ := strings.Cut(kv, "=")
		env[k] = v
	}
	wantEnv := map[string]string{
		"OPENETL_DBT_HOST": "db", "OPENETL_DBT_USER": "u", "DBT_ENV_SECRET_OPENETL_PASSWORD": "{{ p }}",
		"OPENETL_DBT_DBNAME": "shop", "OPENETL_DBT_SCHEMA": "landing",
		"OPENETL_DBT_SSLMODE": "verify-full", "OPENETL_DBT_SSLROOTCERT": "/ca.pem",
	}
	if !reflect.DeepEqual(env, wantEnv) {
		t.Errorf("env = %v, want %v", env, wantEnv)
	}
	if tg.Namespace != "landing" || tg.Output["port"] != 6543 || tg.Output["type"] != "postgres" {
		t.Errorf("target = %+v", tg)
	}
	// Every value but the numbers comes from the environment.
	for k, v := range tg.Output {
		if s, ok := v.(string); ok && k != "type" && !strings.HasPrefix(s, "{{ env_var('") {
			t.Errorf("%s = %q is in the file", k, s)
		}
	}

	// The pipeline's namespace wins over the destination's schema, and the
	// step's schema over both.
	tg, err = DestinationTarget("postgres", cfg, "raw", "marts")
	if err != nil {
		t.Fatal(err)
	}
	if tg.Namespace != "raw" || !slices.Contains(tg.Env, "OPENETL_DBT_SCHEMA=marts") {
		t.Errorf("namespace %q, env %v", tg.Namespace, tg.Env)
	}
	tg, err = DestinationTarget("postgres", []byte(`{"dsn": "host=db user=u"}`), "", "")
	if err != nil {
		t.Fatal(err)
	}
	if tg.Namespace != "public" || tg.Output["port"] != 5432 || !slices.Contains(tg.Env, "OPENETL_DBT_DBNAME=u") {
		t.Errorf("defaults: %+v", tg)
	}
}

func TestTargetRefuses(t *testing.T) {
	for name, c := range map[string]struct{ connector, config string }{
		"connector":  {"snowflake", `{}`},
		"no host":    {"postgres", `{"dsn": "user=u"}`},
		"no user":    {"postgres", `{"dsn": "host=db"}`},
		"two hosts":  {"postgres", `{"dsn": "postgres://u@a:1,b:2/db"}`},
		"two hosts2": {"postgres", `{"dsn": "host=a,b user=u"}`},
		"port":       {"postgres", `{"dsn": "host=db user=u port=x"}`},
		"config":     {"postgres", `[]`},
	} {
		if _, err := DestinationTarget(c.connector, []byte(c.config), "", ""); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestProfiles(t *testing.T) {
	tg, err := DestinationTarget("postgres", []byte(`{"dsn": "host=db user=u"}`), "", "")
	if err != nil {
		t.Fatal(err)
	}
	b, err := Profiles("shop", tg)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]struct {
		Target  string                    `yaml:"target"`
		Outputs map[string]map[string]any `yaml:"outputs"`
	}
	if err := yaml.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	p := got["shop"]
	if p.Target != targetName || !maps.Equal(p.Outputs[targetName], tg.Output) {
		t.Errorf("profiles.yml:\n%s", b)
	}
}
