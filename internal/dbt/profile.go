package dbt

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"unicode"

	"go.yaml.in/yaml/v3"
)

// targetName is the profile's only target.
const targetName = "openetl"

// Target is how dbt reaches a pipeline's destination: a profiles.yml output
// whose values come from the environment, so none is written to the file,
// and dbt doesn't render them as Jinja.
type Target struct {
	Output map[string]any
	Env    []string
	// The schema the pipeline's tables are in.
	Namespace string
}

func (t *Target) set(key, env, value string) {
	t.Output[key] = fmt.Sprintf("{{ env_var('%s') }}", env)
	t.Env = append(t.Env, env+"="+value)
}

// DestinationTarget is how dbt reaches a destination: its connector, its
// config as JSON, the namespace the pipeline writes to (empty: the
// destination's default), and the schema dbt builds into (empty: the
// namespace).
func DestinationTarget(connector string, config []byte, namespace, schema string) (*Target, error) {
	switch connector {
	case "postgres":
		return postgresTarget(config, namespace, schema)
	}
	return nil, fmt.Errorf("dbt can't run against a %s destination yet, only postgres", connector)
}

// Profiles is a profiles.yml with one profile, whose only target is t.
func Profiles(profile string, t *Target) ([]byte, error) {
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	err := enc.Encode(map[string]any{
		profile: map[string]any{"target": targetName, "outputs": map[string]any{targetName: t.Output}},
	})
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), enc.Close()
}

// Passed on to dbt-postgres where the DSN has them.
var postgresSSL = []string{"sslmode", "sslrootcert", "sslcert", "sslkey"}

func postgresTarget(config []byte, namespace, schema string) (*Target, error) {
	var cfg struct {
		DSN    string `json:"dsn"`
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal(config, &cfg); err != nil {
		return nil, fmt.Errorf("the destination's config: %w", err)
	}
	kv, err := parseDSN(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("the destination's dsn: %w", err)
	}
	switch {
	case kv["host"] == "":
		return nil, errors.New("the destination's dsn names no host, which dbt needs")
	case strings.Contains(kv["host"], ","):
		return nil, errors.New("the destination's dsn names several hosts; dbt connects to one")
	case kv["user"] == "":
		return nil, errors.New("the destination's dsn names no user, which dbt needs")
	}
	port, err := strconv.Atoi(cmp.Or(kv["port"], "5432"))
	if err != nil || port <= 0 || port > 65535 {
		return nil, fmt.Errorf("the destination's dsn has port %q, which dbt can't use", kv["port"])
	}
	ns := cmp.Or(namespace, cfg.Schema, "public")
	t := &Target{Output: map[string]any{"type": "postgres", "port": port, "threads": 4}, Namespace: ns}
	t.set("host", "OPENETL_DBT_HOST", kv["host"])
	t.set("user", "OPENETL_DBT_USER", kv["user"])
	// dbt scrubs DBT_ENV_SECRET_ variables' values from its logs.
	t.set("password", "DBT_ENV_SECRET_OPENETL_PASSWORD", kv["password"])
	t.set("dbname", "OPENETL_DBT_DBNAME", cmp.Or(kv["dbname"], kv["user"]))
	t.set("schema", "OPENETL_DBT_SCHEMA", cmp.Or(schema, ns))
	for _, k := range postgresSSL {
		if v := kv[k]; v != "" {
			t.set(k, "OPENETL_DBT_"+strings.ToUpper(k), v)
		}
	}
	return t, nil
}

// parseDSN parses a libpq connection URL or keyword/value string into
// keywords. Unlike libpq, it reads no environment variables or files: the
// destination connector gets none either.
func parseDSN(dsn string) (map[string]string, error) {
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return nil, errors.New("it isn't a valid URL")
		}
		kv := map[string]string{"dbname": strings.TrimPrefix(u.Path, "/")}
		if u.User != nil {
			kv["user"] = u.User.Username()
			kv["password"], _ = u.User.Password()
		}
		if strings.Contains(u.Host, ",") {
			kv["host"] = u.Host
		} else {
			kv["host"], kv["port"] = u.Hostname(), u.Port()
		}
		for k, v := range u.Query() {
			kv[k] = v[len(v)-1]
		}
		return kv, nil
	}
	kv := map[string]string{}
	s := []rune(dsn)
	skip := func(i int) int {
		for i < len(s) && unicode.IsSpace(s[i]) {
			i++
		}
		return i
	}
	for i := skip(0); i < len(s); i = skip(i) {
		start := i
		for i < len(s) && s[i] != '=' && !unicode.IsSpace(s[i]) {
			i++
		}
		key := string(s[start:i])
		if i = skip(i); i == len(s) || s[i] != '=' || key == "" {
			return nil, fmt.Errorf("expected key=value at %q", string(s[start:]))
		}
		i = skip(i + 1)
		var v strings.Builder
		quoted := i < len(s) && s[i] == '\''
		if quoted {
			i++
		}
		for ; i < len(s); i++ {
			c := s[i]
			if (quoted && c == '\'') || (!quoted && unicode.IsSpace(c)) {
				break
			}
			if c == '\\' && i+1 < len(s) {
				i++
				c = s[i]
			}
			v.WriteRune(c)
		}
		if quoted {
			if i == len(s) {
				return nil, fmt.Errorf("unterminated quote in %s's value", key)
			}
			i++
		}
		kv[key] = v.String()
	}
	return kv, nil
}
