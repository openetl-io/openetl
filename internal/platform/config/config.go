// Package config is openetld's configuration: flags, each defaulting to an
// OPENETL_* environment variable.
package config

import (
	"cmp"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
)

type Config struct {
	DatabaseURL       string
	Listen            string
	APIURL            string
	TemporalAddress   string
	TemporalNamespace string
	DataPlane         string
	// A directory, or an s3://, gs:// or azblob:// URL.
	Checkpoints string
	// The openetl-engine executable. Empty: next to openetld, then on PATH.
	Engine string
	// A directory of external connectors' executables. Empty: connectors/
	// next to openetld.
	Connectors string
	// Where runners cache artifacts and connectors' executables.
	Cache string
	// The dbt executable for post-sync steps. Empty: dbt on PATH.
	Dbt string
	// CIDR prefixes, comma-separated, that connectors and registries may
	// reach besides public addresses.
	EgressAllow string
	// The key that wraps orgs' data keys, for the API server: a key file, or
	// a base64key://, awskms://, gcpkms:// or azurekeyvault:// URL. Empty:
	// the server can't store secrets.
	SecretsKey string
	LogLevel   string
	LogFormat  string
}

type option struct {
	flag, env, def, usage string
	dst                   func(*Config) *string
}

var options = []option{
	{"database-url", "OPENETL_DATABASE_URL", "postgres://openetl:openetl@localhost:5432/openetl", "metadata database URL", func(c *Config) *string { return &c.DatabaseURL }},
	{"listen", "OPENETL_LISTEN", "localhost:8080", "API listen address", func(c *Config) *string { return &c.Listen }},
	{"api-url", "OPENETL_API_URL", "http://localhost:8080", "API URL, for runners", func(c *Config) *string { return &c.APIURL }},
	{"temporal-address", "OPENETL_TEMPORAL_ADDRESS", "localhost:7233", "Temporal frontend host:port", func(c *Config) *string { return &c.TemporalAddress }},
	{"temporal-namespace", "OPENETL_TEMPORAL_NAMESPACE", "default", "Temporal namespace", func(c *Config) *string { return &c.TemporalNamespace }},
	{"data-plane", "OPENETL_DATA_PLANE", "default", "data plane ID", func(c *Config) *string { return &c.DataPlane }},
	{"checkpoints", "OPENETL_CHECKPOINTS", ".openetl/checkpoints", "checkpoint store: a directory, s3://bucket?region=…&endpoint=…, gs://bucket, or azblob://container?account=…&endpoint=…; a path after the bucket puts everything under it", func(c *Config) *string { return &c.Checkpoints }},
	{"engine", "OPENETL_ENGINE", "", "openetl-engine executable (default: next to openetld, then on PATH)", func(c *Config) *string { return &c.Engine }},
	{"connectors", "OPENETL_CONNECTORS", "", "directory of external connector executables, named source-NAME and destination-NAME (default: connectors/ next to openetld)", func(c *Config) *string { return &c.Connectors }},
	{"cache", "OPENETL_CACHE", ".openetl/cache", "where runners cache artifacts, such as connector versions and dbt projects, and connectors' executables", func(c *Config) *string { return &c.Cache }},
	{"dbt", "OPENETL_DBT", "", "dbt executable for pipelines' post-sync steps, with the destinations' adapters installed (default: dbt on PATH)", func(c *Config) *string { return &c.Dbt }},
	{"egress-allow", "OPENETL_EGRESS_ALLOW", "", "comma-separated CIDR prefixes connectors and registries may reach besides public addresses, such as a hybrid data plane's own networks", func(c *Config) *string { return &c.EgressAllow }},
	{"secrets-key", "OPENETL_SECRETS_KEY", ".openetl/secrets.key", "the API server's key-encryption key for secrets: a file holding a base64-encoded 32-byte key, created if missing; or base64key://KEY, awskms://…, gcpkms://… or azurekeyvault://…", func(c *Config) *string { return &c.SecretsKey }},
	{"log-level", "OPENETL_LOG_LEVEL", "info", "debug, info, warn or error", func(c *Config) *string { return &c.LogLevel }},
	{"log-format", "OPENETL_LOG_FORMAT", "json", "json or text", func(c *Config) *string { return &c.LogFormat }},
}

// Parse parses args. defaults override the built-in defaults, by flag name;
// environment variables override both, and flags override everything.
func Parse(fs *flag.FlagSet, args []string, defaults map[string]string) (Config, error) {
	var c Config
	for _, o := range options {
		def := cmp.Or(os.Getenv(o.env), defaults[o.flag], o.def)
		fs.StringVar(o.dst(&c), o.flag, def, fmt.Sprintf("%s (env %s)", o.usage, o.env))
	}
	if err := fs.Parse(args); err != nil {
		return c, err
	}
	if fs.NArg() > 0 {
		return c, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	if c.DataPlane == "" || strings.ContainsAny(c.DataPlane, ". /") {
		return c, errors.New("config: the data plane ID must be non-empty, without dots, slashes or spaces")
	}
	return c, nil
}
