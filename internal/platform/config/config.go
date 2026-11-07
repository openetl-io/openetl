// Package config is openetld's configuration: flags, each defaulting to an
// OPENETL_* environment variable.
package config

import (
	"cmp"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	DatabaseURL string
	Listen      string
	APIURL      string
	// What runs workflows: dbos, which needs nothing but Postgres, or temporal.
	Orchestrator string
	// DBOS's database. Empty: on DatabaseURL's server, named after it.
	DBOSDatabaseURL string
	// Which orchestrator this is, on DBOS. Empty: the host name.
	ExecutorID        string
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
	// Spark 3.5's spark-submit, for Spark steps on self-managed clusters.
	// Empty: spark-submit on PATH.
	SparkSubmit string
	// Where the runner serves the bridge to pipelines' Flink jobs, and the
	// host:port they reach it at. Empty listen: no bridge, so the runner
	// can't run Flink pipelines.
	BridgeListen    string
	BridgeAdvertise string
	// CIDR prefixes, comma-separated, that connectors, registries and
	// webhooks may reach besides public addresses.
	EgressAllow string
	// The key that wraps orgs' data keys, for the API server: a key file, or
	// a base64key://, awskms://, gcpkms:// or azurekeyvault:// URL. Empty:
	// the server can't store secrets.
	SecretsKey string
	// Where the server writes the owner key it creates with the org, and
	// openetld bootstrap-key a new one.
	BootstrapKeyFile string
	// How far behind its source a continuous pipeline's read gets before
	// the server sends pipeline.lagging, as a Go duration.
	LaggingAfter string
	// A directory holding the built UI, which the API server serves at /.
	// Empty: no UI.
	Web string
	// The pool a runner serves.
	Pool string
	// What a runner places on itself as CPU,MEMORY. Empty: its cgroup's or
	// host's, less Reserve.
	Capacity string
	// What a runner keeps for itself, as CPU,MEMORY.
	Reserve string
	// A delegated cgroup v2 directory for a cgroup per job. Empty: none.
	CgroupRoot string
	// How often a runner reports, as a Go duration.
	ReportInterval string
	// Makes a runner a pod launcher: kubernetes, or process for development
	// and tests. Empty: it runs its attempts itself.
	Launcher string
	// The jobs a launcher sends to pods, as a preset or CPU,MEMORY: this
	// size or larger, in either.
	PodMinSize string
	// The largest pod a launcher makes. Empty: any size.
	PodMaxSize string
	// How many pods a launcher has at once, and their vCPUs between them.
	MaxPods   int
	MaxPodCPU string
	// A file holding the pod the kubernetes launcher makes pods from, or
	// podtemplate/NAME.
	PodTemplate string
	// Empty: the kubeconfig context's, or the launcher's own.
	PodNamespace string
	// Empty: the launcher's service account.
	Kubeconfig string
	// Each pod's ephemeral storage, such as 10Gi; 0: none set.
	PodScratch string
	// The one attempt a pod's runner runs, and its placement.
	Attempt   string
	Placement int
	// A file holding the runner's token. Empty: OPENETL_RUNNER_TOKEN.
	TokenFile string
	// From OPENETL_RUNNER_TOKEN only, so it isn't in a process listing.
	RunnerToken string
	LogLevel    string
	LogFormat   string
	// stderr, stdout, or a file rotated by size.
	LogOutput   string
	LogMaxSize  int
	LogMaxFiles int
	// Days the audit log keeps entries at least; 0 keeps every entry.
	AuditRetentionDays int
	// How browsers sign in: password, or none, which makes calls without a
	// credential act as an owner. Empty: password.
	Auth string
	// Lets --auth none serve on an address that isn't loopback.
	AuthNoneAllowRemote bool
	// The URL browsers use. Empty: http:// and the listen address.
	PublicURL string
	// Go durations; empty: 12h and 168h.
	SessionIdleTimeout string
	SessionMaxAge      string
	// Sign-ins a minute each client address may make; 0: 20.
	SignInRate int
}

type option struct {
	flag, env, def, usage string
	dst                   func(*Config) *string
}

var options = []option{
	{"database-url", "OPENETL_DATABASE_URL", "postgres://openetl:openetl@localhost:5432/openetl", "metadata database URL", func(c *Config) *string { return &c.DatabaseURL }},
	{"listen", "OPENETL_LISTEN", "localhost:8080", "API listen address", func(c *Config) *string { return &c.Listen }},
	{"api-url", "OPENETL_API_URL", "http://localhost:8080", "API URL, for runners", func(c *Config) *string { return &c.APIURL }},
	{"orchestrator", "OPENETL_ORCHESTRATOR", "dbos", "what runs workflows: dbos, which needs nothing but Postgres, or temporal", func(c *Config) *string { return &c.Orchestrator }},
	{"dbos-database-url", "OPENETL_DBOS_DATABASE_URL", "", "the database DBOS keeps workflows in, which it creates if it can; not through a transaction pooler (default: on the metadata database's server, named after it with _dbos)", func(c *Config) *string { return &c.DBOSDatabaseURL }},
	{"executor-id", "OPENETL_EXECUTOR_ID", "", "which orchestrator this is, unique among them and the same across restarts, such as its pod's name; on DBOS only (default: the host name)", func(c *Config) *string { return &c.ExecutorID }},
	{"temporal-address", "OPENETL_TEMPORAL_ADDRESS", "localhost:7233", "Temporal frontend host:port; on Temporal only", func(c *Config) *string { return &c.TemporalAddress }},
	{"temporal-namespace", "OPENETL_TEMPORAL_NAMESPACE", "default", "Temporal namespace; on Temporal only", func(c *Config) *string { return &c.TemporalNamespace }},
	{"data-plane", "OPENETL_DATA_PLANE", "default", "data plane ID", func(c *Config) *string { return &c.DataPlane }},
	{"checkpoints", "OPENETL_CHECKPOINTS", ".openetl/checkpoints", "checkpoint store: a directory, s3://bucket?region=…&endpoint=…, gs://bucket, or azblob://container?account=…&endpoint=…; a path after the bucket puts everything under it", func(c *Config) *string { return &c.Checkpoints }},
	{"engine", "OPENETL_ENGINE", "", "openetl-engine executable (default: next to openetld, then on PATH)", func(c *Config) *string { return &c.Engine }},
	{"connectors", "OPENETL_CONNECTORS", "", "directory of external connector executables, named source-NAME and destination-NAME (default: connectors/ next to openetld)", func(c *Config) *string { return &c.Connectors }},
	{"cache", "OPENETL_CACHE", ".openetl/cache", "where runners cache artifacts, such as connector versions and dbt projects, and connectors' executables", func(c *Config) *string { return &c.Cache }},
	{"dbt", "OPENETL_DBT", "", "dbt executable for pipelines' post-sync steps, with the destinations' adapters installed (default: dbt on PATH)", func(c *Config) *string { return &c.Dbt }},
	{"spark-submit", "OPENETL_SPARK_SUBMIT", "", "Spark 3.5's spark-submit, for pipelines' Spark steps on self-managed clusters (default: spark-submit on PATH)", func(c *Config) *string { return &c.SparkSubmit }},
	{"bridge-listen", "OPENETL_BRIDGE_LISTEN", "", "where the runner serves the bridge to pipelines' Flink jobs, such as :7710; empty: the runner doesn't run Flink pipelines", func(c *Config) *string { return &c.BridgeListen }},
	{"bridge-advertise", "OPENETL_BRIDGE_ADVERTISE", "", "the host:port Flink jobs reach this runner's bridge at, which must reach this runner and not a load balancer (default: the listen address)", func(c *Config) *string { return &c.BridgeAdvertise }},
	{"pool", "OPENETL_POOL", "default", "the pool this runner serves", func(c *Config) *string { return &c.Pool }},
	{"capacity", "OPENETL_CAPACITY", "", "what this runner places on itself, as CPU,MEMORY such as 4,8Gi (default: its cgroup's or host's, less --reserve)", func(c *Config) *string { return &c.Capacity }},
	{"reserve", "OPENETL_RESERVE", "0.5,512Mi", "what this runner keeps for itself, as CPU,MEMORY", func(c *Config) *string { return &c.Reserve }},
	{"cgroup-root", "OPENETL_CGROUP_ROOT", "", "a cgroup v2 directory delegated to this runner alone, for a cgroup per job on Linux 5.7 or later, which holds a job to its size's CPU and memory; empty: its threads and a memory watchdog do", func(c *Config) *string { return &c.CgroupRoot }},
	{"launcher", "OPENETL_LAUNCHER", "", "makes this runner a pod launcher for its pool, which runs light operations itself and makes a pod for each attempt placed on it: kubernetes, or process, which runs each in a process of its own, for development and tests", func(c *Config) *string { return &c.Launcher }},
	{"pod-min-size", "OPENETL_POD_MIN_SIZE", "m", "a launcher's pool sends jobs this size or larger in CPU or memory to pods, and those no runner of the pool could take: xs to xl, or CPU,MEMORY such as 0,0 for every job", func(c *Config) *string { return &c.PodMinSize }},
	{"pod-max-size", "OPENETL_POD_MAX_SIZE", "", "the largest pod a launcher makes, as a preset or CPU,MEMORY (default: any size)", func(c *Config) *string { return &c.PodMaxSize }},
	{"max-pod-cpu", "OPENETL_MAX_POD_CPU", "64", "the vCPUs a launcher's pods may have between them; 0: no limit", func(c *Config) *string { return &c.MaxPodCPU }},
	{"pod-template", "OPENETL_POD_TEMPLATE", "", "the pod the kubernetes launcher makes each attempt's from, in a YAML file or as podtemplate/NAME, a PodTemplate in the pods' namespace: its container runs openetld's image with the API's URL, the runner token and the checkpoint store, and the launcher sets the rest", func(c *Config) *string { return &c.PodTemplate }},
	{"pod-namespace", "OPENETL_POD_NAMESPACE", "", "the namespace the kubernetes launcher makes pods in (default: the kubeconfig context's, or the launcher's own)", func(c *Config) *string { return &c.PodNamespace }},
	{"kubeconfig", "OPENETL_KUBECONFIG", "", "a kubeconfig file for the kubernetes launcher (default: its service account; it never reads one it isn't given)", func(c *Config) *string { return &c.Kubeconfig }},
	{"pod-scratch", "OPENETL_POD_SCRATCH", "10Gi", "each pod's ephemeral storage, for its runner's scratch; 0: none set", func(c *Config) *string { return &c.PodScratch }},
	{"attempt", "OPENETL_ATTEMPT", "", "the one attempt this runner runs before it stops, as a pod launcher's pod; set by the launcher", func(c *Config) *string { return &c.Attempt }},
	{"report-interval", "OPENETL_REPORT_INTERVAL", "10s", "how often this runner reports what it runs to the API, as a Go duration; keep it well under its work's heartbeat timeouts", func(c *Config) *string { return &c.ReportInterval }},
	{"web", "OPENETL_WEB", "", "a directory holding the built UI (web/dist) for the API server to serve at /; empty: no UI", func(c *Config) *string { return &c.Web }},
	{"egress-allow", "OPENETL_EGRESS_ALLOW", "", "comma-separated CIDR prefixes connectors, registries and webhooks may reach besides public addresses, such as a hybrid data plane's own networks", func(c *Config) *string { return &c.EgressAllow }},
	{"lagging-after", "OPENETL_LAGGING_AFTER", "5m", "how far behind its source a continuous pipeline's read gets before the server sends pipeline.lagging to webhooks", func(c *Config) *string { return &c.LaggingAfter }},
	{"secrets-key", "OPENETL_SECRETS_KEY", ".openetl/secrets.key", "the API server's key-encryption key for secrets: a file holding a base64-encoded 32-byte key, created if missing; or base64key://KEY, awskms://…, gcpkms://… or azurekeyvault://…", func(c *Config) *string { return &c.SecretsKey }},
	{"bootstrap-key-file", "OPENETL_BOOTSTRAP_KEY_FILE", ".openetl/admin.key", "where the server writes the owner key of the org it creates on first start, readable only by its owner; and where openetld bootstrap-key writes a new one", func(c *Config) *string { return &c.BootstrapKeyFile }},
	{"token-file", "OPENETL_RUNNER_TOKEN_FILE", "", "a file holding the runner's token, which openetl dataplane token create makes (default: the OPENETL_RUNNER_TOKEN variable)", func(c *Config) *string { return &c.TokenFile }},
	{"log-level", "OPENETL_LOG_LEVEL", "info", "debug, info, warn or error", func(c *Config) *string { return &c.LogLevel }},
	{"log-format", "OPENETL_LOG_FORMAT", "json", "json or text", func(c *Config) *string { return &c.LogFormat }},
	{"log-output", "OPENETL_LOG_OUTPUT", "stderr", "stderr, stdout, or a file, rotated by size", func(c *Config) *string { return &c.LogOutput }},
	{"auth", "OPENETL_AUTH", "password", "how browsers sign in: password, or none, which makes every call without a credential an owner's (loopback only, unless --auth-none-allow-remote)", func(c *Config) *string { return &c.Auth }},
	{"public-url", "OPENETL_PUBLIC_URL", "", "the URL browsers use, which password links start with; an https one makes the session cookie Secure (default: http:// and the listen address)", func(c *Config) *string { return &c.PublicURL }},
	{"session-idle-timeout", "OPENETL_SESSION_IDLE_TIMEOUT", "12h", "how long a UI session lasts unused, as a Go duration", func(c *Config) *string { return &c.SessionIdleTimeout }},
	{"session-max-age", "OPENETL_SESSION_MAX_AGE", "168h", "how long a UI session lasts in all, as a Go duration", func(c *Config) *string { return &c.SessionMaxAge }},
}

type boolOption struct {
	flag, env, usage string
	dst              func(*Config) *bool
}

var boolOptions = []boolOption{
	{"auth-none-allow-remote", "OPENETL_AUTH_NONE_ALLOW_REMOTE", "let --auth none serve on an address that isn't loopback, such as a private network or behind an authenticating proxy", func(c *Config) *bool { return &c.AuthNoneAllowRemote }},
}

type intOption struct {
	flag, env string
	def       int
	usage     string
	dst       func(*Config) *int
}

var intOptions = []intOption{
	{"max-pods", "OPENETL_MAX_PODS", 20, "how many pods a launcher has at once; 0: no limit", func(c *Config) *int { return &c.MaxPods }},
	{"placement", "OPENETL_PLACEMENT", 0, "which placement of --attempt this pod is for; set by the launcher", func(c *Config) *int { return &c.Placement }},
	{"log-max-size", "OPENETL_LOG_MAX_SIZE", 100, "megabytes a log file reaches before it's rotated", func(c *Config) *int { return &c.LogMaxSize }},
	{"log-max-files", "OPENETL_LOG_MAX_FILES", 5, "rotated log files kept", func(c *Config) *int { return &c.LogMaxFiles }},
	{"sign-in-rate", "OPENETL_SIGN_IN_RATE", 20, "sign-ins a minute each client address may make; behind a proxy, every user shares its address", func(c *Config) *int { return &c.SignInRate }},
	{"audit-retention-days", "OPENETL_AUDIT_RETENTION_DAYS", 90, "days the audit log keeps entries at least: a month's entries go once the month ended that long ago; 0 keeps every entry", func(c *Config) *int { return &c.AuditRetentionDays }},
}

// Parse parses args. defaults override the built-in defaults, by flag name;
// environment variables override both, and flags override everything.
func Parse(fs *flag.FlagSet, args []string, defaults map[string]string) (Config, error) {
	var c Config
	for _, o := range options {
		def := cmp.Or(os.Getenv(o.env), defaults[o.flag], o.def)
		fs.StringVar(o.dst(&c), o.flag, def, fmt.Sprintf("%s (env %s)", o.usage, o.env))
	}
	for _, o := range intOptions {
		def := o.def
		if v := cmp.Or(os.Getenv(o.env), defaults[o.flag]); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil {
				return c, fmt.Errorf("config: %s: %q isn't a number", o.flag, v)
			}
			def = n
		}
		fs.IntVar(o.dst(&c), o.flag, def, fmt.Sprintf("%s (env %s)", o.usage, o.env))
	}
	for _, o := range boolOptions {
		var def bool
		if v := cmp.Or(os.Getenv(o.env), defaults[o.flag]); v != "" {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return c, fmt.Errorf("config: %s: %q isn't true or false", o.flag, v)
			}
			def = b
		}
		fs.BoolVar(o.dst(&c), o.flag, def, fmt.Sprintf("%s (env %s)", o.usage, o.env))
	}
	if err := fs.Parse(args); err != nil {
		return c, err
	}
	if fs.NArg() > 0 {
		return c, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	c.RunnerToken = os.Getenv("OPENETL_RUNNER_TOKEN")
	if c.AuditRetentionDays < 0 {
		return c, fmt.Errorf("config: audit-retention-days is %d; it can't be negative", c.AuditRetentionDays)
	}
	if c.Auth != "password" && c.Auth != "none" {
		return c, fmt.Errorf("config: auth is %q; it must be password or none", c.Auth)
	}
	if c.DataPlane == "" || strings.ContainsAny(c.DataPlane, ". /") {
		return c, errors.New("config: the data plane ID must be non-empty, without dots, slashes or spaces")
	}
	return c, nil
}
