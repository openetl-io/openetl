package main

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/openetl-io/openetl/internal/platform/config"
)

type group struct {
	heading string
	about   string
	// Settings by flag, or by variable for one without a flag.
	settings []string
}

// The settings page's sections. Every setting is in one; the page fails to
// render otherwise.
var groups = []group{
	{
		"Database and API",
		"The server and the orchestrators keep OpenETL's records in the metadata database, which `openetld bootstrap-key` writes to as well. Runners don't connect to it: they reach the API at `--api-url`.",
		[]string{"database-url", "listen", "api-url", "data-plane"},
	},
	{
		"Orchestration",
		"What runs workflows, for the server and the orchestrators: DBOS, which keeps them in Postgres, or Temporal.",
		[]string{"orchestrator", "dbos-database-url", "executor-id", "temporal-address", "temporal-namespace"},
	},
	{
		"Console and sign-in",
		"The console the server serves, and how people sign in to it.",
		[]string{"web", "auth", "auth-none-allow-remote", "public-url", "session-idle-timeout", "session-max-age", "sign-in-rate"},
	},
	{
		"Secrets and keys",
		"The key that protects secrets, and the owner key the server writes when it creates the org.",
		[]string{"secrets-key", "bootstrap-key-file"},
	},
	{
		"Events and the audit log",
		"When a continuous pipeline counts as lagging, and how long the audit log keeps entries.",
		[]string{"lagging-after", "audit-retention-days"},
	},
	{
		"Network and registries",
		"What connectors, registries and webhooks may reach, and where connector packages come from.",
		[]string{"egress-allow", "official-connectors", "registry-plain-http", "github-api"},
	},
	{
		"Runners",
		"Where a runner keeps checkpoints and its cache, the programs it runs, the pool it serves, the room it has, and its token.",
		[]string{"checkpoints", "engine", "connectors", "cache", "dbt", "spark-submit", "bridge-listen", "bridge-advertise", "pool", "capacity", "reserve", "cgroup-root", "report-interval", "token-file", config.RunnerTokenEnv},
	},
	{
		"Pod launchers",
		"A runner with `--launcher` set is a pod launcher: it makes a pod, or a process, for each attempt placed on it, instead of running the attempt itself.",
		[]string{"launcher", "pod-min-size", "pod-max-size", "max-pods", "max-pod-cpu", "pod-template", "pod-namespace", "kubeconfig", "pod-stream-spot", "pod-scratch", "attempt", "placement"},
	},
	{
		"Logs",
		"What each role logs, and where.",
		[]string{"log-level", "log-format", "log-output", "log-max-size", "log-max-files"},
	},
}

const telemetry = `## Traces and metrics

Traces and metrics are set with OpenTelemetry's standard variables rather than flags. A signal is exported only once its exporter (` + "`OTEL_TRACES_EXPORTER`, `OTEL_METRICS_EXPORTER`" + `) or an OTLP endpoint (` + "`OTEL_EXPORTER_OTLP_ENDPOINT`, or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` and `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`" + `) is set, and ` + "`OTEL_SDK_DISABLED=true`" + ` turns both off. ` + "`OTEL_METRICS_EXPORTER=prometheus`" + ` serves a Prometheus endpoint instead, on ` + "`OTEL_EXPORTER_PROMETHEUS_HOST`" + ` and ` + "`OTEL_EXPORTER_PROMETHEUS_PORT`" + `, ` + "`localhost`" + ` and ` + "`9464`" + ` unless they're set.

Each process reports as the service ` + "`openetld`" + `, unless ` + "`OTEL_SERVICE_NAME`" + ` names another, with its role and data plane as ` + "`openetl.role`" + ` and ` + "`openetl.data_plane`" + `, and ` + "`OTEL_RESOURCE_ATTRIBUTES`" + ` adds to them.
`

func settingsPage() (string, error) {
	settings := map[string]config.Setting{}
	for _, s := range config.Settings() {
		settings[key(s)] = s
	}
	var b strings.Builder
	front(&b, "openetld settings", "Every setting of openetld, as a flag and an environment variable, with its default.")
	b.WriteString("Every setting of `openetld` is a flag and an environment variable: `--database-url` is also `OPENETL_DATABASE_URL`. A flag overrides the variable, and the variable overrides the default. Every role takes every setting and uses the ones that concern it. `openetld ROLE -h` lists them, with the defaults the environment gives.\n\n")
	dev := config.DevDefaults()
	var devs []string
	for _, f := range slices.Sorted(maps.Keys(dev)) {
		if _, ok := settings[f]; !ok {
			return "", fmt.Errorf("openetld dev's default for %s, which isn't a setting", f)
		}
		devs = append(devs, fmt.Sprintf("`--%s` is `%s`", f, dev[f]))
	}
	fmt.Fprintf(&b, "`openetld dev` has defaults of its own: %s.\n\n", list(devs, "and"))
	b.WriteString("This page is generated from [`internal/platform/config/config.go`](../../internal/platform/config/config.go), where the settings are defined.\n\n")
	listed := map[string]bool{}
	for _, g := range groups {
		fmt.Fprintf(&b, "## %s\n\n%s\n\n", g.heading, g.about)
		b.WriteString("| Flag | Variable | Default | What it sets |\n|---|---|---|---|\n")
		for _, k := range g.settings {
			s, ok := settings[k]
			if !ok {
				return "", fmt.Errorf("group %q lists %s, which isn't a setting", g.heading, k)
			}
			if listed[k] {
				return "", fmt.Errorf("%s is in two groups", k)
			}
			listed[k] = true
			flag, def := "", ""
			if s.Flag != "" {
				flag = code("--" + s.Flag)
			}
			if s.Default != "" {
				def = code(s.Default)
			}
			fmt.Fprintf(&b, "| %s | %s | %s | %s |\n", flag, code(s.Env), cell(def), cell(text(s.Usage)))
		}
		b.WriteString("\n")
	}
	for _, k := range slices.Sorted(maps.Keys(settings)) {
		if !listed[k] {
			return "", fmt.Errorf("%s isn't in a group of %s", k, "internal/refdocs/settings.go")
		}
	}
	b.WriteString(telemetry)
	return b.String(), nil
}

func key(s config.Setting) string {
	if s.Flag != "" {
		return s.Flag
	}
	return s.Env
}
