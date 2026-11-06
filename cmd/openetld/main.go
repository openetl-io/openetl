// Command openetld runs openetl's server roles.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/openetl-io/openetl/internal/daemon"
	"github.com/openetl-io/openetl/internal/platform/config"
	"github.com/openetl-io/openetl/internal/platform/logging"
	"github.com/openetl-io/openetl/internal/platform/telemetry"
)

const usage = `usage: openetld <role> [flags]

Roles:
  server        the API server
  orchestrator  the Temporal worker for control-plane workflows, and placement
  runner        runs a data plane's attempts, which it takes from the API
  dev           server, orchestrator and runner in one process
  gateway       webhook ingress (not built yet)

Commands:
  bootstrap-key  give the admin service account a new owner key, in
                 --bootstrap-key-file, for when every owner key is lost

Run openetld <role> -h for its flags.
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:])
	stop()
	if err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, "openetld:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		return errors.New("no role")
	}
	role := args[0]
	var roles daemon.Roles
	defaults := map[string]string{}
	switch role {
	case "server":
		roles.Server = true
	case "orchestrator":
		roles.Orchestrator = true
	case "runner":
		roles.Runner = true
	case "dev":
		roles = daemon.Roles{Server: true, Orchestrator: true, Runner: true}
		defaults["log-format"] = "text"
		defaults["auth"] = "none"
	case "bootstrap-key":
	case "gateway":
		return fmt.Errorf("the %s role isn't built yet", role)
	case "-h", "-help", "--help", "help":
		fmt.Fprint(os.Stderr, usage)
		return nil
	default:
		fmt.Fprint(os.Stderr, usage)
		return fmt.Errorf("unknown role %q", role)
	}
	fs := flag.NewFlagSet("openetld "+role, flag.ContinueOnError)
	cfg, err := config.Parse(fs, args[1:], defaults)
	if err != nil {
		return err
	}
	out, err := logging.Output{To: cfg.LogOutput, MaxSize: cfg.LogMaxSize, MaxFiles: cfg.LogMaxFiles}.Open()
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()
	log, err := logging.New(out, cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		return err
	}
	if role == "bootstrap-key" {
		return daemon.BootstrapKey(ctx, cfg, log)
	}
	stop, err := telemetry.Start(ctx, telemetry.Options{
		Service:    "openetld",
		Attributes: []attribute.KeyValue{attribute.String("openetl.role", role), attribute.String("openetl.data_plane", cfg.DataPlane)},
	})
	if err != nil {
		return err
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if err := stop(sctx); err != nil {
			log.Warn("flushing telemetry failed", "err", err)
		}
	}()
	return daemon.Run(ctx, daemon.Options{Config: cfg, Roles: roles, Logger: log})
}
