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

	"github.com/unsuitable001/openetl/internal/daemon"
	"github.com/unsuitable001/openetl/internal/platform/config"
	"github.com/unsuitable001/openetl/internal/platform/logging"
)

const usage = `usage: openetld <role> [flags]

Roles:
  server        the API server
  orchestrator  the Temporal worker for control-plane workflows
  runner        the Temporal worker for a data plane's sync attempts
  dev           server, orchestrator and runner in one process
  gateway       webhook ingress (not built yet)
  agent         LLM-backed activities (not built yet)

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
	case "bootstrap-key":
	case "gateway", "agent":
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
	log, err := logging.New(os.Stderr, cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		return err
	}
	if role == "bootstrap-key" {
		return daemon.BootstrapKey(ctx, cfg, log)
	}
	return daemon.Run(ctx, daemon.Options{Config: cfg, Roles: roles, Logger: log})
}
