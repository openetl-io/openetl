// Command openetl is the CLI.
package main

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"text/tabwriter"

	"connectrpc.com/connect"
	"go.yaml.in/yaml/v3"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/gen/openetl/api/v1/apiv1connect"
	"github.com/unsuitable001/openetl/internal/auth"
)

const usage = `usage: openetl <command> [flags]

Commands:
  apply -f FILE   create or update the connector versions, sources,
                  destinations, dbt projects and pipelines in FILE
  plan -f FILE    show what apply would change, and how each pipeline
                  would sync
  pause NAME      stop a pipeline's scheduled syncs
  resume NAME     start a pipeline's scheduled syncs again
  sync NAME       start a sync of a pipeline now
  syncs NAME      list a pipeline's syncs, newest first
  versions NAME   list a pipeline's versions, newest first
  logs NAME       print a sync's log; -f follows a running one
  check source|destination NAME
                  check that a source or destination is reachable
  discover NAME   list a source's streams and columns
  connector package|apply
                  package and apply connector versions
  secret set|list|delete
                  store the secrets that configs name
  principal create|list|delete|disable|enable|grant|revoke
                  manage users and service accounts, and their roles
  key create|list|delete
                  manage principals' API keys
  dataplane list|token
                  list data planes, and manage their runner tokens
  audit list      list who changed what, and who read secrets

Commands send the API key in OPENETL_API_KEY, or in --api-key-file
(.openetl/admin.key by default, where openetld writes the owner's key).

Run openetl <command> -h for its flags.
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	err := run(ctx, os.Args[1:], os.Stdout)
	stop()
	if err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, "openetl:", err)
		os.Exit(1)
	}
}

type files []string

func (f *files) String() string     { return strings.Join(*f, ",") }
func (f *files) Set(v string) error { *f = append(*f, v); return nil }

func run(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		return errors.New("no command")
	}
	switch args[0] {
	case "apply":
		return runApplyDocuments(ctx, "openetl apply", args[1:], out, applyAll)
	case "plan":
		return runApplyDocuments(ctx, "openetl plan", args[1:], out, planAll)
	case "connector":
		return runConnector(ctx, args[1:], out)
	case "secret":
		return runSecret(ctx, args[1:], out)
	case "principal":
		return runPrincipal(ctx, args[1:], out)
	case "key":
		return runKey(ctx, args[1:], out)
	case "dataplane":
		return runDataPlane(ctx, args[1:], out)
	case "audit":
		return runAudit(ctx, args[1:], out)
	case "pause", "resume", "sync":
		return runPipeline(ctx, args[0], args[1:], out)
	case "syncs":
		return runSyncs(ctx, args[1:], out)
	case "versions":
		return runVersions(ctx, args[1:], out)
	case "logs":
		return runLogs(ctx, args[1:], out)
	case "check":
		return runCheck(ctx, args[1:], out)
	case "discover":
		return runDiscover(ctx, args[1:], out)
	}
	fmt.Fprint(os.Stderr, usage)
	return fmt.Errorf("unknown command %q", args[0])
}

// api is the flags that reach the API.
type api struct{ url, keyFile *string }

func apiFlags(fs *flag.FlagSet) *api {
	return &api{
		url:     fs.String("api-url", cmp.Or(os.Getenv("OPENETL_API_URL"), "http://localhost:8080"), "API URL (env OPENETL_API_URL)"),
		keyFile: fs.String("api-key-file", cmp.Or(os.Getenv("OPENETL_API_KEY_FILE"), ".openetl/admin.key"), "a file holding the API key, unless OPENETL_API_KEY holds it (env OPENETL_API_KEY_FILE)"),
	}
}

// client makes a service's client, which sends the API key.
func client[T any](a *api, newClient func(connect.HTTPClient, string, ...connect.ClientOption) T) (T, error) {
	key := os.Getenv("OPENETL_API_KEY")
	if key == "" {
		var err error
		if key, err = auth.ReadKeyFile(*a.keyFile); err != nil {
			var zero T
			return zero, fmt.Errorf("no API key: set OPENETL_API_KEY, or --api-key-file: %w", err)
		}
	}
	return newClient(http.DefaultClient, *a.url, auth.WithBearer(key)), nil
}

type applyMode int

const (
	applyAll applyMode = iota
	applyConnectors
	planAll
)

// runApplyDocuments applies or plans the documents in -f's files.
func runApplyDocuments(ctx context.Context, cmd string, args []string, out io.Writer, mode applyMode) error {
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	var paths files
	fs.Var(&paths, "f", "a YAML or JSON file, or - for stdin; repeatable")
	conn := apiFlags(fs)
	workspace := fs.String("workspace", "", "workspace for documents that name none (default: default)")
	dryRun := new(bool)
	if mode != planAll {
		dryRun = fs.Bool("dry-run", false, "validate without changing anything")
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if len(paths) == 0 || fs.NArg() > 0 {
		return fmt.Errorf("%s takes one or more -f FILE, and no arguments", strings.TrimPrefix(cmd, "openetl "))
	}
	req := &apiv1.ApplyRequest{ValidateOnly: *dryRun || mode == planAll, Client: "cli"}
	if *workspace != "" {
		req.Workspace = "workspaces/" + *workspace
	}
	for _, p := range paths {
		docs, err := readDocuments(p)
		if err != nil {
			return err
		}
		base := filepath.Dir(p)
		if p == "-" {
			base = "."
		}
		for _, d := range docs {
			if mode == applyConnectors {
				if err := onlyConnectorVersions(d.GetContent(), d.GetOrigin()); err != nil {
					return err
				}
			}
			if err := bundleDbtProject(d, base); err != nil {
				return err
			}
		}
		req.Documents = append(req.Documents, docs...)
	}
	c, err := client(conn, apiv1connect.NewApplyServiceClient)
	if err != nil {
		return err
	}
	res, err := c.Apply(ctx, connect.NewRequest(req))
	if err != nil {
		return err
	}
	verb := func(r *apiv1.AppliedResource) string {
		action := strings.ToLower(strings.TrimPrefix(r.GetAction().String(), "APPLY_ACTION_"))
		if req.GetValidateOnly() {
			action = "would be " + action
		}
		return action
	}
	for _, r := range res.Msg.GetResources() {
		fmt.Fprintf(out, "%s %s (version %.12s)\n", verb(r), r.GetName(), r.GetVersion())
	}
	for _, r := range res.Msg.GetDependents() {
		fmt.Fprintf(out, "%s %s (version %.12s), which uses what changed\n", verb(r), r.GetName(), r.GetVersion())
	}
	if mode == planAll {
		return printPlans(out, res.Msg)
	}
	printWarnings(out, res.Msg)
	return nil
}

// printWarnings prints the plan warnings of each pipeline the apply created
// or changed.
func printWarnings(out io.Writer, res *apiv1.ApplyResponse) {
	changed := changedPipelines(res)
	for _, p := range res.GetPlans() {
		if changed[p.GetPipeline()] && len(p.GetWarnings()) > 0 {
			fmt.Fprintf(out, "\nwarnings for %s:\n", p.GetPipeline())
			for _, w := range p.GetWarnings() {
				fmt.Fprintf(out, "  %s\n", warningText(w))
			}
		}
	}
}

func warningText(w *apiv1.PlanWarning) string {
	if w.GetStream() == "" {
		return w.GetMessage()
	}
	return w.GetStream() + ": " + w.GetMessage()
}

func changedPipelines(res *apiv1.ApplyResponse) map[string]bool {
	changed := map[string]bool{}
	for _, r := range slices.Concat(res.GetResources(), res.GetDependents()) {
		changed[r.GetName()] = r.GetAction() != apiv1.ApplyAction_APPLY_ACTION_UNCHANGED
	}
	return changed
}

// printPlans prints each pipeline's plan, and fails if one the apply would
// create or change has problems.
func printPlans(out io.Writer, res *apiv1.ApplyResponse) error {
	changed := changedPipelines(res)
	failed := false
	for _, p := range res.GetPlans() {
		fmt.Fprintf(out, "\n%s (version %.12s)\n", p.GetPipeline(), p.GetVersion())
		if len(p.GetChanges()) > 0 {
			fmt.Fprintln(out, "  changes:")
			for _, c := range p.GetChanges() {
				fmt.Fprintf(out, "    %s: %s -> %s\n", c.GetField(), cmp.Or(c.GetBefore(), "(unset)"), cmp.Or(c.GetAfter(), "(unset)"))
			}
		}
		if len(p.GetStreams()) > 0 {
			fmt.Fprintln(out, "  streams:")
			tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
			for _, st := range p.GetStreams() {
				fmt.Fprintf(tw, "    %s\t%s\t%s\n", st.GetName(), strings.ToLower(strings.TrimPrefix(st.GetLevel().String(), "DELIVERY_LEVEL_")), st.GetReason())
			}
			_ = tw.Flush()
		}
		var warnings []string
		for _, w := range p.GetWarnings() {
			warnings = append(warnings, warningText(w))
		}
		for _, list := range []struct {
			title string
			items []string
		}{{"problems", p.GetProblems()}, {"warnings", warnings}, {"notes", p.GetNotes()}} {
			if len(list.items) > 0 {
				fmt.Fprintf(out, "  %s:\n", list.title)
				for _, item := range list.items {
					fmt.Fprintf(out, "    %s\n", item)
				}
			}
		}
		failed = failed || changed[p.GetPipeline()] && len(p.GetProblems()) > 0
	}
	if failed {
		return errors.New("the plan has problems, so apply would refuse it")
	}
	return nil
}

func runPipeline(ctx context.Context, cmd string, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("openetl "+cmd, flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "usage: openetl %s [flags] NAME\n", cmd)
		fs.PrintDefaults()
	}
	conn := apiFlags(fs)
	workspace := fs.String("workspace", "default", "the pipeline's workspace")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("%s takes one pipeline name, after its flags", cmd)
	}
	name := "workspaces/" + *workspace + "/pipelines/" + fs.Arg(0)
	c, err := client(conn, apiv1connect.NewPipelineServiceClient)
	if err != nil {
		return err
	}
	var done string
	switch cmd {
	case "pause":
		_, err = c.PausePipeline(ctx, connect.NewRequest(&apiv1.PausePipelineRequest{Name: name}))
		done = "paused"
	case "resume":
		_, err = c.ResumePipeline(ctx, connect.NewRequest(&apiv1.ResumePipelineRequest{Name: name}))
		done = "resumed"
	default:
		_, err = c.SyncPipeline(ctx, connect.NewRequest(&apiv1.SyncPipelineRequest{Name: name}))
		done = "started a sync of"
	}
	if err != nil {
		return err
	}
	fmt.Fprintln(out, done, name)
	return nil
}

func connectorFlags(cmd, usage string) (fs *flag.FlagSet, conn *api, workspace, dataPlane *string) {
	fs = flag.NewFlagSet("openetl "+cmd, flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "usage: openetl %s [flags] %s\n", cmd, usage)
		fs.PrintDefaults()
	}
	conn = apiFlags(fs)
	workspace = fs.String("workspace", "default", "the connector's workspace")
	dataPlane = fs.String("data-plane", "", "the data plane whose runner runs it (default: the server's)")
	return fs, conn, workspace, dataPlane
}

func runCheck(ctx context.Context, args []string, out io.Writer) error {
	fs, conn, workspace, dataPlane := connectorFlags("check", "source|destination NAME")
	if err := fs.Parse(args); err != nil {
		return err
	}
	kind := fs.Arg(0)
	if fs.NArg() != 2 || (kind != "source" && kind != "destination") {
		return errors.New("check takes source or destination, then a name, after its flags")
	}
	name := "workspaces/" + *workspace + "/" + kind + "s/" + fs.Arg(1)
	var res *apiv1.CheckResult
	if kind == "source" {
		c, err := client(conn, apiv1connect.NewSourceServiceClient)
		if err != nil {
			return err
		}
		r, err := c.CheckSource(ctx, connect.NewRequest(&apiv1.CheckSourceRequest{Name: name, DataPlane: *dataPlane}))
		if err != nil {
			return err
		}
		res = r.Msg.GetResult()
	} else {
		c, err := client(conn, apiv1connect.NewDestinationServiceClient)
		if err != nil {
			return err
		}
		r, err := c.CheckDestination(ctx, connect.NewRequest(&apiv1.CheckDestinationRequest{Name: name, DataPlane: *dataPlane}))
		if err != nil {
			return err
		}
		res = r.Msg.GetResult()
	}
	if !res.GetOk() {
		return fmt.Errorf("%s failed its check: %s: %s", name, res.GetCode(), res.GetMessage())
	}
	fmt.Fprintf(out, "%s passed its check (version %.12s)\n", name, res.GetVersion())
	return nil
}

func runDiscover(ctx context.Context, args []string, out io.Writer) error {
	fs, conn, workspace, dataPlane := connectorFlags("discover", "NAME")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("discover takes one source name, after its flags")
	}
	name := "workspaces/" + *workspace + "/sources/" + fs.Arg(0)
	c, err := client(conn, apiv1connect.NewSourceServiceClient)
	if err != nil {
		return err
	}
	r, err := c.DiscoverSource(ctx, connect.NewRequest(&apiv1.DiscoverSourceRequest{Name: name, DataPlane: *dataPlane}))
	if err != nil {
		return err
	}
	for _, s := range r.Msg.GetCatalog().GetStreams() {
		notes := []string{strings.ToLower(strings.TrimPrefix(s.GetReplay().String(), "REPLAY_"))}
		if pk := s.GetPrimaryKey(); len(pk) > 0 {
			notes = append(notes, "key "+strings.Join(pk, ", "))
		}
		if c := s.GetCursorField(); c != "" {
			notes = append(notes, "cursor "+c)
		}
		fmt.Fprintf(out, "%s (%s)\n", s.GetName(), strings.Join(notes, "; "))
		if len(s.GetColumns()) == 0 {
			fmt.Fprintln(out, "  columns are inferred from the records")
			continue
		}
		tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
		for _, c := range s.GetColumns() {
			null := ""
			if c.GetNullable() {
				null = "nullable"
			}
			fmt.Fprintf(tw, "  %s\t%s\t%s\n", c.GetName(), c.GetType(), null)
		}
		if err := tw.Flush(); err != nil {
			return err
		}
	}
	return nil
}

// readDocuments splits a file on ---, skipping empty documents.
func readDocuments(path string) ([]*apiv1.Document, error) {
	var data []byte
	var err error
	if path == "-" {
		data, err = io.ReadAll(os.Stdin)
	} else {
		data, err = os.ReadFile(path) //nolint:gosec // the user names the file
	}
	if err != nil {
		return nil, err
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	var docs []*apiv1.Document
	for i := 1; ; i++ {
		var n yaml.Node
		err := dec.Decode(&n)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("%s#%d: %w", path, i, err)
		}
		if len(n.Content) == 0 || (n.Content[0].Kind == yaml.ScalarNode && n.Content[0].Tag == "!!null") {
			continue
		}
		b, err := yaml.Marshal(&n)
		if err != nil {
			return nil, err
		}
		docs = append(docs, &apiv1.Document{Origin: fmt.Sprintf("%s#%d", path, i), Content: b})
	}
	if len(docs) == 0 {
		return nil, fmt.Errorf("%s has no documents", path)
	}
	return docs, nil
}
