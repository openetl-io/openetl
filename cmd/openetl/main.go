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
	"strings"
	"text/tabwriter"

	"connectrpc.com/connect"
	"go.yaml.in/yaml/v3"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/gen/openetl/api/v1/apiv1connect"
)

const usage = `usage: openetl <command> [flags]

Commands:
  apply -f FILE   create or update the connector versions, sources,
                  destinations and pipelines in FILE
  pause NAME      stop a pipeline's scheduled syncs
  resume NAME     start a pipeline's scheduled syncs again
  sync NAME       start a sync of a pipeline now
  check source|destination NAME
                  check that a source or destination is reachable
  discover NAME   list a source's streams and columns
  connector package|apply
                  package and apply connector versions

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
		return runApplyDocuments(ctx, "openetl apply", args[1:], out, false)
	case "connector":
		return runConnector(ctx, args[1:], out)
	case "pause", "resume", "sync":
		return runPipeline(ctx, args[0], args[1:], out)
	case "check":
		return runCheck(ctx, args[1:], out)
	case "discover":
		return runDiscover(ctx, args[1:], out)
	}
	fmt.Fprint(os.Stderr, usage)
	return fmt.Errorf("unknown command %q", args[0])
}

func apiURL(fs *flag.FlagSet) *string {
	return fs.String("api-url", cmp.Or(os.Getenv("OPENETL_API_URL"), "http://localhost:8080"), "API URL (env OPENETL_API_URL)")
}

// runApplyDocuments applies the documents in -f's files; with connectors,
// only connector versions.
func runApplyDocuments(ctx context.Context, cmd string, args []string, out io.Writer, connectors bool) error {
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	var paths files
	fs.Var(&paths, "f", "a YAML or JSON file, or - for stdin; repeatable")
	api := apiURL(fs)
	workspace := fs.String("workspace", "", "workspace for documents that name none (default: default)")
	dryRun := fs.Bool("dry-run", false, "validate without changing anything")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if len(paths) == 0 || fs.NArg() > 0 {
		return errors.New("apply takes one or more -f FILE, and no arguments")
	}
	req := &apiv1.ApplyRequest{ValidateOnly: *dryRun, Client: "cli"}
	if *workspace != "" {
		req.Workspace = "workspaces/" + *workspace
	}
	for _, p := range paths {
		docs, err := readDocuments(p)
		if err != nil {
			return err
		}
		for _, d := range docs {
			if connectors {
				if err := onlyConnectorVersions(d.GetContent(), d.GetOrigin()); err != nil {
					return err
				}
			}
		}
		req.Documents = append(req.Documents, docs...)
	}
	client := apiv1connect.NewApplyServiceClient(http.DefaultClient, *api)
	res, err := client.Apply(ctx, connect.NewRequest(req))
	if err != nil {
		return err
	}
	for _, r := range res.Msg.GetResources() {
		action := strings.ToLower(strings.TrimPrefix(r.GetAction().String(), "APPLY_ACTION_"))
		if *dryRun {
			action = "would be " + action
		}
		fmt.Fprintf(out, "%s %s (version %.12s)\n", action, r.GetName(), r.GetVersion())
	}
	return nil
}

func runPipeline(ctx context.Context, cmd string, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("openetl "+cmd, flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "usage: openetl %s [flags] NAME\n", cmd)
		fs.PrintDefaults()
	}
	api := apiURL(fs)
	workspace := fs.String("workspace", "default", "the pipeline's workspace")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("%s takes one pipeline name, after its flags", cmd)
	}
	name := "workspaces/" + *workspace + "/pipelines/" + fs.Arg(0)
	client := apiv1connect.NewPipelineServiceClient(http.DefaultClient, *api)
	var err error
	var done string
	switch cmd {
	case "pause":
		_, err = client.PausePipeline(ctx, connect.NewRequest(&apiv1.PausePipelineRequest{Name: name}))
		done = "paused"
	case "resume":
		_, err = client.ResumePipeline(ctx, connect.NewRequest(&apiv1.ResumePipelineRequest{Name: name}))
		done = "resumed"
	default:
		_, err = client.SyncPipeline(ctx, connect.NewRequest(&apiv1.SyncPipelineRequest{Name: name}))
		done = "started a sync of"
	}
	if err != nil {
		return err
	}
	fmt.Fprintln(out, done, name)
	return nil
}

func connectorFlags(cmd, usage string) (fs *flag.FlagSet, api, workspace, dataPlane *string) {
	fs = flag.NewFlagSet("openetl "+cmd, flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "usage: openetl %s [flags] %s\n", cmd, usage)
		fs.PrintDefaults()
	}
	api = apiURL(fs)
	workspace = fs.String("workspace", "default", "the connector's workspace")
	dataPlane = fs.String("data-plane", "", "the data plane whose runner runs it (default: the server's)")
	return fs, api, workspace, dataPlane
}

func runCheck(ctx context.Context, args []string, out io.Writer) error {
	fs, api, workspace, dataPlane := connectorFlags("check", "source|destination NAME")
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
		r, err := apiv1connect.NewSourceServiceClient(http.DefaultClient, *api).CheckSource(ctx, connect.NewRequest(&apiv1.CheckSourceRequest{Name: name, DataPlane: *dataPlane}))
		if err != nil {
			return err
		}
		res = r.Msg.GetResult()
	} else {
		r, err := apiv1connect.NewDestinationServiceClient(http.DefaultClient, *api).CheckDestination(ctx, connect.NewRequest(&apiv1.CheckDestinationRequest{Name: name, DataPlane: *dataPlane}))
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
	fs, api, workspace, dataPlane := connectorFlags("discover", "NAME")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("discover takes one source name, after its flags")
	}
	name := "workspaces/" + *workspace + "/sources/" + fs.Arg(0)
	r, err := apiv1connect.NewSourceServiceClient(http.DefaultClient, *api).DiscoverSource(ctx, connect.NewRequest(&apiv1.DiscoverSourceRequest{Name: name, DataPlane: *dataPlane}))
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
