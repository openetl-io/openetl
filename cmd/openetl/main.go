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

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
	"github.com/openetl-io/openetl/internal/auth"
	"github.com/openetl-io/openetl/internal/platform/resources"
	"github.com/openetl-io/openetl/internal/platform/telemetry"
)

const usage = `usage: openetl <command> [flags]

Commands:
  apply -f FILE   create or update the connector versions, connector
                  sources, sources, destinations, dbt projects, Spark and
                  Flink clusters, pipelines and webhooks in FILE, then
                  check what changed on a runner, which only warns;
                  --no-check skips that
  plan -f FILE    show what apply would change, and how each pipeline
                  would sync
  pause NAME      stop a pipeline's scheduled syncs; --stream STREAM
                  stops only that stream, which syncs skip
  resume NAME     start a pipeline's scheduled syncs again; --stream
                  STREAM resumes a stream paused on its own
  overrides NAME  list the streams paused on their own, and what heal
                  changed in how a pipeline runs
  overrides remove NAME FIELD
                  give the spec back its say in a field heal overrides
  sync NAME       start a sync of a pipeline now
  reset [--stream STREAM]... NAME
                  read a pipeline's streams again from the start on its
                  next sync or run: every stream, or each --stream
  rewind --yes NAME
                  let a pipeline whose latest checkpoint can't be read
                  restart from the one before it, delivering the latest
                  epoch again
  rescale --size SIZE NAME
                  size a pipeline's jobs until its next version, which
                  restarts a running job at a checkpoint; auto gives the
                  spec back its say
  syncs NAME      list a pipeline's syncs, newest first
  versions NAME   list a pipeline's versions, newest first
  logs NAME       print a sync's log; -f follows a running one
  check source|destination|pipeline NAME
                  check that a source or destination is reachable; for a
                  pipeline, both of its connections, then print its plan
  discover NAME   list a source's streams and columns
  connector COMMAND
                  package, publish, validate and apply connector versions,
                  and run and test a connector on its own; openetl
                  connector lists the commands
  secret set|list|delete
                  store the secrets that configs name
  principal create|list|delete|disable|enable|grant|revoke|password-link|sign-out
                  manage users and service accounts, their roles, and users'
                  passwords and sessions
  whoami          show who the API key is, and what it may do
  key create|list|delete
                  manage principals' API keys
  dataplane list|token
                  list data planes, and manage their runner tokens
  audit list      list who changed what, and who read secrets
  webhook list|deliveries|test|disable|enable|delete
                  manage the webhooks platform events go to
  incident list|get|resolve
                  see what's wrong with pipelines, and what heal did
  holds NAME      list the columns and cells a pipeline holds back
  holds release|discard NAME STREAM COLUMN
                  patch a hold's values in, or drop them; release
                  --new-version patches them into COLUMN__v2
  version         print the CLI's version

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
	case "version":
		_, err := fmt.Fprintln(out, telemetry.Version())
		return err
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
	case "whoami":
		return runWhoami(ctx, args[1:], out)
	case "key":
		return runKey(ctx, args[1:], out)
	case "dataplane":
		return runDataPlane(ctx, args[1:], out)
	case "audit":
		return runAudit(ctx, args[1:], out)
	case "webhook":
		return runWebhook(ctx, args[1:], out)
	case "incident":
		return runIncident(ctx, args[1:], out)
	case "holds":
		return runHolds(ctx, args[1:], out)
	case "pause", "resume", "sync":
		return runPipeline(ctx, args[0], args[1:], out)
	case "reset":
		return runReset(ctx, args[1:], out)
	case "rewind":
		return runRewind(ctx, args[1:], out)
	case "rescale":
		return runRescale(ctx, args[1:], out)
	case "overrides":
		return runOverrides(ctx, args[1:], out)
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
	noCheck := new(bool)
	if mode == applyAll {
		noCheck = fs.Bool("no-check", false, "don't ask a runner to check what changed, after applying it")
	}
	var transfer files
	if mode == applyConnectors {
		fs.Var(&transfer, "transfer", "a connector package whose versions may come from another repository than its earlier ones; repeatable")
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	var refs []string
	if mode == applyConnectors {
		refs = fs.Args()
		for _, r := range refs {
			if !strings.HasPrefix(r, "oci://") && !strings.HasPrefix(r, "github.com/") {
				return fmt.Errorf("%q isn't a reference: oci://REGISTRY/REPOSITORY:VERSION or github.com/OWNER/REPO[/PACKAGE]@VERSION", r)
			}
		}
		if len(paths) == 0 && len(refs) == 0 {
			return errors.New("connector apply takes -f FILE, or references such as oci://ghcr.io/openetl-io/openetl-connectors/postgres:0.2.0")
		}
	} else if len(paths) == 0 || fs.NArg() > 0 {
		return fmt.Errorf("%s takes one or more -f FILE, and no arguments", strings.TrimPrefix(cmd, "openetl "))
	}
	req := &apiv1.ApplyRequest{ValidateOnly: *dryRun || mode == planAll, Client: "cli", Transfer: transfer}
	for _, r := range refs {
		req.Documents = append(req.Documents, &apiv1.Document{Reference: r})
	}
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
		fmt.Fprintf(out, "%s %s (%s)\n", verb(r), r.GetName(), versionText(r.GetVersion()))
	}
	for _, r := range res.Msg.GetDependents() {
		fmt.Fprintf(out, "%s %s (%s), which uses what changed\n", verb(r), r.GetName(), versionText(r.GetVersion()))
	}
	if mode == planAll {
		return printPlans(out, res.Msg)
	}
	printWarnings(out, res.Msg)
	if mode == applyAll && !*dryRun && !*noCheck {
		checkApplied(ctx, conn, res.Msg, out)
	}
	return nil
}

// A dry run gives no version to a pipeline that uses a source or destination
// the apply creates.
func versionText(v string) string {
	if v == "" {
		return "version known after apply"
	}
	return fmt.Sprintf("version %.12s", v)
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

// sizeText is a job's size, by its preset's name if it has one.
func sizeText(s *apiv1.JobSize) string {
	amounts := resources.Size{CPU: s.GetCpuMillis(), Memory: s.GetMemoryBytes()}.String()
	if s.GetPreset() != "" {
		return s.GetPreset() + " (" + amounts + ")"
	}
	return amounts
}

// reasonText says what set a job's size.
func reasonText(r apiv1.JobSizeReason) string {
	switch r {
	case apiv1.JobSizeReason_JOB_SIZE_REASON_SPEC:
		return "from spec.resources"
	case apiv1.JobSizeReason_JOB_SIZE_REASON_HINT:
		return "from its connectors' hints"
	case apiv1.JobSizeReason_JOB_SIZE_REASON_HISTORY:
		return "from its recent peaks"
	case apiv1.JobSizeReason_JOB_SIZE_REASON_OUT_OF_MEMORY:
		return "after an attempt ran out of memory"
	case apiv1.JobSizeReason_JOB_SIZE_REASON_LARGEST_RUNNER:
		return "the most a runner can take"
	case apiv1.JobSizeReason_JOB_SIZE_REASON_RESCALE:
		return "rescaled through the API"
	case apiv1.JobSizeReason_JOB_SIZE_REASON_AUTOSCALER:
		return "the lanes its job loop chose"
	case apiv1.JobSizeReason_JOB_SIZE_REASON_FINISH_WITHIN:
		return "the lanes to finish within resources.finishWithin"
	}
	return "the default"
}

// printPlans prints each pipeline's plan, and fails if one the apply would
// create or change has problems.
func printPlans(out io.Writer, res *apiv1.ApplyResponse) error {
	changed := changedPipelines(res)
	failed := false
	for _, p := range res.GetPlans() {
		printPlan(out, p)
		failed = failed || changed[p.GetPipeline()] && len(p.GetProblems()) > 0
	}
	if failed {
		return errors.New("the plan has problems, so apply would refuse it")
	}
	return nil
}

func printPlan(out io.Writer, p *apiv1.PipelinePlan) {
	fmt.Fprintf(out, "\n%s (%s)\n", p.GetPipeline(), versionText(p.GetVersion()))
	if len(p.GetChanges()) > 0 {
		fmt.Fprintln(out, "  changes:")
		for _, c := range p.GetChanges() {
			fmt.Fprintf(out, "    %s: %s -> %s\n", c.GetField(), cmp.Or(c.GetBefore(), "(unset)"), cmp.Or(c.GetAfter(), "(unset)"))
		}
	}
	if j := p.GetJob(); j != nil {
		fmt.Fprintf(out, "  job size: %s, %s, in pool %s\n", sizeText(j.GetSize()), reasonText(j.GetReason()), j.GetPool())
	}
	if len(p.GetStreams()) > 0 {
		fmt.Fprintln(out, "  streams:")
		tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
		for _, st := range p.GetStreams() {
			reason := st.GetReason()
			if st.GetTypeChanges() {
				reason += "; columns' types change in place"
			}
			if st.GetPaused() {
				reason += "; paused, so syncs skip it"
			}
			name := st.GetName()
			if t := st.GetTable(); t != "" {
				name += " -> " + t
			}
			fmt.Fprintf(tw, "    %s\t%s\t%s\t%s\n", name, strings.ToLower(strings.TrimPrefix(st.GetWrite().String(), "WRITE_MODE_")),
				strings.ToLower(strings.TrimPrefix(st.GetLevel().String(), "DELIVERY_LEVEL_")), reason)
		}
		_ = tw.Flush()
	}
	if len(p.GetOverrides()) > 0 {
		fmt.Fprintln(out, "  heal's overrides:")
		for _, o := range p.GetOverrides() {
			fmt.Fprintf(out, "    %s\n", overrideText(o))
		}
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
}

func runPipeline(ctx context.Context, cmd string, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("openetl "+cmd, flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "usage: openetl %s [flags] NAME\n", cmd)
		fs.PrintDefaults()
	}
	conn := apiFlags(fs)
	workspace := fs.String("workspace", "default", "the pipeline's workspace")
	var stream *string
	if cmd != "sync" {
		stream = fs.String("stream", "", "only this stream, which syncs skip while it's paused; not one read with cdc or by a transform")
	}
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
		_, err = c.PausePipeline(ctx, connect.NewRequest(&apiv1.PausePipelineRequest{Name: name, Stream: *stream}))
		done = "paused"
	case "resume":
		_, err = c.ResumePipeline(ctx, connect.NewRequest(&apiv1.ResumePipelineRequest{Name: name, Stream: *stream}))
		done = "resumed"
	default:
		_, err = c.SyncPipeline(ctx, connect.NewRequest(&apiv1.SyncPipelineRequest{Name: name}))
		done = "started a sync of"
	}
	if err != nil {
		return err
	}
	if stream != nil && *stream != "" {
		fmt.Fprintf(out, "%s stream %s of %s\n", done, *stream, name)
		return nil
	}
	fmt.Fprintln(out, done, name)
	return nil
}

func runRescale(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("openetl rescale", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "usage: openetl rescale --size SIZE [flags] NAME")
		fs.PrintDefaults()
	}
	conn := apiFlags(fs)
	workspace := fs.String("workspace", "default", "the pipeline's workspace")
	size := fs.String("size", "", "xs, s, m, l, xl, CPU,MEMORY such as 3,6Gi, or auto for the spec's")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("rescale takes one pipeline name, after its flags")
	}
	req := &apiv1.RescalePipelineRequest{Name: "workspaces/" + *workspace + "/pipelines/" + fs.Arg(0)}
	switch _, preset := resources.Preset(*size); {
	case *size == "":
		return errors.New("rescale needs --size")
	case *size == "auto":
	case preset:
		req.Size = &apiv1.JobSize{Preset: *size}
	default:
		s, err := resources.Parse(*size)
		if err != nil {
			return fmt.Errorf("--size: %w", err)
		}
		req.Size = &apiv1.JobSize{CpuMillis: s.CPU, MemoryBytes: s.Memory}
	}
	c, err := client(conn, apiv1connect.NewPipelineServiceClient)
	if err != nil {
		return err
	}
	if _, err := c.RescalePipeline(ctx, connect.NewRequest(req)); err != nil {
		return err
	}
	if req.Size == nil {
		fmt.Fprintln(out, "gave the spec back its say in the size of", req.GetName())
		return nil
	}
	fmt.Fprintf(out, "rescaled %s to %s until its next version\n", req.GetName(), *size)
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
	fs, conn, workspace, dataPlane := connectorFlags("check", "source|destination|pipeline NAME")
	if err := fs.Parse(args); err != nil {
		return err
	}
	kind := fs.Arg(0)
	if fs.NArg() != 2 || (kind != "source" && kind != "destination" && kind != "pipeline") {
		return errors.New("check takes source, destination or pipeline, then a name, after its flags")
	}
	if kind == "pipeline" {
		return checkPipeline(ctx, conn, *workspace, *dataPlane, fs.Arg(1), out)
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
		data, err = os.ReadFile(filepath.Clean(path))
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
