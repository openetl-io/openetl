package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"
	"go.yaml.in/yaml/v3"
	"google.golang.org/grpc"

	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/connectorhost/external"
	"github.com/openetl-io/openetl/internal/connectorhost/hostservices"
	"github.com/openetl-io/openetl/internal/connectorpkg"
	"github.com/openetl-io/openetl/internal/github"
	"github.com/openetl-io/openetl/internal/netguard"
	"github.com/openetl-io/openetl/internal/oci"
	"github.com/openetl-io/openetl/internal/pkgref"
	"github.com/openetl-io/openetl/internal/spec"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/conformance"
	"github.com/openetl-io/openetl/sdk/go/host"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

const runUsage = `
TARGET is a connector's executable, its descriptor (NAME.connector.yaml), a
manifest (a DeclarativeSource document), or a package version by reference:
oci://REGISTRY/REPOSITORY:VERSION or @sha256:..., with Docker's credentials,
or github.com/OWNER/REPO[/PACKAGE]@VERSION, with GITHUB_TOKEN or GH_TOKEN when
set. A descriptor's binary is fetched by its digest, or taken from beside a
descriptor file, where openetl connector package wrote it. Or give --image REF
to run an image with Docker or Podman, or --attach ADDR to wait for a
connector you start yourself, under a debugger, say. Nothing else is needed:
no openetld, Temporal, Postgres or bucket. Host services are the platform's,
HTTP included.

A config names its secrets as {"secret": NAME}, whose values come from
--secret NAME=VALUE, or else from OPENETL_SECRET_NAME (in capitals, with _ for
anything but letters and digits). A secret's value can also be written in
place.
`

// Where the run commands report, and connectors log.
var stderr io.Writer = os.Stderr

// runFlags are what spec, check, discover and read share.
type runFlags struct {
	fs                    *flag.FlagSet
	image, command        *string
	attach, role          *string
	config, egress, cache *string
	useImage, quiet       *bool
	secrets               files
	log                   *slog.Logger
	logs                  io.Writer
}

func newRunFlags(cmd, args string) *runFlags {
	f := &runFlags{fs: flag.NewFlagSet("openetl connector "+cmd, flag.ContinueOnError)}
	f.fs.Usage = func() {
		fmt.Fprintf(f.fs.Output(), "usage: openetl connector %s [flags] %s\n", cmd, args)
		fmt.Fprint(f.fs.Output(), runUsage+"\n")
		f.fs.PrintDefaults()
	}
	f.image = f.fs.String("image", "", "run this image with Docker or Podman, in place of TARGET")
	f.command = f.fs.String("command", "", "the image's argv, where it isn't its entrypoint, split on spaces")
	f.attach = f.fs.String("attach", "", "serve host services and wait for a connector you start at this address, tcp://host:port or unix:///path, in place of TARGET")
	f.role = f.fs.String("role", "", "source or destination, for a TARGET with both")
	f.useImage = f.fs.Bool("use-image", false, "run a descriptor's image even where it has a binary for this platform")
	f.config = f.fs.String("config", "", "the config: a JSON or YAML file, or - for stdin (default {})")
	f.fs.Var(&f.secrets, "secret", "NAME=VALUE: a secret the config names; repeatable")
	f.egress = f.fs.String("egress-allow", "", "comma-separated CIDR prefixes host services may reach besides public addresses, such as 127.0.0.1/32")
	cache, _ := os.UserCacheDir()
	f.cache = f.fs.String("cache", filepath.Join(cache, "openetl"), "where a descriptor's binaries are downloaded to")
	f.quiet = f.fs.Bool("q", false, "don't show what the connector logs")
	return f
}

func (f *runFlags) parse(args []string) error {
	if err := f.fs.Parse(args); err != nil {
		return err
	}
	f.logs = stderr
	if *f.quiet {
		f.logs = io.Discard
	}
	f.log = slog.New(slog.NewTextHandler(f.logs, nil))
	conformance.Host = external.Host()
	return nil
}

// runTarget is a connector to start, and what it must say it is.
type runTarget struct {
	name   string
	launch conformance.Launcher
	// A descriptor's spec, which the connector's must be; or a manifest's,
	// which has no process to ask.
	want     *protocolv1.SpecResponse
	manifest bool
	hosts    []*hostservices.Services
}

func (t *runTarget) close() {
	for _, h := range t.hosts {
		h.Close()
	}
}

// target is what the flags and TARGET name. role is a command's own role,
// when the flags name none.
func (f *runFlags) target(ctx context.Context, role string) (*runTarget, error) {
	allow, err := netguard.ParsePrefixes(*f.egress)
	if err != nil {
		return nil, fmt.Errorf("--egress-allow: %w", err)
	}
	policy := netguard.Policy{Allow: allow}
	role = cmp.Or(*f.role, role)
	t := &runTarget{}
	opts := conformance.Options{Role: role, Logs: f.logs, HostServices: func(s *protocolv1.SpecResponse, secrets map[string]string) (protocolv1.HostServiceServer, error) {
		st := hostservices.StaticSecrets{}
		for k, v := range secrets {
			st[k] = sdk.NewSecret(v)
		}
		tier := s.GetEgress().GetTier()
		hs := hostservices.New(hostservices.Options{Policy: policy, Secrets: st, Logger: f.log,
			RevealSecrets: tier == protocolv1.EgressTier_EGRESS_TIER_PROXY || tier == protocolv1.EgressTier_EGRESS_TIER_RAW})
		t.hosts = append(t.hosts, hs)
		return hostservices.NewServer(hs), nil
	}}
	targets := f.fs.NArg()
	if *f.image != "" {
		targets++
	}
	if *f.attach != "" {
		targets++
	}
	if targets != 1 {
		f.fs.Usage()
		return nil, errors.New("give one TARGET, --image or --attach, after the flags")
	}
	switch {
	case *f.attach != "":
		t.name = "the connector at " + *f.attach
		t.launch = conformance.Attach(*f.attach, opts, func(env []string) {
			fmt.Fprintf(stderr, "Start the connector with:\n  %s\nWaiting for it at %s...\n", strings.Join(env, " "), *f.attach)
		})
	case *f.image != "":
		engine, err := conformance.ContainerEngine()
		if err != nil {
			return nil, err
		}
		t.name = *f.image
		t.launch = conformance.Container(engine, *f.image, strings.Fields(*f.command), conformance.Resources{}, opts)
	case pkgref.Is(f.fs.Arg(0)):
		res, err := resolver().Resolve(ctx, f.fs.Arg(0))
		if err != nil {
			return nil, err
		}
		return t, f.document(ctx, t, res.Content, f.fs.Arg(0), "", role, policy, opts)
	case document(f.fs.Arg(0)):
		path := f.fs.Arg(0)
		content, err := os.ReadFile(filepath.Clean(path))
		if err != nil {
			return nil, err
		}
		return t, f.document(ctx, t, content, path, filepath.Dir(path), role, policy, opts)
	default:
		t.name = filepath.Base(f.fs.Arg(0))
		t.launch = conformance.ExecWith(f.fs.Arg(0), opts)
	}
	return t, nil
}

func document(path string) bool {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".yaml", ".yml", ".json":
		return true
	}
	return false
}

// document starts a descriptor's role from its binary or its image, or a
// manifest in this process. A descriptor file in dir takes the binary
// beside it that openetl connector package wrote, when its digest matches,
// so a package runs before it's released.
func (f *runFlags) document(ctx context.Context, t *runTarget, content []byte, origin, dir, role string, policy netguard.Policy, opts conformance.Options) error {
	v, err := connectorhost.ParseVersion(content)
	if err != nil {
		return fmt.Errorf("%s: %w", origin, err)
	}
	if role == "" {
		if len(v.Roles) != 1 {
			return fmt.Errorf("%s has a source and a destination; give --role", v.Name)
		}
		for r := range v.Roles {
			role = r
		}
		opts.Role = role
	}
	r := v.Roles[role]
	if r == nil {
		return fmt.Errorf("%s has no %s role", v.Name, role)
	}
	t.name = fmt.Sprintf("%s %s's %s", v.Name, v.Version, role)
	bin, hasBin := r.Binaries[connectorhost.Platform()]
	switch {
	case r.Manifest != nil:
		var m struct {
			Config json.RawMessage `json:"config"`
		}
		_ = json.Unmarshal(r.Role.Manifest, &m)
		t.manifest = true
		t.want = &protocolv1.SpecResponse{Kind: protocolv1.ConnectorKind_CONNECTOR_KIND_SOURCE, ConfigSchema: m.Config,
			Egress: &protocolv1.Egress{Tier: protocolv1.EgressTier_EGRESS_TIER_HOST}, Source: &protocolv1.SourceCapabilities{}}
		env := connectorhost.Env{Egress: policy, Logger: f.log}.With(v, role)
		t.launch = conformance.InProcess(sdk.Connector{Name: v.Name, Egress: sdk.Egress{Tier: sdk.EgressHost},
			Source: func(config []byte, _ host.Host) (source.Source, error) {
				return connectorhost.NewSource(v.Name, config, env)
			}})
	case hasBin && !*f.useImage:
		if bin.Digest == "" {
			return fmt.Errorf("%s's binary for %s has no digest; package pins it", t.name, connectorhost.Platform())
		}
		_, short := spec.SplitName(v.Name)
		exe, err := beside(dir, connectorpkg.Asset(short, role, connectorhost.Platform()), bin.Digest)
		if err != nil {
			return err
		}
		if exe == "" {
			fetch := connectorhost.Fetcher{Client: &http.Client{Timeout: 10 * time.Minute}, Cache: *f.cache, Credential: fetchCredential}
			if exe, err = fetch.Executable(ctx, bin, role+"-"+short); err != nil {
				return err
			}
		}
		t.want = r.Spec
		t.launch = conformance.ExecWith(exe, opts)
	case r.Image != nil:
		engine, err := conformance.ContainerEngine()
		if err != nil {
			return err
		}
		t.want = r.Spec
		res := conformance.Resources{CPU: r.Resources.CPU, Memory: r.Resources.Memory, Disk: r.Resources.Disk}
		t.launch = conformance.Container(engine, r.Image.Ref+"@"+r.Image.Digest, r.Image.Command, res, opts)
	default:
		return fmt.Errorf("%s has no binary for %s, and no image", t.name, connectorhost.Platform())
	}
	return nil
}

// beside is the file name in dir when it has digest; "" when dir is empty
// or has no such file.
func beside(dir, name, digest string) (string, error) {
	if dir == "" {
		return "", nil
	}
	path := filepath.Join(dir, name)
	b, err := os.ReadFile(filepath.Clean(path))
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if d := connectorhost.Digest(b); d != digest {
		return "", fmt.Errorf("%s, beside the descriptor, has digest %s, and the descriptor says %s", path, d, digest)
	}
	return path, nil
}

// resolver resolves references as the user: registries with Docker's
// credentials, and GitHub with GITHUB_TOKEN or GH_TOKEN, at GITHUB_API_URL
// when set.
func resolver() pkgref.Resolver {
	reg, err := registryClient()
	if err != nil {
		reg = &oci.Client{HTTP: &http.Client{Timeout: 10 * time.Minute}}
	}
	return pkgref.Resolver{OCI: reg, GitHub: func(context.Context, string) (*github.Client, error) {
		return &github.Client{HTTP: &http.Client{Timeout: time.Minute}, API: os.Getenv("GITHUB_API_URL"), Token: githubToken()}, nil
	}}
}

func githubToken() string { return cmp.Or(os.Getenv("GITHUB_TOKEN"), os.Getenv("GH_TOKEN")) }

// fetchCredential is the user's credential for a binary's URL: the GitHub
// token for a release asset, and Docker's for a registry.
func fetchCredential(ctx context.Context, url string) (string, string, error) {
	if _, _, _, _, ok := github.ParseAssetURL(url); ok {
		return "", githubToken(), nil
	}
	return dockerCredential(ctx, url)
}

// start starts the connector with config, and reads its spec.
func (t *runTarget) start(ctx context.Context, config []byte) (conformance.Process, *protocolv1.SpecResponse, error) {
	p, err := t.launch(ctx, config)
	if err != nil {
		return nil, nil, fmt.Errorf("start %s: %w", t.name, err)
	}
	if t.manifest {
		return p, t.want, nil
	}
	s, err := protocolv1.NewConnectorServiceClient(p.Conn()).Spec(ctx, &protocolv1.SpecRequest{Host: conformance.Host})
	if err != nil {
		_ = p.Close()
		return nil, nil, fmt.Errorf("%s's spec: %w", t.name, err)
	}
	if t.want != nil {
		if diff := spec.SameSpec(s, t.want); diff != "" {
			_ = p.Close()
			return nil, nil, fmt.Errorf("%s's spec isn't its descriptor's: %s", t.name, diff)
		}
	}
	return p, s, nil
}

// readConfig reads the config file as JSON, with the secrets it names put
// in.
func (f *runFlags) readConfig() ([]byte, error) {
	return f.readConfigFile("config", *f.config)
}

// readConfigFile reads the config file a flag names, as readConfig does.
func (f *runFlags) readConfigFile(flag, path string) ([]byte, error) {
	var b []byte
	var err error
	switch path {
	case "":
		return []byte("{}"), nil
	case "-":
		b, err = io.ReadAll(os.Stdin)
	default:
		b, err = os.ReadFile(filepath.Clean(path))
	}
	if err != nil {
		return nil, err
	}
	var v any
	if err := yaml.Unmarshal(b, &v); err != nil {
		return nil, fmt.Errorf("--%s: %w", flag, err)
	}
	if _, ok := v.(map[string]any); !ok {
		return nil, fmt.Errorf("--%s: the config isn't an object", flag)
	}
	given := map[string]string{}
	for _, s := range f.secrets {
		k, val, ok := strings.Cut(s, "=")
		if !ok {
			return nil, fmt.Errorf("--secret %s: want NAME=VALUE", s)
		}
		given[k] = val
	}
	if v, err = putSecrets(v, given); err != nil {
		return nil, err
	}
	return json.Marshal(v)
}

var notEnvName = regexp.MustCompile(`[^A-Z0-9]`)

// putSecrets replaces each {"secret": NAME} in v with the secret's value.
func putSecrets(v any, given map[string]string) (any, error) {
	switch x := v.(type) {
	case map[string]any:
		if name, ok := x["secret"].(string); ok && len(x) == 1 {
			if val, ok := given[name]; ok {
				return val, nil
			}
			env := "OPENETL_SECRET_" + notEnvName.ReplaceAllString(strings.ToUpper(name), "_")
			if val, ok := os.LookupEnv(env); ok {
				return val, nil
			}
			return nil, fmt.Errorf("the config names the secret %s: give --secret %s=VALUE or set %s", name, name, env)
		}
		for k, e := range x {
			var err error
			if x[k], err = putSecrets(e, given); err != nil {
				return nil, err
			}
		}
	case []any:
		for i, e := range x {
			var err error
			if x[i], err = putSecrets(e, given); err != nil {
				return nil, err
			}
		}
	}
	return v, nil
}

func runConnectorSpec(ctx context.Context, args []string, out io.Writer) error {
	f := newRunFlags("spec", "TARGET")
	format := f.fs.String("format", "yaml", "yaml or json")
	if err := f.parse(args); err != nil {
		return err
	}
	t, err := f.target(ctx, "")
	if err != nil {
		return err
	}
	defer t.close()
	s := t.want
	if !t.manifest {
		p, got, err := t.start(ctx, nil)
		if err != nil {
			return err
		}
		_ = p.Close()
		s = got
	}
	view, err := spec.RoleSpecOf(s)
	if err != nil {
		return err
	}
	return printAs(out, *format, view)
}

func printAs(out io.Writer, format string, v any) error {
	switch format {
	case "yaml":
		b, err := yaml.Marshal(v)
		if err != nil {
			return err
		}
		_, err = out.Write(b)
		return err
	case "json":
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(v)
	}
	return fmt.Errorf("--format %s: want yaml or json", format)
}

func runConnectorCheck(ctx context.Context, args []string, out io.Writer) error {
	f := newRunFlags("check", "TARGET")
	if err := f.parse(args); err != nil {
		return err
	}
	config, err := f.readConfig()
	if err != nil {
		return err
	}
	t, err := f.target(ctx, "")
	if err != nil {
		return err
	}
	defer t.close()
	p, _, err := t.start(ctx, config)
	if err != nil {
		return err
	}
	defer func() { _ = p.Close() }()
	res, err := protocolv1.NewConnectorServiceClient(p.Conn()).Check(ctx, &protocolv1.CheckRequest{Config: p.Config()})
	if err != nil {
		return fmt.Errorf("%s's check: %w", t.name, err)
	}
	if e := res.GetError(); e != nil {
		return fmt.Errorf("%s failed its check: %s: %s", t.name, strings.TrimPrefix(e.GetCode().String(), "ERROR_CODE_"), e.GetMessage())
	}
	fmt.Fprintf(out, "%s passed its check\n", t.name)
	return nil
}

// source starts a source and reads its catalog.
func (f *runFlags) source(ctx context.Context) (*runTarget, conformance.Process, *protocolv1.DiscoverResponse, error) {
	config, err := f.readConfig()
	if err != nil {
		return nil, nil, nil, err
	}
	t, err := f.target(ctx, spec.RoleSource)
	if err != nil {
		return nil, nil, nil, err
	}
	p, s, err := t.start(ctx, config)
	if err != nil {
		t.close()
		return nil, nil, nil, err
	}
	fail := func(err error) (*runTarget, conformance.Process, *protocolv1.DiscoverResponse, error) {
		_ = p.Close()
		t.close()
		return nil, nil, nil, err
	}
	if s.GetKind() != protocolv1.ConnectorKind_CONNECTOR_KIND_SOURCE {
		return fail(fmt.Errorf("%s is a %s; discover and read take a source", t.name, spec.RoleOf(s)))
	}
	cat, err := protocolv1.NewSourceServiceClient(p.Conn()).Discover(ctx, &protocolv1.DiscoverRequest{Config: p.Config()})
	if err != nil {
		return fail(fmt.Errorf("%s's discover: %w", t.name, err))
	}
	return t, p, cat, nil
}

type columnView struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable,omitempty"`
}

type streamView struct {
	Name          string          `json:"name"`
	SyncModes     []string        `json:"syncModes,omitempty"`
	Replay        string          `json:"replay"`
	PrimaryKey    []string        `json:"primaryKey,omitempty"`
	DefaultCursor string          `json:"defaultCursor,omitempty"`
	Columns       []columnView    `json:"columns,omitempty"`
	JSONSchema    json.RawMessage `json:"jsonSchema,omitempty"`
}

func viewStream(s *protocolv1.CatalogStream) (streamView, error) {
	v := streamView{Name: s.GetName(), PrimaryKey: s.GetPrimaryKey(), DefaultCursor: s.GetDefaultCursor(),
		Replay: strings.ToLower(strings.TrimPrefix(s.GetReplay().String(), "REPLAY_"))}
	if v.Replay == "unspecified" {
		v.Replay = "superset"
	}
	for _, m := range s.GetSyncModes() {
		v.SyncModes = append(v.SyncModes, strings.ToLower(strings.TrimPrefix(m.String(), "SYNC_MODE_")))
	}
	if len(s.GetJsonSchema()) > 0 {
		v.JSONSchema = s.GetJsonSchema()
	}
	if len(s.GetArrowSchema()) > 0 {
		sch, err := arrowipc.DecodeSchema(s.GetArrowSchema())
		if err != nil {
			return v, fmt.Errorf("stream %s's schema: %w", s.GetName(), err)
		}
		for _, fl := range sch.Fields() {
			v.Columns = append(v.Columns, columnView{Name: fl.Name, Type: fl.Type.String(), Nullable: fl.Nullable})
		}
	}
	return v, nil
}

func runConnectorDiscover(ctx context.Context, args []string, out io.Writer) error {
	f := newRunFlags("discover", "TARGET")
	format := f.fs.String("format", "table", "table or json")
	if err := f.parse(args); err != nil {
		return err
	}
	if *format != "table" && *format != "json" {
		return fmt.Errorf("--format %s: want table or json", *format)
	}
	t, p, cat, err := f.source(ctx)
	if err != nil {
		return err
	}
	defer t.close()
	defer func() { _ = p.Close() }()
	var views []streamView
	for _, s := range cat.GetCatalog().GetStreams() {
		v, err := viewStream(s)
		if err != nil {
			return err
		}
		views = append(views, v)
	}
	if *format == "json" {
		return printAs(out, "json", map[string]any{"streams": views})
	}
	for _, v := range views {
		notes := []string{v.Replay}
		if len(v.SyncModes) > 0 {
			notes = append(notes, strings.Join(v.SyncModes, ", "))
		}
		if len(v.PrimaryKey) > 0 {
			notes = append(notes, "key "+strings.Join(v.PrimaryKey, ", "))
		}
		if v.DefaultCursor != "" {
			notes = append(notes, "cursor "+v.DefaultCursor)
		}
		fmt.Fprintf(out, "%s (%s)\n", v.Name, strings.Join(notes, "; "))
		if len(v.Columns) == 0 {
			fmt.Fprintln(out, "  columns are inferred from the records")
			continue
		}
		tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
		for _, c := range v.Columns {
			null := ""
			if c.Nullable {
				null = "nullable"
			}
			fmt.Fprintf(tw, "  %s\t%s\t%s\n", c.Name, c.Type, null)
		}
		if err := tw.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func runConnectorRead(ctx context.Context, args []string, out io.Writer) error {
	f := newRunFlags("read", "TARGET")
	var only files
	f.fs.Var(&only, "stream", "a stream to read; repeatable (default: every stream)")
	mode := f.fs.String("mode", "", "full_refresh, incremental or cdc (default: each stream's first)")
	cursor := f.fs.String("cursor", "", "an incremental read's cursor field (default: each stream's)")
	limit := f.fs.Int("limit", 0, "stop each stream after this many records (default: no limit)")
	format := f.fs.String("format", "table", "table, jsonl or parquet")
	outPath := f.fs.String("out", "", "a file for a table or JSON lines (default: stdout), or a directory for Parquet, a file per stream")
	pipeline := f.fs.String("pipeline", "", "the pipeline the read is for, which names what a source keeps upstream, such as a replication slot (default: a new one, torn down afterwards)")
	stateIn := f.fs.String("state", "", "a JSON file of each stream's state to resume from, as --state-out writes it")
	stateOut := f.fs.String("state-out", "", "write each stream's state at the end of the read to this file")
	if err := f.parse(args); err != nil {
		return err
	}
	w, err := newRecordWriter(*format, *outPath, out)
	if err != nil {
		return err
	}
	states := map[string]json.RawMessage{}
	if *stateIn != "" {
		b, err := os.ReadFile(*stateIn)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(b, &states); err != nil {
			return fmt.Errorf("--state: %w", err)
		}
	}
	t, p, cat, err := f.source(ctx)
	if err != nil {
		return err
	}
	defer t.close()
	defer func() { _ = p.Close() }()

	req := &protocolv1.ReadRequest{Config: p.Config(), Pipeline: cmp.Or(*pipeline, uuid.NewString())}
	for _, s := range cat.GetCatalog().GetStreams() {
		if len(only) > 0 && !slices.Contains(only, s.GetName()) {
			continue
		}
		cs, err := configure(s, *mode, *cursor)
		if err != nil {
			return err
		}
		if st, ok := states[s.GetName()]; ok {
			cs.State = st
		}
		req.Streams = append(req.Streams, cs)
	}
	for _, name := range only {
		if !slices.ContainsFunc(req.Streams, func(s *protocolv1.ConfiguredStream) bool { return s.GetName() == name }) {
			return fmt.Errorf("%s has no stream %s", t.name, name)
		}
	}
	if *pipeline == "" {
		defer func() {
			tctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			if _, err := protocolv1.NewConnectorServiceClient(p.Conn()).Teardown(tctx, &protocolv1.TeardownRequest{Config: p.Config(), Pipeline: req.Pipeline}); err != nil {
				fmt.Fprintf(stderr, "tearing down pipeline %s: %v\n", req.Pipeline, err)
			}
		}()
	}
	r := &reading{w: w, limit: *limit, streams: map[string]*streamRead{}}
	for _, s := range req.Streams {
		r.streams[s.GetName()] = &streamRead{}
		r.order = append(r.order, s.GetName())
	}
	readErr := r.run(ctx, p.Conn(), req)
	err = errors.Join(readErr, w.close())
	r.summary(stderr)
	if *stateOut != "" {
		got := map[string]json.RawMessage{}
		for name, s := range r.streams {
			if !s.cut && s.state != nil && json.Valid(s.state) {
				got[name] = s.state
			}
		}
		b, _ := json.MarshalIndent(got, "", "  ")
		err = errors.Join(err, os.WriteFile(*stateOut, append(b, '\n'), 0o600))
	}
	return err
}

// configure reads s with mode, or the first mode it lists.
func configure(s *protocolv1.CatalogStream, mode, cursor string) (*protocolv1.ConfiguredStream, error) {
	cs := &protocolv1.ConfiguredStream{Name: s.GetName(), SyncMode: protocolv1.SyncMode_SYNC_MODE_FULL_REFRESH}
	if len(s.GetSyncModes()) > 0 {
		cs.SyncMode = s.GetSyncModes()[0]
	}
	if mode != "" {
		m, ok := protocolv1.SyncMode_value["SYNC_MODE_"+strings.ToUpper(mode)]
		if !ok || m == 0 {
			return nil, fmt.Errorf("--mode %s: want full_refresh, incremental or cdc", mode)
		}
		cs.SyncMode = protocolv1.SyncMode(m)
		if len(s.GetSyncModes()) > 0 && !slices.Contains(s.GetSyncModes(), cs.SyncMode) {
			return nil, fmt.Errorf("stream %s can't be read with %s", s.GetName(), mode)
		}
	}
	if cs.SyncMode == protocolv1.SyncMode_SYNC_MODE_INCREMENTAL {
		cs.Cursor = cmp.Or(cursor, s.GetDefaultCursor())
	}
	return cs, nil
}

type streamRead struct {
	records  int
	complete bool
	// Records were left out for --limit.
	cut bool
	// The last resume point's, or where the next read starts once complete.
	state []byte
}

// reading is one read's progress.
type reading struct {
	w       recordWriter
	limit   int
	streams map[string]*streamRead
	order   []string
}

func (r *reading) run(ctx context.Context, conn grpc.ClientConnInterface, req *protocolv1.ReadRequest) error {
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := protocolv1.NewSourceServiceClient(conn).Read(rctx, req)
	if err != nil {
		return err
	}
	for {
		m, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			if r.done() {
				return nil
			}
			return fmt.Errorf("read: %w", err)
		}
		switch msg := m.GetMsg().(type) {
		case *protocolv1.ReadResponse_Batch:
			if err := r.batch(msg.Batch); err != nil {
				return err
			}
		case *protocolv1.ReadResponse_ResumePoint:
			if s := r.streams[msg.ResumePoint.GetStream()]; s != nil {
				s.state = msg.ResumePoint.GetState()
			}
		case *protocolv1.ReadResponse_Complete:
			if s := r.streams[msg.Complete.GetStream()]; s != nil {
				s.complete, s.state = true, msg.Complete.GetNext()
			}
		}
		if r.done() {
			cancel()
		}
	}
}

// done reports whether every stream is complete or at the limit.
func (r *reading) done() bool {
	for _, s := range r.streams {
		if !s.complete && (r.limit == 0 || s.records < r.limit) {
			return false
		}
	}
	return true
}

func (r *reading) batch(b *protocolv1.RecordBatch) error {
	s := r.streams[b.GetStream()]
	if s == nil {
		return fmt.Errorf("records for %s, which the read didn't ask for", b.GetStream())
	}
	room := -1
	if r.limit > 0 {
		room = r.limit - s.records
		if room <= 0 {
			s.cut = true
			return nil
		}
	}
	if rows := b.GetRows(); rows != nil {
		rs := rows.GetRows()
		if room >= 0 && len(rs) > room {
			rs, s.cut = rs[:room], true
		}
		s.records += len(rs)
		return r.w.rows(b.GetStream(), rs)
	}
	rec, err := arrowipc.DecodeBatch(b.GetArrow(), memory.DefaultAllocator)
	if err != nil {
		return fmt.Errorf("stream %s: %w", b.GetStream(), err)
	}
	defer rec.Release()
	if room >= 0 && rec.NumRows() > int64(room) {
		s.cut = true
		sliced := rec.NewSlice(0, int64(room))
		defer sliced.Release()
		rec = sliced
	}
	s.records += int(rec.NumRows())
	return r.w.records(b.GetStream(), rec)
}

func (r *reading) summary(w io.Writer) {
	for _, name := range r.order {
		s := r.streams[name]
		line := fmt.Sprintf("%s: %d records", name, s.records)
		switch {
		case s.cut || !s.complete && r.limit > 0 && s.records >= r.limit:
			line += ", stopped at --limit"
		case s.complete:
			line += ", complete"
		default:
			line += ", not complete"
		}
		switch {
		case s.cut:
		case len(s.state) > 0 && len(s.state) <= 200:
			line += "; state " + string(s.state)
		case len(s.state) > 0:
			line += fmt.Sprintf("; state of %d bytes", len(s.state))
		}
		fmt.Fprintln(w, line)
	}
}
