package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/openetl-io/openetl/internal/connectorhost/external"
	"github.com/openetl-io/openetl/internal/connectorpkg"
	"github.com/openetl-io/openetl/internal/declarative"
	"github.com/openetl-io/openetl/internal/oci"
	"github.com/openetl-io/openetl/internal/spec"
	"github.com/openetl-io/openetl/sdk/go/conformance"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

const connectorUsage = `usage: openetl connector <command> [flags]

Commands:
  package [DIR...] (--tag TAG | --version VERSION)
            build each directory's openetl-connector.yaml for every platform,
            and write its executables and descriptor, for a release
  publish oci://REGISTRY/NAMESPACE [DESCRIPTOR...]
            push what package wrote to an OCI registry, and list it in the
            namespace's index
  validate [DIR...] | --artifact oci://REGISTRY/REPOSITORY:VERSION
            check each directory's openetl-connector.yaml, without building;
            or a package version publish pushed to a registry
  apply (-f FILE | REFERENCE...)
            apply connector versions: descriptors and manifests in FILE, or
            oci://REGISTRY/REPOSITORY[:VERSION] and
            github.com/OWNER/REPO[/PACKAGE][@VERSION] references; without a
            version, the newest that the connected source covering the
            reference offers: its highest stable version, or its highest
            prerelease when it has no stable one
  catalog [--refresh]
            list the packages of the connected sources of connectors, with
            the reference to apply each by, and the packages applied that no
            source lists
  spec TARGET
            print a connector's spec
  check --config FILE TARGET
            check that a connector reaches its upstream with a config
  discover --config FILE TARGET
            list a source's streams and columns
  read --config FILE TARGET
            read a source's records into a table, JSON lines or Parquet
  test --config FILE TARGET
            run the conformance kit's checks against a source or destination
  schema file|descriptor|manifest
            print the JSON Schema of openetl-connector.yaml, a descriptor
            or a manifest, for editors

TARGET is an executable, a descriptor, a manifest, or a package version by
reference, oci://REGISTRY/REPOSITORY:VERSION or
github.com/OWNER/REPO[/PACKAGE]@VERSION; --image REF or --attach ADDR stand in
for it. Run openetl connector <command> -h for more.
`

func runConnector(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, connectorUsage)
		return errors.New("no connector command")
	}
	switch args[0] {
	case "package":
		return runPackage(ctx, args[1:], out)
	case "publish":
		return runPublish(ctx, args[1:], out)
	case "catalog":
		return runCatalog(ctx, args[1:], out)
	case "validate":
		return runValidate(ctx, args[1:], out)
	case "apply":
		return runApplyDocuments(ctx, "openetl connector apply", args[1:], out, applyConnectors)
	case "spec":
		return runConnectorSpec(ctx, args[1:], out)
	case "check":
		return runConnectorCheck(ctx, args[1:], out)
	case "discover":
		return runConnectorDiscover(ctx, args[1:], out)
	case "read":
		return runConnectorRead(ctx, args[1:], out)
	case "test":
		return runConnectorTest(ctx, args[1:], out)
	case "schema":
		schemas := map[string]func() []byte{"file": connectorpkg.Schema, "descriptor": spec.DescriptorSchema, "manifest": declarative.Schema}
		if len(args) != 2 || schemas[args[1]] == nil {
			return errors.New("schema takes file, descriptor or manifest")
		}
		_, err := out.Write(schemas[args[1]]())
		return err
	}
	fmt.Fprint(os.Stderr, connectorUsage)
	return fmt.Errorf("unknown connector command %q", args[0])
}

func runPackage(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("openetl connector package", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "usage: openetl connector package [DIR...] (--tag TAG | --version VERSION) [--out DIR] [--url-template URL] [--platforms LIST] [--publish oci://REGISTRY/NAMESPACE]")
		fmt.Fprintln(fs.Output(), "\nBuilds the package in each DIR (default .) from its openetl-connector.yaml, for every platform, and writes")
		fmt.Fprintln(fs.Output(), "each executable as its release asset, NAME-ROLE-OS-ARCH, and the descriptor, NAME.connector.yaml, into --out.")
		fmt.Fprintln(fs.Output(), "--tag NAME/vX.Y.Z picks the DIR whose package is NAME, among DIRs or the subdirectories of ., and --tag vX.Y.Z")
		fmt.Fprintln(fs.Output(), "takes a repository with one package. --version without --tag packages every DIR, each tagged v{version}, or")
		fmt.Fprintln(fs.Output(), "NAME/v{version} when there are several. --publish then pushes each, as openetl connector publish does.")
		fs.PrintDefaults()
	}
	tag := fs.String("tag", "", "the release's git tag: NAME/vX.Y.Z, or vX.Y.Z")
	version := fs.String("version", "", "the version, when there's no tag")
	outDir := fs.String("out", "dist", "where to write the executables and descriptors")
	urls := fs.String("url-template", "", "where runners download each executable, with {tag}, {asset}, {version}, {name}, {role}, {os} and {arch} (default: the GitHub release's assets)")
	platforms := fs.String("platforms", "", "the os/arch pairs to build, comma-separated (default: the file's)")
	verbose := fs.Bool("v", false, "show the builds' output")
	publish := fs.String("publish", "", "an oci://REGISTRY/NAMESPACE to push each package to, once packaged")
	dirs, err := parseInterspersed(fs, args)
	if err != nil {
		return err
	}
	if (*tag == "") == (*version == "") {
		fs.Usage()
		return errors.New("package needs --tag or --version")
	}
	jobs, err := packageJobs(dirs, *tag, *version)
	if err != nil {
		return err
	}
	var ns oci.Ref
	if *publish != "" {
		if ns, err = namespace(*publish); err != nil {
			return err
		}
	}
	reg, err := registryClient()
	if err != nil {
		return err
	}
	o := connectorpkg.Options{Out: *outDir, URLTemplate: *urls, Images: reg, ImageSpec: imageSpec}
	if *platforms != "" {
		o.Platforms = strings.Split(*platforms, ",")
	}
	if *verbose {
		o.Log = os.Stderr
	}
	for _, j := range jobs {
		o.Version, o.Tag = j.version, j.tag
		res, err := connectorpkg.Package(ctx, j.file, o)
		if err != nil {
			return fmt.Errorf("%s: %w", j.file.Name, err)
		}
		fmt.Fprintf(out, "%s %s: %s\n", j.file.FullName(), j.version, res.Descriptor)
		if *publish != "" {
			if err := publishOne(ctx, reg, ns, res.Descriptor, out); err != nil {
				return err
			}
		}
	}
	return nil
}

// parseInterspersed parses flags wherever they are among the positional
// arguments, which it returns, up to a -- after which all are positional.
func parseInterspersed(fs *flag.FlagSet, args []string) ([]string, error) {
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		rest := fs.Args()
		if n := len(args) - len(rest); n > 0 && args[n-1] == "--" {
			return append(pos, rest...), nil
		}
		if len(rest) == 0 {
			return pos, nil
		}
		pos = append(pos, rest[0])
		args = rest[1:]
	}
}

func runPublish(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("openetl connector publish", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "usage: openetl connector publish oci://REGISTRY/NAMESPACE [DESCRIPTOR...]")
		fmt.Fprintln(fs.Output(), "\nPushes each package version openetl connector package wrote (default: every dist/*.connector.yaml), with the")
		fmt.Fprintln(fs.Output(), "executables beside it and its logo, downloaded if it's an https URL, to NAMESPACE/NAME, tagged with its version, and lists it in NAMESPACE/index,")
		fmt.Fprintln(fs.Output(), "tagged NAME, when it's the package's highest version. Credentials come from Docker's config and credential")
		fmt.Fprintln(fs.Output(), "helpers, as docker login and oras login write them.")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() == 0 {
		fs.Usage()
		return errors.New("publish needs an oci://REGISTRY/NAMESPACE")
	}
	ns, err := namespace(fs.Arg(0))
	if err != nil {
		return err
	}
	descriptors := fs.Args()[1:]
	if len(descriptors) == 0 {
		if descriptors, err = filepath.Glob(filepath.Join("dist", "*.connector.yaml")); err != nil {
			return err
		}
		if len(descriptors) == 0 {
			return errors.New("dist has no descriptors; run openetl connector package first, or name them")
		}
	}
	reg, err := registryClient()
	if err != nil {
		return err
	}
	for _, d := range descriptors {
		if err := publishOne(ctx, reg, ns, d, out); err != nil {
			return err
		}
	}
	return nil
}

func publishOne(ctx context.Context, reg *oci.Client, ns oci.Ref, descriptor string, out io.Writer) error {
	p, err := connectorpkg.Publish(ctx, reg, ns, descriptor)
	if err != nil {
		return fmt.Errorf("%s: %w", descriptor, err)
	}
	indexed := "not indexed: the index lists a higher version"
	if p.Indexed {
		indexed = "indexed"
	}
	fmt.Fprintf(out, "%s %s: %s (%s)\n", p.Name, p.Version, p.Ref, indexed)
	return nil
}

// namespace reads oci://REGISTRY/NAMESPACE.
func namespace(s string) (oci.Ref, error) {
	ns, err := oci.Parse(s)
	if err == nil && ns.Reference() != "" {
		err = fmt.Errorf("%s names a tag or digest; give the namespace alone, such as oci://ghcr.io/acme/connectors", s)
	}
	return ns, err
}

// dockerCredential is Docker's credential for an oci:// URL's registry, for
// connectorhost.Fetcher.
func dockerCredential(ctx context.Context, url string) (string, string, error) {
	r, err := oci.Parse(url)
	if err != nil {
		return "", "", nil
	}
	creds, err := oci.DockerCredentials()
	if err != nil {
		return "", "", err
	}
	c, err := creds(ctx, r)
	return c.Username, c.Password, err
}

// registryClient reaches registries as the user, with Docker's credentials.
func registryClient() (*oci.Client, error) {
	creds, err := oci.DockerCredentials()
	if err != nil {
		return nil, fmt.Errorf("docker's credentials: %w", err)
	}
	return &oci.Client{HTTP: &http.Client{Timeout: 10 * time.Minute}, Credentials: creds}, nil
}

// imageSpec reads an image-only role's spec, through Docker or Podman.
func imageSpec(ctx context.Context, image, role string, command []string) (*protocolv1.SpecResponse, error) {
	engine, err := conformance.ContainerEngine()
	if err != nil {
		return nil, err
	}
	p, err := conformance.Container(engine, image, command, conformance.Resources{}, conformance.Options{Role: role})(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = p.Close() }()
	return protocolv1.NewConnectorServiceClient(p.Conn()).Spec(ctx, &protocolv1.SpecRequest{Host: external.Host()})
}

type packageJob struct {
	file         *connectorpkg.File
	version, tag string
}

// packageJobs are the packages to build, each with its version and tag.
func packageJobs(dirs []string, tag, version string) ([]packageJob, error) {
	name, v, ok := strings.Cut(tag, "/v")
	if tag != "" && !ok {
		name, v = "", strings.TrimPrefix(tag, "v")
	}
	if tag != "" {
		version = v
	}
	if !spec.ValidVersion(version) {
		return nil, fmt.Errorf("version %q isn't a semantic version, such as 1.2.0", version)
	}
	searched := len(dirs) == 0 && name != ""
	if searched {
		ents, err := os.ReadDir(".")
		if err != nil {
			return nil, err
		}
		for _, e := range ents {
			if _, err := os.Stat(filepath.Join(e.Name(), connectorpkg.FileName)); e.IsDir() && err == nil {
				dirs = append(dirs, e.Name())
			}
		}
	}
	if len(dirs) == 0 {
		dirs = []string{"."}
	}
	var jobs []packageJob
	for _, d := range dirs {
		f, err := connectorpkg.Load(d)
		if err != nil {
			return nil, err
		}
		if name != "" && f.ShortName() != name {
			continue
		}
		t := tag
		switch {
		case t != "":
		case len(dirs) > 1:
			t = f.ShortName() + "/v" + version
		default:
			t = "v" + version
		}
		jobs = append(jobs, packageJob{file: f, version: version, tag: t})
	}
	switch {
	case len(jobs) == 0 && searched:
		return nil, fmt.Errorf("no directory below . has a package named %s", name)
	case len(jobs) == 0:
		return nil, fmt.Errorf("none of %s is the package %s", strings.Join(dirs, ", "), name)
	case tag != "" && len(jobs) > 1:
		return nil, fmt.Errorf("tag %s names one package, and %d match it", tag, len(jobs))
	}
	return jobs, nil
}

func runValidate(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("openetl connector validate", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "usage: openetl connector validate [DIR...] [--require FIELD,...]")
		fmt.Fprintln(fs.Output(), "       openetl connector validate --artifact oci://REGISTRY/NAMESPACE/NAME:VERSION")
		fmt.Fprintln(fs.Output(), "\nChecks each DIR's openetl-connector.yaml (default .) as package does, without building. With --artifact,")
		fmt.Fprintln(fs.Output(), "checks a package version in a registry, by tag or @sha256:..., as publish pushes one: its artifact and")
		fmt.Fprintln(fs.Output(), "config types, the descriptor as its config, a layer for every binary and the logo, the annotations, and")
		fmt.Fprintln(fs.Output(), "the namespace's index. Credentials come from Docker's config and credential helpers.")
		fs.PrintDefaults()
	}
	require := fs.String("require", "", "fields each file must set, comma-separated: description, license, docs, logo, author or tags")
	artifact := fs.String("artifact", "", "an oci:// package version to check, in place of directories")
	dirs, err := parseInterspersed(fs, args)
	if err != nil {
		return err
	}
	if *artifact != "" {
		if len(dirs) > 0 || *require != "" {
			return errors.New("--artifact takes no directories or --require")
		}
		return validateArtifact(ctx, *artifact, out)
	}
	if len(dirs) == 0 {
		dirs = []string{"."}
	}
	var failed []error
	for _, d := range dirs {
		f, err := connectorpkg.Load(d)
		if err == nil && *require != "" {
			err = f.Require(strings.Split(*require, ","))
		}
		if err == nil {
			err = f.CheckManifests()
		}
		if err != nil {
			failed = append(failed, fmt.Errorf("%s: %w", d, err))
			continue
		}
		fmt.Fprintf(out, "%s: %s\n", d, f.FullName())
	}
	return errors.Join(failed...)
}

// validateArtifact prints each of an artifact's checks, and fails if any
// found a problem.
func validateArtifact(ctx context.Context, ref string, out io.Writer) error {
	r, err := oci.Parse(ref)
	if err != nil {
		return err
	}
	reg, err := registryClient()
	if err != nil {
		return err
	}
	findings, err := connectorpkg.CheckArtifact(ctx, reg, r)
	if err != nil {
		return err
	}
	failed := 0
	for _, f := range findings {
		if len(f.Problems) == 0 {
			fmt.Fprintf(out, "ok   %s\n", f.Check)
			continue
		}
		failed++
		fmt.Fprintf(out, "FAIL %s\n", f.Check)
		for _, p := range f.Problems {
			fmt.Fprintf(out, "     %s\n", p)
		}
	}
	if failed > 0 {
		return fmt.Errorf("%s: %d of %d checks failed", ref, failed, len(findings))
	}
	return nil
}

// onlyConnectorVersions checks that every document is a Connector or a
// DeclarativeSource.
func onlyConnectorVersions(content []byte, origin string) error {
	var head struct {
		Kind string `yaml:"kind"`
	}
	if err := yaml.Unmarshal(content, &head); err != nil {
		return fmt.Errorf("%s: %w", origin, err)
	}
	if head.Kind != spec.KindConnector && head.Kind != spec.KindDeclarativeSource && head.Kind != spec.KindConnectorSource {
		return fmt.Errorf("%s is a %s; connector apply takes Connector, DeclarativeSource and ConnectorSource documents, and openetl apply takes the rest", origin, head.Kind)
	}
	return nil
}
