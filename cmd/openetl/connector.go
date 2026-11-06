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
	"github.com/openetl-io/openetl/internal/spec"
	"github.com/openetl-io/openetl/sdk/go/conformance"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

const connectorUsage = `usage: openetl connector <command> [flags]

Commands:
  package [DIR...] (--tag TAG | --version VERSION)
            build each directory's openetl-connector.yaml for every platform,
            and write its executables and descriptor, for a release
  validate [DIR...]
            check each directory's openetl-connector.yaml, without building
  apply -f FILE
            apply the connector versions in FILE: descriptors and manifests
  spec TARGET
            print a connector's spec
  check --config FILE TARGET
            check that a connector reaches its upstream with a config
  discover --config FILE TARGET
            list a source's streams and columns
  read --config FILE TARGET
            read a source's records into a table, JSON lines or Parquet
  schema file|descriptor|manifest
            print the JSON Schema of openetl-connector.yaml, a descriptor
            or a manifest, for editors

TARGET is an executable, a descriptor or a manifest; --image REF or
--attach ADDR stand in for it. Run openetl connector <command> -h for more.
`

func runConnector(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, connectorUsage)
		return errors.New("no connector command")
	}
	switch args[0] {
	case "package":
		return runPackage(ctx, args[1:], out)
	case "validate":
		return runValidate(args[1:], out)
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
		fmt.Fprintln(fs.Output(), "usage: openetl connector package [DIR...] (--tag TAG | --version VERSION) [--out DIR] [--url-template URL] [--platforms LIST]")
		fmt.Fprintln(fs.Output(), "\nBuilds the package in each DIR (default .) from its openetl-connector.yaml, for every platform, and writes")
		fmt.Fprintln(fs.Output(), "each executable as its release asset, NAME-ROLE-OS-ARCH, and the descriptor, NAME.connector.yaml, into --out.")
		fmt.Fprintln(fs.Output(), "--tag NAME/vX.Y.Z picks the DIR whose package is NAME, among DIRs or the subdirectories of ., and --tag vX.Y.Z")
		fmt.Fprintln(fs.Output(), "takes a repository with one package. --version without --tag packages every DIR, each tagged v{version}, or")
		fmt.Fprintln(fs.Output(), "NAME/v{version} when there are several.")
		fs.PrintDefaults()
	}
	tag := fs.String("tag", "", "the release's git tag: NAME/vX.Y.Z, or vX.Y.Z")
	version := fs.String("version", "", "the version, when there's no tag")
	outDir := fs.String("out", "dist", "where to write the executables and descriptors")
	urls := fs.String("url-template", "", "where runners download each executable, with {tag}, {asset}, {version}, {name}, {role}, {os} and {arch} (default: the GitHub release's assets)")
	platforms := fs.String("platforms", "", "the os/arch pairs to build, comma-separated (default: the file's)")
	verbose := fs.Bool("v", false, "show the builds' output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if (*tag == "") == (*version == "") {
		fs.Usage()
		return errors.New("package needs --tag or --version")
	}
	jobs, err := packageJobs(fs.Args(), *tag, *version)
	if err != nil {
		return err
	}
	o := connectorpkg.Options{
		Out: *outDir, URLTemplate: *urls,
		Registry:  &connectorpkg.Registry{Client: &http.Client{Timeout: time.Minute}},
		ImageSpec: imageSpec,
	}
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
	}
	return nil
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

func runValidate(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("openetl connector validate", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "usage: openetl connector validate [DIR...] [--require FIELD,...]")
		fmt.Fprintln(fs.Output(), "\nChecks each DIR's openetl-connector.yaml (default .) as package does, without building.")
		fs.PrintDefaults()
	}
	require := fs.String("require", "", "fields each file must set, comma-separated: description, license, docs, logo, author or tags")
	if err := fs.Parse(args); err != nil {
		return err
	}
	dirs := fs.Args()
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

// onlyConnectorVersions checks that every document is a Connector or a
// DeclarativeSource.
func onlyConnectorVersions(content []byte, origin string) error {
	var head struct {
		Kind string `yaml:"kind"`
	}
	if err := yaml.Unmarshal(content, &head); err != nil {
		return fmt.Errorf("%s: %w", origin, err)
	}
	if head.Kind != spec.KindConnector && head.Kind != spec.KindDeclarativeSource {
		return fmt.Errorf("%s is a %s; connector apply takes Connector and DeclarativeSource documents, and openetl apply takes the rest", origin, head.Kind)
	}
	return nil
}
