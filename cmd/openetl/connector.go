package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"go.yaml.in/yaml/v3"

	"github.com/unsuitable001/openetl/internal/connectorhost"
	"github.com/unsuitable001/openetl/internal/spec"
)

const connectorUsage = `usage: openetl connector <command> [flags]

Commands:
  package   write the descriptor of an external connector's executables,
            which runners download from the URLs you give
  apply -f FILE
            apply the connector versions in FILE: descriptors and manifests
`

func runConnector(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, connectorUsage)
		return errors.New("no connector command")
	}
	switch args[0] {
	case "package":
		return runPackage(ctx, args[1:], out)
	case "apply":
		return runApplyDocuments(ctx, "openetl connector apply", args[1:], out, applyConnectors)
	}
	fmt.Fprint(os.Stderr, connectorUsage)
	return fmt.Errorf("unknown connector command %q", args[0])
}

func runPackage(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("openetl connector package", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "usage: openetl connector package --name NAME --version VERSION --exe OS/ARCH=PATH ... (--url OS/ARCH=URL ... | --url-template URL) [-o FILE]")
		fmt.Fprintln(fs.Output(), "\nWrites the descriptor of executables you publish yourself, such as a git release's assets. One must be for this machine's platform, which is run once to read its spec.")
		fs.PrintDefaults()
	}
	var exes, urls files
	fs.Var(&exes, "exe", "an executable for a platform, as os/arch=path; repeatable")
	fs.Var(&urls, "url", "where runners download a platform's executable, as os/arch=url; repeatable")
	template := fs.String("url-template", "", "where runners download each executable, with {os} and {arch} in it")
	name := fs.String("name", "", "the connector's name, such as postgres")
	version := fs.String("version", "", "the connector's version, such as 0.1.0")
	var info spec.Info
	fs.StringVar(&info.Author, "author", "", "who made the connector")
	fs.StringVar(&info.Logo, "logo", "", "an https URL of its logo, for the UI")
	fs.StringVar(&info.ReleaseTag, "release-tag", "", "the git tag the executables were built from")
	fs.StringVar(&info.ReleaseURL, "release-url", "", "an https URL of the release, such as its page")
	output := fs.String("o", "-", "where to write the descriptor")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" || *version == "" || len(exes) == 0 || (len(urls) == 0) == (*template == "") || fs.NArg() > 0 {
		fs.Usage()
		return errors.New("package needs --name, --version, at least one --exe, and --url or --url-template")
	}
	byPlatform := map[string]string{}
	for _, u := range urls {
		platform, url, ok := strings.Cut(u, "=")
		if !ok {
			return fmt.Errorf("--url %q isn't os/arch=url", u)
		}
		byPlatform[platform] = url
	}
	var pkg []connectorhost.Executable
	for _, e := range exes {
		platform, path, ok := strings.Cut(e, "=")
		if !ok {
			return fmt.Errorf("--exe %q isn't os/arch=path", e)
		}
		url := byPlatform[platform]
		if *template != "" {
			goos, arch, _ := strings.Cut(platform, "/")
			url = strings.NewReplacer("{os}", goos, "{arch}", arch).Replace(*template)
		}
		if url == "" {
			return fmt.Errorf("--exe for %s has no --url", platform)
		}
		pkg = append(pkg, connectorhost.Executable{Platform: platform, Path: path, URL: url})
	}
	doc, err := connectorhost.Package(ctx, *name, *version, info, pkg)
	if err != nil {
		return err
	}
	if *output == "-" {
		_, err = out.Write(doc)
		return err
	}
	return os.WriteFile(*output, doc, 0o644) //nolint:gosec // a descriptor, not a secret
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
