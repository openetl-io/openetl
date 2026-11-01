package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/netip"
	"os"
	"strings"

	"github.com/google/go-containerregistry/pkg/authn"
	"go.yaml.in/yaml/v3"

	"github.com/unsuitable001/openetl/internal/connectorhost"
	"github.com/unsuitable001/openetl/internal/netguard"
	"github.com/unsuitable001/openetl/internal/registry"
	"github.com/unsuitable001/openetl/internal/spec"
)

const connectorUsage = `usage: openetl connector <command> [flags]

Commands:
  package   push an external connector's executables as an image, and write
            its descriptor
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
		return runApplyDocuments(ctx, "openetl connector apply", args[1:], out, true)
	}
	fmt.Fprint(os.Stderr, connectorUsage)
	return fmt.Errorf("unknown connector command %q", args[0])
}

func runPackage(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("openetl connector package", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "usage: openetl connector package --image REGISTRY/REPOSITORY:TAG --name NAME --version VERSION --exe OS/ARCH=PATH ... [-o FILE]")
		fmt.Fprintln(fs.Output(), "\nOne executable must be for this machine's platform, which is run once to read its spec.")
		fs.PrintDefaults()
	}
	var exes files
	fs.Var(&exes, "exe", "an executable for a platform, as os/arch=path; repeatable")
	image := fs.String("image", "", "the image to push, with a tag")
	name := fs.String("name", "", "the connector's name, such as postgres")
	version := fs.String("version", "", "the connector's version, such as 0.1.0")
	output := fs.String("o", "-", "where to write the descriptor")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *image == "" || *name == "" || *version == "" || len(exes) == 0 || fs.NArg() > 0 {
		fs.Usage()
		return errors.New("package needs --image, --name, --version and at least one --exe")
	}
	var push []registry.Executable
	for _, e := range exes {
		platform, path, ok := strings.Cut(e, "=")
		if !ok {
			return fmt.Errorf("--exe %q isn't os/arch=path", e)
		}
		push = append(push, registry.Executable{Platform: platform, Path: path})
	}
	// The user names the registry, so any address is allowed.
	c := &registry.Client{
		Policy:   netguard.Policy{Allow: []netip.Prefix{netip.MustParsePrefix("0.0.0.0/0"), netip.MustParsePrefix("::/0")}},
		Keychain: authn.DefaultKeychain,
	}
	doc, err := connectorhost.Package(ctx, c, *image, *name, *version, push)
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
