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

	"connectrpc.com/connect"
	"go.yaml.in/yaml/v3"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/gen/openetl/api/v1/apiv1connect"
)

const usage = `usage: openetl <command> [flags]

Commands:
  apply -f FILE   create or update the sources, destinations and pipelines in FILE

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
	if len(args) == 0 || args[0] != "apply" {
		fmt.Fprint(os.Stderr, usage)
		if len(args) == 0 {
			return errors.New("no command")
		}
		return fmt.Errorf("unknown command %q", args[0])
	}
	fs := flag.NewFlagSet("openetl apply", flag.ContinueOnError)
	var paths files
	fs.Var(&paths, "f", "a YAML or JSON file, or - for stdin; repeatable")
	api := fs.String("api-url", cmp.Or(os.Getenv("OPENETL_API_URL"), "http://localhost:8080"), "API URL (env OPENETL_API_URL)")
	workspace := fs.String("workspace", "", "workspace for documents that name none (default: default)")
	dryRun := fs.Bool("dry-run", false, "validate without changing anything")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if len(paths) == 0 || fs.NArg() > 0 {
		return errors.New("apply takes one or more -f FILE, and no arguments")
	}
	req := &apiv1.ApplyRequest{ValidateOnly: *dryRun}
	if *workspace != "" {
		req.Workspace = "workspaces/" + *workspace
	}
	for _, p := range paths {
		docs, err := readDocuments(p)
		if err != nil {
			return err
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
