package main

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"
	"golang.org/x/term"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
)

const secretUsage = `usage: openetl secret <command> [flags]

Commands:
  set NAME      create a secret or set its value, from stdin or --from-file,
                or point it at a cloud secret store with --ref
  list          list a workspace's secrets, and what uses each
  delete NAME   delete a secret no source or destination names

A config names a secret as {secret: NAME}. Runners read a --ref themselves,
with their own cloud identity:
  aws-sm://{name or ARN}[?region=…][&version=…][&stage=…][&field=…]
  gcp-sm://projects/{p}/[locations/{l}/]secrets/{s}[/versions/{v}][?field=…]
  azure-kv://{vault}/{name}[?version=…][&field=…]
field picks a top-level field of a JSON value.
`

func runSecret(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, secretUsage)
		return errors.New("no secret command")
	}
	cmd, args := args[0], args[1:]
	var name string
	switch cmd {
	case "set":
		name = "openetl secret set"
	case "list":
		name = "openetl secret list"
	case "delete":
		name = "openetl secret delete"
	}
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	conn := apiFlags(fs)
	workspace := fs.String("workspace", "default", "the secret's workspace")
	switch cmd {
	case "set":
		file := fs.String("from-file", "", "read the value from this file instead of stdin")
		ref := fs.String("ref", "", "a reference to a cloud secret store, which runners read, instead of a value")
		if err := fs.Parse(args); err != nil {
			return err
		}
		if fs.NArg() != 1 {
			return errors.New("secret set takes one name, after its flags")
		}
		if *file != "" && *ref != "" {
			return errors.New("secret set takes --from-file or --ref, not both")
		}
		return setSecret(ctx, conn, *workspace, fs.Arg(0), *file, *ref, out)
	case "list":
		if err := fs.Parse(args); err != nil {
			return err
		}
		if fs.NArg() != 0 {
			return errors.New("secret list takes no arguments")
		}
		return listSecrets(ctx, conn, *workspace, out)
	case "delete":
		if err := fs.Parse(args); err != nil {
			return err
		}
		if fs.NArg() != 1 {
			return errors.New("secret delete takes one name, after its flags")
		}
		name := "workspaces/" + *workspace + "/secrets/" + fs.Arg(0)
		c, err := client(conn, apiv1connect.NewSecretServiceClient)
		if err != nil {
			return err
		}
		if _, err := c.DeleteSecret(ctx, connect.NewRequest(&apiv1.DeleteSecretRequest{Name: name})); err != nil {
			return err
		}
		fmt.Fprintln(out, "deleted", name)
		return nil
	}
	fmt.Fprint(os.Stderr, secretUsage)
	return fmt.Errorf("unknown secret command %q", cmd)
}

// readValue reads a secret's value without echoing it from a terminal. One
// trailing newline is dropped, as echo and editors add one.
func readValue(name, file string) (string, error) {
	var b []byte
	var err error
	switch fd := int(os.Stdin.Fd()); {
	case file != "":
		b, err = os.ReadFile(filepath.Clean(file))
	case term.IsTerminal(fd):
		fmt.Fprintf(os.Stderr, "value of %s: ", name)
		b, err = term.ReadPassword(fd)
		fmt.Fprintln(os.Stderr)
	default:
		b, err = io.ReadAll(io.LimitReader(os.Stdin, 1<<20))
	}
	if err != nil {
		return "", err
	}
	b = bytes.TrimSuffix(b, []byte("\n"))
	b = bytes.TrimSuffix(b, []byte("\r"))
	return string(b), nil
}

func setSecret(ctx context.Context, conn *api, workspace, secret, file, ref string, out io.Writer) error {
	c, err := client(conn, apiv1connect.NewSecretServiceClient)
	if err != nil {
		return err
	}
	name := "workspaces/" + workspace + "/secrets/" + secret
	in := &apiv1.Secret{Name: name, Ref: ref}
	if ref == "" {
		if in.Value, err = readValue(secret, file); err != nil {
			return err
		}
	}
	res, err := c.UpdateSecret(ctx, connect.NewRequest(&apiv1.UpdateSecretRequest{Secret: in, AllowMissing: true}))
	if err != nil {
		return err
	}
	done := "updated"
	if res.Msg.GetCreated() {
		done = "created"
	}
	fmt.Fprintln(out, done, name)
	return nil
}

func listSecrets(ctx context.Context, conn *api, workspace string, out io.Writer) error {
	c, err := client(conn, apiv1connect.NewSecretServiceClient)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME\tVALUE\tUPDATED\tUSED BY")
	req := &apiv1.ListSecretsRequest{Parent: "workspaces/" + workspace}
	for {
		res, err := c.ListSecrets(ctx, connect.NewRequest(req))
		if err != nil {
			return err
		}
		for _, s := range res.Msg.GetSecrets() {
			name := s.GetName()[strings.LastIndex(s.GetName(), "/")+1:]
			users := make([]string, len(s.GetUsedBy()))
			for i, u := range s.GetUsedBy() {
				users[i] = strings.TrimPrefix(u, req.GetParent()+"/")
			}
			value := cmp.Or(s.GetRef(), "sealed")
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", name, value, s.GetUpdateTime().AsTime().Local().Format(time.DateTime), strings.Join(users, ", "))
		}
		if req.PageToken = res.Msg.GetNextPageToken(); req.PageToken == "" {
			return tw.Flush()
		}
	}
}
