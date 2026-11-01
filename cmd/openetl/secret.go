package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"
	"golang.org/x/term"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/gen/openetl/api/v1/apiv1connect"
)

//nolint:gosec // usage text, not a credential
const secretUsage = `usage: openetl secret <command> [flags]

Commands:
  set NAME      create a secret or set its value, from stdin or --from-file
  list          list a workspace's secrets, and what uses each
  delete NAME   delete a secret no source or destination names

A config names a secret as {secret: NAME}.
`

func runSecret(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, secretUsage)
		return errors.New("no secret command")
	}
	cmd, args := args[0], args[1:]
	fs := flag.NewFlagSet("openetl secret "+cmd, flag.ContinueOnError)
	conn := apiFlags(fs)
	workspace := fs.String("workspace", "default", "the secret's workspace")
	switch cmd {
	case "set":
		file := fs.String("from-file", "", "read the value from this file instead of stdin")
		if err := fs.Parse(args); err != nil {
			return err
		}
		if fs.NArg() != 1 {
			return errors.New("secret set takes one name, after its flags")
		}
		return setSecret(ctx, conn, *workspace, fs.Arg(0), *file, out)
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
		b, err = os.ReadFile(file) //nolint:gosec // the user names the file
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

func setSecret(ctx context.Context, conn *api, workspace, secret, file string, out io.Writer) error {
	c, err := client(conn, apiv1connect.NewSecretServiceClient)
	if err != nil {
		return err
	}
	value, err := readValue(secret, file)
	if err != nil {
		return err
	}
	name := "workspaces/" + workspace + "/secrets/" + secret
	res, err := c.UpdateSecret(ctx, connect.NewRequest(&apiv1.UpdateSecretRequest{
		Secret: &apiv1.Secret{Name: name, Value: value}, AllowMissing: true,
	}))
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
	fmt.Fprintln(tw, "NAME\tUPDATED\tUSED BY")
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
			fmt.Fprintf(tw, "%s\t%s\t%s\n", name, s.GetUpdateTime().AsTime().Local().Format(time.DateTime), strings.Join(users, ", "))
		}
		if req.PageToken = res.Msg.GetNextPageToken(); req.PageToken == "" {
			return tw.Flush()
		}
	}
}
