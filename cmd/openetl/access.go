package main

import (
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/gen/openetl/api/v1/apiv1connect"
	"github.com/unsuitable001/openetl/internal/auth"
)

const principalUsage = `usage: openetl principal <command> [flags]

Commands:
  create NAME        create a service account, or a user with --user
  list               list principals and their roles
  delete NAME        delete a principal, with its keys and roles
  disable NAME       stop a principal's keys from working
  enable NAME        let them work again
  grant NAME ROLE    grant viewer, operator, editor, admin or owner, on the
                     org or, with --workspace, one workspace
  revoke NAME ROLE   revoke it
`

const keyUsage = `usage: openetl key <command> [flags]

Commands:
  create PRINCIPAL      create an API key, and print it once
  list PRINCIPAL        list a principal's keys
  delete PRINCIPAL ID   delete a key; it stops working at once
`

const dataPlaneUsage = `usage: openetl dataplane <command> [flags]

Commands:
  list                         list data planes
  token create DATAPLANE       create a runner token, and print it once
  token list DATAPLANE         list a data plane's runner tokens
  token delete DATAPLANE ID    delete one; it stops working at once
`

// subcommand parses a subcommand's flags, and checks it got n arguments.
func subcommand(cmd string, args []string, n int, usage string, flags func(fs *flag.FlagSet)) (*api, []string, error) {
	fs := flag.NewFlagSet("openetl "+cmd, flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "usage: openetl %s [flags] %s\n", cmd, usage)
		fs.PrintDefaults()
	}
	conn := apiFlags(fs)
	if flags != nil {
		flags(fs)
	}
	if err := fs.Parse(args); err != nil {
		return nil, nil, err
	}
	if fs.NArg() != n {
		return nil, nil, fmt.Errorf("%s takes %s, after its flags", cmd, cmp.Or(usage, "no arguments"))
	}
	return conn, fs.Args(), nil
}

var done = map[string]string{"delete": "deleted", "disable": "disabled", "enable": "enabled", "grant": "granted", "revoke": "revoked"}

func runPrincipal(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, principalUsage)
		return errors.New("no principal command")
	}
	sub, args := args[0], args[1:]
	cmd := "principal " + sub
	switch sub {
	case "create":
		var user *bool
		var email, display *string
		conn, a, err := subcommand(cmd, args, 1, "NAME", func(fs *flag.FlagSet) {
			user = fs.Bool("user", false, "a user, rather than a service account")
			email = fs.String("email", "", "the user's email")
			display = fs.String("display-name", "", "a name to show")
		})
		if err != nil {
			return err
		}
		p := &apiv1.Principal{Type: apiv1.PrincipalType_PRINCIPAL_TYPE_SERVICE_ACCOUNT, DisplayName: *display, Email: *email}
		if *user {
			p.Type = apiv1.PrincipalType_PRINCIPAL_TYPE_USER
		}
		c, err := client(conn, apiv1connect.NewAccessServiceClient)
		if err != nil {
			return err
		}
		res, err := c.CreatePrincipal(ctx, connect.NewRequest(&apiv1.CreatePrincipalRequest{Principal: p, PrincipalId: a[0]}))
		if err != nil {
			return err
		}
		fmt.Fprintln(out, "created", res.Msg.GetPrincipal().GetName())
		return nil
	case "list":
		conn, _, err := subcommand(cmd, args, 0, "", nil)
		if err != nil {
			return err
		}
		return listPrincipals(ctx, conn, out)
	case "delete", "disable", "enable":
		conn, a, err := subcommand(cmd, args, 1, "NAME", nil)
		if err != nil {
			return err
		}
		c, err := client(conn, apiv1connect.NewAccessServiceClient)
		if err != nil {
			return err
		}
		name := "principals/" + a[0]
		switch sub {
		case "delete":
			_, err = c.DeletePrincipal(ctx, connect.NewRequest(&apiv1.DeletePrincipalRequest{Name: name}))
		case "disable":
			_, err = c.DisablePrincipal(ctx, connect.NewRequest(&apiv1.DisablePrincipalRequest{Name: name}))
		default:
			_, err = c.EnablePrincipal(ctx, connect.NewRequest(&apiv1.EnablePrincipalRequest{Name: name}))
		}
		if err != nil {
			return err
		}
		fmt.Fprintln(out, done[sub], name)
		return nil
	case "grant", "revoke":
		var workspace *string
		conn, a, err := subcommand(cmd, args, 2, "NAME ROLE", func(fs *flag.FlagSet) {
			workspace = fs.String("workspace", "", "the workspace the role is in (default: the org, which covers every workspace)")
		})
		if err != nil {
			return err
		}
		r, ok := auth.ParseRole(a[1])
		if !ok {
			return fmt.Errorf("no role %q: viewer, operator, editor, admin or owner", a[1])
		}
		b := &apiv1.RoleBinding{Role: apiv1.Role(r)}
		where := "the org"
		if *workspace != "" {
			b.Workspace, where = "workspaces/"+*workspace, "workspace "+*workspace
		}
		c, err := client(conn, apiv1connect.NewAccessServiceClient)
		if err != nil {
			return err
		}
		name := "principals/" + a[0]
		if sub == "grant" {
			_, err = c.GrantRole(ctx, connect.NewRequest(&apiv1.GrantRoleRequest{Name: name, Binding: b}))
		} else {
			_, err = c.RevokeRole(ctx, connect.NewRequest(&apiv1.RevokeRoleRequest{Name: name, Binding: b}))
		}
		if err != nil {
			return err
		}
		fmt.Fprintln(out, done[sub], name, r, "on", where)
		return nil
	}
	fmt.Fprint(os.Stderr, principalUsage)
	return fmt.Errorf("unknown principal command %q", sub)
}

func roleNames(bs []*apiv1.RoleBinding) string {
	var out []string
	for _, b := range bs {
		r := auth.Role(b.GetRole()).String()
		if ws := strings.TrimPrefix(b.GetWorkspace(), "workspaces/"); ws != "" {
			r += " in " + ws
		}
		out = append(out, r)
	}
	return strings.Join(out, ", ")
}

func listPrincipals(ctx context.Context, conn *api, out io.Writer) error {
	c, err := client(conn, apiv1connect.NewAccessServiceClient)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME\tTYPE\tROLES\t")
	req := &apiv1.ListPrincipalsRequest{}
	for {
		res, err := c.ListPrincipals(ctx, connect.NewRequest(req))
		if err != nil {
			return err
		}
		for _, p := range res.Msg.GetPrincipals() {
			typ := "service account"
			if p.GetType() == apiv1.PrincipalType_PRINCIPAL_TYPE_USER {
				typ = "user"
			}
			note := ""
			if p.GetDisabled() {
				note = "disabled"
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", strings.TrimPrefix(p.GetName(), "principals/"), typ, roleNames(p.GetRoleBindings()), note)
		}
		if req.PageToken = res.Msg.GetNextPageToken(); req.PageToken == "" {
			return tw.Flush()
		}
	}
}

// printSecret prints a new key or token, or writes it to file.
func printSecret(out io.Writer, what, secret, file string) error {
	if file == "" {
		fmt.Fprintf(out, "%s; it isn't shown again:\n%s\n", what, secret)
		return nil
	}
	if err := auth.WriteKeyFile(file, secret); err != nil {
		return err
	}
	fmt.Fprintf(out, "%s, in %s\n", what, file)
	return nil
}

func when(t *timestamppb.Timestamp) string {
	if t == nil {
		return "never"
	}
	return t.AsTime().Local().Format(time.DateTime)
}

// parseExpiry takes a duration, or a number of days such as 90d.
func parseExpiry(s string) (time.Duration, error) {
	if days, ok := strings.CutSuffix(s, "d"); ok {
		n, err := strconv.Atoi(days)
		return time.Duration(n) * 24 * time.Hour, err
	}
	return time.ParseDuration(s)
}

func runKey(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, keyUsage)
		return errors.New("no key command")
	}
	sub, args := args[0], args[1:]
	cmd := "key " + sub
	switch sub {
	case "create":
		var expires, file *string
		conn, a, err := subcommand(cmd, args, 1, "PRINCIPAL", func(fs *flag.FlagSet) {
			expires = fs.String("expires", "", "how long until the key expires, such as 90d or 12h (default: never)")
			file = fs.String("out", "", "write the key to this file, readable only by you, instead of printing it")
		})
		if err != nil {
			return err
		}
		req := &apiv1.CreateKeyRequest{Parent: "principals/" + a[0]}
		if *expires != "" {
			d, err := parseExpiry(*expires)
			if err != nil || d <= 0 {
				return fmt.Errorf("--expires %q isn't a positive duration", *expires)
			}
			req.ExpireTime = timestamppb.New(time.Now().Add(d))
		}
		c, err := client(conn, apiv1connect.NewAccessServiceClient)
		if err != nil {
			return err
		}
		res, err := c.CreateKey(ctx, connect.NewRequest(req))
		if err != nil {
			return err
		}
		return printSecret(out, "created "+res.Msg.GetKey().GetName(), res.Msg.GetSecret(), *file)
	case "list":
		conn, a, err := subcommand(cmd, args, 1, "PRINCIPAL", nil)
		if err != nil {
			return err
		}
		c, err := client(conn, apiv1connect.NewAccessServiceClient)
		if err != nil {
			return err
		}
		res, err := c.ListKeys(ctx, connect.NewRequest(&apiv1.ListKeysRequest{Parent: "principals/" + a[0]}))
		if err != nil {
			return err
		}
		tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "ID\tPREFIX\tCREATED\tLAST USED\tEXPIRES")
		for _, k := range res.Msg.GetKeys() {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", k.GetName()[strings.LastIndex(k.GetName(), "/")+1:], k.GetPrefix(), when(k.GetCreateTime()), when(k.GetLastUsedTime()), when(k.GetExpireTime()))
		}
		return tw.Flush()
	case "delete":
		conn, a, err := subcommand(cmd, args, 2, "PRINCIPAL ID", nil)
		if err != nil {
			return err
		}
		c, err := client(conn, apiv1connect.NewAccessServiceClient)
		if err != nil {
			return err
		}
		name := "principals/" + a[0] + "/keys/" + a[1]
		if _, err := c.DeleteKey(ctx, connect.NewRequest(&apiv1.DeleteKeyRequest{Name: name})); err != nil {
			return err
		}
		fmt.Fprintln(out, "deleted", name)
		return nil
	}
	fmt.Fprint(os.Stderr, keyUsage)
	return fmt.Errorf("unknown key command %q", sub)
}

func runDataPlane(ctx context.Context, args []string, out io.Writer) error {
	var sub string
	switch {
	case len(args) >= 2 && args[0] == "token":
		sub, args = "token "+args[1], args[2:]
	case len(args) > 0:
		sub, args = args[0], args[1:]
	}
	cmd := "dataplane " + sub
	switch sub {
	case "list":
		conn, _, err := subcommand(cmd, args, 0, "", nil)
		if err != nil {
			return err
		}
		c, err := client(conn, apiv1connect.NewAccessServiceClient)
		if err != nil {
			return err
		}
		res, err := c.ListDataPlanes(ctx, connect.NewRequest(&apiv1.ListDataPlanesRequest{}))
		if err != nil {
			return err
		}
		for _, dp := range res.Msg.GetDataPlanes() {
			fmt.Fprintln(out, strings.TrimPrefix(dp.GetName(), "dataPlanes/"))
		}
		return nil
	case "token create":
		var file *string
		conn, a, err := subcommand(cmd, args, 1, "DATAPLANE", func(fs *flag.FlagSet) {
			file = fs.String("out", "", "write the token to this file, readable only by you, instead of printing it; openetld runner --token-file reads it")
		})
		if err != nil {
			return err
		}
		c, err := client(conn, apiv1connect.NewAccessServiceClient)
		if err != nil {
			return err
		}
		res, err := c.CreateRunnerToken(ctx, connect.NewRequest(&apiv1.CreateRunnerTokenRequest{Parent: "dataPlanes/" + a[0]}))
		if err != nil {
			return err
		}
		return printSecret(out, "created "+res.Msg.GetToken().GetName(), res.Msg.GetSecret(), *file)
	case "token list":
		conn, a, err := subcommand(cmd, args, 1, "DATAPLANE", nil)
		if err != nil {
			return err
		}
		c, err := client(conn, apiv1connect.NewAccessServiceClient)
		if err != nil {
			return err
		}
		res, err := c.ListRunnerTokens(ctx, connect.NewRequest(&apiv1.ListRunnerTokensRequest{Parent: "dataPlanes/" + a[0]}))
		if err != nil {
			return err
		}
		tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "ID\tPREFIX\tCREATED\tLAST USED")
		for _, t := range res.Msg.GetTokens() {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", t.GetName()[strings.LastIndex(t.GetName(), "/")+1:], t.GetPrefix(), when(t.GetCreateTime()), when(t.GetLastUsedTime()))
		}
		return tw.Flush()
	case "token delete":
		conn, a, err := subcommand(cmd, args, 2, "DATAPLANE ID", nil)
		if err != nil {
			return err
		}
		c, err := client(conn, apiv1connect.NewAccessServiceClient)
		if err != nil {
			return err
		}
		name := "dataPlanes/" + a[0] + "/tokens/" + a[1]
		if _, err := c.DeleteRunnerToken(ctx, connect.NewRequest(&apiv1.DeleteRunnerTokenRequest{Name: name})); err != nil {
			return err
		}
		fmt.Fprintln(out, "deleted", name)
		return nil
	}
	fmt.Fprint(os.Stderr, dataPlaneUsage)
	return fmt.Errorf("unknown dataplane command %q", sub)
}
