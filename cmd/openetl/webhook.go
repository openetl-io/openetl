package main

import (
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
	"google.golang.org/protobuf/encoding/protojson"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
)

const webhookUsage = `usage: openetl webhook <command> [flags]

Commands:
  list              list a workspace's webhooks
  deliveries NAME   list a webhook's deliveries, newest first, for 7 days
  test NAME         send a webhook a webhook.test event
  disable NAME      stop delivering to a webhook
  enable NAME       deliver to a disabled webhook again
  delete NAME       delete a webhook

openetl apply creates and changes webhooks (kind: Webhook). Each delivery
is signed as Standard Webhooks says, with the secrets the webhook names;
make one with:
  echo "whsec_$(openssl rand -base64 32)" | openetl secret set NAME
`

func runWebhook(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, webhookUsage)
		return errors.New("no webhook command")
	}
	cmd, args := args[0], args[1:]
	fs := flag.NewFlagSet("openetl webhook "+cmd, flag.ContinueOnError)
	conn := apiFlags(fs)
	workspace := fs.String("workspace", "default", "the webhook's workspace")
	var asJSON *bool
	if cmd == "deliveries" {
		asJSON = fs.Bool("json", false, "print each delivery as a line of JSON")
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	switch {
	case cmd == "list" && fs.NArg() != 0:
		return errors.New("webhook list takes no arguments")
	case cmd != "list" && fs.NArg() != 1:
		return fmt.Errorf("webhook %s takes one name, after its flags", cmd)
	}
	c, err := client(conn, apiv1connect.NewWebhookServiceClient)
	if err != nil {
		return err
	}
	parent := "workspaces/" + *workspace
	name := parent + "/webhooks/" + fs.Arg(0)
	switch cmd {
	case "list":
		return listWebhooks(ctx, c, parent, out)
	case "deliveries":
		return listDeliveries(ctx, c, name, *asJSON, out)
	case "test":
		res, err := c.TestWebhook(ctx, connect.NewRequest(&apiv1.TestWebhookRequest{Name: name}))
		if err != nil {
			return err
		}
		fmt.Fprintln(out, "sent", res.Msg.GetDelivery())
		return nil
	case "disable":
		if _, err := c.DisableWebhook(ctx, connect.NewRequest(&apiv1.DisableWebhookRequest{Name: name})); err != nil {
			return err
		}
		fmt.Fprintln(out, "disabled", name)
		return nil
	case "enable":
		if _, err := c.EnableWebhook(ctx, connect.NewRequest(&apiv1.EnableWebhookRequest{Name: name})); err != nil {
			return err
		}
		fmt.Fprintln(out, "enabled", name)
		return nil
	case "delete":
		if _, err := c.DeleteWebhook(ctx, connect.NewRequest(&apiv1.DeleteWebhookRequest{Name: name})); err != nil {
			return err
		}
		fmt.Fprintln(out, "deleted", name)
		return nil
	}
	fmt.Fprint(os.Stderr, webhookUsage)
	return fmt.Errorf("unknown webhook command %q", cmd)
}

func listWebhooks(ctx context.Context, c apiv1connect.WebhookServiceClient, parent string, out io.Writer) error {
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME\tSTATE\tURL\tEVENTS\tPIPELINES")
	req := &apiv1.ListWebhooksRequest{Parent: parent}
	for {
		res, err := c.ListWebhooks(ctx, connect.NewRequest(req))
		if err != nil {
			return err
		}
		for _, w := range res.Msg.GetWebhooks() {
			state := strings.ToLower(strings.TrimPrefix(w.GetState().String(), "WEBHOOK_STATE_"))
			if w.GetDisabledReason() != "" {
				state += ": " + w.GetDisabledReason()
			} else if w.GetFailingSince() != nil {
				state += ", failing since " + w.GetFailingSince().AsTime().Local().Format(time.DateTime)
			}
			pipelines := strings.Join(w.GetSpec().GetPipelines(), ",")
			if pipelines == "" {
				pipelines = "all"
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", strings.TrimPrefix(w.GetName(), parent+"/webhooks/"), state,
				w.GetSpec().GetUrl(), strings.Join(w.GetSpec().GetEvents(), ","), pipelines)
		}
		if req.PageToken = res.Msg.GetNextPageToken(); req.PageToken == "" {
			return tw.Flush()
		}
	}
}

func listDeliveries(ctx context.Context, c apiv1connect.WebhookServiceClient, webhook string, asJSON bool, out io.Writer) error {
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	if !asJSON {
		fmt.Fprintln(tw, "CREATED\tEVENT\tSTATE\tATTEMPTS\tSTATUS\tERROR")
	}
	req := &apiv1.ListWebhookDeliveriesRequest{Parent: webhook}
	for {
		res, err := c.ListWebhookDeliveries(ctx, connect.NewRequest(req))
		if err != nil {
			return err
		}
		for _, d := range res.Msg.GetDeliveries() {
			if asJSON {
				b, err := protojson.Marshal(d)
				if err != nil {
					return err
				}
				fmt.Fprintln(out, string(b))
				continue
			}
			state := strings.ToLower(strings.TrimPrefix(d.GetState().String(), "DELIVERY_STATE_"))
			if d.GetNextAttemptTime() != nil {
				state += ", next " + d.GetNextAttemptTime().AsTime().Local().Format(time.TimeOnly)
			}
			status := "-"
			if d.GetResponseStatus() != 0 {
				status = fmt.Sprint(d.GetResponseStatus())
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\t%s\n", d.GetCreateTime().AsTime().Local().Format(time.DateTime), d.GetEventType(),
				state, d.GetAttempts(), status, d.GetError())
		}
		if req.PageToken = res.Msg.GetNextPageToken(); req.PageToken == "" {
			return tw.Flush()
		}
	}
}
