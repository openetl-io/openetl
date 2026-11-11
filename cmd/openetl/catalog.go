package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
)

func runCatalog(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("openetl connector catalog", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "usage: openetl connector catalog [--refresh] [--json] [--delete-source NAME]")
		fmt.Fprintln(fs.Output(), "\nLists the packages of the connected sources of connectors, each at its highest version, with the reference")
		fmt.Fprintln(fs.Output(), "openetl connector apply takes, and the sources. Sources are listed every 15 minutes; --refresh lists them now.")
		fmt.Fprintln(fs.Output(), "Connect a source by applying a ConnectorSource document.")
		fs.PrintDefaults()
	}
	conn := apiFlags(fs)
	refresh := fs.Bool("refresh", false, "list every source now, and wait")
	asJSON := fs.Bool("json", false, "print the packages and sources as JSON")
	deleteSource := fs.String("delete-source", "", "disconnect a source, whose packages leave the catalog")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		fs.Usage()
		return errors.New("catalog takes no arguments")
	}
	c, err := client(conn, apiv1connect.NewConnectorServiceClient)
	if err != nil {
		return err
	}
	if *deleteSource != "" {
		if _, err := c.DeleteConnectorSource(ctx, connect.NewRequest(&apiv1.DeleteConnectorSourceRequest{Name: "connectorSources/" + *deleteSource})); err != nil {
			return err
		}
		fmt.Fprintf(out, "connectorSources/%s deleted\n", *deleteSource)
		return nil
	}
	var sources []*apiv1.ConnectorSource
	if *refresh {
		res, err := c.RefreshCatalog(ctx, connect.NewRequest(&apiv1.RefreshCatalogRequest{}))
		if err != nil {
			return err
		}
		sources = res.Msg.GetSources()
	} else {
		res, err := c.ListConnectorSources(ctx, connect.NewRequest(&apiv1.ListConnectorSourcesRequest{}))
		if err != nil {
			return err
		}
		sources = res.Msg.GetSources()
	}
	pkgs, err := c.ListCatalog(ctx, connect.NewRequest(&apiv1.ListCatalogRequest{}))
	if err != nil {
		return err
	}
	if *asJSON {
		p, err := protojson.Marshal(pkgs.Msg)
		if err != nil {
			return err
		}
		s, err := protojson.Marshal(&apiv1.ListConnectorSourcesResponse{Sources: sources})
		if err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(map[string]json.RawMessage{"catalog": p, "sources": s})
	}
	printCatalog(out, pkgs.Msg.GetPackages(), sources)
	return nil
}

func printCatalog(out io.Writer, pkgs []*apiv1.CatalogPackage, sources []*apiv1.ConnectorSource) {
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "PACKAGE\tVERSION\tROLES\tAPPLIED\tSOURCE\tAPPLY")
	for _, p := range pkgs {
		name := p.GetPackage()
		switch {
		case p.GetOfficial():
			name += " (official)"
		case p.GetImpostor():
			name += " (not official)"
		case p.GetManual():
			name += " (manually installed)"
		}
		applied := p.GetAppliedVersion()
		switch {
		case applied == "":
			applied = "-"
		case p.GetUpdateAvailable():
			applied += " (update available)"
		}
		source, apply := "-", "-"
		if !p.GetManual() {
			source, apply = strings.TrimPrefix(p.GetSource(), "connectorSources/"), "openetl connector apply "+p.GetReference()
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", name, p.GetVersion(), strings.Join(p.GetRoles(), ","), applied, source, apply)
	}
	_ = tw.Flush()
	fmt.Fprintln(out)
	tw = tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "SOURCE\tURL\tPACKAGES\tLISTED\tERROR")
	for _, s := range sources {
		listed := "never"
		if s.GetRefreshTime() != nil {
			listed = s.GetRefreshTime().AsTime().Local().Format(time.DateTime)
		}
		fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%s\n", strings.TrimPrefix(s.GetName(), "connectorSources/"), s.GetSpec().GetUrl(), s.GetPackages(), listed, s.GetError())
	}
	_ = tw.Flush()
}
