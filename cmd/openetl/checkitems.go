package main

import (
	"fmt"
	"io"
	"strings"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// checkItem is one thing a connector's check looked at, from the API or from
// the connector itself.
type checkItem struct {
	title, status, code, message, fix, field string
}

func apiCheckItems(items []*apiv1.CheckItem) []checkItem {
	out := make([]checkItem, 0, len(items))
	for _, it := range items {
		out = append(out, checkItem{title: it.GetTitle(), status: it.GetStatus(), code: it.GetCode(), message: it.GetMessage(), fix: it.GetFix(), field: it.GetField()})
	}
	return out
}

func protoCheckItems(items []*protocolv1.CheckItem) []checkItem {
	out := make([]checkItem, 0, len(items))
	for _, it := range items {
		c := checkItem{
			title: it.GetTitle(), status: strings.ToLower(strings.TrimPrefix(it.GetStatus().String(), "CHECK_STATUS_")),
			message: it.GetMessage(), fix: it.GetFix(), field: it.GetField(),
		}
		if e := it.GetError(); e != nil {
			c.code = strings.TrimPrefix(e.GetCode().String(), "ERROR_CODE_")
		}
		out = append(out, c)
	}
	return out
}

// printCheckItems prints a check's items, one a line under indent, and the
// fix and the config field of each that failed or warned.
func printCheckItems(out io.Writer, indent string, items []checkItem) {
	for _, it := range items {
		line := fmt.Sprintf("%s%-8s %s", indent, it.status, it.title)
		for _, s := range []string{it.code, it.message} {
			if s != "" {
				line += ": " + s
			}
		}
		fmt.Fprintln(out, line)
		if it.status != "failed" && it.status != "warning" {
			continue
		}
		if it.field != "" {
			fmt.Fprintf(out, "%s         field: %s\n", indent, it.field)
		}
		if it.fix != "" {
			fmt.Fprintf(out, "%s         fix: %s\n", indent, it.fix)
		}
	}
}
