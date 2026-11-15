package main

import (
	"fmt"
	"io"
	"strings"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/sdk/go/check"
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

// reportItems are a connector's items as it answered, filled in, a failed
// or warning one's message followed by its detail.
func reportItems(items []check.Item) []checkItem {
	out := make([]checkItem, 0, len(items))
	for _, it := range items {
		it = it.Rendered()
		c := checkItem{title: it.Title, status: it.Status.String(), message: it.Message, field: it.Field}
		if f := it.Failure; f != nil {
			c.code, c.fix = errs.Code(f.ErrorCode()).String(), f.Fix
			if f.Detail != "" {
				c.message = strings.TrimSuffix(c.message, ".") + ": " + f.Detail
			}
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
