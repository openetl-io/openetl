package activities

import (
	"context"
	"strings"
	"testing"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
)

func TestReadQuarantine(t *testing.T) {
	const org, pipeline = "6f1c1c40-7a4a-4d62-9a3b-2a1f5c7d9e01", "0d3b8f6e-5b0e-4a51-8c1e-0f2b6a7c9d12"
	store := checkpoint.NewMemStore()
	lines := []string{`{"stream":"orders","code":"type"}`, `{"stream":"orders","code":"nan"}`, `{"stream":"orders","code":"object"}`}
	key := org + "/" + pipeline + "/_quarantine/3/17/public%2Forders.jsonl"
	if _, err := store.Create(t.Context(), key, []byte(strings.Join(lines, "\n")+"\n")); err != nil {
		t.Fatal(err)
	}
	big := org + "/" + pipeline + "/_quarantine/3/18/orders.jsonl"
	if _, err := store.Create(t.Context(), big, []byte(strings.Repeat("x", contract.MaxQuarantineRead+1)+"\n")); err != nil {
		t.Fatal(err)
	}
	a := &Quarantine{Store: store}
	in := contract.ReadQuarantineInput{OrgID: org, PipelineID: pipeline, Path: "3/17/public%2Forders.jsonl", Lines: 2}
	res, err := a.ReadQuarantine(t.Context(), in)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(res.Lines, "\n") != strings.Join(lines[:2], "\n") || !res.More || res.Missing {
		t.Errorf("two lines: %+v", res)
	}
	in.Lines = 0
	if res, err = a.ReadQuarantine(t.Context(), in); err != nil || len(res.Lines) != 3 || res.More {
		t.Errorf("every line: %+v, %v", res, err)
	}
	in.Path = "3/19/orders.jsonl"
	if res, err = a.ReadQuarantine(t.Context(), in); err != nil || !res.Missing || len(res.Lines) != 0 {
		t.Errorf("a file that's gone: %+v, %v", res, err)
	}
	in.Path = "3/18/orders.jsonl"
	if _, err = a.ReadQuarantine(t.Context(), in); err == nil || !strings.Contains(err.Error(), "larger than 256 KiB") {
		t.Errorf("a first line past the bound: %v", err)
	}
	// A file far past the bound: the store gives one byte past it, and the
	// read stops at the last whole line within it.
	long := org + "/" + pipeline + "/_quarantine/3/20/orders.jsonl"
	if _, err := store.Create(t.Context(), long, []byte(strings.Repeat(strings.Repeat("y", 4096)+"\n", 200))); err != nil {
		t.Fatal(err)
	}
	counted := &prefixCounter{MemStore: store}
	in = contract.ReadQuarantineInput{OrgID: org, PipelineID: pipeline, Path: "3/20/orders.jsonl", Lines: 100}
	res, err = (&Quarantine{Store: counted}).ReadQuarantine(t.Context(), in)
	if err != nil || len(res.Lines) != 64 || !res.More || counted.n != contract.MaxQuarantineRead+maxQuarantineLines+1 {
		t.Errorf("a long file: %d lines, more %v, read %d bytes, %v", len(res.Lines), res.More, counted.n, err)
	}
	// A first line of exactly the bound, then more: the newline after it
	// isn't the end of the file.
	edge := org + "/" + pipeline + "/_quarantine/3/21/orders.jsonl"
	first := strings.Repeat("z", contract.MaxQuarantineRead)
	if _, err := store.Create(t.Context(), edge, []byte(first+"\nnext\n")); err != nil {
		t.Fatal(err)
	}
	in = contract.ReadQuarantineInput{OrgID: org, PipelineID: pipeline, Path: "3/21/orders.jsonl", Lines: 100}
	if res, err = a.ReadQuarantine(t.Context(), in); err != nil || len(res.Lines) != 1 || !res.More {
		t.Errorf("a first line of the bound, then more: %d lines, more %v, %v", len(res.Lines), res.More, err)
	}
	// The same line alone: nothing more.
	if _, err := store.Create(t.Context(), org+"/"+pipeline+"/_quarantine/3/22/orders.jsonl", []byte(first+"\n")); err != nil {
		t.Fatal(err)
	}
	in.Path = "3/22/orders.jsonl"
	if res, err = a.ReadQuarantine(t.Context(), in); err != nil || len(res.Lines) != 1 || res.More {
		t.Errorf("a first line of the bound alone: %d lines, more %v, %v", len(res.Lines), res.More, err)
	}
	// Short lines that fill the window and go on: the limit stops the read.
	short := org + "/" + pipeline + "/_quarantine/3/23/orders.jsonl"
	if _, err := store.Create(t.Context(), short, []byte(strings.Repeat(strings.Repeat("s", 2620)+"\n", 120))); err != nil {
		t.Fatal(err)
	}
	in.Path = "3/23/orders.jsonl"
	if res, err = a.ReadQuarantine(t.Context(), in); err != nil || len(res.Lines) != 100 || !res.More {
		t.Errorf("a full window of short lines: %d lines, more %v, %v", len(res.Lines), res.More, err)
	}
	for _, bad := range []contract.ReadQuarantineInput{
		{OrgID: org, PipelineID: pipeline, Path: "../../other/_quarantine/1/1/x.jsonl"},
		{OrgID: org, PipelineID: pipeline, Path: "/3/17/orders.jsonl"},
		{OrgID: org, PipelineID: pipeline, Path: ""},
		{OrgID: org, PipelineID: "../x", Path: "3/17/orders.jsonl"},
	} {
		if _, err := a.ReadQuarantine(t.Context(), bad); err == nil {
			t.Errorf("read %+v", bad)
		}
	}
}

// prefixCounter records the most GetPrefix was asked for.
type prefixCounter struct {
	*checkpoint.MemStore
	n int
}

func (p *prefixCounter) GetPrefix(ctx context.Context, key string, n int) ([]byte, error) {
	p.n = max(p.n, n)
	return p.MemStore.GetPrefix(ctx, key, n)
}
