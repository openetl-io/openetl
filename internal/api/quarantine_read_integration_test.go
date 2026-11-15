//go:build integration

package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
	"github.com/openetl-io/openetl/internal/api"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/orchestration/dispatch"
)

// runQuarantineReads plays a runner of the data plane until ctx ends: it
// takes reads of quarantine files, and answers each with answer.
func (e *holdsEnv) runQuarantineReads(ctx context.Context, t *testing.T, answer func(contract.ReadQuarantineInput) contract.ReadQuarantineResult) <-chan contract.ReadQuarantineInput {
	t.Helper()
	got := make(chan contract.ReadQuarantineInput, 8)
	go func() {
		for ctx.Err() == nil {
			res, err := e.runner.PollWork(ctx, connect.NewRequest(&dataplanev1.PollWorkRequest{Runner: "r1", FreeLogReads: 1}))
			if err != nil {
				return
			}
			for _, w := range res.Msg.GetWork() {
				if w.GetKind() != contract.ReadQuarantine {
					continue
				}
				var in contract.ReadQuarantineInput
				if err := json.Unmarshal(w.GetInput(), &in); err != nil {
					return
				}
				got <- in
				out, _ := json.Marshal(answer(in))
				_, _ = e.runner.ReportResult(ctx, connect.NewRequest(&dataplanev1.ReportResultRequest{Runner: "r1", Id: w.GetId(), Result: out}))
			}
		}
	}()
	return got
}

// An editor reads a file the index lists through a runner of the data
// plane; the read is audited, and the control plane keeps none of it.
func TestReadQuarantineFile(t *testing.T) {
	ctx := t.Context()
	e := newHoldsEnv(t, func(s *api.Server) { s.Dispatch = &dispatch.Dispatcher{Store: s.Store} })
	_, err := e.runner.ReportCheckpoint(ctx, connect.NewRequest(&dataplanev1.ReportCheckpointRequest{
		PipelineId: e.pipeline.String(), RunId: "run1", Generation: 2, Epoch: 5, CreateTime: timestamppb.Now(),
		Streams: []*dataplanev1.CheckpointStream{{Name: "orders", Rows: 10, QuarantinedRows: 3, QuarantineFiles: []*dataplanev1.CheckpointQuarantineFile{
			{Path: "2/5/orders.jsonl", Url: "s3://lake/o/p/_quarantine/2/5/orders.jsonl", Rows: 3},
		}}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	lines := []string{`{"stream":"orders","code":"type","column":"amount","reason":"r","record":"{\"amount\": \"x\"}"}`, `{"stream":"orders","code":"object","reason":"r","record":"[1]"}`}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	reads := e.runQuarantineReads(runCtx, t, func(in contract.ReadQuarantineInput) contract.ReadQuarantineResult {
		if in.Path != "2/5/orders.jsonl" {
			return contract.ReadQuarantineResult{Missing: true}
		}
		return contract.ReadQuarantineResult{Lines: lines[:min(in.Lines, len(lines))], More: in.Lines < 3}
	})

	res, err := e.editor.ReadQuarantineFile(ctx, connect.NewRequest(&apiv1.ReadQuarantineFileRequest{Parent: e.parent, Path: "2/5/orders.jsonl", Limit: 2}))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(res.Msg.GetLines(), "\n") != strings.Join(lines, "\n") || !res.Msg.GetMore() {
		t.Errorf("read %v", res.Msg)
	}
	select {
	case in := <-reads:
		if in.PipelineID != e.pipeline.String() || in.Lines != 2 || in.Path != "2/5/orders.jsonl" {
			t.Errorf("the runner was asked for %+v", in)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no runner was asked")
	}
	var n int
	if err := e.conn.QueryRow(ctx, `SELECT count(*) FROM dispatches WHERE kind = $1`, contract.ReadQuarantine).Scan(&n); err != nil || n != 0 {
		t.Errorf("%d reads kept (%v), want none", n, err)
	}
	var details []byte
	err = e.conn.QueryRow(ctx, `SELECT details FROM audit_log WHERE method LIKE '%/ReadQuarantineFile' AND code = 'ok'`).Scan(&details)
	if err != nil || !strings.Contains(string(details), `"path": "2/5/orders.jsonl"`) {
		t.Errorf("audit entry %s, %v", details, err)
	}

	// A viewer may list the files, but not read them.
	if _, err := e.viewer.ListQuarantine(ctx, connect.NewRequest(&apiv1.ListQuarantineRequest{Parent: e.parent})); err != nil {
		t.Errorf("a viewer listing files: %v", err)
	}
	_, err = e.viewer.ReadQuarantineFile(ctx, connect.NewRequest(&apiv1.ReadQuarantineFileRequest{Parent: e.parent, Path: "2/5/orders.jsonl"}))
	if code(err) != connect.CodePermissionDenied {
		t.Errorf("a viewer read a file: %v", err)
	}
	// Only a path the index has is read.
	_, err = e.owner.ReadQuarantineFile(ctx, connect.NewRequest(&apiv1.ReadQuarantineFileRequest{Parent: e.parent, Path: "../other/_quarantine/1/1/x.jsonl"}))
	if code(err) != connect.CodeNotFound {
		t.Errorf("a path the index doesn't have: %v", err)
	}
	// A file the store no longer has.
	if _, err := e.conn.Exec(ctx, `UPDATE quarantine_files SET path = '2/6/orders.jsonl'`); err != nil {
		t.Fatal(err)
	}
	_, err = e.owner.ReadQuarantineFile(ctx, connect.NewRequest(&apiv1.ReadQuarantineFileRequest{Parent: e.parent, Path: "2/6/orders.jsonl"}))
	var ce *connect.Error
	if code(err) != connect.CodeNotFound || !errors.As(err, &ce) || !strings.Contains(ce.Message(), "deleted it") {
		t.Errorf("a file that's gone: %v", err)
	}
}
