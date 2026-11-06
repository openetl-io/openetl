package activities

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/platform/errs"
)

// An attempt's lines reach the runner's log, with its attributes, and the
// attempt's log, without them; ReadLogs reads them back.
func TestAttemptLogAndReadLogs(t *testing.T) {
	store := checkpoint.NewMemStore()
	org, pipeline, sync := uuid.NewString(), uuid.NewString(), uuid.Must(uuid.NewV7()).String()
	var runner bytes.Buffer
	log, done := attemptLog(t.Context(), store, slog.New(slog.NewTextHandler(&runner, nil)).With("pipeline", pipeline), org, pipeline, sync, 0, 2)
	log.With("component", "engine").Info("started")
	done()
	if !strings.Contains(runner.String(), "pipeline="+pipeline) || !strings.Contains(runner.String(), "component=engine") {
		t.Errorf("runner log: %s", runner.String())
	}

	a := &Logs{Store: store}
	res, err := a.read(t.Context(), contract.ReadLogsInput{OrgID: org, PipelineID: pipeline, SyncID: sync})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Lines) != 1 || res.Lines[0].Task != "attempt-002" || strings.Contains(res.Lines[0].Text, pipeline) || !strings.Contains(res.Lines[0].Text, `"component":"engine"`) {
		t.Fatalf("read %+v", res)
	}
	again, err := a.read(t.Context(), contract.ReadLogsInput{OrgID: org, PipelineID: pipeline, SyncID: sync, After: res.Next})
	if err != nil || len(again.Lines) != 0 || again.Next != res.Next {
		t.Fatalf("read again: %+v, %v", again, err)
	}

	for _, in := range []contract.ReadLogsInput{
		{OrgID: org, PipelineID: pipeline, SyncID: "../" + sync},
		{OrgID: org, PipelineID: pipeline, SyncID: strings.ToUpper(sync)},
		{OrgID: org, PipelineID: pipeline, SyncID: sync, After: contract.LogCursor{Key: org + "/other/_logs/x/attempt-001/000000.jsonl"}},
	} {
		if _, err := a.read(t.Context(), in); errs.CodeOf(err) != errs.ConfigInvalid {
			t.Errorf("%+v: got %v, want CONFIG_INVALID", in, err)
		}
	}
}

func TestNoStoreNoAttemptLog(t *testing.T) {
	base := slog.New(slog.DiscardHandler)
	log, done := attemptLog(t.Context(), nil, base, "o", "p", "s", 0, 1)
	done()
	if log != base {
		t.Error("got another logger without a store")
	}
}
