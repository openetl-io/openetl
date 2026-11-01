package api

import (
	"testing"
	"time"

	"github.com/unsuitable001/openetl/internal/orchestration/contract"
)

func TestLogEntry(t *testing.T) {
	e := logEntry(contract.LogLine{Task: "step-02-attempt-003", Text: `{"time":"2026-11-02T09:00:00.123+01:00","level":"WARN","msg":"dbt failed","component":"dbt","nodes":{"error":1}}`})
	if want := time.Date(2026, 11, 2, 8, 0, 0, 123e6, time.UTC); !e.GetTime().AsTime().Equal(want) {
		t.Errorf("time %v, want %v", e.GetTime().AsTime(), want)
	}
	if e.GetLevel() != "WARN" || e.GetMessage() != "dbt failed" || e.GetPostSyncStep() != 2 || e.GetAttempt() != 3 {
		t.Errorf("entry %v", e)
	}
	attrs := e.GetAttrs().AsMap()
	if len(attrs) != 2 || attrs["component"] != "dbt" || attrs["nodes"].(map[string]any)["error"] != 1.0 {
		t.Errorf("attrs %v", attrs)
	}

	e = logEntry(contract.LogLine{Task: "attempt-001", Text: "not json"})
	if e.GetMessage() != "not json" || e.GetAttempt() != 1 || e.GetAttrs() != nil {
		t.Errorf("entry %v", e)
	}
}
