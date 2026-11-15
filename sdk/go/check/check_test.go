package check

import (
	"errors"
	"testing"
)

func TestReport(t *testing.T) {
	var r Report
	boom := errors.New("access denied")
	if !r.Run("connect", "Reach the service", func() error { return nil }) {
		t.Fatal("a passing item reports failure")
	}
	if r.Run("stream.a.read", "Read stream a", func() error { return Fix(boom, "allow kinesis:GetRecords") }, Field("streams"), WithFix("ignored")) {
		t.Fatal("a failing item reports success")
	}
	r.Warn("stream.a.retention", "Retention", "24 hours")
	r.Skip("stream.a.read", "Read stream a again", "the stream failed")
	r.Pass("registry", "Schema registry", "3 subjects")

	items := r.Items()
	if len(items) != 5 {
		t.Fatalf("%d items, want 5", len(items))
	}
	f := items[1]
	if f.Status != Failed || f.Fix != "allow kinesis:GetRecords" || f.Field != "streams" || f.Message != "access denied" || !errors.Is(f.Err, boom) {
		t.Errorf("failed item: %+v", f)
	}
	if items[3].Name != "stream.a.read.2" {
		t.Errorf("a repeated name is %q, want it made unique", items[3].Name)
	}
	if !errors.Is(r.Err(), boom) {
		t.Errorf("Err is %v, want the first failure's", r.Err())
	}
	if items[2].Status.String() != "warning" || items[3].Status.String() != "skipped" {
		t.Errorf("statuses: %v, %v", items[2].Status, items[3].Status)
	}
}

func TestReportWithoutFailures(t *testing.T) {
	var r Report
	r.Warn("a", "A", "slow")
	if r.Err() != nil {
		t.Errorf("a warning is an error: %v", r.Err())
	}
	if Fix(nil, "x") != nil {
		t.Error("Fix(nil) isn't nil")
	}
}
