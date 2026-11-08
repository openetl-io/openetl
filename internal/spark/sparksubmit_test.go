package spark

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/platform/errs"
)

func standaloneSubmit(ctx context.Context, t *testing.T) (Submitter, string) {
	t.Helper()
	bin, dir := fakeSubmit(t)
	spec := &apiv1.SparkClusterSpec{Target: &apiv1.SparkClusterSpec_Submit{Submit: &apiv1.SparkSubmit{Master: "spark://m:7077", Ui: "http://m:8080"}}}
	sub, err := New(ctx, spec, nil, Options{SparkSubmit: bin})
	if err != nil {
		t.Fatal(err)
	}
	return sub, dir
}

// A standalone master's drivers can be found only by the ID spark-submit
// prints, so one that exits without printing it may have left a driver no
// retry can find, and a retry would submit another.
func TestStandaloneWithoutADriverIDIsntRetried(t *testing.T) {
	ctx := t.Context()
	sub, dir := standaloneSubmit(ctx, t)
	say(t, dir, "submit", "26/11/04 10:00:00 WARN Utils: nothing about a driver\n")
	_, err := sub.Submit(ctx, app)
	if errs.CodeOf(err) != errs.ConfigInvalid {
		t.Fatalf("exit 0 without a driver ID: %v, want CONFIG_INVALID", err)
	}
	for _, want := range []string{"may still be running", "INFO"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("%q isn't in %q", want, err)
		}
	}

	if err := os.WriteFile(filepath.Join(dir, "exit-submit"), []byte("1"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Submit(ctx, app); errs.CodeOf(err) != errs.Transient {
		t.Errorf("a failed spark-submit without a driver ID: %v, want TRANSIENT", err)
	}
}

// A line too long to keep doesn't stop the reading: spark-submit would block
// writing to a pipe nobody reads, and Wait with it.
func TestSubmitGoesOnAfterAVeryLongLine(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	sub, dir := standaloneSubmit(ctx, t)
	say(t, dir, "submit", strings.Repeat("x", 3<<20)+"\n26/11/04 10:00:00 INFO Client: Driver successfully submitted as driver-1\n")
	if id, err := sub.Submit(ctx, app); err != nil || id != "driver-1" {
		t.Fatalf("submitted %q, %v", id, err)
	}
}
