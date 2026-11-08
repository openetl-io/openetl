package spark

import (
	"context"
	"strings"
	"testing"
	"time"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
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
