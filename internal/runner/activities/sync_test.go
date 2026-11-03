package activities

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	"github.com/unsuitable001/openetl/internal/platform/errs"
)

func TestQuarantineURL(t *testing.T) {
	abs, _ := filepath.Abs("cp")
	for storage, want := range map[string]string{
		"/var/openetl":                          "file:///var/openetl/o/p/_quarantine",
		"cp":                                    "file://" + filepath.ToSlash(abs) + "/o/p/_quarantine",
		"s3://b?region=r&endpoint=http://h:1":   "s3://b/o/p/_quarantine?region=r&endpoint=http://h:1",
		"s3://b/base?endpoint=https://s3.x.com": "s3://b/base/o/p/_quarantine?endpoint=https://s3.x.com",
		"gs://b":                                "gs://b/o/p/_quarantine",
		"azblob://c?account=a":                  "azblob://c/o/p/_quarantine?account=a",
	} {
		got, err := quarantineURL(storage, "o", "p")
		if err != nil || got != want {
			t.Errorf("quarantineURL(%q) = %q, %v; want %q", storage, got, err, want)
		}
	}
}

func TestReplaced(t *testing.T) {
	for _, err := range []error{
		temporal.NewCanceledError(),
		serviceerror.NewNotFound("invalid activityID or activity already timed out or invoking workflow is completed"),
		serviceerror.NewNamespaceNotFound("ns"),
		fmt.Errorf("heartbeat: %w", activity.ErrActivityReset),
	} {
		if !replaced(err) {
			t.Errorf("replaced(%v) = false", err)
		}
	}
	for _, err := range []error{
		serviceerror.NewUnavailable("connection refused"),
		context.DeadlineExceeded,
		serviceerror.NewResourceExhausted(0, "busy"),
	} {
		if replaced(err) {
			t.Errorf("replaced(%v) = true: the job would stop when Temporal can't be reached", err)
		}
	}
}

func TestLateStart(t *testing.T) {
	info := activity.Info{HeartbeatTimeout: time.Minute, StartedTime: time.Now().Add(-30 * time.Second)}
	if err := lateStart(info); err != nil {
		t.Errorf("30 s late: %v", err)
	}
	info.StartedTime = time.Now().Add(-2 * time.Minute)
	if err := lateStart(info); errs.CodeOf(err) != errs.Transient {
		t.Errorf("2 min late: %v, want TRANSIENT", err)
	}
	if err := lateStart(activity.Info{StartedTime: info.StartedTime}); err != nil {
		t.Errorf("no heartbeat timeout: %v", err)
	}
}
