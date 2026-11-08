package webhooks

import (
	"testing"
	"time"
)

// A delivery late in a batch is still leased when its attempt starts, or a
// second notifier would claim it and post it again.
func TestLeaseOutlastsABatch(t *testing.T) {
	if worst := time.Duration(batch/concurrency) * attemptTimeout; lease <= worst {
		t.Errorf("a batch can take %v, but its deliveries are leased for %v", worst, lease)
	}
}
