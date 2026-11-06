//go:build integration

package temporalbackend_test

import (
	"cmp"
	"context"
	"crypto/rand"
	"os"
	"strings"
	"testing"
	"time"

	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/openetl-io/openetl/internal/orchestration/orchestrationtest"
	"github.com/openetl-io/openetl/internal/orchestration/temporalbackend"
	"github.com/openetl-io/openetl/internal/platform/temporalx"
)

func TestBackend(t *testing.T) {
	address := cmp.Or(os.Getenv("OPENETL_TEST_TEMPORAL_ADDRESS"), "localhost:7233")
	namespace := "openetl-it-" + strings.ToLower(rand.Text())[:10]
	c, err := client.DialContext(t.Context(), client.Options{HostPort: address})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.WorkflowService().RegisterNamespace(t.Context(), &workflowservice.RegisterNamespaceRequest{
		Namespace: namespace, WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = c.OperatorService().DeleteNamespace(context.Background(), &operatorservice.DeleteNamespaceRequest{Namespace: namespace})
	})
	b, err := temporalbackend.Open(t.Context(), temporalx.Options{Address: address, Namespace: namespace})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	orchestrationtest.Check(t, b)
}
