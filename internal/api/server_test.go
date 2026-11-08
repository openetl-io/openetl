package api

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgconn"
)

// A deadlock or a serialization failure rolled the request back, so the
// caller is told to try again rather than given an internal error.
func TestConflictsWithOtherTransactionsAskForARetry(t *testing.T) {
	s := &Server{}
	for _, code := range []string{"40P01", "40001"} {
		err := s.connectError("/apply", fmt.Errorf("orders.yaml: %w", &pgconn.PgError{Code: code}))
		var ce *connect.Error
		if !errors.As(err, &ce) || ce.Code() != connect.CodeAborted || !strings.Contains(ce.Message(), "try again") {
			t.Errorf("%s: %v, want aborted and to try again", code, err)
		}
	}
}
