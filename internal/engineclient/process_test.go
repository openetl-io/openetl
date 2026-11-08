package engineclient

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

// A line too long to keep doesn't stop the relay: the engine would block
// writing to a pipe nobody reads, and what it logs later would be lost.
func TestRelayLogsGoesOnAfterAVeryLongLine(t *testing.T) {
	var logged bytes.Buffer
	p := &process{log: slog.New(slog.NewTextHandler(&logged, nil))}
	p.relayLogs(strings.NewReader(`{"message":"before","level":"INFO"}` + "\n" + strings.Repeat("x", 3<<20) + "\n" + `{"message":"after","level":"ERROR"}` + "\n"))
	for _, want := range []string{"msg=before", "cut at", "level=ERROR msg=after"} {
		if !strings.Contains(logged.String(), want) {
			t.Errorf("%q isn't in what was logged, %d bytes", want, logged.Len())
		}
	}
}
