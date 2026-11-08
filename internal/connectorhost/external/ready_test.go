package external

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestAwaitReady(t *testing.T) {
	for _, tt := range []struct {
		name, out, logged string
		ready             bool
	}{
		{name: "first line", out: "ready\n", ready: true},
		{name: "after a library's output", out: "starting up\nversion 3\nready\n", logged: "starting up", ready: true},
		{name: "with a carriage return", out: "ready\r\n", ready: true},
		{name: "ignores what follows", out: "ready\nmore\n", ready: true},
		{name: "never ready", out: "starting up\nnot ready\n", logged: "not ready"},
		{name: "a line that only starts with it", out: "ready to go\n"},
		{name: "nothing", out: ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			ready := make(chan struct{})
			awaitReady(strings.NewReader(tt.out), ready, slog.New(slog.NewTextHandler(&logs, nil)))
			select {
			case <-ready:
				if !tt.ready {
					t.Fatal("it was ready")
				}
			default:
				if tt.ready {
					t.Fatal("it wasn't ready")
				}
			}
			if tt.logged != "" && !strings.Contains(logs.String(), tt.logged) {
				t.Errorf("logged %q, without %q", logs.String(), tt.logged)
			}
		})
	}
}
