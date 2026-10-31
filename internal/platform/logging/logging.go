// Package logging sets up log/slog.
package logging

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
)

// New returns a logger at level ("debug", "info", "warn" or "error") in
// format ("json" or "text").
func New(w io.Writer, level, format string) (*slog.Logger, error) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("logging: level %q: %w", level, err)
	}
	opts := &slog.HandlerOptions{Level: lvl}
	switch strings.ToLower(format) {
	case "json":
		return slog.New(slog.NewJSONHandler(w, opts)), nil
	case "text":
		return slog.New(slog.NewTextHandler(w, opts)), nil
	default:
		return nil, fmt.Errorf("logging: format %q is neither json nor text", format)
	}
}
