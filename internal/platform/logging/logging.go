// Package logging sets up log/slog.
package logging

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"gopkg.in/natefinch/lumberjack.v2"
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

// Output is where a process logs.
type Output struct {
	// "stderr", "stdout", or a file's path.
	To string
	// A file is rotated before it passes MaxSize megabytes, and MaxFiles
	// rotated files are kept.
	MaxSize, MaxFiles int
}

// Open opens the output. Closing stderr or stdout does nothing.
func (o Output) Open() (io.WriteCloser, error) {
	switch o.To {
	case "", "stderr":
		return nopCloser{os.Stderr}, nil
	case "stdout":
		return nopCloser{os.Stdout}, nil
	}
	if o.MaxSize < 1 || o.MaxFiles < 0 {
		return nil, fmt.Errorf("logging: a log file needs a size of at least 1 MB and a number of rotated files to keep, not %d MB and %d", o.MaxSize, o.MaxFiles)
	}
	// A file this creates is 0600. One that exists keeps its mode, and
	// rotation copies it, so a log collector in the file's group can be let
	// in by creating the file first.
	f, err := os.OpenFile(o.To, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("logging: %w", err)
	}
	_ = f.Close()
	return &lumberjack.Logger{Filename: o.To, MaxSize: o.MaxSize, MaxBackups: o.MaxFiles}, nil
}

type nopCloser struct{ io.Writer }

func (nopCloser) Close() error { return nil }
