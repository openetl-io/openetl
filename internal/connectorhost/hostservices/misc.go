package hostservices

import (
	"bytes"
	"context"
	"log/slog"
	"maps"
	"slices"
	"time"

	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/sdk/go/host"
)

const (
	maxLogMessage = 8 << 10
	maxLogAttrs   = 32
	maxKVKey      = 512
	maxKVValue    = 1 << 20
	maxKVTotal    = 64 << 20
)

// Log writes to the instance's logger, with secret values scrubbed.
func (s *Services) Log(ctx context.Context, entries []host.LogEntry) error {
	for _, e := range entries {
		msg := e.Message
		if len(msg) > maxLogMessage {
			msg = msg[:maxLogMessage] + "…"
		}
		attrs := make([]slog.Attr, 0, min(len(e.Attrs), maxLogAttrs)+1)
		for _, k := range slices.Sorted(maps.Keys(e.Attrs)) {
			if len(attrs) == maxLogAttrs {
				break
			}
			attrs = append(attrs, slog.String(k, s.scrub(e.Attrs[k])))
		}
		if !e.Time.IsZero() {
			attrs = append(attrs, slog.Time("connector_time", e.Time))
		}
		s.log.LogAttrs(ctx, e.Level, s.scrub(msg), attrs...)
	}
	return nil
}

type kvEntry struct {
	value   []byte
	expires time.Time
}

func (s *Services) KVGet(_ context.Context, key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.kv[key]
	if !ok {
		return nil, false, nil
	}
	if !e.expires.IsZero() && !s.now().Before(e.expires) {
		s.dropKV(key)
		return nil, false, nil
	}
	return bytes.Clone(e.value), true, nil
}

// KVPut makes room by dropping expired values, then any others.
func (s *Services) KVPut(_ context.Context, key string, value []byte, ttl time.Duration) error {
	switch {
	case key == "" || len(key) > maxKVKey:
		return errs.New(errs.Internal, "KVPut: keys are 1 to %d bytes", maxKVKey)
	case len(value) > maxKVValue:
		return errs.New(errs.Internal, "KVPut: values are at most %d bytes", maxKVValue)
	case ttl < 0:
		return errs.New(errs.Internal, "KVPut: a negative ttl")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropKV(key)
	now := s.now()
	for k, e := range s.kv {
		if s.kvBytes+len(value) <= maxKVTotal {
			break
		}
		if !e.expires.IsZero() && !now.Before(e.expires) {
			s.dropKV(k)
		}
	}
	for k := range s.kv {
		if s.kvBytes+len(value) <= maxKVTotal {
			break
		}
		s.dropKV(k)
	}
	e := kvEntry{value: bytes.Clone(value)}
	if ttl > 0 {
		e.expires = now.Add(ttl)
	}
	s.kv[key] = e
	s.kvBytes += len(value)
	return nil
}

func (s *Services) dropKV(key string) {
	if e, ok := s.kv[key]; ok {
		s.kvBytes -= len(e.value)
		delete(s.kv, key)
	}
}

func (s *Services) Now(context.Context) (time.Time, error) { return s.now(), nil }
