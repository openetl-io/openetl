package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"

	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/host"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// jsonlConnector is a stage-and-apply destination of JSON lines:
// {dir}/{namespace}/{stream}.jsonl, appended to by each applied epoch, with
// the epoch's staged rows, its marker and the fence beside them. A reader
// cats the files.
var jsonlConnector = sdk.Connector{
	Name:         "jsonl",
	ConfigSchema: []byte(`{"type": "object", "properties": {"dir": {"type": "string"}, "token": {"type": "string", "x-secret": true}}, "required": ["dir"]}`),
	Egress:       sdk.Egress{Tier: sdk.EgressRaw},
	Destination:  newJSONL,
	Capabilities: destination.Capabilities{
		Class: destination.StageAndApply, Scopes: []destination.Scope{destination.ScopePipeline}, MaxIdentifierLength: 63,
		WriteModes: []destination.WriteMode{destination.Append},
	},
}

type jsonl struct{ dir, token string }

func newJSONL(config []byte, _ host.Host) (destination.Destination, error) {
	var c struct {
		Dir   string `json:"dir"`
		Token string `json:"token"`
	}
	if err := json.Unmarshal(config, &c); err != nil {
		return nil, err
	}
	return &jsonl{dir: c.Dir, token: c.Token}, nil
}

func (d *jsonl) Capabilities() destination.Capabilities { return destination.Capabilities{} }

func (d *jsonl) Check(context.Context) error {
	if d.token != "s3cr3t" {
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID, errors.New("jsonl: wrong token"))
	}
	return nil
}

func (d *jsonl) Open(ctx context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	if err := d.Check(ctx); err != nil {
		return nil, err
	}
	ns := filepath.Join(d.dir, cfg.Namespace)
	if err := os.MkdirAll(filepath.Join(ns, ".epochs"), 0o750); err != nil {
		return nil, err
	}
	return &jsonlWriter{ns: ns, gen: cfg.Generation, pending: map[string]map[string][]json.RawMessage{}}, nil
}

type jsonlWriter struct {
	ns      string
	gen     uint64
	mu      sync.Mutex
	pending map[string]map[string][]json.RawMessage
}

func (w *jsonlWriter) path(parts ...string) string {
	return filepath.Clean(filepath.Join(append([]string{w.ns}, parts...)...))
}

func (w *jsonlWriter) Write(_ context.Context, e destination.Epoch, stream string, b arrow.RecordBatch) error {
	j, err := b.MarshalJSON()
	if err != nil {
		return err
	}
	var rows []json.RawMessage
	if err := json.Unmarshal(j, &rows); err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	name := epochName(e)
	if w.pending[name] == nil {
		w.pending[name] = map[string][]json.RawMessage{}
	}
	w.pending[name][stream] = append(w.pending[name][stream], rows...)
	return nil
}

func (w *jsonlWriter) PreCommit(_ context.Context, e destination.Epoch) (destination.Staged, error) {
	name := epochName(e)
	w.mu.Lock()
	rows := w.pending[name]
	delete(w.pending, name)
	w.mu.Unlock()
	b, err := json.Marshal(rows)
	if err != nil {
		return nil, err
	}
	return destination.Staged(name), os.WriteFile(w.path(".epochs", name+".staged"), b, 0o600)
}

func (w *jsonlWriter) fence() uint64 {
	b, _ := os.ReadFile(w.path(".fence"))
	n, _ := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	return n
}

func (w *jsonlWriter) Commit(_ context.Context, _ destination.Epoch, staged []destination.Staged) (destination.Committed, error) {
	s, err := destination.One(staged)
	if err != nil {
		return destination.Committed{}, err
	}
	if w.fence() > w.gen {
		return destination.Committed{}, destination.ErrFenced
	}
	return destination.Committed{}, w.apply(string(s))
}

// apply appends an epoch's staged rows to its streams' files, once.
func (w *jsonlWriter) apply(name string) error {
	marker := w.path(".epochs", name+".applied")
	if _, err := os.Stat(marker); err == nil {
		return nil
	}
	b, err := os.ReadFile(w.path(".epochs", name+".staged"))
	if err != nil {
		return err
	}
	var rows map[string][]json.RawMessage
	if err := json.Unmarshal(b, &rows); err != nil {
		return err
	}
	for stream, rs := range rows {
		var buf bytes.Buffer
		for _, r := range rs {
			buf.Write(r)
			buf.WriteByte('\n')
		}
		f, err := os.OpenFile(w.path(stream+".jsonl"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			return err
		}
		_, err = f.Write(buf.Bytes())
		if err = errors.Join(err, f.Close()); err != nil {
			return err
		}
	}
	return os.WriteFile(marker, nil, 0o600)
}

func (w *jsonlWriter) Recover(_ context.Context, gen uint64, e destination.Epoch, staged []destination.Staged) (destination.Recovery, error) {
	s, err := destination.One(staged)
	if err != nil {
		return 0, err
	}
	if w.fence() > gen {
		return 0, destination.ErrFenced
	}
	if err := os.WriteFile(w.path(".fence"), []byte(strconv.FormatUint(gen, 10)), 0o600); err != nil {
		return 0, err
	}
	if e.IsZero() {
		return destination.Applied, nil
	}
	if _, err := os.Stat(w.path(".epochs", string(s)+".applied")); err == nil {
		return destination.Applied, nil
	}
	if _, err := os.Stat(w.path(".epochs", string(s)+".staged")); err != nil {
		return destination.Lost, nil
	}
	if err := w.apply(string(s)); err != nil {
		return 0, fmt.Errorf("redo %s: %w", s, err)
	}
	return destination.Redone, nil
}

func (w *jsonlWriter) Close(context.Context) error { return nil }
