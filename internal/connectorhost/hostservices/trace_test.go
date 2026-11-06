package hostservices

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/host"
)

func TestTracesAreRedacted(t *testing.T) {
	const secret = "s3cret-token"
	srv, _ := server(t,
		func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Set-Cookie", "session=abc")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"error":"bad token `+secret+`"}`)
		},
		func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"data":[{"email":"a@example.com","n":5,"ok":true,"x":null}],"next":"p2"}`)
		})
	s, _ := newServices(t, Options{Policy: loopback(), Secrets: StaticSecrets{"k": sdk.NewSecret(secret)}})
	r := &host.Request{URL: srv.URL + "/v1/users?since=2026", Header: http.Header{"Accept": {"application/json"}}, Auth: host.Bearer{TokenSecret: "k"}}
	if _, err := do(t, s, r); err == nil {
		t.Fatal("the 400 succeeded")
	}
	if _, err := do(t, s, r); err != nil {
		t.Fatal(err)
	}
	tr := s.Traces()
	if len(tr) != 2 {
		t.Fatalf("%d traces", len(tr))
	}
	if tr[0].Status != 400 || string(tr[0].Body) != `{"error":"bad token [redacted]"}` || tr[0].ResponseHeader.Get("Set-Cookie") != redacted {
		t.Errorf("failure: %d %s %v", tr[0].Status, tr[0].Body, tr[0].ResponseHeader)
	}
	if want := `{"data":[{"email":"","n":0,"ok":false,"x":null}],"next":""}`; tr[1].Status != 200 || string(tr[1].Body) != want {
		t.Errorf("success body %s, want %s", tr[1].Body, want)
	}
	if tr[1].URL != srv.URL+"/v1/users?since=2026" || tr[1].RequestHeader.Get("Accept") != "application/json" {
		t.Errorf("request %s %v", tr[1].URL, tr[1].RequestHeader)
	}
	b, _ := json.Marshal(tr)
	if bytes.Contains(b, []byte(secret)) || bytes.Contains(b, []byte("a@example.com")) {
		t.Errorf("a trace holds a secret or a record: %s", b)
	}
}

func TestShape(t *testing.T) {
	for in, want := range map[string]string{
		`{"a":[1,2,{"b":"x`: `{"a":[0,0,{"b"`,
		`[]`:                `[]`,
		`[{"k":{}},"s"]`:    `[{"k":{}},""]`,
		`not json`:          ``,
		`<html>`:            ``,
	} {
		if got := string(shape([]byte(in))); got != want {
			t.Errorf("shape(%s) = %s, want %s", in, got, want)
		}
	}
}

func TestTracesKeepTheLatest(t *testing.T) {
	s := New(Options{})
	for i := range maxTraces + 5 {
		s.addTrace(Trace{Try: i})
	}
	tr := s.Traces()
	if len(tr) != maxTraces || tr[0].Try != 5 || tr[len(tr)-1].Try != maxTraces+4 {
		t.Errorf("%d traces, %d to %d", len(tr), tr[0].Try, tr[len(tr)-1].Try)
	}
}

func TestLogScrubsSecrets(t *testing.T) {
	var buf bytes.Buffer
	s, _ := newServices(t, Options{Secrets: StaticSecrets{"k": sdk.NewSecret("s3cret-token")}, Logger: slog.New(slog.NewTextHandler(&buf, nil))})
	if _, err := s.Sign(t.Context(), "k", host.HMAC{Hash: host.SHA256, Message: []byte("m")}); err != nil {
		t.Fatal(err)
	}
	err := s.Log(t.Context(), []host.LogEntry{{Level: slog.LevelWarn, Message: "token s3cret-token refused", Attrs: map[string]string{"token": "s3cret-token"}}})
	if err != nil {
		t.Fatal(err)
	}
	if out := buf.String(); strings.Contains(out, "s3cret") || !strings.Contains(out, "level=WARN") || !strings.Contains(out, "token=[redacted]") {
		t.Errorf("logged %s", out)
	}
}

func TestKV(t *testing.T) {
	s, c := newServices(t, Options{})
	ctx := t.Context()
	if err := s.KVPut(ctx, "job", []byte("42"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := s.KVPut(ctx, "forever", []byte("x"), 0); err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := s.KVGet(ctx, "job"); !ok || string(v) != "42" {
		t.Errorf("job = %q, %v", v, ok)
	}
	_ = c.sleep(ctx, time.Minute)
	if _, ok, _ := s.KVGet(ctx, "job"); ok {
		t.Error("job outlived its ttl")
	}
	if _, ok, _ := s.KVGet(ctx, "forever"); !ok {
		t.Error("forever expired")
	}
	if s.KVPut(ctx, "", nil, 0) == nil || s.KVPut(ctx, "big", make([]byte, maxKVValue+1), 0) == nil {
		t.Error("a bad put was taken")
	}

	// Full: a put drops others to make room.
	mib := make([]byte, maxKVValue)
	for i := range maxKVTotal/maxKVValue + 3 {
		if err := s.KVPut(ctx, string(rune('a'+i)), mib, 0); err != nil {
			t.Fatal(err)
		}
	}
	if s.kvBytes > maxKVTotal || len(s.kv) != maxKVTotal/maxKVValue {
		t.Errorf("%d bytes in %d values", s.kvBytes, len(s.kv))
	}
}
