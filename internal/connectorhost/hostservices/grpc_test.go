package hostservices

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/unsuitable001/openetl/internal/platform/errs"
	sdk "github.com/unsuitable001/openetl/sdk/go"
	"github.com/unsuitable001/openetl/sdk/go/host"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
)

// overSocket serves s over a Unix socket and returns the SDK's client for it.
func overSocket(t *testing.T, s *Services) *host.Client {
	t.Helper()
	// t.TempDir is too long for sun_path.
	dir, err := os.MkdirTemp("", "hs-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	path := filepath.Join(dir, "h.sock")
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "unix", path)
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	protocolv1.RegisterHostServiceServer(srv, NewServer(s))
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("unix://"+path, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return host.NewClient(conn)
}

func TestOverTheSocket(t *testing.T) {
	big := bytes.Repeat([]byte("x"), 3*bodyChunk+7)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/big":
			if r.Header.Get("Authorization") != "Bearer tok" || r.Header.Get("X-Extra") != "1" {
				w.WriteHeader(http.StatusTeapot)
				return
			}
			w.Header().Set("X-Reply", "yes")
			_, _ = w.Write(big)
		default:
			w.Header().Set("Retry-After", "7")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = io.WriteString(w, "slow down")
		}
	}))
	defer api.Close()
	s := New(Options{Policy: loopback(), Secrets: StaticSecrets{"token": sdk.NewSecret("tok"), "key": sdk.NewSecret("k")}, Budget: time.Second})
	defer s.Close()
	c := overSocket(t, s)
	ctx := t.Context()

	resp, err := c.HTTP(ctx, &host.Request{URL: api.URL + "/big", Header: http.Header{"X-Extra": {"1"}}, Auth: host.Bearer{TokenSecret: "token"}})
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil || resp.Status != 200 || resp.Header.Get("X-Reply") != "yes" || !bytes.Equal(body, big) {
		t.Fatalf("status %d, %d bytes, err %v", resp.Status, len(body), err)
	}

	_, err = c.HTTP(ctx, &host.Request{URL: api.URL + "/slow", Budget: time.Millisecond})
	se, ok := err.(*host.StatusError) //nolint:errorlint // the client returns it unwrapped
	if !ok || se.Status != 429 || se.Code != protocolv1.ErrorCode_ERROR_CODE_RATE_LIMITED || se.After != 7*time.Second || string(se.Body) != "slow down" {
		t.Fatalf("err = %#v", err)
	}

	_, err = c.HTTP(ctx, &host.Request{URL: "ftp://example.com"})
	if errs.CodeOf(err) != errs.ConfigInvalid {
		t.Fatalf("a bad URL: %v", err)
	}

	sig, err := c.Sign(ctx, "key", host.HMAC{Hash: host.SHA256, Message: []byte("m")})
	mac := hmac.New(sha256.New, []byte("k"))
	mac.Write([]byte("m"))
	if err != nil || !bytes.Equal(sig, mac.Sum(nil)) {
		t.Fatalf("sign = %x, %v", sig, err)
	}

	if err := c.KVPut(ctx, "a", []byte{}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := c.KVGet(ctx, "a"); err != nil || !ok || len(v) != 0 {
		t.Fatalf("an empty value: %q, %v, %v", v, ok, err)
	}
	if _, ok, err := c.KVGet(ctx, "missing"); err != nil || ok {
		t.Fatalf("a missing key: %v, %v", ok, err)
	}
	if now, err := c.Now(ctx); err != nil || time.Since(now) > time.Minute {
		t.Fatalf("now = %v, %v", now, err)
	}
	if err := c.Log(ctx, []host.LogEntry{{Message: "hi"}}); err != nil {
		t.Fatal(err)
	}
}

func TestGetSecretOnlyForTheTiersThatBuildAuth(t *testing.T) {
	secrets := StaticSecrets{"dsn": sdk.NewSecret("postgres://u:p@h/db")}
	denied := overSocket(t, New(Options{Secrets: secrets}))
	if _, err := denied.GetSecret(t.Context(), "dsn"); errs.CodeOf(err) != errs.PermissionDenied {
		t.Fatalf("host tier: %v", err)
	}
	allowed := overSocket(t, New(Options{Secrets: secrets, RevealSecrets: true}))
	if v, err := allowed.GetSecret(t.Context(), "dsn"); err != nil || v != "postgres://u:p@h/db" {
		t.Fatalf("raw tier: %q, %v", v, err)
	}
	if _, err := allowed.GetSecret(t.Context(), "other"); errs.CodeOf(err) != errs.ConfigInvalid {
		t.Fatalf("a missing secret: %v", err)
	}
}
