package daemon

import (
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/unsuitable001/openetl/internal/api"
	"github.com/unsuitable001/openetl/internal/platform/config"
)

func TestConfigureSignIn(t *testing.T) {
	addr := func(ip string) net.Addr { return &net.TCPAddr{IP: net.ParseIP(ip), Port: 8080} }
	cases := []struct {
		name   string
		cfg    config.Config
		addr   net.Addr
		public string
		err    string
	}{
		{"passwords on loopback", config.Config{Auth: "password"}, addr("127.0.0.1"), "http://localhost:8080", ""},
		{"the struct's zero value", config.Config{}, addr("10.0.0.5"), "http://10.0.0.5:8080", ""},
		{"every address", config.Config{Auth: "password"}, addr("::"), "http://localhost:8080", ""},
		{"authless on loopback", config.Config{Auth: "none"}, addr("::1"), "http://localhost:8080", ""},
		{"authless elsewhere", config.Config{Auth: "none"}, addr("0.0.0.0"), "", "isn't a loopback address"},
		{"authless elsewhere, allowed", config.Config{Auth: "none", AuthNoneAllowRemote: true}, addr("10.0.0.5"), "http://10.0.0.5:8080", ""},
		{"a public URL", config.Config{PublicURL: "https://etl.example.com"}, addr("10.0.0.5"), "https://etl.example.com", ""},
		{"not a URL", config.Config{PublicURL: "etl.example.com"}, addr("10.0.0.5"), "", "isn't an http or https URL"},
		{"a fragment", config.Config{PublicURL: "https://etl.example.com/#x"}, addr("10.0.0.5"), "", "isn't an http or https URL"},
		{"a bad duration", config.Config{SessionMaxAge: "7d"}, addr("127.0.0.1"), "", "isn't a positive duration"},
	}
	for _, c := range cases {
		srv := &api.Server{}
		err := configureSignIn(srv, c.cfg, c.addr, slog.New(slog.DiscardHandler))
		switch {
		case c.err == "" && err != nil:
			t.Errorf("%s: %v", c.name, err)
		case c.err != "" && (err == nil || !strings.Contains(err.Error(), c.err)):
			t.Errorf("%s: got %v, want %q", c.name, err, c.err)
		case c.err == "" && srv.PublicURL != c.public:
			t.Errorf("%s: public URL %q, want %q", c.name, srv.PublicURL, c.public)
		case c.err == "" && srv.Authless != (c.cfg.Auth == "none"):
			t.Errorf("%s: authless %v", c.name, srv.Authless)
		}
	}
	srv := &api.Server{}
	if err := configureSignIn(srv, config.Config{SessionIdleTimeout: "30m", SessionMaxAge: "24h"}, addr("127.0.0.1"), slog.New(slog.DiscardHandler)); err != nil {
		t.Fatal(err)
	}
	if srv.SessionIdle != 30*time.Minute || srv.SessionMaxAge != 24*time.Hour {
		t.Fatalf("sessions %v and %v", srv.SessionIdle, srv.SessionMaxAge)
	}
}
