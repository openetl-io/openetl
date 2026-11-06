package netguard_test

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"testing"

	"github.com/openetl-io/openetl/internal/netguard"
)

func TestCheck(t *testing.T) {
	blocked := []string{
		"127.0.0.1", "10.1.2.3", "172.31.255.255", "192.168.0.1", "169.254.169.254",
		"100.64.0.1", "0.0.0.0", "192.0.0.192", "198.18.0.1", "224.0.0.1", "255.255.255.255",
		"::1", "::", "fe80::1", "fe80::1%eth0", "fd00:ec2::254", "fc00::1", "ff02::1",
		"::ffff:127.0.0.1", "::ffff:169.254.169.254", "::127.0.0.1", "64:ff9b::a00:1",
		"64:ff9b:1::1", "2002:a00:1::", "2001:db8::1", "2001::1",
	}
	public := []string{
		"8.8.8.8", "1.1.1.1", "93.184.216.34", "2606:4700:4700::1111", "::ffff:8.8.8.8",
		"64:ff9b::808:808", "172.32.0.1", "100.128.0.1",
	}
	var p netguard.Policy
	for _, s := range blocked {
		err := p.Check(netip.MustParseAddr(s))
		if _, ok := errors.AsType[*netguard.BlockedError](err); !ok {
			t.Errorf("%s: got %v, want blocked", s, err)
		}
	}
	for _, s := range public {
		if err := p.Check(netip.MustParseAddr(s)); err != nil {
			t.Errorf("%s: %v", s, err)
		}
	}
}

func TestAllow(t *testing.T) {
	allow, err := netguard.ParsePrefixes("10.0.0.0/8, 127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	p := netguard.Policy{Allow: allow}
	for _, s := range []string{"10.9.9.9", "127.0.0.1", "::ffff:10.0.0.1", "64:ff9b::a00:1"} {
		if err := p.Check(netip.MustParseAddr(s)); err != nil {
			t.Errorf("%s: %v", s, err)
		}
	}
	for _, s := range []string{"127.0.0.2", "192.168.1.1", "169.254.169.254"} {
		if p.Check(netip.MustParseAddr(s)) == nil {
			t.Errorf("%s: allowed", s)
		}
	}
	if _, err := netguard.ParsePrefixes("10.0.0.0/33"); err == nil {
		t.Error("bad CIDR parsed")
	}
}

// The check runs on the address dialed, after resolution, so names that
// resolve to blocked addresses are refused too.
func TestClientRefusesBlockedAddresses(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()
	_, port, _ := net.SplitHostPort(srv.Listener.Addr().String())
	c := (&netguard.Policy{}).Client()
	for _, u := range []string{srv.URL, "http://localhost:" + port} {
		_, err := get(t, c, u)
		if _, ok := errors.AsType[*netguard.BlockedError](err); !ok {
			t.Errorf("%s: got %v, want blocked", u, err)
		}
	}
}

func TestRedirects(t *testing.T) {
	var hops int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/loop":
			hops++
			http.Redirect(w, r, "/loop", http.StatusFound)
		case "/private":
			http.Redirect(w, r, "http://127.0.0.2:1/", http.StatusFound)
		case "/ftp":
			http.Redirect(w, r, "ftp://example.com/", http.StatusFound)
		}
	}))
	defer srv.Close()
	c := (&netguard.Policy{Allow: []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}}).Client()

	_, err := get(t, c, srv.URL+"/loop")
	if _, ok := errors.AsType[*netguard.RedirectError](err); !ok || hops != netguard.MaxRedirects {
		t.Errorf("loop: %d hops, %v", hops, err)
	}
	_, err = get(t, c, srv.URL+"/private")
	if _, ok := errors.AsType[*netguard.BlockedError](err); !ok {
		t.Errorf("redirect to a blocked address: %v", err)
	}
	_, err = get(t, c, srv.URL+"/ftp")
	if _, ok := errors.AsType[*netguard.RedirectError](err); !ok {
		t.Errorf("redirect to ftp: %v", err)
	}
}

func TestTransportIgnoresProxyEnvironment(t *testing.T) {
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	tr := (&netguard.Policy{}).Transport()
	if tr.Proxy != nil {
		u, _ := tr.Proxy(&http.Request{URL: &url.URL{Scheme: "https", Host: "example.com"}})
		t.Errorf("proxy %v", u)
	}
}

func TestLimitBody(t *testing.T) {
	for _, tc := range []struct {
		body  string
		limit int64
		ok    bool
	}{{"hello", 5, true}, {"hello", 4, false}, {"", 0, true}, {"x", 0, false}} {
		b, err := io.ReadAll(netguard.LimitBody(io.NopCloser(strings.NewReader(tc.body)), tc.limit))
		if tc.ok != (err == nil) || (!tc.ok && !errors.Is(err, netguard.ErrTooLarge)) {
			t.Errorf("%q, limit %d: %v", tc.body, tc.limit, err)
		}
		if int64(len(b)) > tc.limit {
			t.Errorf("%q, limit %d: read %d bytes", tc.body, tc.limit, len(b))
		}
	}
}

func get(t *testing.T, c *http.Client, u string) (string, error) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, u, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return fmt.Sprint(resp.StatusCode, string(b)), err
}
