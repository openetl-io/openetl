// Package netguard guards outbound connections against SSRF. Only it
// constructs outbound HTTP transports.
//
// The dialer checks the address of every connection it opens, after DNS
// resolution, so a name can't resolve to one address for a check and
// another for the connection. Redirects dial through the same dialer.
package netguard

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"syscall"
	"time"
)

// MaxRedirects is how many redirects a client follows.
const MaxRedirects = 5

// Everything but public unicast: private, loopback, link-local (and so
// cloud metadata), shared, documentation, benchmarking, multicast and
// reserved ranges. ::/8 also covers IPv4-compatible addresses and the
// local-use NAT64 prefix.
var blocked = prefixes(
	"0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16",
	"172.16.0.0/12", "192.0.0.0/24", "192.0.2.0/24", "192.88.99.0/24", "192.168.0.0/16",
	"198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "224.0.0.0/4", "240.0.0.0/4",
	"::/8", "100::/64", "2001::/23", "2001:db8::/32", "2002::/16", "3fff::/20",
	"5f00::/16", "fc00::/7", "fe80::/10", "fec0::/10", "ff00::/8",
)

// The well-known NAT64 prefix embeds an IPv4 address, which is checked
// instead.
var nat64 = netip.MustParsePrefix("64:ff9b::/96")

// Policy decides which addresses connections may reach. The zero value
// allows public unicast addresses only.
type Policy struct {
	// Addresses allowed even though they're blocked by default, such as a
	// hybrid data plane's internal networks.
	Allow []netip.Prefix
}

// BlockedError is a connection the policy refused.
type BlockedError struct {
	Addr netip.Addr
}

func (e *BlockedError) Error() string {
	return fmt.Sprintf("netguard: %s is not a public address", e.Addr)
}

// Check returns a *BlockedError if a connection to addr isn't allowed.
func (p *Policy) Check(addr netip.Addr) error {
	a := addr.Unmap().WithZone("")
	if p.allowed(a) {
		return nil
	}
	if nat64.Contains(a) {
		b := a.As16()
		v4 := netip.AddrFrom4([4]byte(b[12:]))
		if p.allowed(v4) || !isBlocked(v4) {
			return nil
		}
		return &BlockedError{Addr: addr}
	}
	if isBlocked(a) {
		return &BlockedError{Addr: addr}
	}
	return nil
}

func (p *Policy) allowed(a netip.Addr) bool {
	for _, q := range p.Allow {
		if q.Contains(a) {
			return true
		}
	}
	return false
}

func isBlocked(a netip.Addr) bool {
	for _, q := range blocked {
		if q.Contains(a) {
			return true
		}
	}
	return false
}

// Dialer returns a TCP dialer that refuses addresses the policy blocks.
func (p *Policy) Dialer() *net.Dialer {
	return &net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
		Control: func(network, address string, _ syscall.RawConn) error {
			if network != "tcp4" && network != "tcp6" {
				return fmt.Errorf("netguard: network %s isn't allowed", network)
			}
			ap, err := netip.ParseAddrPort(address)
			if err != nil {
				return fmt.Errorf("netguard: %w", err)
			}
			return p.Check(ap.Addr())
		},
	}
}

// Transport returns an HTTP transport that dials through Dialer. It ignores
// the proxy environment variables.
func (p *Policy) Transport() *http.Transport {
	return &http.Transport{
		DialContext:           p.Dialer().DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: time.Minute,
		ExpectContinueTimeout: time.Second,
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
	}
}

// Client returns a client on Transport that follows at most MaxRedirects
// redirects, to http and https URLs only.
func (p *Policy) Client() *http.Client {
	return p.ClientVia(nil)
}

// ClientVia is Client, with each hop going through wrap's RoundTripper
// before Transport. Redirects are hops of their own.
func (p *Policy) ClientVia(wrap func(http.RoundTripper) http.RoundTripper) *http.Client {
	var rt http.RoundTripper = p.Transport()
	if wrap != nil {
		rt = wrap(rt)
	}
	return &http.Client{Transport: rt, CheckRedirect: checkRedirect}
}

func checkRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= MaxRedirects {
		return &RedirectError{Msg: fmt.Sprintf("stopped after %d redirects", MaxRedirects)}
	}
	if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
		return &RedirectError{Msg: fmt.Sprintf("redirect to scheme %q", req.URL.Scheme)}
	}
	return nil
}

// RedirectError is a redirect a client refused.
type RedirectError struct {
	Msg string
}

func (e *RedirectError) Error() string { return "netguard: " + e.Msg }

// ParsePrefixes parses a comma-separated list of CIDRs or addresses, for
// Policy.Allow.
func ParsePrefixes(s string) ([]netip.Prefix, error) {
	var out []netip.Prefix
	for f := range strings.SplitSeq(s, ",") {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		if !strings.Contains(f, "/") {
			a, err := netip.ParseAddr(f)
			if err != nil {
				return nil, err
			}
			out = append(out, netip.PrefixFrom(a, a.BitLen()))
			continue
		}
		q, err := netip.ParsePrefix(f)
		if err != nil {
			return nil, err
		}
		out = append(out, q.Masked())
	}
	return out, nil
}

// ErrTooLarge is a read past LimitBody's limit.
var ErrTooLarge = errors.New("netguard: response body is too large")

// LimitBody fails reads past n bytes with ErrTooLarge.
func LimitBody(rc io.ReadCloser, n int64) io.ReadCloser {
	return &limited{rc: rc, n: n}
}

type limited struct {
	rc io.ReadCloser
	n  int64
}

func (l *limited) Read(b []byte) (int, error) {
	if l.n < 0 {
		return 0, ErrTooLarge
	}
	// One byte past the limit tells a body of exactly n bytes from a
	// longer one.
	if int64(len(b)) > l.n+1 {
		b = b[:l.n+1]
	}
	k, err := l.rc.Read(b)
	l.n -= int64(k)
	if l.n < 0 {
		return k + int(l.n), ErrTooLarge
	}
	return k, err
}

func (l *limited) Close() error { return l.rc.Close() }

func prefixes(ss ...string) []netip.Prefix {
	out := make([]netip.Prefix, len(ss))
	for i, s := range ss {
		out[i] = netip.MustParsePrefix(s)
	}
	return out
}
