// Package netcheck checks the network path to a host: that its name
// resolves, that it accepts a TCP connection, and, for a port that starts
// with TLS, that the handshake succeeds.
//
//	checks := netcheck.ReachTLS(cfg.Address, nil) // net.dns, net.connect, net.tls
//
// A database that negotiates TLS inside its own protocol, such as
// PostgreSQL, MySQL or SQL Server, fails the TLS check: use Reach for it, and
// let its driver's connect check cover TLS.
//
// The checks hold no state: one may run in any number of runs at once.
package netcheck

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strings"
	"time"

	"github.com/openetl-io/openetl/sdk/go/check"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

const (
	// Bounds each dial, and each TLS handshake, besides the check's context.
	DialTimeout = 10 * time.Second
	// TLS warns of a certificate that expires within this.
	ExpiresSoon = 14 * 24 * time.Hour
)

// CertificateExpiring is the reason TLS warns with, with the params address
// and expires, the date the certificate expires in UTC.
var CertificateExpiring = check.Reason{
	Name:    "certificate_expiring",
	Code:    protocolv1.ErrorCode_ERROR_CODE_TRANSIENT,
	Message: "The certificate of {address} expires on {expires}.",
	Fix:     "Renew the certificate.",
}

var (
	lookup  = net.DefaultResolver.LookupHost
	timeout = DialTimeout
)

// hostOf is address's host: a host:port's, or address itself, without the
// brackets of an IPv6 literal.
func hostOf(address string) string {
	if h, _, err := net.SplitHostPort(address); err == nil {
		return h
	}
	if strings.HasPrefix(address, "[") && strings.HasSuffix(address, "]") {
		return address[1 : len(address)-1]
	}
	return address
}

// Resolve is "net.dns", a Connection check that host, or a host:port's host,
// resolves. An IP address passes without a lookup. It sets the param host,
// and on a pass ips, the addresses the name resolves to; it fails as
// check.DNSFailed.
func Resolve(host string) check.Check {
	host = hostOf(host)
	return check.Check{
		Name: "net.dns", Kind: check.Connection, Title: "Resolve {host}",
		Params: check.Params{"host": host},
		Run: func(ctx context.Context, p *check.Probe) error {
			if net.ParseIP(host) != nil {
				p.Found("{host} is an IP address.")
				return nil
			}
			ips, err := lookup(ctx, host)
			if err != nil {
				return check.DNSFailed.Wrap(err, "host", host)
			}
			p.Param("ips", strings.Join(ips, ", "))
			p.Found("{host} resolves to {ips}.")
			return nil
		},
	}
}

func dial(ctx context.Context, address string) (net.Conn, error) {
	d := net.Dialer{Timeout: timeout}
	return d.DialContext(ctx, "tcp", address)
}

// dialFailure is err, of a dial to address, classified.
func dialFailure(err error, address string) error {
	return check.Classify(err).With("address", address)
}

// Dial is "net.connect", a Connection check that address, a host:port,
// accepts a TCP connection within DialTimeout. It sets the param address,
// and fails as check.Classify classifies the dial's error, such as
// check.ConnectionRefused or check.TimedOut.
func Dial(address string) check.Check {
	return check.Check{
		Name: "net.connect", Kind: check.Connection, Title: "Reach {address}",
		Params: check.Params{"address": address},
		Run: func(ctx context.Context, _ *check.Probe) error {
			conn, err := dial(ctx, address)
			if err != nil {
				return dialFailure(err, address)
			}
			return conn.Close()
		},
	}
}

// TLS is "net.tls", a Connection check that address, a host:port that
// starts with TLS such as an HTTPS or LDAPS port, completes a handshake that
// cfg verifies, dialling and handshaking within DialTimeout each. A database
// port that starts TLS inside its own protocol fails it. Nil cfg: TLS 1.2 or
// later, verified against the system's roots; an empty ServerName: address's
// host.
//
// It sets the param address, and on a pass tls_version and expires. It
// fails as check.TLSFailed, or as check.TimedOut when the handshake runs out
// of time, and warns as CertificateExpiring of a certificate that expires
// within ExpiresSoon.
func TLS(address string, cfg *tls.Config) check.Check {
	if cfg == nil {
		cfg = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	if cfg.ServerName == "" {
		cfg = cfg.Clone()
		cfg.ServerName = hostOf(address)
	}
	return check.Check{
		Name: "net.tls", Kind: check.Connection, Title: "Agree on TLS with {address}",
		Params: check.Params{"address": address},
		Run: func(ctx context.Context, p *check.Probe) error {
			raw, err := dial(ctx, address)
			if err != nil {
				return dialFailure(err, address)
			}
			hctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			conn := tls.Client(raw, cfg)
			defer func() { _ = conn.Close() }()
			if err := conn.HandshakeContext(hctx); err != nil {
				return handshakeFailure(hctx, err, address)
			}
			st := conn.ConnectionState()
			p.Param("tls_version", tls.VersionName(st.Version))
			if len(st.PeerCertificates) == 0 {
				p.Found("{tls_version}.")
				return nil
			}
			expires := st.PeerCertificates[0].NotAfter
			p.Param("expires", expires.UTC().Format(time.DateOnly))
			p.Found("{tls_version}, with a certificate that expires on {expires}.")
			if time.Until(expires) < ExpiresSoon {
				return check.Warn(CertificateExpiring.Fail())
			}
			return nil
		},
	}
}

func handshakeFailure(ctx context.Context, err error, address string) error {
	if ne, ok := errors.AsType[net.Error](err); ctx.Err() != nil || ok && ne.Timeout() {
		f := check.TimedOut.Wrap(err, "address", address)
		f.Message = "{address} didn't finish the TLS handshake in time."
		return f
	}
	f := check.TLSFailed.Wrap(err, "address", address)
	if _, ok := errors.AsType[tls.RecordHeaderError](err); ok {
		f.Message = "{address} didn't answer with TLS."
		f.Fix = "Check that the port starts with TLS, as HTTPS and LDAPS ports do. A database that starts TLS inside its own protocol, such as PostgreSQL, MySQL or SQL Server, can't be checked this way."
	}
	return f
}

// Reach is Resolve, then Dial, which needs "net.dns".
func Reach(address string) []check.Check {
	c := Dial(address)
	c.Needs = []string{"net.dns"}
	return []check.Check{Resolve(address), c}
}

// ReachTLS is Reach, then TLS, which needs "net.connect". It's for a port
// that starts with TLS; see TLS.
func ReachTLS(address string, cfg *tls.Config) []check.Check {
	c := TLS(address, cfg)
	c.Needs = []string{"net.connect"}
	return append(Reach(address), c)
}
