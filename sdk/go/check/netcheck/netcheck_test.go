package netcheck

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/openetl-io/openetl/sdk/go/check"
)

// serveTLS serves handshakes for 127.0.0.1 with a certificate that expires
// after life, and returns its address and a pool that trusts it.
func serveTLS(t *testing.T, life time.Duration) (string, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(life),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				_ = c.(*tls.Conn).Handshake()
				_ = c.Close()
			}()
		}
	}()
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return ln.Addr().String(), pool
}

// serve accepts connections on 127.0.0.1 and gives each to handle.
func serve(t *testing.T, handle func(net.Conn)) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go handle(c)
		}
	}()
	return ln.Addr().String()
}

func closedPort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	return port
}

func statuses(r check.Report) string {
	var s []string
	for _, it := range r.Items {
		s = append(s, it.Name+"="+it.Status.String())
	}
	return strings.Join(s, " ")
}

func TestReachTLS(t *testing.T) {
	addr, pool := serveTLS(t, 365*24*time.Hour)
	r := check.Run(t.Context(), ReachTLS(addr, &tls.Config{RootCAs: pool}))
	if got := statuses(r); got != "net.dns=passed net.connect=passed net.tls=passed" {
		t.Fatalf("%s: %+v", got, r.Items[2].Failure)
	}
	if got := r.Items[1].Rendered().Title; got != "Reach "+addr {
		t.Errorf("title %q", got)
	}
	if got := r.Items[2].Rendered().Message; !strings.HasPrefix(got, "TLS 1.3, with a certificate that expires on ") {
		t.Errorf("tls's message %q", got)
	}

	r = check.Run(t.Context(), ReachTLS(addr, nil))
	f := r.Items[2].Failure
	if statuses(r) != "net.dns=passed net.connect=passed net.tls=failed" || f.Reason != "tls_failed" || f.Params["address"] != addr {
		t.Errorf("an untrusted certificate: %s %+v", statuses(r), f)
	}

	soon, pool := serveTLS(t, 24*time.Hour)
	r = check.Run(t.Context(), ReachTLS(soon, &tls.Config{RootCAs: pool}))
	it := r.Items[2].Rendered()
	if statuses(r) != "net.dns=passed net.connect=passed net.tls=warning" || it.Failure.Reason != "certificate_expiring" ||
		it.Message != "The certificate of "+soon+" expires on "+it.Params["expires"]+"." || it.Params["expires"] == "" || !r.OK() {
		t.Errorf("a certificate that expires tomorrow: %s %+v", statuses(r), it)
	}
}

func TestReachFailures(t *testing.T) {
	addr := "127.0.0.1:" + closedPort(t)
	r := check.Run(t.Context(), ReachTLS(addr, nil))
	it := r.Items[1].Rendered()
	if statuses(r) != "net.dns=passed net.connect=failed net.tls=skipped" || it.Failure.Reason != "connection_refused" ||
		it.Message != addr+" refused the connection." {
		t.Errorf("a closed port: %s %+v", statuses(r), it)
	}

	old := lookup
	lookup = func(_ context.Context, host string) ([]string, error) {
		return nil, &net.DNSError{Name: host, Err: "no such host", IsNotFound: true}
	}
	defer func() { lookup = old }()
	r = check.Run(t.Context(), Reach("db.nowhere.example:5432"))
	f := r.Items[0].Failure
	if statuses(r) != "net.dns=failed net.connect=skipped" || f.Reason != "dns_failed" || f.Params["host"] != "db.nowhere.example" {
		t.Errorf("a name that doesn't resolve: %s %+v", statuses(r), f)
	}
	if got := r.Err().Error(); !strings.HasPrefix(got, "Resolve db.nowhere.example: The host name db.nowhere.example doesn't resolve: ") {
		t.Errorf("the error: %s", got)
	}
}

// The title names the address as configured, whether the check passes or
// fails, and not the IP address it dialled.
func TestTitleKeepsTheConfiguredAddress(t *testing.T) {
	open := serve(t, func(c net.Conn) { _ = c.Close() })
	_, port, _ := net.SplitHostPort(open)
	for _, addr := range []string{"localhost:" + port, "localhost:" + closedPort(t)} {
		it := check.Run(t.Context(), Reach(addr)).Items[1].Rendered()
		if it.Title != "Reach "+addr || it.Params["address"] != addr {
			t.Errorf("%s, %s: title %q, params %v", addr, it.Status, it.Title, it.Params)
		}
		if it.Status == check.Failed && it.Message != addr+" refused the connection." {
			t.Errorf("%s: message %q", addr, it.Message)
		}
	}
}

func TestTLSAgainstAServerThatDoesntStartWithTLS(t *testing.T) {
	// As PostgreSQL does: read a startup packet, refuse it, and close.
	addr := serve(t, func(c net.Conn) {
		_, _ = c.Read(make([]byte, 8))
		_, _ = c.Write([]byte("EFATAL invalid length of startup packet\x00"))
		_ = c.Close()
	})
	it := check.Run(t.Context(), ReachTLS(addr, nil)).Items[2].Rendered()
	if it.Failure.Reason != "tls_failed" || it.Message != addr+" didn't answer with TLS." || !strings.Contains(it.Failure.Fix, "PostgreSQL") {
		t.Errorf("%s %+v", it.Status, it.Failure)
	}
}

func TestTLSHandshakeTimesOut(t *testing.T) {
	addr := serve(t, func(c net.Conn) {
		time.Sleep(5 * time.Second)
		_ = c.Close()
	})
	old := timeout
	timeout = 200 * time.Millisecond
	defer func() { timeout = old }()
	start := time.Now()
	it := check.Run(t.Context(), []check.Check{TLS(addr, &tls.Config{InsecureSkipVerify: true})}).Items[0].Rendered()
	if it.Status != check.Failed || it.Failure.Reason != "timed_out" || it.Message != addr+" didn't finish the TLS handshake in time." {
		t.Errorf("%s %+v", it.Status, it.Failure)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Errorf("the handshake took %s", d)
	}
}

func TestResolveLiterals(t *testing.T) {
	old := lookup
	lookup = func(context.Context, string) ([]string, error) {
		t.Error("an IP address was looked up")
		return nil, nil
	}
	defer func() { lookup = old }()
	for _, in := range []string{"[::1]", "[::1]:5432", "::1", "127.0.0.1", "127.0.0.1:5432"} {
		it := check.Run(t.Context(), []check.Check{Resolve(in)}).Items[0].Rendered()
		if it.Status != check.Passed || strings.ContainsAny(it.Params["host"], "[]") {
			t.Errorf("%s: %s %v %+v", in, it.Status, it.Params, it.Failure)
		}
	}
}
