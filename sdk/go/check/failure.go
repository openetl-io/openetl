package check

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"maps"
	"net"
	"net/url"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/openetl-io/openetl/sdk/go/internal/redact"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

type coded interface {
	error
	ErrorCode() protocolv1.ErrorCode
}

type delayed interface {
	error
	RetryAfter() time.Duration
}

// Reason is a kind of failure: a standard one, or a connector's own.
type Reason struct {
	// Lowercase words joined by "_".
	Name string
	// Unspecified: INTERNAL.
	Code protocolv1.ErrorCode
	// Templates. Fix is optional.
	Message string
	Fix     string
}

// The standard reasons.
var (
	DNSFailed           = Reason{"dns_failed", transient, "The host name {host} doesn't resolve.", "Check the host name in the config, and that the runner's network resolves it."}
	ConnectionRefused   = Reason{"connection_refused", transient, "{address} refused the connection.", "Check the host and port, and that the service listens there."}
	TimedOut            = Reason{"timed_out", transient, "{address} didn't answer in time.", "Check that firewalls, security groups and allowlists let the runner reach it."}
	TLSFailed           = Reason{"tls_failed", transient, "The TLS handshake with {address} failed.", "Check the certificate the upstream presents, and the TLS settings in the config."}
	Unreachable         = Reason{"unreachable", transient, "Can't reach {address}.", "Check the address in the config, and the network path from the runner."}
	CredentialsRejected = Reason{"credentials_rejected", protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID, "The upstream rejected the credentials.", "Check the credentials in the config and the secrets they name."}
	CredentialsExpired  = Reason{"credentials_expired", protocolv1.ErrorCode_ERROR_CODE_AUTH_EXPIRED, "The credentials have expired.", "Renew the credentials and update their secret."}
	PermissionDenied    = Reason{"permission_denied", protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, "The upstream denied access.", "Grant the connector's identity the access this check names."}
	PrivilegeMissing    = Reason{"privilege_missing", protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, "{principal} has no {privilege} on {object}.", "Grant {privilege} on {object} to {principal}."}
	NotFound            = Reason{"not_found", configInvalid, "{resource} doesn't exist.", "Check the name in the config, or create it."}
	ConfigInvalid       = Reason{"config_invalid", configInvalid, "The config doesn't work with the upstream.", "Correct the config."}
	UnsupportedVersion  = Reason{"unsupported_version", configInvalid, "The upstream's version is {found}, and the connector needs {required}.", ""}
	SettingMismatch     = Reason{"setting_mismatch", configInvalid, "{setting} is {found}, and the connector needs {required}.", "Set {setting} to {required}."}
	RateLimited         = Reason{"rate_limited", protocolv1.ErrorCode_ERROR_CODE_RATE_LIMITED, "The upstream is limiting requests.", "Try again later, or raise the limit."}
	QuotaExceeded       = Reason{"quota_exceeded", protocolv1.ErrorCode_ERROR_CODE_RESOURCE_EXHAUSTED, "A quota of the upstream is used up.", "Free capacity, or raise the quota."}
	Unavailable         = Reason{"unavailable", transient, "The upstream can't serve requests now.", "Try again later."}
	Unknown             = Reason{"unknown", protocolv1.ErrorCode_ERROR_CODE_INTERNAL, "The check failed.", ""}
)

const (
	transient     = protocolv1.ErrorCode_ERROR_CODE_TRANSIENT
	configInvalid = protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID
)

// unknownWarning is Unknown's message on a warning, which didn't fail.
const unknownWarning = "The check raised a warning."

// Fail takes params as name and value pairs; a name without a value gets
// the value "!MISSING".
func (r Reason) Fail(kv ...string) *Failure {
	return normal(&Failure{Reason: r.Name, Code: r.Code, Message: r.Message, Fix: r.Fix, Params: pairs(kv)})
}

// Wrap is Fail with err as the cause, its text the Detail. The code stays
// the reason's. Nil for a nil err.
func (r Reason) Wrap(err error, kv ...string) *Failure {
	if isNil(err) {
		return nil
	}
	f := r.Fail(kv...)
	f.Detail, f.After, f.cause = detailOf(err, err.Error()), afterIn(err), err
	return f
}

// Failure is the error a check fails with. Its methods take a nil receiver.
//
// A Failure built by hand is read as Unknown's where it leaves something
// out: an empty Reason as "unknown", an unspecified Code as INTERNAL, and an
// empty Message as Unknown's.
type Failure struct {
	// Lowercase words joined by "_".
	Reason string
	Code   protocolv1.ErrorCode
	// Templates. Fix is optional.
	Message string
	Fix     string
	// The cause's text. Not a template. Never secrets.
	Detail string
	Params Params
	// The protocol's retry_after.
	After time.Duration
	cause error
}

// normal is f with what a hand-built Failure leaves out filled in, as a
// copy; nil for a nil f.
func normal(f *Failure) *Failure {
	if f == nil {
		return nil
	}
	c := *f
	if c.Reason == "" {
		c.Reason = Unknown.Name
	}
	if c.Code == protocolv1.ErrorCode_ERROR_CODE_UNSPECIFIED {
		c.Code = Unknown.Code
	}
	if c.Message == "" {
		c.Message = Unknown.Message
	}
	return &c
}

// Error is the message with its placeholders filled in, then ": " and the
// detail when there is one.
func (f *Failure) Error() string {
	if f == nil {
		return ""
	}
	m := Render(normal(f).Message, f.Params)
	if f.Detail == "" {
		return m
	}
	return strings.TrimSuffix(m, ".") + ": " + f.Detail
}

func (f *Failure) Unwrap() error {
	if f == nil {
		return nil
	}
	return f.cause
}

// ErrorCode is UNSPECIFIED only for a nil f.
func (f *Failure) ErrorCode() protocolv1.ErrorCode {
	if f == nil {
		return protocolv1.ErrorCode_ERROR_CODE_UNSPECIFIED
	}
	return normal(f).Code
}

func (f *Failure) RetryAfter() time.Duration {
	if f == nil {
		return 0
	}
	return f.After
}

// With is a copy of f with params added as name and value pairs, as Fail
// takes them; f is unchanged. Nil for a nil f.
func (f *Failure) With(kv ...string) *Failure {
	if f == nil {
		return nil
	}
	c := *f
	c.Params = maps.Clone(f.Params)
	if c.Params == nil {
		c.Params = Params{}
	}
	maps.Copy(c.Params, pairs(kv))
	return &c
}

// Classify makes err a failure, as a copy that changing doesn't change err:
// the first non-nil *Failure in err's chain, with the text of what wraps it
// kept in Detail; or else a standard reason by the error's shape (an address
// or URL that doesn't parse, DNS, TLS, refused, timeout, other network), or
// by the ErrorCode in its chain, or Unknown, with the chain's ErrorCode, if
// it has one, over the reason's. Nil for a nil err.
func Classify(err error) *Failure {
	if isNil(err) {
		return nil
	}
	if f := failureIn(err); f != nil {
		c := normal(f)
		c.Params = maps.Clone(f.Params)
		if w, own := err.Error(), f.Error(); w != own {
			c.Detail = detailOf(err, kept(w, own, f.Detail))
		}
		return c
	}
	code := codeIn(err)
	f := byShape(err)
	if f == nil {
		f = byCode(code).Wrap(err)
	}
	if code != protocolv1.ErrorCode_ERROR_CODE_UNSPECIFIED {
		f.Code = code
	}
	return f
}

// kept is the text w of an error that wraps a failure whose own text is
// own, with own replaced by the failure's detail.
func kept(w, own, detail string) string {
	if i := strings.Index(w, own); own != "" && i >= 0 {
		w = w[:i] + detail + w[i+len(own):]
	} else if detail != "" {
		w += ": " + detail
	}
	return strings.TrimRight(strings.TrimSpace(w), ": ")
}

// byShape is nil for an error that isn't the network's or an address's.
func byShape(err error) *Failure {
	if badAddress(err) {
		f := ConfigInvalid.Wrap(err)
		f.Message = "An address or URL in the config isn't valid."
		return f
	}
	address := ""
	if op, ok := errors.AsType[*net.OpError](err); ok && op.Addr != nil {
		address = op.Addr.String()
	}
	// The message without its placeholder, where the error doesn't say.
	at := func(r Reason, plain, name, value string) *Failure {
		if value == "" {
			f := r.Wrap(err)
			f.Message = plain
			return f
		}
		return r.Wrap(err, name, value)
	}
	if dns, ok := errors.AsType[*net.DNSError](err); ok {
		return at(DNSFailed, "The host name doesn't resolve.", "host", dns.Name)
	}
	if isTLS(err) {
		return at(TLSFailed, "The TLS handshake failed.", "address", address)
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return at(ConnectionRefused, "The upstream refused the connection.", "address", address)
	}
	ne := netError(err)
	if errors.Is(err, context.DeadlineExceeded) || ne != nil && ne.Timeout() {
		return at(TimedOut, "The upstream didn't answer in time.", "address", address)
	}
	if ne != nil {
		return at(Unreachable, "Can't reach the upstream.", "address", address)
	}
	return nil
}

// badAddress reports whether err is an address or URL that doesn't parse:
// a mistake in the config, not the network's.
func badAddress(err error) bool {
	if u, ok := errors.AsType[*url.Error](err); ok && u.Op == "parse" {
		return true
	}
	if _, ok := errors.AsType[*net.AddrError](err); ok {
		return true
	}
	_, ok := errors.AsType[*net.ParseError](err)
	return ok
}

// netError is the first net.Error in err's chain other than a *url.Error,
// which is one whatever it wraps.
func netError(err error) net.Error {
	var ne net.Error
	walk(err, func(e error) bool {
		if _, ok := any(e).(*url.Error); ok {
			return false
		}
		n, ok := any(e).(net.Error)
		if ok {
			ne = n
		}
		return ok
	})
	return ne
}

func isTLS(err error) bool {
	if _, ok := errors.AsType[*tls.CertificateVerificationError](err); ok {
		return true
	}
	if _, ok := errors.AsType[x509.UnknownAuthorityError](err); ok {
		return true
	}
	if _, ok := errors.AsType[x509.HostnameError](err); ok {
		return true
	}
	if _, ok := errors.AsType[x509.CertificateInvalidError](err); ok {
		return true
	}
	if _, ok := errors.AsType[tls.RecordHeaderError](err); ok {
		return true
	}
	_, ok := errors.AsType[tls.AlertError](err)
	return ok
}

func byCode(c protocolv1.ErrorCode) Reason {
	switch c {
	case protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID:
		return CredentialsRejected
	case protocolv1.ErrorCode_ERROR_CODE_AUTH_EXPIRED:
		return CredentialsExpired
	case protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED:
		return PermissionDenied
	case protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID:
		return ConfigInvalid
	case protocolv1.ErrorCode_ERROR_CODE_RATE_LIMITED:
		return RateLimited
	case protocolv1.ErrorCode_ERROR_CODE_RESOURCE_EXHAUSTED, protocolv1.ErrorCode_ERROR_CODE_DESTINATION_QUOTA:
		return QuotaExceeded
	case protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, protocolv1.ErrorCode_ERROR_CODE_DESTINATION_UNAVAILABLE:
		return Unavailable
	}
	return Unknown
}

// walk visits err's chain depth first, as errors.As does, until visit
// returns true. It and its visitors look at one error at a time, not a
// chain, so they assert on any(e) rather than use errors.As.
func walk(err error, visit func(error) bool) bool {
	for err != nil {
		if visit(err) {
			return true
		}
		switch u := any(err).(type) {
		case interface{ Unwrap() error }:
			err = u.Unwrap()
		case interface{ Unwrap() []error }:
			for _, e := range u.Unwrap() {
				if walk(e, visit) {
					return true
				}
			}
			return false
		default:
			return false
		}
	}
	return false
}

// failureIn is the first non-nil *Failure in err's chain.
func failureIn(err error) *Failure {
	var f *Failure
	walk(err, func(e error) bool {
		g, ok := any(e).(*Failure)
		if ok && g != nil {
			f = g
		}
		return f != nil
	})
	return f
}

// codeIn is the first code in err's chain other than UNSPECIFIED, which a
// nil *Failure has.
func codeIn(err error) protocolv1.ErrorCode {
	code := protocolv1.ErrorCode_ERROR_CODE_UNSPECIFIED
	walk(err, func(e error) bool {
		if c, ok := any(e).(coded); ok {
			code = c.ErrorCode()
		}
		return code != protocolv1.ErrorCode_ERROR_CODE_UNSPECIFIED
	})
	return code
}

// afterIn is the first positive wait before a retry in err's chain.
func afterIn(err error) time.Duration {
	var d time.Duration
	walk(err, func(e error) bool {
		if r, ok := any(e).(delayed); ok {
			d = r.RetryAfter()
		}
		return d > 0
	})
	return max(d, 0)
}

// detailOf is text, err's or a part of it, as valid UTF-8, without the
// userinfo, query and fragment of the URL of each *url.Error in err's chain.
func detailOf(err error, text string) string {
	walk(err, func(e error) bool {
		if u, ok := any(e).(*url.Error); ok && u != nil && u.URL != "" {
			r := redact.URL(u.URL)
			text = strings.ReplaceAll(text, strconv.Quote(u.URL), strconv.Quote(r))
			text = strings.ReplaceAll(text, u.URL, r)
		}
		return false
	})
	return clean(strings.TrimSpace(text))
}
