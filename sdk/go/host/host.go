// Package host is the host services API: how connectors reach the outside
// world. Requests name secrets rather than carry them, and the host injects
// the values when a request leaves, so connector code never holds them.
package host

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/unsuitable001/openetl/sdk/go/destination"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
)

type Host interface {
	// HTTP sends req, retrying within its budget. A final status of 400 or
	// more is returned as a *StatusError.
	HTTP(ctx context.Context, req *Request) (*Response, error)
	// Sign signs with the key in the named secret. An HMAC returns the raw
	// MAC, a JWT the compact JWS.
	Sign(ctx context.Context, keySecret string, m SignMethod) ([]byte, error)
	Log(ctx context.Context, entries []LogEntry) error
	// KVGet and KVPut are scratch storage for the connector instance: the
	// host may drop a value before it expires. ok is false if the key is
	// missing or expired.
	KVGet(ctx context.Context, key string) (value []byte, ok bool, err error)
	// A ttl of zero never expires.
	KVPut(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Now(ctx context.Context) (time.Time, error)
	// StagedFileURL returns a URL to GET one file of a staged epoch the
	// destination was handed, and when it expires; zero if it doesn't. The
	// staging package calls it.
	StagedFileURL(ctx context.Context, path string) (url string, expires time.Time, err error)
	// PutDeliveryCursor saves what a staged epoch's Commit has delivered,
	// for a later attempt to resume from. Save it only once the API has
	// confirmed, so it never leads what was delivered. It returns at once;
	// one saved before Commit or Recover returns is kept.
	PutDeliveryCursor(ctx context.Context, e destination.Epoch, cursor []byte) error
	// GetDeliveryCursor returns the cursor last saved for e. ok is false if
	// none was.
	GetDeliveryCursor(ctx context.Context, e destination.Epoch) (cursor []byte, ok bool, err error)
}

type LogEntry struct {
	// Zero: when the host receives it.
	Time    time.Time
	Level   slog.Level
	Message string
	Attrs   map[string]string
}

type Request struct {
	Method string
	// Absolute.
	URL    string
	Header http.Header
	Body   []byte
	// Nil: none.
	Auth Auth
	// Covers retries and waiting for the rate limit. Zero: the host's
	// default.
	Budget time.Duration
	// Allows retrying a non-idempotent method, such as a POST with an
	// idempotency key.
	Idempotent bool
	// Requests with the same key share a rate limit. Empty: the URL's host.
	RateLimitKey string
}

// Auth is Bearer, Basic, APIKey, OAuth2 or AWSSigV4. Fields ending in
// Secret hold secret names. The host sends credentials only to the
// request's own scheme, host and port, not to redirects elsewhere.
type Auth interface{ auth() }

type Bearer struct {
	TokenSecret string
}

type Basic struct {
	Username       string
	PasswordSecret string
}

type APIKey struct {
	KeySecret string
	// One of Header and QueryParam.
	Header     string
	QueryParam string
	// Prepended to the key, such as "Token ".
	Prefix string
}

type OAuth2 struct {
	// Holds the client, the token URL and the refresh token.
	CredentialSecret string
}

type AWSSigV4 struct {
	// Holds the access key ID, the secret key and an optional session token.
	CredentialsSecret string
	Region            string
	Service           string
}

func (Bearer) auth()   {}
func (Basic) auth()    {}
func (APIKey) auth()   {}
func (OAuth2) auth()   {}
func (AWSSigV4) auth() {}

// Response is a response with a status below 400. Close its body.
type Response struct {
	Status int
	Header http.Header
	Body   io.ReadCloser
}

// StatusError is a final response with a status of 400 or more, classified
// by the host.
type StatusError struct {
	Status int
	Header http.Header
	// The start of the body.
	Body []byte
	Code protocolv1.ErrorCode
	// When to try again, from Retry-After or the rate limit's reset.
	After time.Duration
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("HTTP %d %s", e.Status, http.StatusText(e.Status))
}

func (e *StatusError) ErrorCode() protocolv1.ErrorCode { return e.Code }
func (e *StatusError) RetryAfter() time.Duration       { return e.After }

// SignMethod is HMAC or JWT.
type SignMethod interface{ signMethod() }

type HMAC struct {
	Hash    Hash
	Message []byte
}

// Hash numbers match the protocol's HashAlgorithm.
type Hash int32

const (
	SHA1   Hash = 1
	SHA256 Hash = 2
	SHA512 Hash = 3
)

type JWT struct {
	Algorithm JWTAlgorithm
	// A JSON object.
	Claims []byte
	// Extra header fields, such as kid, as a JSON object. The host sets alg
	// and typ.
	Header []byte
}

// JWTAlgorithm numbers match the protocol's. The key is the secret's bytes
// for HS256, and a PEM private key (PKCS #1, SEC 1 or PKCS #8) otherwise.
type JWTAlgorithm int32

const (
	HS256 JWTAlgorithm = 1
	RS256 JWTAlgorithm = 2
	ES256 JWTAlgorithm = 3
)

func (HMAC) signMethod() {}
func (JWT) signMethod()  {}
