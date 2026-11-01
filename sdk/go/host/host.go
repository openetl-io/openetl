// Package host is the host services API: how connectors reach the outside
// world. Requests name secrets rather than carry them, and the host injects
// the values when a request leaves, so connector code never holds them.
package host

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
)

type Host interface {
	// HTTP sends req, retrying within its budget. A final status of 400 or
	// more is returned as a *StatusError.
	HTTP(ctx context.Context, req *Request) (*Response, error)
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
