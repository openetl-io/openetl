package hostservices

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/url"
	"strings"

	"github.com/unsuitable001/openetl/internal/platform/errs"
	sdk "github.com/unsuitable001/openetl/sdk/go"
	"github.com/unsuitable001/openetl/sdk/go/host"
)

// Secrets resolves the secret names in a connector instance's requests.
type Secrets interface {
	Secret(ctx context.Context, name string) (sdk.Secret, error)
}

// StaticSecrets is a fixed set of secrets, by name.
type StaticSecrets map[string]sdk.Secret

func (s StaticSecrets) Secret(_ context.Context, name string) (sdk.Secret, error) {
	v, ok := s[name]
	if !ok {
		return sdk.Secret{}, errs.New(errs.ConfigInvalid, "HTTP: no secret named %q", name)
	}
	return v, nil
}

// A credential is added to each hop of a request that goes to the
// request's own origin.
type credential interface {
	apply(req *http.Request) error
}

// authHeader is the header an auth sets, which the request mustn't set too.
func authHeader(a host.Auth) string {
	switch a := a.(type) {
	case nil:
		return ""
	case host.APIKey:
		return http.CanonicalHeaderKey(a.Header)
	default:
		return "Authorization"
	}
}

type headerCredential struct {
	name  string
	value sdk.Secret
}

func (c headerCredential) apply(req *http.Request) error {
	req.Header.Set(c.name, c.value.Reveal())
	return nil
}

type queryCredential struct {
	name  string
	value sdk.Secret
}

// apply appends the key, leaving the rest of the query as the connector
// wrote it.
func (c queryCredential) apply(req *http.Request) error {
	kv := url.QueryEscape(c.name) + "=" + url.QueryEscape(c.value.Reveal())
	if req.URL.RawQuery != "" {
		kv = req.URL.RawQuery + "&" + kv
	}
	req.URL.RawQuery = kv
	return nil
}

// credential resolves a request's auth. OAuth2 returns its token cache
// instead, since each try takes the token current then.
func (s *Services) credential(ctx context.Context, a host.Auth) (credential, *oauthCredential, error) {
	switch a := a.(type) {
	case nil:
		return nil, nil, nil
	case host.Bearer:
		v, err := s.secret(ctx, a.TokenSecret)
		return headerCredential{"Authorization", sdk.NewSecret("Bearer " + v.Reveal())}, nil, err
	case host.Basic:
		v, err := s.secret(ctx, a.PasswordSecret)
		enc := base64.StdEncoding.EncodeToString([]byte(a.Username + ":" + v.Reveal()))
		return headerCredential{"Authorization", sdk.NewSecret("Basic " + enc)}, nil, err
	case host.APIKey:
		if (a.Header == "") == (a.QueryParam == "") {
			return nil, nil, errs.New(errs.Internal, "HTTP: an API key goes in a header or a query parameter, one of them")
		}
		v, err := s.secret(ctx, a.KeySecret)
		if a.Header != "" {
			return headerCredential{http.CanonicalHeaderKey(a.Header), sdk.NewSecret(a.Prefix + v.Reveal())}, nil, err
		}
		return queryCredential{a.QueryParam, sdk.NewSecret(a.Prefix + v.Reveal())}, nil, err
	case host.OAuth2:
		oc, err := s.oauth(ctx, a.CredentialSecret)
		return nil, oc, err
	case host.AWSSigV4:
		c, err := s.sigv4(ctx, a)
		return c, nil, err
	default:
		return nil, nil, errs.New(errs.Internal, "HTTP: unknown auth %T", a)
	}
}

func (s *Services) secret(ctx context.Context, name string) (sdk.Secret, error) {
	if s.opts.Secrets == nil {
		return sdk.Secret{}, errs.New(errs.ConfigInvalid, "HTTP: no secret named %q", name)
	}
	return s.opts.Secrets.Secret(ctx, name)
}

// A request's credential and origin travel in its context, so every hop of
// it, redirects included, carries them to egress.
type authKey struct{}

type authed struct {
	origin *url.URL
	cred   credential
}

func withAuth(ctx context.Context, origin *url.URL, cred credential) context.Context {
	if cred == nil {
		return ctx
	}
	return context.WithValue(ctx, authKey{}, authed{origin, cred})
}

// egress adds a request's credential to each hop that goes to its origin.
type egress struct {
	base http.RoundTripper
}

func (e *egress) RoundTrip(req *http.Request) (*http.Response, error) {
	if a, ok := req.Context().Value(authKey{}).(authed); ok && sameOrigin(req.URL, a.origin) {
		req = req.Clone(req.Context())
		if err := a.cred.apply(req); err != nil {
			return nil, err
		}
	}
	return e.base.RoundTrip(req)
}

func (e *egress) CloseIdleConnections() {
	if c, ok := e.base.(interface{ CloseIdleConnections() }); ok {
		c.CloseIdleConnections()
	}
}

func sameOrigin(a, b *url.URL) bool {
	return a.Scheme == b.Scheme && strings.EqualFold(a.Hostname(), b.Hostname()) && port(a) == port(b)
}

func port(u *url.URL) string {
	if p := u.Port(); p != "" {
		return p
	}
	if u.Scheme == "https" {
		return "443"
	}
	return "80"
}
