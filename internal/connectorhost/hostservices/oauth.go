package hostservices

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"

	"github.com/openetl-io/openetl/internal/platform/errs"
	sdk "github.com/openetl-io/openetl/sdk/go"
)

// Rotator is Secrets that keeps the secrets the host rotates, such as a
// refresh token the OAuth2 server replaced.
type Rotator interface {
	Rotate(ctx context.Context, name string, value sdk.Secret) error
}

// oauthSecret is an OAuth2 credential secret's value. Without a refresh
// token it's the client credentials grant.
type oauthSecret struct {
	ClientID     string   `json:"client_id"`
	ClientSecret string   `json:"client_secret"`
	TokenURL     string   `json:"token_url"`
	RefreshToken string   `json:"refresh_token,omitempty"`
	Scopes       []string `json:"scopes,omitempty"`
	// "header" or "params"; empty tries both.
	AuthStyle string `json:"auth_style,omitempty"`
}

// oauthCredential caches an access token, for every request of the
// instance that names the same secret.
type oauthCredential struct {
	s    *Services
	name string

	mu     sync.Mutex
	secret oauthSecret
	tok    *oauth2.Token
}

func (s *Services) oauth(ctx context.Context, name string) (*oauthCredential, error) {
	s.mu.Lock()
	c, ok := s.tokens[name]
	s.mu.Unlock()
	if ok {
		return c, nil
	}
	v, err := s.secret(ctx, name)
	if err != nil {
		return nil, err
	}
	var sec oauthSecret
	dec := json.NewDecoder(bytes.NewReader([]byte(v.Reveal())))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&sec); err != nil || sec.ClientID == "" || sec.TokenURL == "" {
		return nil, errs.New(errs.ConfigInvalid, "HTTP: secret %q isn't an OAuth2 credential: it needs client_id, client_secret, token_url and optionally refresh_token", name)
	}
	s.remember(sec.ClientSecret, sec.RefreshToken)
	if sec.AuthStyle != "" && sec.AuthStyle != "header" && sec.AuthStyle != "params" {
		return nil, errs.New(errs.ConfigInvalid, "HTTP: secret %q: auth_style is header or params", name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.tokens[name]; ok {
		return c, nil
	}
	c = &oauthCredential{s: s, name: name, secret: sec}
	s.tokens[name] = c
	return c, nil
}

// bearer is the credential for a try that uses tok.
func bearer(tok *oauth2.Token) credential {
	return headerCredential{"Authorization", sdk.NewSecret(tok.Type() + " " + tok.AccessToken)}
}

// invalidate drops tok if it's still the current token, so one that
// another request already refreshed stands.
func (c *oauthCredential) invalidate(tok *oauth2.Token) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tok == tok {
		c.tok = nil
	}
}

func (c *oauthCredential) token(ctx context.Context) (*oauth2.Token, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tok.Valid() {
		return c.tok, nil
	}
	// The token request goes out through the same guarded client.
	ctx = context.WithValue(ctx, oauth2.HTTPClient, c.s.client)
	style := map[string]oauth2.AuthStyle{"header": oauth2.AuthStyleInHeader, "params": oauth2.AuthStyleInParams}[c.secret.AuthStyle]
	var (
		tok *oauth2.Token
		err error
	)
	if c.secret.RefreshToken == "" {
		cc := clientcredentials.Config{ClientID: c.secret.ClientID, ClientSecret: c.secret.ClientSecret, TokenURL: c.secret.TokenURL, Scopes: c.secret.Scopes, AuthStyle: style}
		tok, err = cc.Token(ctx)
	} else {
		conf := oauth2.Config{ClientID: c.secret.ClientID, ClientSecret: c.secret.ClientSecret, Endpoint: oauth2.Endpoint{TokenURL: c.secret.TokenURL, AuthStyle: style}, Scopes: c.secret.Scopes}
		tok, err = conf.TokenSource(ctx, &oauth2.Token{RefreshToken: c.secret.RefreshToken}).Token()
	}
	if err != nil {
		return nil, tokenError(err)
	}
	c.tok = tok
	c.s.remember(tok.AccessToken, tok.RefreshToken)
	if tok.RefreshToken != "" && tok.RefreshToken != c.secret.RefreshToken {
		c.secret.RefreshToken = tok.RefreshToken
		c.rotate(ctx)
	}
	return tok, nil
}

// rotate reports a new refresh token. If it can't, this instance carries
// on with it, and a later one may find the old token revoked.
func (c *oauthCredential) rotate(ctx context.Context) {
	r, ok := c.s.opts.Secrets.(Rotator)
	if !ok {
		c.s.log.WarnContext(ctx, "the OAuth2 server rotated the refresh token, and nothing keeps secrets here", "secret", c.name)
		return
	}
	b, err := json.Marshal(c.secret) //nolint:gosec // the rotated secret goes back to Secrets, whole
	if err == nil {
		err = r.Rotate(ctx, c.name, sdk.NewSecret(string(b)))
	}
	if err != nil {
		c.s.log.WarnContext(ctx, "couldn't keep a rotated refresh token", "secret", c.name, "err", err)
	}
}

func tokenError(err error) error {
	re, ok := errors.AsType[*oauth2.RetrieveError](err)
	if !ok {
		return fmt.Errorf("OAuth2 token: %w", classifyErr(err))
	}
	code := errs.AuthInvalid
	switch re.ErrorCode {
	case "invalid_scope":
		code = errs.PermissionDenied
	case "":
		if re.Response != nil {
			code = classify(re.Response.StatusCode, re.Response.Header)
			if code == errs.ContractChanged {
				code = errs.AuthInvalid
			}
		}
	}
	status := 0
	if re.Response != nil {
		status = re.Response.StatusCode
	}
	return errs.New(code, "HTTP: OAuth2 token: HTTP %d %s %s", status, re.ErrorCode, re.ErrorDescription)
}
