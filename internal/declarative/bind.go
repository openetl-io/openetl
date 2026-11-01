package declarative

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/unsuitable001/openetl/internal/platform/errs"
	sdk "github.com/unsuitable001/openetl/sdk/go"
	"github.com/unsuitable001/openetl/sdk/go/host"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

// Names of the secrets a binding composes for the host. Config property
// names can't contain a dot, so they don't collide.
const (
	oauthSecret = "auth.oauth2"    //nolint:gosec // a name, not a credential
	awsSecret   = "auth.aws_sigv4" //nolint:gosec // a name, not a credential
)

// Binding is a connector bound to its config.
type Binding struct {
	c *Connector
	// The config without its secrets, as expressions see it.
	config map[string]any
	// What the host resolves secret names to: the config's secrets, and the
	// credentials composed for OAuth2 and AWS SigV4.
	Secrets map[string]sdk.Secret
	// Requests per second at most, by rate limit key.
	RateLimits map[string]float64
	base       *url.URL
	auth       host.Auth
}

// Bind checks config, a JSON object, against the manifest's config schema.
// Unknown properties are an error. It does no I/O.
func (c *Connector) Bind(config []byte) (*Binding, error) {
	if len(bytes.TrimSpace(config)) == 0 {
		config = []byte("{}")
	}
	v, err := decodeJSON(config)
	if err != nil {
		return nil, errs.Wrap(errs.ConfigInvalid, err, c.name+": config")
	}
	obj, ok := v.(map[string]any)
	if !ok {
		return nil, errs.New(errs.ConfigInvalid, "%s: the config is a JSON object", c.name)
	}
	b := &Binding{c: c, config: map[string]any{}, Secrets: map[string]sdk.Secret{}, RateLimits: map[string]float64{}}
	for _, k := range sortedKeys(obj) {
		secret, ok := c.props[k]
		switch {
		case !ok:
			return nil, errs.New(errs.ConfigInvalid, "%s: the config has no property %q", c.name, k)
		case secret:
			s, _ := obj[k].(string)
			b.Secrets[k] = sdk.NewSecret(s)
		default:
			b.config[k] = obj[k]
		}
	}
	for k, d := range c.defaults {
		if _, ok := obj[k]; ok {
			continue
		}
		obj[k] = d
		if s, ok := d.(string); ok && c.props[k] {
			b.Secrets[k] = sdk.NewSecret(s)
		} else if !c.props[k] {
			b.config[k] = d
		}
	}
	if err := c.config.Validate(obj); err != nil {
		return nil, errs.New(errs.ConfigInvalid, "%s: config: %s", c.name, b.scrub(err.Error()))
	}
	if err := b.bindHTTP(); err != nil {
		return nil, errs.Wrap(errs.ConfigInvalid, err, c.name)
	}
	return b, nil
}

// scrub removes secret values from s.
func (b *Binding) scrub(s string) string {
	for _, v := range b.Secrets {
		if r := v.Reveal(); r != "" {
			s = strings.ReplaceAll(s, r, "[redacted]")
		}
	}
	return s
}

func (b *Binding) bindHTTP() error {
	vars := map[string]any{varConfig: b.config}
	raw, err := b.c.baseURL.text(vars, nil)
	if err != nil {
		return err
	}
	b.base, err = url.Parse(raw)
	if err != nil || (b.base.Scheme != "http" && b.base.Scheme != "https") || b.base.Host == "" || b.base.RawQuery != "" || b.base.Fragment != "" {
		return fmt.Errorf("http.base_url %q isn't an absolute http or https URL without a query", raw)
	}
	if b.c.rps > 0 {
		b.RateLimits[strings.ToLower(b.base.Host)] = b.c.rps
	}
	a := b.c.auth
	if a == nil {
		return nil
	}
	for _, name := range []string{a.secret, a.clientSecret, a.secretKey, a.clientID.secret, a.accessKeyID.secret} {
		if _, ok := b.Secrets[name]; name != "" && !ok {
			return fmt.Errorf("http.auth: the config has no %s", name)
		}
	}
	switch a.kind {
	case "bearer":
		b.auth = host.Bearer{TokenSecret: a.secret}
	case "basic":
		user, err := a.username.text(vars, nil)
		if err != nil {
			return err
		}
		b.auth = host.Basic{Username: user, PasswordSecret: a.secret}
	case "api_key":
		b.auth = host.APIKey{KeySecret: a.secret, Header: a.header, QueryParam: a.query, Prefix: a.prefix}
	case "oauth2":
		tokenURL, err := a.tokenURL.text(vars, nil)
		if err != nil {
			return err
		}
		id, err := b.value(a.clientID, vars)
		if err != nil {
			return err
		}
		cred := map[string]any{
			"client_id":     id,
			"client_secret": b.Secrets[a.clientSecret].Reveal(),
			"token_url":     tokenURL,
		}
		if v := b.Secrets[a.refresh].Reveal(); v != "" {
			cred["refresh_token"] = v
		}
		if len(a.scopes) > 0 {
			cred["scopes"] = a.scopes
		}
		if a.authStyle != "" {
			cred["auth_style"] = a.authStyle
		}
		if err := b.compose(oauthSecret, cred); err != nil {
			return err
		}
		b.auth = host.OAuth2{CredentialSecret: oauthSecret}
	case "aws_sigv4":
		id, err := b.value(a.accessKeyID, vars)
		if err != nil {
			return err
		}
		region, err := a.region.text(vars, nil)
		if err != nil {
			return err
		}
		cred := map[string]any{"access_key_id": id, "secret_access_key": b.Secrets[a.secretKey].Reveal()}
		if v := b.Secrets[a.sessionTok].Reveal(); v != "" {
			cred["session_token"] = v
		}
		if err := b.compose(awsSecret, cred); err != nil {
			return err
		}
		b.auth = host.AWSSigV4{CredentialsSecret: awsSecret, Region: region, Service: a.service}
	}
	return nil
}

func (b *Binding) value(r valueRef, vars map[string]any) (string, error) {
	if r.secret != "" {
		return b.Secrets[r.secret].Reveal(), nil
	}
	return r.t.text(vars, nil)
}

func (b *Binding) compose(name string, cred map[string]any) error {
	j, err := json.Marshal(cred)
	if err != nil {
		return err
	}
	b.Secrets[name] = sdk.NewSecret(string(j))
	return nil
}

// Source is the binding as a source whose requests go through h, which
// resolves the binding's Secrets.
func (b *Binding) Source(h host.Host) source.Source {
	return &src{b: b, h: h}
}
