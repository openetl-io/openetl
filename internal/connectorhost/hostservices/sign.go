package hostservices

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1" //nolint:gosec // HMAC-SHA1 is what some APIs sign with
	"crypto/sha256"
	"crypto/sha512"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"hash"

	"github.com/openetl-io/openetl/internal/platform/errs"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/host"
)

func (s *Services) Sign(ctx context.Context, keySecret string, m host.SignMethod) ([]byte, error) {
	key, err := s.secret(ctx, keySecret)
	if err != nil {
		return nil, err
	}
	switch m := m.(type) {
	case host.HMAC:
		h := map[host.Hash]func() hash.Hash{host.SHA1: sha1.New, host.SHA256: sha256.New, host.SHA512: sha512.New}[m.Hash]
		if h == nil {
			return nil, errs.New(errs.Internal, "Sign: unknown HMAC hash %d", m.Hash)
		}
		mac := hmac.New(h, []byte(key.Reveal()))
		mac.Write(m.Message)
		return mac.Sum(nil), nil
	case host.JWT:
		return signJWT(keySecret, key, m)
	default:
		return nil, errs.New(errs.Internal, "Sign: unknown method %T", m)
	}
}

func signJWT(name string, key sdk.Secret, m host.JWT) ([]byte, error) {
	alg := map[host.JWTAlgorithm]string{host.HS256: "HS256", host.RS256: "RS256", host.ES256: "ES256"}[m.Algorithm]
	if alg == "" {
		return nil, errs.New(errs.Internal, "Sign: unknown JWT algorithm %d", m.Algorithm)
	}
	var claims map[string]json.RawMessage
	if err := json.Unmarshal(m.Claims, &claims); err != nil || claims == nil {
		return nil, errs.New(errs.Internal, "Sign: JWT claims must be a JSON object")
	}
	hdr := map[string]json.RawMessage{}
	if len(m.Header) > 0 {
		if err := json.Unmarshal(m.Header, &hdr); err != nil || hdr == nil {
			return nil, errs.New(errs.Internal, "Sign: a JWT header must be a JSON object")
		}
	}
	hdr["alg"], hdr["typ"] = json.RawMessage(`"`+alg+`"`), json.RawMessage(`"JWT"`)
	hb, err := json.Marshal(hdr)
	if err != nil {
		return nil, errs.Wrap(errs.Internal, err, "Sign")
	}
	b64 := base64.RawURLEncoding
	input := b64.EncodeToString(hb) + "." + b64.EncodeToString(m.Claims)
	digest := sha256.Sum256([]byte(input))

	var sig []byte
	switch m.Algorithm {
	case host.HS256:
		mac := hmac.New(sha256.New, []byte(key.Reveal()))
		mac.Write([]byte(input))
		sig = mac.Sum(nil)
	case host.RS256:
		k, ok := privateKey(key).(*rsa.PrivateKey)
		if !ok {
			return nil, errs.New(errs.ConfigInvalid, "Sign: secret %q isn't a PEM RSA private key", name)
		}
		if sig, err = rsa.SignPKCS1v15(rand.Reader, k, crypto.SHA256, digest[:]); err != nil {
			return nil, errs.Wrap(errs.ConfigInvalid, err, "Sign")
		}
	case host.ES256:
		k, ok := privateKey(key).(*ecdsa.PrivateKey)
		if !ok || k.Curve != elliptic.P256() {
			return nil, errs.New(errs.ConfigInvalid, "Sign: secret %q isn't a PEM P-256 private key", name)
		}
		r, s, err := ecdsa.Sign(rand.Reader, k, digest[:])
		if err != nil {
			return nil, errs.Wrap(errs.Internal, err, "Sign")
		}
		sig = make([]byte, 64)
		r.FillBytes(sig[:32])
		s.FillBytes(sig[32:])
	}
	return []byte(input + "." + b64.EncodeToString(sig)), nil
}

// privateKey parses a PEM private key in PKCS #1, SEC 1 or PKCS #8, or
// returns nil.
func privateKey(key sdk.Secret) crypto.PrivateKey {
	b, _ := pem.Decode([]byte(key.Reveal()))
	if b == nil {
		return nil
	}
	if k, err := x509.ParsePKCS8PrivateKey(b.Bytes); err == nil {
		return k
	}
	if k, err := x509.ParsePKCS1PrivateKey(b.Bytes); err == nil {
		return k
	}
	if k, err := x509.ParseECPrivateKey(b.Bytes); err == nil {
		return k
	}
	return nil
}
