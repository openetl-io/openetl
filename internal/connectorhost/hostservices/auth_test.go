package hostservices

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"

	"github.com/openetl-io/openetl/internal/platform/errs"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/host"
)

// rotating keeps what the host rotates.
type rotating struct {
	StaticSecrets
	mu      sync.Mutex
	rotated []string
}

func (r *rotating) Rotate(_ context.Context, name string, v sdk.Secret) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.StaticSecrets[name] = v
	r.rotated = append(r.rotated, v.Reveal())
	return nil
}

func oauthJSON(tokenURL, refresh string) sdk.Secret {
	b, _ := json.Marshal(oauthSecret{ClientID: "id", ClientSecret: "cs", TokenURL: tokenURL, RefreshToken: refresh})
	return sdk.NewSecret(string(b))
}

// One server is the API and its token endpoint, as for many APIs.
type oauthServer struct {
	mu sync.Mutex
	// Answers for the token endpoint, in turn; then the last.
	tokens []func(http.ResponseWriter, *http.Request)
	grants []string
	// Access tokens the API accepts.
	valid  map[string]bool
	bearer []string
}

func (o *oauthServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if r.URL.Path == "/token" {
		_ = r.ParseForm()
		o.grants = append(o.grants, r.PostForm.Get("grant_type")+":"+r.PostForm.Get("refresh_token"))
		o.tokens[min(len(o.grants), len(o.tokens))-1](w, r)
		return
	}
	tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	o.bearer = append(o.bearer, tok)
	if !o.valid[tok] {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	_, _ = io.WriteString(w, "OK")
}

func issue(access, refresh string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"access_token":%q,"token_type":"bearer","expires_in":3600,"refresh_token":%q}`, access, refresh)
	}
}

func oauthFailure(code int, body string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_, _ = io.WriteString(w, body)
	}
}

func TestOAuth2Refresh(t *testing.T) {
	o := &oauthServer{tokens: []func(http.ResponseWriter, *http.Request){issue("at1", "rt2")}, valid: map[string]bool{"at1": true}}
	srv, _ := server(t, o.ServeHTTP)
	secrets := &rotating{StaticSecrets: StaticSecrets{"oauth": oauthJSON(srv.URL+"/token", "rt1")}}
	s, _ := newServices(t, Options{Policy: loopback(), Secrets: secrets})
	r := &host.Request{URL: srv.URL + "/me", Auth: host.OAuth2{CredentialSecret: "oauth"}}
	for range 2 {
		if b, err := do(t, s, r); err != nil || b != "OK" {
			t.Fatalf("%q, %v", b, err)
		}
	}
	if fmt.Sprint(o.grants) != "[refresh_token:rt1]" {
		t.Errorf("grants %v, want one refresh", o.grants)
	}
	if len(secrets.rotated) != 1 || !strings.Contains(secrets.rotated[0], `"refresh_token":"rt2"`) {
		t.Errorf("rotated %v", secrets.rotated)
	}
}

// A 401 refreshes the token once and tries again at once.
func TestOAuth2RefreshesOnUnauthorized(t *testing.T) {
	o := &oauthServer{tokens: []func(http.ResponseWriter, *http.Request){issue("revoked", "rt1"), issue("at2", "rt1")}, valid: map[string]bool{"at2": true}}
	srv, _ := server(t, o.ServeHTTP)
	s, c := newServices(t, Options{Policy: loopback(), Secrets: StaticSecrets{"oauth": oauthJSON(srv.URL+"/token", "rt1")}})
	r := &host.Request{URL: srv.URL + "/me", Auth: host.OAuth2{CredentialSecret: "oauth"}}
	if b, err := do(t, s, r); err != nil || b != "OK" {
		t.Fatalf("%q, %v", b, err)
	}
	if fmt.Sprint(o.bearer) != "[revoked at2]" || len(o.grants) != 2 || len(c.slept) != 0 {
		t.Errorf("bearer %v, grants %v, slept %v", o.bearer, o.grants, c.slept)
	}

	// Once only: an API that refuses every token is AUTH_INVALID.
	o.valid = nil
	_, err := do(t, s, r)
	if errs.CodeOf(err) != errs.AuthInvalid || len(o.grants) != 3 {
		t.Errorf("%v after %d grants", err, len(o.grants))
	}
}

func TestOAuth2TokenFailures(t *testing.T) {
	for _, tc := range []struct {
		name   string
		token  func(http.ResponseWriter, *http.Request)
		code   errs.Code
		grants int
	}{
		{"invalid_grant", oauthFailure(400, `{"error":"invalid_grant","error_description":"revoked"}`), errs.AuthInvalid, 1},
		{"invalid_scope", oauthFailure(400, `{"error":"invalid_scope"}`), errs.PermissionDenied, 1},
		{"503", oauthFailure(503, ``), errs.Transient, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := &oauthServer{tokens: []func(http.ResponseWriter, *http.Request){tc.token}}
			srv, _ := server(t, o.ServeHTTP)
			// With no auth style, x/oauth2 tries the other style after a failure.
			sec, _ := json.Marshal(oauthSecret{ClientID: "id", ClientSecret: "cs", TokenURL: srv.URL + "/token", RefreshToken: "rt1", AuthStyle: "header"})
			s, _ := newServices(t, Options{Policy: loopback(), Budget: 5 * time.Second, Secrets: StaticSecrets{"oauth": sdk.NewSecret(string(sec))}})
			_, err := do(t, s, &host.Request{URL: srv.URL + "/me", Auth: host.OAuth2{CredentialSecret: "oauth"}})
			if errs.CodeOf(err) != tc.code || len(o.grants) < tc.grants || (tc.grants == 1 && len(o.grants) != 1) {
				t.Errorf("%v after %d grants", err, len(o.grants))
			}
			if strings.Contains(err.Error(), "cs") || strings.Contains(err.Error(), "rt1") {
				t.Errorf("the error shows a secret: %v", err)
			}
		})
	}
}

func TestOAuth2ClientCredentials(t *testing.T) {
	o := &oauthServer{tokens: []func(http.ResponseWriter, *http.Request){issue("at1", "")}, valid: map[string]bool{"at1": true}}
	srv, _ := server(t, o.ServeHTTP)
	s, _ := newServices(t, Options{Policy: loopback(), Secrets: StaticSecrets{"oauth": oauthJSON(srv.URL+"/token", "")}})
	if b, err := do(t, s, &host.Request{URL: srv.URL + "/me", Auth: host.OAuth2{CredentialSecret: "oauth"}}); err != nil || b != "OK" {
		t.Fatalf("%q, %v", b, err)
	}
	if fmt.Sprint(o.grants) != "[client_credentials:]" {
		t.Errorf("grants %v", o.grants)
	}
}

// The server checks the signature by signing what it received again.
func TestAWSSigV4(t *testing.T) {
	creds := aws.Credentials{AccessKeyID: "AKID", SecretAccessKey: "secret", SessionToken: "session"}
	failures := make(chan string, 1)
	srv, _ := server(t, func(_ http.ResponseWriter, r *http.Request) {
		failures <- checkSigV4(r, creds)
	})
	b, _ := json.Marshal(awsSecret{AccessKeyID: creds.AccessKeyID, SecretAccessKey: creds.SecretAccessKey, SessionToken: creds.SessionToken})
	s, _ := newServices(t, Options{Policy: loopback(), Secrets: StaticSecrets{"aws": sdk.NewSecret(string(b))}})
	auth := host.AWSSigV4{CredentialsSecret: "aws", Region: "eu-west-1", Service: "execute-api"}
	for _, r := range []*host.Request{
		{URL: srv.URL + "/items?b=2&a=1", Auth: auth},
		{Method: http.MethodPost, URL: srv.URL + "/items", Body: []byte(`{"x":1}`), Header: http.Header{"Content-Type": {"application/json"}}, Auth: auth},
	} {
		if _, err := do(t, s, r); err != nil {
			t.Fatal(err)
		}
		if f := <-failures; f != "" {
			t.Errorf("%s %s: %s", r.Method, r.URL, f)
		}
	}
}

func checkSigV4(r *http.Request, creds aws.Credentials) string {
	got := r.Header.Get("Authorization")
	_, signed, ok := strings.Cut(got, "SignedHeaders=")
	if !ok || r.Header.Get("X-Amz-Security-Token") != creds.SessionToken {
		return "no signature or session token: " + got
	}
	signed, _, _ = strings.Cut(signed, ",")
	at, err := time.Parse("20060102T150405Z", r.Header.Get("X-Amz-Date"))
	if err != nil {
		return err.Error()
	}
	body, _ := io.ReadAll(r.Body)
	again, _ := http.NewRequestWithContext(context.Background(), r.Method, "http://"+r.Host+r.URL.RequestURI(), bytes.NewReader(body))
	for h := range strings.SplitSeq(signed, ";") {
		if h != "host" {
			again.Header.Set(h, r.Header.Get(h))
		}
	}
	hash := sha256.Sum256(body)
	if err := v4.NewSigner().SignHTTP(context.Background(), creds, again, hex.EncodeToString(hash[:]), "execute-api", "eu-west-1", at); err != nil {
		return err.Error()
	}
	if want := again.Header.Get("Authorization"); got != want {
		return fmt.Sprintf("signature\n%s\nwant\n%s", got, want)
	}
	return ""
}

func TestSignHMAC(t *testing.T) {
	// RFC 4231, test case 2.
	s, _ := newServices(t, Options{Secrets: StaticSecrets{"k": sdk.NewSecret("Jefe")}})
	mac, err := s.Sign(t.Context(), "k", host.HMAC{Hash: host.SHA256, Message: []byte("what do ya want for nothing?")})
	if err != nil || hex.EncodeToString(mac) != "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843" {
		t.Errorf("%x, %v", mac, err)
	}
}

func TestSignJWT(t *testing.T) {
	rk, _ := rsa.GenerateKey(rand.Reader, 2048)
	ek, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	ekDER, _ := x509.MarshalPKCS8PrivateKey(ek)
	secrets := StaticSecrets{
		"hs":  sdk.NewSecret("shared"),
		"rsa": sdk.NewSecret(string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(rk)}))),
		"ec":  sdk.NewSecret(string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: ekDER}))),
	}
	s, _ := newServices(t, Options{Secrets: secrets})
	claims := []byte(`{"iss":"openetl","exp":1800000000}`)
	for _, tc := range []struct {
		key    string
		alg    host.JWTAlgorithm
		name   string
		verify func(input, sig []byte) bool
	}{
		{"hs", host.HS256, "HS256", func(input, sig []byte) bool {
			mac := hmac.New(sha256.New, []byte("shared"))
			mac.Write(input)
			return hmac.Equal(mac.Sum(nil), sig)
		}},
		{"rsa", host.RS256, "RS256", func(input, sig []byte) bool {
			d := sha256.Sum256(input)
			return rsa.VerifyPKCS1v15(&rk.PublicKey, 0x5, d[:], sig) == nil
		}},
		{"ec", host.ES256, "ES256", func(input, sig []byte) bool {
			d := sha256.Sum256(input)
			return len(sig) == 64 && ecdsa.Verify(&ek.PublicKey, d[:], new(big.Int).SetBytes(sig[:32]), new(big.Int).SetBytes(sig[32:]))
		}},
	} {
		jws, err := s.Sign(t.Context(), tc.key, host.JWT{Algorithm: tc.alg, Claims: claims, Header: []byte(`{"kid":"k1","alg":"none"}`)})
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		parts := strings.Split(string(jws), ".")
		if len(parts) != 3 {
			t.Fatalf("%s: %s", tc.name, jws)
		}
		hdr, _ := base64.RawURLEncoding.DecodeString(parts[0])
		body, _ := base64.RawURLEncoding.DecodeString(parts[1])
		sig, _ := base64.RawURLEncoding.DecodeString(parts[2])
		if want := `{"alg":"` + tc.name + `","kid":"k1","typ":"JWT"}`; string(hdr) != want {
			t.Errorf("%s: header %s, want %s", tc.name, hdr, want)
		}
		if !bytes.Equal(body, claims) {
			t.Errorf("%s: claims %s", tc.name, body)
		}
		if !tc.verify([]byte(parts[0]+"."+parts[1]), sig) {
			t.Errorf("%s: the signature doesn't verify", tc.name)
		}
	}

	if _, err := s.Sign(t.Context(), "hs", host.JWT{Algorithm: host.RS256, Claims: claims}); errs.CodeOf(err) != errs.ConfigInvalid {
		t.Errorf("RS256 with a shared key: %v", err)
	}
	if _, err := s.Sign(t.Context(), "rsa", host.JWT{Algorithm: host.ES256, Claims: claims}); errs.CodeOf(err) != errs.ConfigInvalid {
		t.Errorf("ES256 with an RSA key: %v", err)
	}
	if _, err := s.Sign(t.Context(), "hs", host.JWT{Algorithm: host.HS256, Claims: []byte(`[1]`)}); errs.CodeOf(err) != errs.Internal {
		t.Errorf("claims that aren't an object: %v", err)
	}
}

// A request whose token another request replaced meanwhile tries again with
// the new one, without refreshing it again.
func TestOAuth2KeepsATokenRefreshedMeanwhile(t *testing.T) {
	var (
		mu     sync.Mutex
		grants int
		first  atomic.Bool
		inner  = make(chan error, 1)
		s      *Services
		url    string
	)
	auth := host.OAuth2{CredentialSecret: "oauth"}
	srv, _ := server(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			mu.Lock()
			grants++
			n := grants
			mu.Unlock()
			issue([]string{"old", "new", "newest"}[min(n, 3)-1], "rt1")(w, r)
			return
		}
		if r.Header.Get("Authorization") == "Bearer old" {
			// Another request finds the token revoked, and refreshes it
			// before this one's 401 arrives.
			if first.CompareAndSwap(false, true) {
				resp, err := s.HTTP(context.Background(), &host.Request{URL: url + "/other", Auth: auth})
				if err == nil {
					_ = resp.Body.Close()
				}
				inner <- err
			}
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = io.WriteString(w, "OK")
	})
	url = srv.URL
	s, _ = newServices(t, Options{Policy: loopback(), Secrets: StaticSecrets{"oauth": oauthJSON(srv.URL+"/token", "rt1")}})
	if b, err := do(t, s, &host.Request{URL: srv.URL + "/me", Auth: auth}); err != nil || b != "OK" {
		t.Fatalf("%q, %v", b, err)
	}
	if err := <-inner; err != nil {
		t.Fatal(err)
	}
	if grants != 2 {
		t.Errorf("%d grants, want 2", grants)
	}
}
