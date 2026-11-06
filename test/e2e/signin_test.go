//go:build e2e

package e2e

import (
	"context"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
	"github.com/openetl-io/openetl/internal/daemon"
	"github.com/openetl-io/openetl/internal/platform/config"
)

type headers map[string]string

func (h headers) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	for k, v := range h {
		r.Header.Set(k, v)
	}
	return http.DefaultTransport.RoundTrip(r)
}

// browser is a client with its own cookies, sending header's pairs.
func browser(t *testing.T, header ...string) *http.Client {
	t.Helper()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	h := headers{"Sec-Fetch-Site": "same-origin"}
	for i := 0; i+1 < len(header); i += 2 {
		h[header[i]] = header[i+1]
	}
	return &http.Client{Jar: jar, Transport: h}
}

func accessAs(c *http.Client) apiv1connect.AccessServiceClient {
	return apiv1connect.NewAccessServiceClient(c, env.apiURL)
}

func caller(c *http.Client) (*apiv1.GetCallerResponse, error) {
	res, err := accessAs(c).GetCaller(context.Background(), connect.NewRequest(&apiv1.GetCallerRequest{Workspace: "workspaces/default"}))
	if err != nil {
		return nil, err
	}
	return res.Msg, nil
}

func passwordLink(t *testing.T, principal string) (link, token string) {
	t.Helper()
	res, err := accessAs(env.http).CreatePasswordLink(t.Context(), connect.NewRequest(&apiv1.CreatePasswordLinkRequest{Name: principal}))
	if err != nil {
		t.Fatal(err)
	}
	_, token, _ = strings.Cut(res.Msg.GetUrl(), "#")
	return res.Msg.GetUrl(), token
}

func signIn(c *http.Client, email, password string, remember bool) (*connect.Response[apiv1.CreateSessionResponse], error) {
	return accessAs(c).CreateSession(context.Background(), connect.NewRequest(&apiv1.CreateSessionRequest{Email: email, Password: password, Remember: remember}))
}

// A user sets a password from a link and signs in with it. The session's
// cookie works only on Connect calls from the server's own origin, and ends
// when the user signs out or changes their password, an admin signs them
// out, or they're disabled. Too many failures lock the account until a new
// link sets a password (ADR-0020).
func TestSignIn(t *testing.T) {
	ctx := t.Context()
	owner := accessAs(env.http)
	name := unique("ada")
	principal, email := "principals/"+name, name+"@example.com"
	if _, err := owner.CreatePrincipal(ctx, connect.NewRequest(&apiv1.CreatePrincipalRequest{
		PrincipalId: name, Principal: &apiv1.Principal{Type: apiv1.PrincipalType_PRINCIPAL_TYPE_USER, Email: email},
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.GrantRole(ctx, connect.NewRequest(&apiv1.GrantRoleRequest{
		Name: principal, Binding: &apiv1.RoleBinding{Role: apiv1.Role_ROLE_VIEWER, Workspace: "workspaces/default"},
	})); err != nil {
		t.Fatal(err)
	}
	bot := unique("bot")
	if _, err := owner.CreatePrincipal(ctx, connect.NewRequest(&apiv1.CreatePrincipalRequest{
		PrincipalId: bot, Principal: &apiv1.Principal{Type: apiv1.PrincipalType_PRINCIPAL_TYPE_SERVICE_ACCOUNT},
	})); err != nil {
		t.Fatal(err)
	}
	_, err := owner.CreatePasswordLink(ctx, connect.NewRequest(&apiv1.CreatePasswordLinkRequest{Name: "principals/" + bot}))
	wantCode(t, err, connect.CodeFailedPrecondition, "a service account's password link")

	link, token := passwordLink(t, principal)
	api, err := url.Parse(env.apiURL)
	if err != nil {
		t.Fatal(err)
	}
	if want := "http://localhost:" + api.Port() + "/set-password#oetl_pw_"; !strings.HasPrefix(link, want) {
		t.Fatalf("link %q, want it to start with %q", link, want)
	}

	a := browser(t)
	set := func(c *http.Client, token, password string) (*connect.Response[apiv1.SetPasswordResponse], error) {
		return accessAs(c).SetPassword(ctx, connect.NewRequest(&apiv1.SetPasswordRequest{Token: token, Password: password}))
	}
	_, err = set(a, token, "too short")
	wantCode(t, err, connect.CodeInvalidArgument, "a short password")
	_, err = set(a, token, name)
	wantCode(t, err, connect.CodeInvalidArgument, "the user's name as a password")
	const pw = "correct horse battery staple"
	res, err := set(a, token, pw)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(res.Msg.GetSession().GetName(), principal+"/sessions/") {
		t.Fatalf("session %q", res.Msg.GetSession().GetName())
	}
	cookie := res.Header().Get("Set-Cookie")
	for _, want := range []string{"openetl_session=oetl_s_", "Path=/", "HttpOnly", "SameSite=Strict"} {
		if !strings.Contains(cookie, want) || strings.Contains(cookie, "Max-Age") {
			t.Fatalf("cookie %q, want %q and no Max-Age", cookie, want)
		}
	}
	_, err = set(browser(t), token, "another long passphrase")
	wantCode(t, err, connect.CodeFailedPrecondition, "a link used twice")

	me, err := caller(a)
	if err != nil {
		t.Fatal(err)
	}
	if me.GetPrincipal().GetName() != principal || me.GetCredential() != res.Msg.GetSession().GetName() || me.GetExpireTime() == nil || me.GetAuthless() {
		t.Fatalf("caller %v", me)
	}
	if !slices.Contains(me.GetPermissions(), "pipelines.get") || slices.Contains(me.GetPermissions(), "pipelines.sync") {
		t.Fatalf("a viewer's permissions: %v", me.GetPermissions())
	}
	if _, err := apiv1connect.NewPipelineServiceClient(a, env.apiURL).ListPipelines(ctx, connect.NewRequest(&apiv1.ListPipelinesRequest{Parent: "workspaces/default"})); err != nil {
		t.Fatal(err)
	}

	// Not from another origin, nor over REST.
	evil := browser(t, "Sec-Fetch-Site", "cross-site", "Origin", "https://evil.example")
	evil.Jar.SetCookies(api, a.Jar.Cookies(api))
	_, err = caller(evil)
	wantCode(t, err, connect.CodeUnauthenticated, "the cookie from another origin")
	_, err = signIn(evil, email, pw, false)
	wantCode(t, err, connect.CodePermissionDenied, "signing in from another origin")
	if code, _ := restCall(t, a, http.MethodGet, "/v1/caller", nil); code != http.StatusUnauthorized {
		t.Fatalf("the cookie over REST: %d", code)
	}

	b := browser(t)
	_, wrong := signIn(b, email, "a wrong password, but long", false)
	wantCode(t, wrong, connect.CodeUnauthenticated, "a wrong password")
	_, unknown := signIn(b, "nobody-"+email, pw, false)
	wantCode(t, unknown, connect.CodeUnauthenticated, "an unknown email")
	if connect.CodeOf(wrong) == connect.CodeUnauthenticated && wrong.Error() != unknown.Error() {
		t.Fatalf("a wrong password says %q, an unknown email %q", wrong, unknown)
	}
	remembered, err := signIn(b, strings.ToUpper(email), pw, true)
	if err != nil {
		t.Fatal(err)
	}
	if c := remembered.Header().Get("Set-Cookie"); !strings.Contains(c, "Max-Age=604800") {
		t.Fatalf("a remembered session's cookie %q", c)
	}

	_, err = accessAs(a).ChangePassword(ctx, connect.NewRequest(&apiv1.ChangePasswordRequest{CurrentPassword: "not the password at all", NewPassword: "a different passphrase entirely"}))
	wantCode(t, err, connect.CodeInvalidArgument, "changing the password without the current one")
	const pw2 = "a different passphrase entirely"
	if _, err := accessAs(a).ChangePassword(ctx, connect.NewRequest(&apiv1.ChangePasswordRequest{CurrentPassword: pw, NewPassword: pw2})); err != nil {
		t.Fatal(err)
	}
	if _, err := caller(a); err != nil {
		t.Fatalf("the session that changed the password: %v", err)
	}
	_, err = caller(b)
	wantCode(t, err, connect.CodeUnauthenticated, "another session after a password change")
	_, err = signIn(b, email, pw, false)
	wantCode(t, err, connect.CodeUnauthenticated, "the old password")
	if _, err := signIn(b, email, pw2, false); err != nil {
		t.Fatal(err)
	}

	saved := a.Jar.Cookies(api)
	if _, err := accessAs(a).DeleteSession(ctx, connect.NewRequest(&apiv1.DeleteSessionRequest{})); err != nil {
		t.Fatal(err)
	}
	_, err = caller(a)
	wantCode(t, err, connect.CodeUnauthenticated, "after signing out")
	replay := browser(t)
	replay.Jar.SetCookies(api, saved)
	_, err = caller(replay)
	wantCode(t, err, connect.CodeUnauthenticated, "a signed-out session's cookie")

	if _, err := owner.RevokeSessions(ctx, connect.NewRequest(&apiv1.RevokeSessionsRequest{Name: principal})); err != nil {
		t.Fatal(err)
	}
	_, err = caller(b)
	wantCode(t, err, connect.CodeUnauthenticated, "after an admin signs the user out")
	c := browser(t)
	if _, err := signIn(c, email, pw2, false); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.DisablePrincipal(ctx, connect.NewRequest(&apiv1.DisablePrincipalRequest{Name: principal})); err != nil {
		t.Fatal(err)
	}
	_, err = signIn(browser(t), email, pw2, false)
	wantCode(t, err, connect.CodeUnauthenticated, "a disabled user signing in")
	if _, err := owner.EnablePrincipal(ctx, connect.NewRequest(&apiv1.EnablePrincipalRequest{Name: principal})); err != nil {
		t.Fatal(err)
	}
	_, err = caller(c)
	wantCode(t, err, connect.CodeUnauthenticated, "a session from before the user was disabled")

	d := browser(t)
	for range 10 {
		_, err = signIn(d, email, "guessing at the password", false)
		wantCode(t, err, connect.CodeUnauthenticated, "a guess")
	}
	_, err = signIn(d, email, pw2, false)
	wantCode(t, err, connect.CodeUnauthenticated, "the right password on a locked account")
	_, token = passwordLink(t, principal)
	if _, err := set(d, token, "a third passphrase, after the lock"); err != nil {
		t.Fatal(err)
	}
	if _, err := signIn(browser(t), email, "a third passphrase, after the lock", false); err != nil {
		t.Fatalf("signing in after a link unlocked the account: %v", err)
	}

	entries, err := apiv1connect.NewAuditServiceClient(env.http, env.apiURL).ListAuditEntries(ctx, connect.NewRequest(&apiv1.ListAuditEntriesRequest{Actor: principal}))
	if err != nil {
		t.Fatal(err)
	}
	methods := map[string]int{}
	for _, e := range entries.Msg.GetEntries() {
		m := e.GetMethod()[strings.LastIndex(e.GetMethod(), "/")+1:]
		if e.GetCode() == "ok" {
			methods[m]++
		}
		if !strings.HasPrefix(e.GetCredential(), principal+"/sessions/") {
			t.Errorf("entry %s by %s with %s", m, e.GetActor(), e.GetCredential())
		}
	}
	for m, n := range map[string]int{"SetPassword": 2, "CreateSession": 4, "ChangePassword": 1, "DeleteSession": 1} {
		if methods[m] != n {
			t.Errorf("%d %s entries by the user, want %d: %v", methods[m], m, n, methods)
		}
	}
}

// With sign-in off, calls without a credential act as an owner, and keys
// still act as their principals.
func TestAuthless(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ready, done := make(chan struct{}), make(chan error, 1)
	go func() {
		done <- daemon.Run(ctx, daemon.Options{
			Config: config.Config{
				DatabaseURL: env.dbURL, TemporalAddress: env.temporal, TemporalNamespace: env.ns, DataPlane: "default",
				Connectors: env.noConnectors, BootstrapKeyFile: env.keyFile, Auth: "none",
			},
			Roles:    daemon.Roles{Server: true},
			Listener: ln,
			Ready:    func(string) { close(ready) },
		})
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	select {
	case <-ready:
	case err := <-done:
		t.Fatal(err)
	case <-time.After(30 * time.Second):
		t.Fatal("the authless server didn't start")
	}
	apiURL := "http://" + ln.Addr().String()
	access := apiv1connect.NewAccessServiceClient(http.DefaultClient, apiURL)
	res, err := access.GetCaller(ctx, connect.NewRequest(&apiv1.GetCallerRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	if m := res.Msg; m.GetPrincipal().GetName() != "principals/local" || !m.GetAuthless() || m.GetCredential() != "" || !slices.Contains(m.GetPermissions(), "owners.manage") {
		t.Fatalf("caller %v", m)
	}
	if _, err := apiv1connect.NewPipelineServiceClient(http.DefaultClient, apiURL).ListPipelines(ctx, connect.NewRequest(&apiv1.ListPipelinesRequest{Parent: "workspaces/default"})); err != nil {
		t.Fatal(err)
	}
	keyed, err := apiv1connect.NewAccessServiceClient(env.http, apiURL).GetCaller(ctx, connect.NewRequest(&apiv1.GetCallerRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	if keyed.Msg.GetPrincipal().GetName() != "principals/admin" {
		t.Fatalf("a key's caller with sign-in off: %v", keyed.Msg.GetPrincipal().GetName())
	}
	_, err = access.CreateSession(ctx, connect.NewRequest(&apiv1.CreateSessionRequest{Email: "a@example.com", Password: "doesn't matter here"}))
	wantCode(t, err, connect.CodeFailedPrecondition, "signing in with sign-in off")

	name := unique("made-authless")
	if _, err := access.CreatePrincipal(ctx, connect.NewRequest(&apiv1.CreatePrincipalRequest{
		PrincipalId: name, Principal: &apiv1.Principal{Type: apiv1.PrincipalType_PRINCIPAL_TYPE_SERVICE_ACCOUNT},
	})); err != nil {
		t.Fatal(err)
	}
	entries, err := apiv1connect.NewAuditServiceClient(env.http, env.apiURL).ListAuditEntries(ctx, connect.NewRequest(&apiv1.ListAuditEntriesRequest{Resource: "principals/" + name}))
	if err != nil {
		t.Fatal(err)
	}
	if e := entries.Msg.GetEntries(); len(e) != 1 || e[0].GetActor() != "principals/local" || e[0].GetCredential() != "" {
		t.Fatalf("audit entries %v", e)
	}
}
