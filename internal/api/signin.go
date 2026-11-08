package api

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
	"github.com/openetl-io/openetl/internal/auth"
	"github.com/openetl-io/openetl/internal/store/db"
)

// Sign-in.
const (
	defaultSessionIdle   = 12 * time.Hour
	defaultSessionMaxAge = 7 * 24 * time.Hour
	defaultLinkTTL       = 24 * time.Hour
	maxLinkTTL           = 7 * 24 * time.Hour
	maxSignInFailures    = 10
	signInLock           = 15 * time.Minute
	defaultSignInRate    = 20
	maxHashes            = 4

	// LocalPrincipal is who calls without a credential act as when sign-in
	// is off. It isn't stored.
	LocalPrincipal = "principals/local"
)

// browserOnly are the calls that set or clear the session's cookie, which
// only the UI makes.
var browserOnly = map[string]bool{
	apiv1connect.AccessServiceCreateSessionProcedure: true,
	apiv1connect.AccessServiceDeleteSessionProcedure: true,
	apiv1connect.AccessServiceSetPasswordProcedure:   true,
}

func failedPrecondition(format string, args ...any) error {
	return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(format, args...))
}

func (s *Server) publicURL() *url.URL {
	u, err := url.Parse(s.PublicURL)
	if err != nil || u.Host == "" {
		return &url.URL{Scheme: "http", Host: "localhost"}
	}
	return u
}

func (s *Server) sessionTimes() (idle, maxAge time.Duration) {
	return cmp.Or(s.SessionIdle, defaultSessionIdle), cmp.Or(s.SessionMaxAge, defaultSessionMaxAge)
}

// cookie is the session's cookie, Secure and with the __Host- prefix when
// browsers reach the server over https.
func (s *Server) cookie(value string) *http.Cookie {
	c := &http.Cookie{Name: "openetl_session", Value: value, Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode}
	if s.publicURL().Scheme == "https" {
		c.Name, c.Secure = "__Host-openetl_session", true
	}
	return c
}

func sessionCookie(h http.Header, name string) string {
	for _, line := range h.Values("Cookie") {
		cs, err := http.ParseCookie(line)
		if err != nil {
			continue
		}
		for _, c := range cs {
			if c.Name == name {
				return c.Value
			}
		}
	}
	return ""
}

// guardCookies lets the session's cookie through only on Connect calls from
// the server's own origin. A cross-site page can't make one: it needs a
// header that only a CORS preflight would allow, and the server answers
// none. The calls that set the cookie need the same.
func (s *Server) guardCookies(next http.Handler) (http.Handler, error) {
	cop := http.NewCrossOriginProtection()
	u := s.publicURL()
	if err := cop.AddTrustedOrigin(u.Scheme + "://" + u.Host); err != nil {
		return nil, fmt.Errorf("api: the public URL: %w", err)
	}
	name := s.cookie("").Name
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := r.Cookie(name)
		only := browserOnly[r.URL.Path]
		if err != nil && !only {
			next.ServeHTTP(w, r)
			return
		}
		if r.Method == http.MethodPost && r.Header.Get("Connect-Protocol-Version") == "1" && cop.Check(r) == nil {
			next.ServeHTTP(w, r)
			return
		}
		if only {
			_ = connect.NewErrorWriter().Write(w, r, connect.NewError(connect.CodePermissionDenied, errors.New("only the UI signs in, over Connect from the server's own origin")))
			return
		}
		// REST, gRPC and calls from other origins take keys only.
		r.Header.Del("Cookie")
		next.ServeHTTP(w, r)
	}), nil
}

// sessionCaller is the principal of the call's session cookie, if it has
// one.
func (s *Server) sessionCaller(ctx context.Context, h http.Header) (*caller, bool, error) {
	secret := sessionCookie(h, s.cookie("").Name)
	if secret == "" {
		return nil, false, nil
	}
	idle, _ := s.sessionTimes()
	now := time.Now()
	r, err := s.Store.AuthenticateSession(ctx, db.AuthenticateSessionParams{Hash: auth.Hash(secret), IdleSince: now.Add(-idle)})
	if errors.Is(err, pgx.ErrNoRows) || err == nil && r.OrgID != s.Tenant.Org {
		return nil, true, unauthenticated("the session has ended; sign in again")
	}
	if err != nil {
		return nil, true, err
	}
	used := r.LastUsedTime
	if stale(&used) {
		if err := s.Store.TouchSession(ctx, db.TouchSessionParams{OrgID: r.OrgID, ID: r.ID}); err != nil {
			return nil, true, err
		}
		used = now
	}
	expire := earlier(used.Add(idle), r.ExpireTime)
	return &caller{
		org: r.OrgID, principal: principalName(r.Principal), principalID: r.PrincipalID, session: r.ID, expire: &expire, roles: auth.ParseBindings(r.Bindings),
	}, true, nil
}

func earlier(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func (s *Server) local() *caller {
	return &caller{org: s.Tenant.Org, principal: LocalPrincipal, roles: auth.Bindings{Org: auth.Owner}}
}

// signIns limits sign-ins by client address, and how many passwords are
// hashed at once, so a flood of them can't use up the server's memory.
type signIns struct {
	once   sync.Once
	hashes chan struct{}
	mu     sync.Mutex
	peers  map[string]*rate.Limiter
}

func (l *signIns) init() {
	l.once.Do(func() {
		l.hashes = make(chan struct{}, maxHashes)
		l.peers = map[string]*rate.Limiter{}
	})
}

// allow fails once peer has made more than perMinute sign-ins in a minute.
func (l *signIns) allow(peer string, perMinute int) error {
	l.init()
	host, _, err := net.SplitHostPort(peer)
	if err != nil {
		host = peer
	}
	l.mu.Lock()
	lim := l.peers[host]
	if lim == nil {
		if len(l.peers) >= 10_000 {
			clear(l.peers)
		}
		lim = rate.NewLimiter(rate.Every(time.Minute/time.Duration(perMinute)), perMinute)
		l.peers[host] = lim
	}
	l.mu.Unlock()
	if !lim.Allow() {
		return connect.NewError(connect.CodeResourceExhausted, errors.New("too many sign-ins from this address; try again in a minute"))
	}
	return nil
}

// hash runs f, which hashes a password, once fewer than maxHashes run.
func (l *signIns) hash(ctx context.Context, f func()) error {
	l.init()
	select {
	case l.hashes <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-l.hashes }()
	f()
	return nil
}

// auditSignIn records one of sign-in's calls, by principal with
// credential, in q's transaction.
func (s *Server) auditSignIn(ctx context.Context, q *db.Queries, req connect.AnyRequest, principal, credential string) error {
	id, err := uuid.NewV7()
	if err != nil {
		return err
	}
	now, ok := time.Now(), "ok"
	return q.InsertAuditEntry(ctx, db.InsertAuditEntryParams{
		OrgID: s.Tenant.Org, ID: id, Time: now, Actor: principal, Credential: credential, Method: req.Spec().Procedure,
		Resources: []string{principal}, Details: []byte("{}"), Peer: req.Peer().Addr,
		UserAgent: truncate(req.Header().Get("User-Agent"), maxUserAgent), Code: &ok, EndTime: &now,
	})
}

// startSession signs a principal in, in q's transaction: a session, its
// cookie in h, and its audit entry.
func (s *Server) startSession(ctx context.Context, q *db.Queries, req connect.AnyRequest, h http.Header, principal uuid.UUID, name string, remember bool) (*apiv1.Session, error) {
	now := time.Now()
	idle, maxAge := s.sessionTimes()
	if err := q.DeleteEndedSessions(ctx, db.DeleteEndedSessionsParams{OrgID: s.Tenant.Org, PrincipalID: principal, IdleSince: now.Add(-idle)}); err != nil {
		return nil, err
	}
	c, id := auth.NewSession(), uuid.New()
	expire := now.Add(maxAge)
	err := q.InsertSession(ctx, db.InsertSessionParams{
		OrgID: s.Tenant.Org, ID: id, PrincipalID: principal, Hash: c.Hash, ExpireTime: expire,
		Peer: req.Peer().Addr, UserAgent: truncate(req.Header().Get("User-Agent"), maxUserAgent),
	})
	if err != nil {
		return nil, err
	}
	session := principalName(name) + "/sessions/" + id.String()
	if err := s.auditSignIn(ctx, q, req, principalName(name), session); err != nil {
		return nil, err
	}
	cookie := s.cookie(c.Secret)
	if remember {
		cookie.MaxAge = int(maxAge / time.Second)
	}
	h.Add("Set-Cookie", cookie.String())
	return &apiv1.Session{Name: session, CreateTime: timestamppb.New(now), ExpireTime: timestamppb.New(earlier(now.Add(idle), expire))}, nil
}

const refusedSignIn = "wrong email or password, or the account is locked after too many failed sign-ins; try again later, or ask an admin for a password link"

func (s *Server) CreateSession(ctx context.Context, req *connect.Request[apiv1.CreateSessionRequest]) (*connect.Response[apiv1.CreateSessionResponse], error) {
	if s.Authless {
		return nil, failedPrecondition("sign-in is off (openetld --auth none)")
	}
	if err := s.signIns.allow(req.Peer().Addr, cmp.Or(s.SignInRate, defaultSignInRate)); err != nil {
		return nil, err
	}
	email, password := strings.TrimSpace(req.Msg.GetEmail()), req.Msg.GetPassword()
	u, err := s.Store.GetSignIn(ctx, db.GetSignInParams{OrgID: s.Tenant.Org, Email: email})
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	var ok, rehash bool
	var newHash string
	err = s.signIns.hash(ctx, func() {
		if u.Hash == nil {
			auth.DummyCheck(password)
			return
		}
		if ok, rehash = auth.CheckPassword(*u.Hash, password); ok && rehash {
			newHash = auth.HashPassword(password)
		}
	})
	if err != nil {
		return nil, err
	}
	locked := u.LockedUntil != nil && time.Now().Before(*u.LockedUntil)
	if !ok || locked {
		if u.Hash != nil && !ok {
			if err := s.Store.RecordSignInFailure(ctx, db.RecordSignInFailureParams{
				OrgID: s.Tenant.Org, PrincipalID: u.ID, MaxFailures: maxSignInFailures, LockSeconds: int32(signInLock / time.Second),
			}); err != nil {
				return nil, err
			}
		}
		s.log().Info("sign-in refused", "email", email, "peer", req.Peer().Addr, "locked", locked)
		return nil, unauthenticated("%s", refusedSignIn)
	}
	res := connect.NewResponse(&apiv1.CreateSessionResponse{})
	err = s.Store.Tx(ctx, func(q *db.Queries) error {
		var err error
		if newHash != "" {
			err = q.SetPassword(ctx, db.SetPasswordParams{OrgID: s.Tenant.Org, PrincipalID: u.ID, Hash: newHash})
		} else {
			err = q.ClearSignInFailures(ctx, db.ClearSignInFailuresParams{OrgID: s.Tenant.Org, PrincipalID: u.ID})
		}
		if err != nil {
			return err
		}
		res.Msg.Session, err = s.startSession(ctx, q, req, res.Header(), u.ID, u.Name, req.Msg.GetRemember())
		return err
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (s *Server) DeleteSession(ctx context.Context, req *connect.Request[apiv1.DeleteSessionRequest]) (*connect.Response[apiv1.DeleteSessionResponse], error) {
	res := connect.NewResponse(&apiv1.DeleteSessionResponse{})
	gone := s.cookie("")
	gone.MaxAge = -1
	res.Header().Add("Set-Cookie", gone.String())
	secret := sessionCookie(req.Header(), gone.Name)
	if secret == "" {
		return res, nil
	}
	err := s.Store.Tx(ctx, func(q *db.Queries) error {
		d, err := q.DeleteSessionByHash(ctx, db.DeleteSessionByHashParams{OrgID: s.Tenant.Org, Hash: auth.Hash(secret)})
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		principal := principalName(d.Principal)
		return s.auditSignIn(ctx, q, req, principal, principal+"/sessions/"+d.ID.String())
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (s *Server) RevokeSessions(ctx context.Context, req *connect.Request[apiv1.RevokeSessionsRequest]) (*connect.Response[apiv1.RevokeSessionsResponse], error) {
	err := s.Store.Tx(ctx, func(q *db.Queries) error {
		p, err := s.getPrincipal(ctx, q, req.Msg.GetName())
		if err != nil {
			return err
		}
		if _, err := s.changeOwner(ctx, q, p); err != nil {
			return err
		}
		n, err := q.DeleteSessions(ctx, db.DeleteSessionsParams{OrgID: s.Tenant.Org, PrincipalID: p.ID})
		audit(ctx).set("sessions", n)
		return err
	})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.RevokeSessionsResponse{}), nil
}

var linkGone = failedPrecondition("this link was used already, has expired, or was replaced by a newer one; ask an admin for a new one")

func (s *Server) CreatePasswordLink(ctx context.Context, req *connect.Request[apiv1.CreatePasswordLinkRequest]) (*connect.Response[apiv1.CreatePasswordLinkResponse], error) {
	now := time.Now()
	expire := now.Add(defaultLinkTTL)
	if e := req.Msg.GetExpireTime(); e != nil {
		expire = e.AsTime()
		switch {
		case !expire.After(now):
			return nil, invalid("expire_time is in the past")
		case expire.After(now.Add(maxLinkTTL)):
			return nil, invalid("a password link lasts at most 7 days")
		}
	}
	audit(ctx).set("expireTime", expire.UTC().Format(time.RFC3339))
	res := &apiv1.CreatePasswordLinkResponse{ExpireTime: timestamppb.New(expire)}
	err := s.Store.Tx(ctx, func(q *db.Queries) error {
		p, err := s.getPrincipal(ctx, q, req.Msg.GetName())
		switch {
		case err != nil:
			return err
		case p.Type != "user":
			return failedPrecondition("%s is a service account; only users sign in with a password", req.Msg.GetName())
		case p.Email == "":
			return failedPrecondition("%s has no email to sign in with", req.Msg.GetName())
		}
		if _, err := s.changeOwner(ctx, q, p); err != nil {
			return err
		}
		link := auth.NewPasswordLink()
		if err := q.UpsertPasswordLink(ctx, db.UpsertPasswordLinkParams{OrgID: s.Tenant.Org, PrincipalID: p.ID, Hash: link.Hash, ExpireTime: expire}); err != nil {
			return err
		}
		res.Url = s.publicURL().JoinPath("set-password").String() + "#" + link.Secret
		return nil
	})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(res), nil
}

func (s *Server) SetPassword(ctx context.Context, req *connect.Request[apiv1.SetPasswordRequest]) (*connect.Response[apiv1.SetPasswordResponse], error) {
	if err := s.signIns.allow(req.Peer().Addr, cmp.Or(s.SignInRate, defaultSignInRate)); err != nil {
		return nil, err
	}
	hash := auth.Hash(strings.TrimSpace(req.Msg.GetToken()))
	link, err := s.Store.GetPasswordLink(ctx, db.GetPasswordLinkParams{OrgID: s.Tenant.Org, Hash: hash})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, linkGone
	}
	if err != nil {
		return nil, err
	}
	if err := auth.PasswordRules(req.Msg.GetPassword(), link.Email, link.Name); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	var pw string
	if err := s.signIns.hash(ctx, func() { pw = auth.HashPassword(req.Msg.GetPassword()) }); err != nil {
		return nil, err
	}
	res := connect.NewResponse(&apiv1.SetPasswordResponse{})
	err = s.Store.Tx(ctx, func(q *db.Queries) error {
		n, err := q.DeletePasswordLink(ctx, db.DeletePasswordLinkParams{OrgID: s.Tenant.Org, Hash: hash})
		if err == nil && n == 0 {
			return linkGone
		}
		if err == nil {
			err = q.SetPassword(ctx, db.SetPasswordParams{OrgID: s.Tenant.Org, PrincipalID: link.ID, Hash: pw})
		}
		if err == nil {
			_, err = q.DeleteSessions(ctx, db.DeleteSessionsParams{OrgID: s.Tenant.Org, PrincipalID: link.ID})
		}
		if err != nil {
			return err
		}
		res.Msg.Session, err = s.startSession(ctx, q, req, res.Header(), link.ID, link.Name, req.Msg.GetRemember())
		return err
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (s *Server) ChangePassword(ctx context.Context, req *connect.Request[apiv1.ChangePasswordRequest]) (*connect.Response[apiv1.ChangePasswordResponse], error) {
	c := callerOf(ctx)
	if c.principalID == uuid.Nil {
		return nil, failedPrecondition("%s has no password", c.principal)
	}
	if err := s.signIns.allow(req.Peer().Addr, cmp.Or(s.SignInRate, defaultSignInRate)); err != nil {
		return nil, err
	}
	p, err := s.getPrincipal(ctx, s.Store.Queries, c.principal)
	if err != nil {
		return nil, err
	}
	cur, err := s.Store.GetPassword(ctx, db.GetPasswordParams{OrgID: s.Tenant.Org, PrincipalID: p.ID})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, failedPrecondition("%s has no password to change; an admin can make a password link", c.principal)
	}
	if err != nil {
		return nil, err
	}
	if cur.LockedUntil != nil && time.Now().Before(*cur.LockedUntil) {
		return nil, failedPrecondition("the password is locked after too many wrong tries; try again later")
	}
	var ok bool
	if err := s.signIns.hash(ctx, func() { ok, _ = auth.CheckPassword(cur.Hash, req.Msg.GetCurrentPassword()) }); err != nil {
		return nil, err
	}
	if !ok {
		if err := s.Store.RecordSignInFailure(ctx, db.RecordSignInFailureParams{
			OrgID: s.Tenant.Org, PrincipalID: p.ID, MaxFailures: maxSignInFailures, LockSeconds: int32(signInLock / time.Second),
		}); err != nil {
			return nil, err
		}
		return nil, invalid("the current password is wrong")
	}
	if err := auth.PasswordRules(req.Msg.GetNewPassword(), p.Email, p.Name); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	var pw string
	if err := s.signIns.hash(ctx, func() { pw = auth.HashPassword(req.Msg.GetNewPassword()) }); err != nil {
		return nil, err
	}
	err = s.Store.Tx(ctx, func(q *db.Queries) error {
		if err := q.SetPassword(ctx, db.SetPasswordParams{OrgID: s.Tenant.Org, PrincipalID: p.ID, Hash: pw}); err != nil {
			return err
		}
		n, err := q.DeleteSessions(ctx, db.DeleteSessionsParams{OrgID: s.Tenant.Org, PrincipalID: p.ID, Keep: c.session})
		audit(ctx).set("sessions", n)
		return err
	})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.ChangePasswordResponse{}), nil
}

func (s *Server) GetCaller(ctx context.Context, req *connect.Request[apiv1.GetCallerRequest]) (*connect.Response[apiv1.GetCallerResponse], error) {
	c := callerOf(ctx)
	var ws string
	if w := req.Msg.GetWorkspace(); w != "" {
		var err error
		if ws, err = parseParent(w); err != nil {
			return nil, err
		}
	}
	res := &apiv1.GetCallerResponse{Credential: c.credential(), Authless: s.Authless}
	if c.expire != nil {
		res.ExpireTime = timestamppb.New(*c.expire)
	}
	if c.principal == LocalPrincipal {
		res.Principal = &apiv1.Principal{
			Name: LocalPrincipal, Type: apiv1.PrincipalType_PRINCIPAL_TYPE_USER, DisplayName: "Local",
			RoleBindings: []*apiv1.RoleBinding{{Role: apiv1.Role_ROLE_OWNER}},
		}
	} else {
		p, err := s.getPrincipal(ctx, s.Store.Queries, c.principal)
		if err != nil {
			return nil, err
		}
		if res.Principal, err = s.onePrincipal(ctx, s.Store.Queries, p); err != nil {
			return nil, err
		}
	}
	have := c.roles.In(ws)
	for name, need := range permissions {
		if need > auth.None && have >= need {
			res.Permissions = append(res.Permissions, name)
		}
	}
	slices.Sort(res.Permissions)
	return connect.NewResponse(res), nil
}
