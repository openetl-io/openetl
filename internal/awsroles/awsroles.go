// Package awsroles assumes the IAM roles that sources and destinations name
// in aws.role, with the runner's own AWS credentials, and serves each
// connector process its role's credentials alone. The endpoint is on
// loopback, in the format AWS SDKs read from
// AWS_CONTAINER_CREDENTIALS_FULL_URI, so a connector that uses the SDK's
// default credential chain needs no change.
package awsroles

import (
	"cmp"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	ststypes "github.com/aws/aws-sdk-go-v2/service/sts/types"
	"github.com/aws/smithy-go"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/platform/errs"
)

const (
	// How long each session asks for: a chained session's most.
	sessionDuration = time.Hour
	// Credentials served have at least this long left, more than any SDK
	// refreshes ahead of expiry (botocore's is 15 minutes).
	expiryWindow = 20 * time.Minute
	// Where a lease's credentials are served, with its ID after.
	credentialsPath = "/v1/credentials/"
)

// Broker leases roles to connectors. Its zero value isn't usable; New makes
// one. It serves nothing until its first lease.
type Broker struct {
	// The roles it may assume; none if empty.
	allow   []*regexp.Regexp
	setting string

	// The runner's own AWS config, loaded once.
	base func() (aws.Config, error)

	mu     sync.Mutex
	url    string
	server *http.Server
	leases map[string]*lease
}

type lease struct {
	token string
	creds *aws.CredentialsCache
}

// New makes a broker that assumes the roles allow names: IAM role ARNs,
// comma-separated, where * matches any characters. Empty allows none.
func New(allow string) (*Broker, error) {
	b := &Broker{setting: allow, leases: map[string]*lease{}}
	b.base = sync.OnceValues(func() (aws.Config, error) {
		cfg, err := config.LoadDefaultConfig(context.Background(), config.WithEC2IMDSRegion())
		cfg.Region = cmp.Or(cfg.Region, "us-east-1")
		return cfg, err
	})
	for p := range strings.SplitSeq(allow, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if !strings.HasPrefix(p, "arn:") {
			return nil, fmt.Errorf("awsroles: %q isn't a role's ARN or a pattern of one, such as arn:aws:iam::123456789012:role/openetl-*", p)
		}
		parts := strings.Split(p, "*")
		for i, s := range parts {
			parts[i] = regexp.QuoteMeta(s)
		}
		b.allow = append(b.allow, regexp.MustCompile("^"+strings.Join(parts, ".*")+"$"))
	}
	return b, nil
}

// Allows reports whether the broker may assume role.
func (b *Broker) Allows(role string) bool {
	for _, re := range b.allow {
		if re.MatchString(role) {
			return true
		}
	}
	return false
}

// Session says whom a role's session is for. The runner sets it from the
// API's names, never from a spec, so a role's trust policy can rely on its
// tags.
type Session struct {
	Workspace string
	// source or destination.
	Kind string
	// The source's or destination's name.
	Name string
	// The pipeline's name; empty outside a sync.
	Pipeline string
}

// Tags are the session tags: openetl:workspace, openetl:source or
// openetl:destination, and openetl:pipeline for a sync.
func (s Session) Tags() []ststypes.Tag {
	tags := []ststypes.Tag{{Key: aws.String("openetl:workspace"), Value: aws.String(s.Workspace)}, {Key: aws.String("openetl:" + s.Kind), Value: aws.String(s.Name)}}
	if s.Pipeline != "" {
		tags = append(tags, ststypes.Tag{Key: aws.String("openetl:pipeline"), Value: aws.String(s.Pipeline)})
	}
	return tags
}

var unsafeSessionName = regexp.MustCompile(`[^\w+=,.@-]`)

// SessionName is the role session's name, which CloudTrail shows:
// openetl-KIND-NAME, cut to STS's 64 characters.
func (s Session) SessionName() string {
	n := unsafeSessionName.ReplaceAllString("openetl-"+s.Kind+"-"+s.Name, "-")
	return n[:min(len(n), 64)]
}

// Lease is a role's credentials for one connector process, until Close.
type Lease struct {
	// AWS_CONTAINER_CREDENTIALS_FULL_URI and
	// AWS_CONTAINER_AUTHORIZATION_TOKEN.
	URL, Token string
	close      func()
}

// Close stops serving the lease.
func (l *Lease) Close() { l.close() }

// Lease assumes access's role for the session, and serves its credentials
// until the lease is closed. The role must be one the broker allows, and
// the first credentials are fetched now, so a refusal is the lease's error.
func (b *Broker) Lease(ctx context.Context, access *apiv1.AWSAccess, s Session) (*Lease, error) {
	role := access.GetRole()
	if !b.Allows(role) {
		if len(b.allow) == 0 {
			return nil, errs.New(errs.ConfigInvalid, "%s %s: aws.role names %s, and this runner may assume no roles: list it in OPENETL_AWS_ASSUME_ROLES", s.Kind, s.Name, role)
		}
		return nil, errs.New(errs.ConfigInvalid, "%s %s: aws.role names %s, which OPENETL_AWS_ASSUME_ROLES (%s) doesn't list", s.Kind, s.Name, role, b.setting)
	}
	cfg, err := b.base()
	if err != nil {
		return nil, errs.Wrap(errs.ConfigInvalid, err, "awsroles: load the runner's AWS config")
	}
	client := sts.NewFromConfig(cfg)
	provider := stscreds.NewAssumeRoleProvider(client, role, func(o *stscreds.AssumeRoleOptions) {
		o.RoleSessionName = s.SessionName()
		o.Duration = sessionDuration
		o.Tags = s.Tags()
		if id := access.GetExternalId(); id != "" {
			o.ExternalID = aws.String(id)
		}
	})
	creds := aws.NewCredentialsCache(provider, func(o *aws.CredentialsCacheOptions) { o.ExpiryWindow = expiryWindow })
	if _, err := creds.Retrieve(ctx); err != nil {
		return nil, b.refused(ctx, client, role, s, err)
	}

	id, token := rand.Text(), rand.Text()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.server == nil {
		if err := b.serve(ctx); err != nil {
			return nil, err
		}
	}
	b.leases[id] = &lease{token: token, creds: creds}
	return &Lease{URL: b.url + id, Token: token, close: func() {
		b.mu.Lock()
		delete(b.leases, id)
		b.mu.Unlock()
	}}, nil
}

// refused says why role couldn't be assumed, naming the runner's own
// identity when STS refused it.
func (b *Broker) refused(ctx context.Context, client *sts.Client, role string, s Session, err error) error {
	what := fmt.Sprintf("%s %s: assume aws.role %s", s.Kind, s.Name, role)
	var api smithy.APIError
	if !errors.As(err, &api) {
		return errs.Wrap(errs.AuthInvalid, err, what+" with the runner's AWS credentials")
	}
	switch api.ErrorCode() {
	case "AccessDenied":
		who := "the runner's identity"
		if out, cerr := client.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{}); cerr == nil {
			who = aws.ToString(out.Arn)
		}
		return errs.Wrap(errs.PermissionDenied, err, fmt.Sprintf("%s: STS refused it to %s; the role's trust policy must allow it sts:AssumeRole and sts:TagSession, with any tags it requires, "+
			"and its own policy must allow sts:AssumeRole on the role", what, who))
	case "Throttling", "ThrottlingException":
		return errs.Wrap(errs.RateLimited, err, what)
	case "ExpiredToken", "InvalidClientTokenId", "SignatureDoesNotMatch":
		return errs.Wrap(errs.AuthInvalid, err, what+" with the runner's AWS credentials")
	}
	return errs.Wrap(errs.Transient, err, what)
}

// serve starts the endpoint on loopback; b.mu is held.
func (b *Broker) serve(ctx context.Context) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		return errs.Wrap(errs.Internal, err, "awsroles: listen on loopback")
	}
	b.url = "http://" + ln.Addr().String() + credentialsPath
	b.server = &http.Server{Handler: b, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = b.server.Serve(ln) }()
	return nil
}

// Close stops the endpoint.
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.server == nil {
		return nil
	}
	return b.server.Close()
}

// ServeHTTP answers a lease's credentials to the request that carries its
// token, as AWS SDKs' container credentials provider reads them.
func (b *Broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	id, ok := strings.CutPrefix(r.URL.Path, credentialsPath)
	b.mu.Lock()
	l := b.leases[id]
	b.mu.Unlock()
	if !ok || l == nil || r.Method != http.MethodGet {
		refuse(w, http.StatusNotFound, "NotFound", "no such lease")
		return
	}
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte(l.token)) != 1 {
		refuse(w, http.StatusForbidden, "AccessDenied", "the lease's token is missing or wrong")
		return
	}
	creds, err := l.creds.Retrieve(r.Context())
	if err != nil {
		refuse(w, http.StatusInternalServerError, "CredentialsUnavailable", err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"AccessKeyId": creds.AccessKeyID, "SecretAccessKey": creds.SecretAccessKey, "Token": creds.SessionToken,
		"Expiration": creds.Expires.UTC().Format(time.RFC3339),
	})
}

func refuse(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"code": code, "message": msg})
}
