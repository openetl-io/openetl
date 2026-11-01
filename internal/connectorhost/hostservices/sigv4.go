package hostservices

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"

	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/sdk/go/host"
)

// awsSecret is an AWS credentials secret's value.
type awsSecret struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	SessionToken    string `json:"session_token,omitempty"`
}

// sigv4Credential signs each hop, since a signature covers one request.
type sigv4Credential struct {
	creds   aws.Credentials
	region  string
	service string
	signer  *v4.Signer
}

func (s *Services) sigv4(ctx context.Context, a host.AWSSigV4) (credential, error) {
	if a.Region == "" || a.Service == "" {
		return nil, errs.New(errs.Internal, "HTTP: AWS SigV4 needs a region and a service")
	}
	v, err := s.secret(ctx, a.CredentialsSecret)
	if err != nil {
		return nil, err
	}
	var sec awsSecret
	dec := json.NewDecoder(bytes.NewReader([]byte(v.Reveal())))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&sec); err != nil || sec.AccessKeyID == "" || sec.SecretAccessKey == "" {
		return nil, errs.New(errs.ConfigInvalid, "HTTP: secret %q isn't AWS credentials: it needs access_key_id, secret_access_key and optionally session_token", a.CredentialsSecret)
	}
	s.remember(sec.SecretAccessKey, sec.SessionToken)
	return &sigv4Credential{
		creds:   aws.Credentials{AccessKeyID: sec.AccessKeyID, SecretAccessKey: sec.SecretAccessKey, SessionToken: sec.SessionToken},
		region:  a.Region,
		service: a.Service,
		signer:  v4.NewSigner(),
	}, nil
}

func (c *sigv4Credential) apply(req *http.Request) error {
	h := sha256.New()
	if req.GetBody != nil {
		b, err := req.GetBody()
		if err != nil {
			return errs.Wrap(errs.Internal, err, "HTTP: AWS SigV4")
		}
		_, err = io.Copy(h, b)
		_ = b.Close()
		if err != nil {
			return errs.Wrap(errs.Internal, err, "HTTP: AWS SigV4")
		}
	}
	hash := hex.EncodeToString(h.Sum(nil))
	if c.service == "s3" {
		req.Header.Set("X-Amz-Content-Sha256", hash)
	}
	if err := c.signer.SignHTTP(req.Context(), c.creds, req, hash, c.service, c.region, time.Now()); err != nil {
		return errs.Wrap(errs.Internal, err, "HTTP: AWS SigV4")
	}
	return nil
}
