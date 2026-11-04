package host

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/internal/rpcerr"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
)

// Client is the Host an external connector reaches over its host services
// socket.
type Client struct {
	c protocolv1.HostServiceClient
}

var _ Host = (*Client)(nil)

func NewClient(conn grpc.ClientConnInterface) *Client {
	return &Client{c: protocolv1.NewHostServiceClient(conn)}
}

func (c *Client) HTTP(ctx context.Context, req *Request) (*Response, error) {
	p := &protocolv1.HTTPRequest{
		Method:       req.Method,
		Url:          req.URL,
		Body:         req.Body,
		Idempotent:   req.Idempotent,
		RateLimitKey: req.RateLimitKey,
	}
	if req.Budget > 0 {
		p.Budget = durationpb.New(req.Budget)
	}
	for k, vs := range req.Header {
		for _, v := range vs {
			p.Headers = append(p.Headers, &protocolv1.Header{Name: k, Value: v})
		}
	}
	auth, err := authToProto(req.Auth)
	if err != nil {
		return nil, err
	}
	p.Auth = auth

	ctx, cancel := context.WithCancel(ctx)
	stream, err := c.c.HTTP(ctx, p)
	if err != nil {
		cancel()
		return nil, rpcerr.FromStatus(err)
	}
	first, err := stream.Recv()
	if err != nil {
		cancel()
		return nil, rpcerr.FromStatus(err)
	}
	head := first.GetHead()
	if head == nil {
		cancel()
		return nil, &rpcerr.Error{Code: protocolv1.ErrorCode_ERROR_CODE_INTERNAL, Message: "HTTP: the host sent no response head"}
	}
	hdr := http.Header{}
	for _, h := range head.GetHeaders() {
		hdr.Add(h.GetName(), h.GetValue())
	}
	body := &streamBody{stream: stream, cancel: cancel}
	if pe := head.GetError(); pe != nil {
		b, _ := io.ReadAll(body)
		_ = body.Close()
		return nil, &StatusError{Status: int(head.GetStatus()), Header: hdr, Body: b, Code: pe.GetCode(), After: pe.GetRetryAfter().AsDuration()}
	}
	return &Response{Status: int(head.GetStatus()), Header: hdr, Body: body}, nil
}

type streamBody struct {
	stream grpc.ServerStreamingClient[protocolv1.HTTPResponse]
	cancel context.CancelFunc
	buf    []byte
	err    error
}

func (b *streamBody) Read(p []byte) (int, error) {
	for len(b.buf) == 0 {
		if b.err != nil {
			return 0, b.err
		}
		m, err := b.stream.Recv()
		switch {
		case errors.Is(err, io.EOF):
			b.err = io.EOF
		case err != nil:
			b.err = rpcerr.FromStatus(err)
		default:
			b.buf = m.GetBody()
		}
	}
	n := copy(p, b.buf)
	b.buf = b.buf[n:]
	return n, nil
}

func (b *streamBody) Close() error {
	b.cancel()
	return nil
}

func authToProto(a Auth) (*protocolv1.HTTPAuth, error) {
	switch a := a.(type) {
	case nil:
		return nil, nil
	case Bearer:
		return &protocolv1.HTTPAuth{Scheme: &protocolv1.HTTPAuth_Bearer{Bearer: &protocolv1.BearerAuth{TokenSecret: a.TokenSecret}}}, nil
	case Basic:
		return &protocolv1.HTTPAuth{Scheme: &protocolv1.HTTPAuth_Basic{Basic: &protocolv1.BasicAuth{Username: a.Username, PasswordSecret: a.PasswordSecret}}}, nil
	case APIKey:
		k := &protocolv1.APIKeyAuth{KeySecret: a.KeySecret, Prefix: a.Prefix}
		if a.QueryParam != "" {
			k.Location = &protocolv1.APIKeyAuth_QueryParam{QueryParam: a.QueryParam}
		} else {
			k.Location = &protocolv1.APIKeyAuth_Header{Header: a.Header}
		}
		return &protocolv1.HTTPAuth{Scheme: &protocolv1.HTTPAuth_ApiKey{ApiKey: k}}, nil
	case OAuth2:
		return &protocolv1.HTTPAuth{Scheme: &protocolv1.HTTPAuth_Oauth2{Oauth2: &protocolv1.OAuth2Auth{CredentialSecret: a.CredentialSecret}}}, nil
	case AWSSigV4:
		return &protocolv1.HTTPAuth{Scheme: &protocolv1.HTTPAuth_AwsSigv4{AwsSigv4: &protocolv1.AWSSigV4Auth{CredentialsSecret: a.CredentialsSecret, Region: a.Region, Service: a.Service}}}, nil
	default:
		return nil, &rpcerr.Error{Code: protocolv1.ErrorCode_ERROR_CODE_INTERNAL, Message: "HTTP: unknown auth"}
	}
}

func (c *Client) Sign(ctx context.Context, keySecret string, m SignMethod) ([]byte, error) {
	req := &protocolv1.SignRequest{KeySecret: keySecret}
	switch m := m.(type) {
	case HMAC:
		req.Method = &protocolv1.SignRequest_Hmac{Hmac: &protocolv1.HMACSign{Hash: protocolv1.HashAlgorithm(m.Hash), Message: m.Message}}
	case JWT:
		req.Method = &protocolv1.SignRequest_Jwt{Jwt: &protocolv1.JWTSign{Algorithm: protocolv1.JWTAlgorithm(m.Algorithm), Claims: m.Claims, Header: m.Header}}
	default:
		return nil, &rpcerr.Error{Code: protocolv1.ErrorCode_ERROR_CODE_INTERNAL, Message: "Sign: unknown method"}
	}
	resp, err := c.c.Sign(ctx, req)
	if err != nil {
		return nil, rpcerr.FromStatus(err)
	}
	return resp.GetSignature(), nil
}

// GetSecret returns a secret's value. The host answers only connectors on
// the proxy and raw egress tiers.
func (c *Client) GetSecret(ctx context.Context, name string) (string, error) {
	resp, err := c.c.GetSecret(ctx, &protocolv1.GetSecretRequest{Name: name})
	if err != nil {
		return "", rpcerr.FromStatus(err)
	}
	return string(resp.GetValue()), nil
}

func (c *Client) Log(ctx context.Context, entries []LogEntry) error {
	req := &protocolv1.LogRequest{Entries: make([]*protocolv1.LogEntry, 0, len(entries))}
	for _, e := range entries {
		pe := &protocolv1.LogEntry{Level: logLevel(e.Level), Message: e.Message, Attrs: e.Attrs}
		if !e.Time.IsZero() {
			pe.Time = timestamppb.New(e.Time)
		}
		req.Entries = append(req.Entries, pe)
	}
	_, err := c.c.Log(ctx, req)
	return rpcerr.FromStatus(err)
}

func logLevel(l slog.Level) protocolv1.LogLevel {
	switch {
	case l < slog.LevelInfo:
		return protocolv1.LogLevel_LOG_LEVEL_DEBUG
	case l < slog.LevelWarn:
		return protocolv1.LogLevel_LOG_LEVEL_INFO
	case l < slog.LevelError:
		return protocolv1.LogLevel_LOG_LEVEL_WARN
	default:
		return protocolv1.LogLevel_LOG_LEVEL_ERROR
	}
}

func (c *Client) KVGet(ctx context.Context, key string) ([]byte, bool, error) {
	resp, err := c.c.KVGet(ctx, &protocolv1.KVGetRequest{Key: key})
	if err != nil {
		return nil, false, rpcerr.FromStatus(err)
	}
	return resp.GetValue(), resp.Value != nil, nil
}

func (c *Client) KVPut(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	req := &protocolv1.KVPutRequest{Key: key, Value: value}
	if ttl > 0 {
		req.Ttl = durationpb.New(ttl)
	}
	_, err := c.c.KVPut(ctx, req)
	return rpcerr.FromStatus(err)
}

func (c *Client) Now(ctx context.Context) (time.Time, error) {
	resp, err := c.c.Now(ctx, &protocolv1.NowRequest{})
	if err != nil {
		return time.Time{}, rpcerr.FromStatus(err)
	}
	return resp.GetNow().AsTime(), nil
}

func (c *Client) StagedFileURL(ctx context.Context, path string) (string, time.Time, error) {
	resp, err := c.c.StagedFileURL(ctx, &protocolv1.StagedFileURLRequest{Path: path})
	if err != nil {
		return "", time.Time{}, rpcerr.FromStatus(err)
	}
	var expires time.Time
	if resp.Expires != nil {
		expires = resp.GetExpires().AsTime()
	}
	return resp.GetUrl(), expires, nil
}

func (c *Client) PutDeliveryCursor(ctx context.Context, e destination.Epoch, cursor []byte) error {
	_, err := c.c.PutDeliveryCursor(ctx, &protocolv1.PutDeliveryCursorRequest{Epoch: &protocolv1.Epoch{Generation: e.Generation, N: e.N}, Cursor: cursor})
	return rpcerr.FromStatus(err)
}

func (c *Client) GetDeliveryCursor(ctx context.Context, e destination.Epoch) ([]byte, bool, error) {
	resp, err := c.c.GetDeliveryCursor(ctx, &protocolv1.GetDeliveryCursorRequest{Epoch: &protocolv1.Epoch{Generation: e.Generation, N: e.N}})
	if err != nil {
		return nil, false, rpcerr.FromStatus(err)
	}
	return resp.GetCursor(), resp.Cursor != nil, nil
}
