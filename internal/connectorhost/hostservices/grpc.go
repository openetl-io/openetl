package hostservices

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/platform/ints"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/host"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

const bodyChunk = 64 << 10

// Server is s as the host services gRPC service, for external connectors.
type Server struct {
	protocolv1.UnimplementedHostServiceServer
	s *Services
}

func NewServer(s *Services) *Server { return &Server{s: s} }

func (g *Server) HTTP(req *protocolv1.HTTPRequest, stream grpc.ServerStreamingServer[protocolv1.HTTPResponse]) error {
	r := &host.Request{
		Method:       req.GetMethod(),
		URL:          req.GetUrl(),
		Body:         req.GetBody(),
		Budget:       req.GetBudget().AsDuration(),
		Idempotent:   req.GetIdempotent(),
		RateLimitKey: req.GetRateLimitKey(),
	}
	if len(req.GetHeaders()) > 0 {
		r.Header = http.Header{}
		for _, h := range req.GetHeaders() {
			r.Header.Add(h.GetName(), h.GetValue())
		}
	}
	var err error
	if r.Auth, err = authFromProto(req.GetAuth()); err != nil {
		return errs.Status(err)
	}
	resp, err := g.s.HTTP(stream.Context(), r)
	if se, ok := errors.AsType[*host.StatusError](err); ok {
		head := &protocolv1.HTTPResponseHead{Status: ints.Int32(se.Status), Headers: headersToProto(se.Header), Error: errs.ToProto(se)}
		if err := stream.Send(&protocolv1.HTTPResponse{Part: &protocolv1.HTTPResponse_Head{Head: head}}); err != nil {
			return err
		}
		return sendBody(stream, bytes.NewReader(se.Body))
	}
	if err != nil {
		return errs.Status(err)
	}
	defer func() { _ = resp.Body.Close() }()
	head := &protocolv1.HTTPResponseHead{Status: ints.Int32(resp.Status), Headers: headersToProto(resp.Header)}
	if err := stream.Send(&protocolv1.HTTPResponse{Part: &protocolv1.HTTPResponse_Head{Head: head}}); err != nil {
		return err
	}
	return sendBody(stream, resp.Body)
}

func sendBody(stream grpc.ServerStreamingServer[protocolv1.HTTPResponse], r io.Reader) error {
	buf := make([]byte, bodyChunk)
	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			if serr := stream.Send(&protocolv1.HTTPResponse{Part: &protocolv1.HTTPResponse_Body{Body: bytes.Clone(buf[:n])}}); serr != nil {
				return serr
			}
		}
		switch {
		case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
			return nil
		case err != nil:
			return errs.Status(err)
		}
	}
}

func headersToProto(h http.Header) []*protocolv1.Header {
	var out []*protocolv1.Header
	for k, vs := range h {
		for _, v := range vs {
			out = append(out, &protocolv1.Header{Name: k, Value: v})
		}
	}
	return out
}

func authFromProto(a *protocolv1.HTTPAuth) (host.Auth, error) {
	switch s := a.GetScheme().(type) {
	case nil:
		return nil, nil
	case *protocolv1.HTTPAuth_Bearer:
		return host.Bearer{TokenSecret: s.Bearer.GetTokenSecret()}, nil
	case *protocolv1.HTTPAuth_Basic:
		return host.Basic{Username: s.Basic.GetUsername(), PasswordSecret: s.Basic.GetPasswordSecret()}, nil
	case *protocolv1.HTTPAuth_ApiKey:
		return host.APIKey{KeySecret: s.ApiKey.GetKeySecret(), Header: s.ApiKey.GetHeader(), QueryParam: s.ApiKey.GetQueryParam(), Prefix: s.ApiKey.GetPrefix()}, nil
	case *protocolv1.HTTPAuth_Oauth2:
		return host.OAuth2{CredentialSecret: s.Oauth2.GetCredentialSecret()}, nil
	case *protocolv1.HTTPAuth_AwsSigv4:
		return host.AWSSigV4{CredentialsSecret: s.AwsSigv4.GetCredentialsSecret(), Region: s.AwsSigv4.GetRegion(), Service: s.AwsSigv4.GetService()}, nil
	default:
		return nil, errs.New(errs.Internal, "HTTP: unknown auth %T", s)
	}
}

func (g *Server) Sign(ctx context.Context, req *protocolv1.SignRequest) (*protocolv1.SignResponse, error) {
	var m host.SignMethod
	switch s := req.GetMethod().(type) {
	case *protocolv1.SignRequest_Hmac:
		m = host.HMAC{Hash: host.Hash(s.Hmac.GetHash()), Message: s.Hmac.GetMessage()}
	case *protocolv1.SignRequest_Jwt:
		m = host.JWT{Algorithm: host.JWTAlgorithm(s.Jwt.GetAlgorithm()), Claims: s.Jwt.GetClaims(), Header: s.Jwt.GetHeader()}
	default:
		return nil, errs.Status(errs.New(errs.Internal, "Sign: unknown method %T", s))
	}
	sig, err := g.s.Sign(ctx, req.GetKeySecret(), m)
	if err != nil {
		return nil, errs.Status(err)
	}
	return &protocolv1.SignResponse{Signature: sig}, nil
}

func (g *Server) GetSecret(ctx context.Context, req *protocolv1.GetSecretRequest) (*protocolv1.GetSecretResponse, error) {
	v, err := g.s.GetSecret(ctx, req.GetName())
	if err != nil {
		return nil, errs.Status(err)
	}
	return &protocolv1.GetSecretResponse{Value: []byte(v.Reveal())}, nil
}

func (g *Server) Log(ctx context.Context, req *protocolv1.LogRequest) (*protocolv1.LogResponse, error) {
	entries := make([]host.LogEntry, 0, len(req.GetEntries()))
	for _, e := range req.GetEntries() {
		le := host.LogEntry{Level: logLevel(e.GetLevel()), Message: e.GetMessage(), Attrs: e.GetAttrs()}
		if e.GetTime() != nil {
			le.Time = e.GetTime().AsTime()
		}
		entries = append(entries, le)
	}
	if err := g.s.Log(ctx, entries); err != nil {
		return nil, errs.Status(err)
	}
	return &protocolv1.LogResponse{}, nil
}

func logLevel(l protocolv1.LogLevel) slog.Level {
	switch l {
	case protocolv1.LogLevel_LOG_LEVEL_DEBUG:
		return slog.LevelDebug
	case protocolv1.LogLevel_LOG_LEVEL_WARN:
		return slog.LevelWarn
	case protocolv1.LogLevel_LOG_LEVEL_ERROR:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func (g *Server) KVGet(ctx context.Context, req *protocolv1.KVGetRequest) (*protocolv1.KVGetResponse, error) {
	v, ok, err := g.s.KVGet(ctx, req.GetKey())
	if err != nil {
		return nil, errs.Status(err)
	}
	if !ok {
		return &protocolv1.KVGetResponse{}, nil
	}
	if v == nil {
		v = []byte{}
	}
	return &protocolv1.KVGetResponse{Value: v}, nil
}

func (g *Server) KVPut(ctx context.Context, req *protocolv1.KVPutRequest) (*protocolv1.KVPutResponse, error) {
	if err := g.s.KVPut(ctx, req.GetKey(), req.GetValue(), req.GetTtl().AsDuration()); err != nil {
		return nil, errs.Status(err)
	}
	return &protocolv1.KVPutResponse{}, nil
}

func (g *Server) Now(ctx context.Context, _ *protocolv1.NowRequest) (*protocolv1.NowResponse, error) {
	t, err := g.s.Now(ctx)
	if err != nil {
		return nil, errs.Status(err)
	}
	return &protocolv1.NowResponse{Now: timestamppb.New(t)}, nil
}

func (g *Server) StagedFileURL(ctx context.Context, req *protocolv1.StagedFileURLRequest) (*protocolv1.StagedFileURLResponse, error) {
	u, expires, err := g.s.StagedFileURL(ctx, req.GetPath())
	if err != nil {
		return nil, errs.Status(err)
	}
	resp := &protocolv1.StagedFileURLResponse{Url: u}
	if !expires.IsZero() {
		resp.Expires = timestamppb.New(expires)
	}
	return resp, nil
}

func (g *Server) PutDeliveryCursor(ctx context.Context, req *protocolv1.PutDeliveryCursorRequest) (*protocolv1.PutDeliveryCursorResponse, error) {
	if err := g.s.PutDeliveryCursor(ctx, epochFromProto(req.GetEpoch()), req.GetCursor()); err != nil {
		return nil, errs.Status(err)
	}
	return &protocolv1.PutDeliveryCursorResponse{}, nil
}

func (g *Server) GetDeliveryCursor(ctx context.Context, req *protocolv1.GetDeliveryCursorRequest) (*protocolv1.GetDeliveryCursorResponse, error) {
	v, ok, err := g.s.GetDeliveryCursor(ctx, epochFromProto(req.GetEpoch()))
	if err != nil {
		return nil, errs.Status(err)
	}
	if !ok {
		return &protocolv1.GetDeliveryCursorResponse{}, nil
	}
	if v == nil {
		v = []byte{}
	}
	return &protocolv1.GetDeliveryCursorResponse{Cursor: v}, nil
}

func epochFromProto(e *protocolv1.Epoch) destination.Epoch {
	return destination.Epoch{Generation: e.GetGeneration(), N: e.GetN()}
}
