package api

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
)

func (s *Server) ListHolds(context.Context, *connect.Request[apiv1.ListHoldsRequest]) (*connect.Response[apiv1.ListHoldsResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("holds aren't served yet"))
}

func (s *Server) ReleaseHold(context.Context, *connect.Request[apiv1.ReleaseHoldRequest]) (*connect.Response[apiv1.ReleaseHoldResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("holds aren't served yet"))
}

func (s *Server) DiscardHold(context.Context, *connect.Request[apiv1.DiscardHoldRequest]) (*connect.Response[apiv1.DiscardHoldResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("holds aren't served yet"))
}
