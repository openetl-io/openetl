package hostservices

import (
	"context"
	"time"

	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/sdk/go/destination"
)

// Staging serves a staged destination's files and delivery cursors, for the
// epochs the runner handed it.
type Staging interface {
	SignedURL(ctx context.Context, path string) (string, time.Time, error)
	PutCursor(ctx context.Context, e destination.Epoch, cursor []byte) error
	Cursor(ctx context.Context, e destination.Epoch) ([]byte, bool, error)
}

// SetStaging serves st to the instance, once the runner knows its
// destination takes staged epochs. Nil stops serving.
func (s *Services) SetStaging(st Staging) {
	s.mu.Lock()
	s.staging = st
	s.mu.Unlock()
}

func (s *Services) stagingArea(call string) (Staging, error) {
	s.mu.Lock()
	st := s.staging
	s.mu.Unlock()
	if st == nil {
		return nil, errs.New(errs.Internal, "%s: the destination wasn't handed staged epochs", call)
	}
	return st, nil
}

func (s *Services) StagedFileURL(ctx context.Context, path string) (string, time.Time, error) {
	st, err := s.stagingArea("StagedFileURL")
	if err != nil {
		return "", time.Time{}, err
	}
	return st.SignedURL(ctx, path)
}

func (s *Services) PutDeliveryCursor(ctx context.Context, e destination.Epoch, cursor []byte) error {
	st, err := s.stagingArea("PutDeliveryCursor")
	if err != nil {
		return err
	}
	return st.PutCursor(ctx, e, cursor)
}

func (s *Services) GetDeliveryCursor(ctx context.Context, e destination.Epoch) ([]byte, bool, error) {
	st, err := s.stagingArea("GetDeliveryCursor")
	if err != nil {
		return nil, false, err
	}
	return st.Cursor(ctx, e)
}
