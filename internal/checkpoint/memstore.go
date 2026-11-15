package checkpoint

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"time"
)

// MemStore is an in-memory Store. Versions are derived from content, as on
// S3.
type MemStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	written map[string]time.Time
	// When a write happens; nil: time.Now.
	now func() time.Time
}

func NewMemStore() *MemStore {
	return &MemStore{objects: make(map[string][]byte), written: make(map[string]time.Time)}
}

// SetClock makes writes from now on happen at now's times.
func (s *MemStore) SetClock(now func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = now
}

func (s *MemStore) put(key string, data []byte) {
	s.objects[key] = bytes.Clone(data)
	now := time.Now
	if s.now != nil {
		now = s.now
	}
	s.written[key] = now()
}

func (s *MemStore) Get(_ context.Context, key string) ([]byte, Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.objects[key]
	if !ok {
		return nil, "", ErrNotFound
	}
	return bytes.Clone(data), contentVersion(data), nil
}

func (s *MemStore) GetPrefix(_ context.Context, key string, n int) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.objects[key]
	if !ok {
		return nil, ErrNotFound
	}
	return bytes.Clone(data[:min(n, len(data))]), nil
}

func (s *MemStore) Create(_ context.Context, key string, data []byte) (Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.objects[key]; ok {
		return "", ErrPrecondition
	}
	s.put(key, data)
	return contentVersion(data), nil
}

func (s *MemStore) Swap(_ context.Context, key string, data []byte, prev Version) (Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.objects[key]
	if !ok || contentVersion(cur) != prev {
		return "", ErrPrecondition
	}
	s.put(key, data)
	return contentVersion(data), nil
}

func (s *MemStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, key)
	delete(s.written, key)
	return nil
}

func (s *MemStore) List(ctx context.Context, prefix string) ([]string, error) {
	objects, err := s.ListObjects(ctx, prefix)
	return Keys(objects), err
}

func (s *MemStore) ListObjects(_ context.Context, prefix string) ([]Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Object
	for k := range s.objects {
		if strings.HasPrefix(k, prefix) {
			out = append(out, Object{Key: k, Modified: s.written[k]})
		}
	}
	SortObjects(out)
	return out, nil
}
