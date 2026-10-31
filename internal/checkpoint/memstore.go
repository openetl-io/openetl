package checkpoint

import (
	"bytes"
	"context"
	"slices"
	"strings"
	"sync"
)

// MemStore is an in-memory Store. Versions are derived from content, as on
// S3.
type MemStore struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func NewMemStore() *MemStore {
	return &MemStore{objects: make(map[string][]byte)}
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

func (s *MemStore) Create(_ context.Context, key string, data []byte) (Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.objects[key]; ok {
		return "", ErrPrecondition
	}
	s.objects[key] = bytes.Clone(data)
	return contentVersion(data), nil
}

func (s *MemStore) Swap(_ context.Context, key string, data []byte, prev Version) (Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.objects[key]
	if !ok || contentVersion(cur) != prev {
		return "", ErrPrecondition
	}
	s.objects[key] = bytes.Clone(data)
	return contentVersion(data), nil
}

func (s *MemStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, key)
	return nil
}

func (s *MemStore) List(_ context.Context, prefix string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var keys []string
	for k := range s.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	return keys, nil
}
