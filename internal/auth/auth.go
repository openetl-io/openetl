// Package auth makes API keys and runner tokens, and defines the roles keys
// act with.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"

	"connectrpc.com/connect"
)

const (
	KeyPrefix   = "oetl_"
	TokenPrefix = "oetl_dp_"
	// How much of a key or token the API shows.
	shown = 12
	// 62^43 > 2^256.
	encodedLen = 43
)

// A Credential is a new API key or runner token: its prefix, then 32 random
// bytes in base62.
type Credential struct {
	Secret string
	Hash   []byte
	// Its first characters, which the API shows to tell credentials apart.
	Shown string
}

func NewKey() Credential { return newCredential(KeyPrefix) }

func NewRunnerToken() Credential { return newCredential(TokenPrefix) }

func newCredential(prefix string) Credential {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	enc := new(big.Int).SetBytes(b).Text(62)
	secret := prefix + strings.Repeat("0", encodedLen-len(enc)) + enc
	return Credential{Secret: secret, Hash: Hash(secret), Shown: secret[:shown]}
}

func Hash(secret string) []byte {
	h := sha256.Sum256([]byte(secret))
	return h[:]
}

// IsRunnerToken tells runner tokens from API keys.
func IsRunnerToken(secret string) bool { return strings.HasPrefix(secret, TokenPrefix) }

type Role int32

const (
	None Role = iota
	Viewer
	Operator
	Editor
	Admin
	Owner
)

var roleNames = []string{"none", "viewer", "operator", "editor", "admin", "owner"}

func (r Role) String() string {
	if r < 0 || int(r) >= len(roleNames) {
		return fmt.Sprintf("unknown role (%d)", int32(r))
	}
	return roleNames[r]
}

// OrgOnly roles are bound only on the org.
func (r Role) OrgOnly() bool { return r >= Admin }

func ParseRole(s string) (Role, bool) {
	for i, n := range roleNames[1:] {
		if s == n {
			return Role(i + 1), true
		}
	}
	return None, false
}

// Bindings are a principal's roles, on the org and by workspace name.
type Bindings struct {
	Org        Role
	Workspaces map[string]Role
}

// ParseBindings parses role, for the org, and role/workspace.
func ParseBindings(bs []string) Bindings {
	var b Bindings
	for _, s := range bs {
		name, ws, inWS := strings.Cut(s, "/")
		r, ok := ParseRole(name)
		switch {
		case !ok:
		case !inWS:
			b.Org = max(b.Org, r)
		default:
			if b.Workspaces == nil {
				b.Workspaces = map[string]Role{}
			}
			b.Workspaces[ws] = max(b.Workspaces[ws], r)
		}
	}
	return b
}

// In is the highest role in workspace; "" for the org alone.
func (b Bindings) In(workspace string) Role {
	if workspace == "" {
		return b.Org
	}
	return max(b.Org, b.Workspaces[workspace])
}

// WriteKeyFile replaces path with a file, readable only by its owner, that
// holds secret.
func WriteKeyFile(path, secret string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".key-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(f.Name()) }()
	if _, err := f.WriteString(secret + "\n"); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}

func ReadKeyFile(path string) (string, error) {
	b, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return "", err
	}
	s := strings.TrimSpace(string(b))
	if s == "" {
		return "", fmt.Errorf("%s is empty", path)
	}
	return s, nil
}

// WithBearer sends secret with every call, as Authorization: Bearer.
func WithBearer(secret string) connect.ClientOption {
	return connect.WithInterceptors(bearer(secret))
}

type bearer string

func (b bearer) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		req.Header().Set("Authorization", "Bearer "+string(b))
		return next(ctx, req)
	}
}

func (b bearer) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		conn := next(ctx, spec)
		conn.RequestHeader().Set("Authorization", "Bearer "+string(b))
		return conn
	}
}

func (bearer) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}
