package sdk

import (
	"fmt"
	"io"
	"log/slog"
)

const redacted = "[redacted]"

// Secret is a config value that never prints: fmt, slog and encoders show
// "[redacted]". It decodes from a JSON string. Reveal returns the value.
type Secret struct {
	// Behind a pointer, so printing a struct that holds a Secret in an
	// unexported field shows an address, not the value.
	v *string
}

func NewSecret(v string) Secret { return Secret{v: &v} }

func (s Secret) Reveal() string {
	if s.v == nil {
		return ""
	}
	return *s.v
}

func (s Secret) IsZero() bool { return s.Reveal() == "" }

func (s Secret) String() string                { return redacted }
func (s Secret) GoString() string              { return redacted }
func (s Secret) Format(f fmt.State, _ rune)    { _, _ = io.WriteString(f, redacted) }
func (s Secret) LogValue() slog.Value          { return slog.StringValue(redacted) }
func (s Secret) MarshalText() ([]byte, error)  { return []byte(redacted), nil }
func (s *Secret) UnmarshalText(b []byte) error { *s = NewSecret(string(b)); return nil }
