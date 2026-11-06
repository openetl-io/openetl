package auth

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"crypto/subtle"
	_ "embed"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"sync"
	"unicode/utf8"

	"golang.org/x/crypto/argon2"
)

// Passwords are hashed with argon2id at OWASP's settings, as PHC strings
// (ADR-0020).
const (
	argonMemory  = 19 * 1024
	argonTime    = 2
	argonThreads = 1
	argonKeyLen  = 32
	argonSaltLen = 16

	MinPassword = 15
	MaxPassword = 256
)

const (
	LinkPrefix    = "oetl_pw_"
	SessionPrefix = "oetl_s_"
)

func NewPasswordLink() Credential { return newCredential(LinkPrefix) }

func NewSession() Credential { return newCredential(SessionPrefix) }

func HashPassword(password string) string {
	salt := make([]byte, argonSaltLen)
	_, _ = rand.Read(salt)
	return phc(argonMemory, argonTime, argonThreads, salt, argon2.IDKey([]byte(password), salt, argonTime, argonMemory, argonThreads, argonKeyLen))
}

func phc(m, t uint32, p uint8, salt, key []byte) string {
	enc := base64.RawStdEncoding
	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s", argon2.Version, m, t, p, enc.EncodeToString(salt), enc.EncodeToString(key))
}

// CheckPassword says whether password matches hash, and whether hash was
// made with other settings than HashPassword's, so it should be made again.
func CheckPassword(hash, password string) (ok, rehash bool) {
	var v int
	var m, t uint32
	var p uint8
	parts := strings.Split(hash, "$")
	if len(parts) != 6 || parts[1] != "argon2id" {
		return false, false
	}
	if _, err := fmt.Sscanf(parts[2], "v=%d", &v); err != nil || v != argon2.Version {
		return false, false
	}
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &m, &t, &p); err != nil || m == 0 || t == 0 || p == 0 {
		return false, false
	}
	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return false, false
	}
	key, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil || len(key) == 0 {
		return false, false
	}
	got := argon2.IDKey([]byte(password), salt, t, m, p, uint32(len(key))) //nolint:gosec // len(key) is small
	if subtle.ConstantTimeCompare(got, key) != 1 {
		return false, false
	}
	return true, m != argonMemory || t != argonTime || p != argonThreads || len(key) != argonKeyLen
}

// dummyHash is checked for users without a password, so a sign-in takes as
// long whether or not the email is known.
var dummyHash = sync.OnceValue(func() string { return HashPassword("not anyone's password") })

func DummyCheck(password string) { CheckPassword(dummyHash(), password) }

//go:embed common_passwords.txt
var commonList []byte

var common = sync.OnceValue(func() map[string]bool {
	m := map[string]bool{}
	s := bufio.NewScanner(bytes.NewReader(commonList))
	for s.Scan() {
		m[s.Text()] = true
	}
	return m
})

// ErrPasswordRules is the error PasswordRules wraps.
var ErrPasswordRules = errors.New("the password breaks a rule")

// PasswordRules checks a new password against NIST SP 800-63B-4's rules for
// a password that is the only factor: its length, and that it's none of
// the common ones nor one of the user's names.
func PasswordRules(password string, names ...string) error {
	n := utf8.RuneCountInString(password)
	lower := strings.ToLower(password)
	distinct := map[rune]bool{}
	for _, r := range lower {
		distinct[r] = true
	}
	switch {
	case !utf8.ValidString(password):
		return fmt.Errorf("%w: it isn't valid UTF-8", ErrPasswordRules)
	case n < MinPassword || n > MaxPassword:
		return fmt.Errorf("%w: a password needs %d to %d characters; this one has %d", ErrPasswordRules, MinPassword, MaxPassword, n)
	case len(distinct) < 4:
		return fmt.Errorf("%w: it repeats too few characters", ErrPasswordRules)
	case common()[lower]:
		return fmt.Errorf("%w: it's one of the most common passwords", ErrPasswordRules)
	}
	for _, name := range names {
		local, _, _ := strings.Cut(name, "@")
		if name != "" && (lower == strings.ToLower(name) || lower == strings.ToLower(local)) {
			return fmt.Errorf("%w: it can't be your email or name", ErrPasswordRules)
		}
	}
	return nil
}
