package auth

import (
	"errors"
	"strings"
	"testing"

	"golang.org/x/crypto/argon2"
)

func TestPasswords(t *testing.T) {
	h := HashPassword("correct horse battery staple")
	if !strings.HasPrefix(h, "$argon2id$v=19$m=19456,t=2,p=1$") {
		t.Fatalf("hash %q", h)
	}
	if h == HashPassword("correct horse battery staple") {
		t.Fatal("two hashes of one password are the same; the salt isn't random")
	}
	if ok, rehash := CheckPassword(h, "correct horse battery staple"); !ok || rehash {
		t.Fatalf("check = %v, rehash = %v", ok, rehash)
	}
	if ok, _ := CheckPassword(h, "correct horse battery stapl"); ok {
		t.Fatal("a wrong password matched")
	}
	salt := []byte("0123456789abcdef")
	old := phc(8*1024, 1, 1, salt, argon2.IDKey([]byte("an older password hash"), salt, 1, 8*1024, 1, 32))
	if ok, rehash := CheckPassword(old, "an older password hash"); !ok || !rehash {
		t.Fatalf("older settings: check = %v, rehash = %v", ok, rehash)
	}
	for _, bad := range []string{"", "$argon2i$v=19$m=1,t=1,p=1$c2FsdA$a2V5", "$argon2id$v=19$m=0,t=1,p=1$c2FsdA$a2V5", "$argon2id$v=19$m=1,t=1,p=1$!$a2V5"} {
		if ok, _ := CheckPassword(bad, "x"); ok {
			t.Fatalf("%q matched", bad)
		}
	}
}

func TestPasswordRules(t *testing.T) {
	for pw, want := range map[string]string{
		"correct horse battery staple":       "",
		"ünïcödé pässwörd":                   "",
		"short one":                          "15 to 256",
		strings.Repeat("ab", 129):            "15 to 256",
		"abababababababab":                   "too few",
		"1qaz2wsx3edc4rfv":                   "most common",
		"PasswordPassword":                   "most common",
		"ada.lovelace.1815":                  "email or name",
		"ADA.LOVELACE.1815@EXAMPLE.COM":      "email or name",
		"ada-of-analytical-engines":          "email or name",
		"\xff\xfe" + strings.Repeat("x", 20): "UTF-8",
	} {
		err := PasswordRules(pw, "ada.lovelace.1815@example.com", "ada-of-analytical-engines")
		switch {
		case want == "" && err != nil:
			t.Errorf("%q: %v", pw, err)
		case want != "" && (err == nil || !errors.Is(err, ErrPasswordRules) || !strings.Contains(err.Error(), want)):
			t.Errorf("%q: got %v, want %q", pw, err, want)
		}
	}
}
