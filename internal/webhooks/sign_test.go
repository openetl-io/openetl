package webhooks

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

// The Standard Webhooks spec's own example.
func TestSignatureMatchesTheSpec(t *testing.T) {
	key, err := ParseSecret("whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw")
	if err != nil {
		t.Fatal(err)
	}
	got := Signature("msg_p5jXN8AQM9LWM0D4loKWxJek", time.Unix(1614265330, 0), []byte(`{"test": 2432232314}`), key)
	if want := "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="; got != want {
		t.Fatalf("signature %q, want %q", got, want)
	}
}

func TestVerify(t *testing.T) {
	old, _ := ParseSecret("whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw")
	cur, _ := ParseSecret("whsec_" + strings.Repeat("A", 32))
	now := time.Now()
	body := []byte(`{"type":"sync.failed"}`)
	h := http.Header{}
	SetHeaders(h, "evt_1", now, body, cur, old)
	if n := len(strings.Fields(h.Get("webhook-signature"))); n != 2 {
		t.Fatalf("%d signatures, want one per key", n)
	}
	for _, k := range [][]byte{cur, old} {
		if err := Verify(k, h, body, now, 5*time.Minute); err != nil {
			t.Errorf("a key that signed it: %v", err)
		}
	}
	other, _ := ParseSecret("whsec_" + strings.Repeat("B", 32))
	for name, check := range map[string]error{
		"another key":      Verify(other, h, body, now, 5*time.Minute),
		"another body":     Verify(cur, h, []byte(`{}`), now, 5*time.Minute),
		"an old timestamp": Verify(cur, h, body, now.Add(10*time.Minute), 5*time.Minute),
	} {
		if check == nil {
			t.Errorf("verified with %s", name)
		}
	}
}

func TestParseSecret(t *testing.T) {
	for _, bad := range []string{"", "MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw", "whsec_!!!", "whsec_" + strings.Repeat("A", 8), "whsec_" + strings.Repeat("A", 100)} {
		if _, err := ParseSecret(bad); err == nil {
			t.Errorf("%q parsed", bad)
		}
	}
	if _, err := ParseSecret("whsec_" + strings.Repeat("A", 32) + "\n"); err != nil {
		t.Errorf("a trailing newline: %v", err)
	}
}

func TestRetryAfter(t *testing.T) {
	if d := retryAfter("120"); d != 2*time.Minute {
		t.Errorf("seconds: %s", d)
	}
	if d := retryAfter(time.Now().Add(time.Hour).UTC().Format(http.TimeFormat)); d < 59*time.Minute || d > time.Hour {
		t.Errorf("a date: %s", d)
	}
	for _, v := range []string{"", "soon", "-5"} {
		if d := retryAfter(v); d != 0 {
			t.Errorf("%q: %s", v, d)
		}
	}
}

func TestJitter(t *testing.T) {
	for range 1000 {
		if d := jitter(10 * time.Second); d < 9*time.Second || d > 11*time.Second {
			t.Fatalf("jitter(10s) = %s", d)
		}
	}
	if d := jitter(0); d != 0 {
		t.Errorf("jitter(0) = %s", d)
	}
}
