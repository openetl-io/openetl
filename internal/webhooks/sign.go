// Package webhooks signs webhooks as the Standard Webhooks spec says, and
// delivers platform events to them.
package webhooks

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const secretPrefix = "whsec_"

// ParseSecret decodes a Standard Webhooks secret: whsec_ and the base64 of
// a key of 24 to 64 bytes.
func ParseSecret(s string) ([]byte, error) {
	b64, ok := strings.CutPrefix(strings.TrimSpace(s), secretPrefix)
	if !ok {
		return nil, errors.New("a webhook secret starts with whsec_")
	}
	key, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, errors.New("a webhook secret is whsec_ and base64")
	}
	if len(key) < 24 || len(key) > 64 {
		return nil, fmt.Errorf("a webhook secret's key is 24 to 64 bytes, not %d", len(key))
	}
	return key, nil
}

// Signature is webhook-signature's value: v1 and the HMAC-SHA256 of
// id.timestamp.body under each key, space-separated.
func Signature(id string, ts time.Time, body []byte, keys ...[]byte) string {
	sigs := make([]string, len(keys))
	for i, k := range keys {
		sigs[i] = "v1," + sign(k, id, ts.Unix(), body)
	}
	return strings.Join(sigs, " ")
}

func sign(key []byte, id string, ts int64, body []byte) string {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(id + "." + strconv.FormatInt(ts, 10) + "."))
	m.Write(body)
	return base64.StdEncoding.EncodeToString(m.Sum(nil))
}

// SetHeaders sets webhook-id, webhook-timestamp and webhook-signature.
func SetHeaders(h http.Header, id string, ts time.Time, body []byte, keys ...[]byte) {
	h.Set("webhook-id", id)
	h.Set("webhook-timestamp", strconv.FormatInt(ts.Unix(), 10))
	h.Set("webhook-signature", Signature(id, ts, body, keys...))
}

// Verify checks that one of h's signatures is key's, and that its timestamp
// is within tolerance of now.
func Verify(key []byte, h http.Header, body []byte, now time.Time, tolerance time.Duration) error {
	id, sigs := h.Get("webhook-id"), h.Get("webhook-signature")
	ts, err := strconv.ParseInt(h.Get("webhook-timestamp"), 10, 64)
	switch {
	case id == "" || sigs == "":
		return errors.New("webhooks: missing webhook-id or webhook-signature")
	case err != nil:
		return errors.New("webhooks: bad webhook-timestamp")
	case now.Sub(time.Unix(ts, 0)).Abs() > tolerance:
		return errors.New("webhooks: webhook-timestamp is too far from now")
	}
	want := sign(key, id, ts, body)
	for s := range strings.FieldsSeq(sigs) {
		v, sig, ok := strings.Cut(s, ",")
		if ok && v == "v1" && hmac.Equal([]byte(sig), []byte(want)) {
			return nil
		}
	}
	return errors.New("webhooks: no signature matches")
}
