//go:build integration

package oci

import (
	"cmp"
	"crypto/rand"
	"net/http"
	"os"
	"strings"
	"testing"
)

// The compose registry (make deps-up), CNCF Distribution, or
// OPENETL_TEST_REGISTRY.
func TestDistribution(t *testing.T) {
	host := cmp.Or(os.Getenv("OPENETL_TEST_REGISTRY"), "127.0.0.1:5001")
	ns, err := Parse(Scheme + host + "/test-" + strings.ToLower(rand.Text())[:10] + "/connectors")
	if err != nil {
		t.Fatal(err)
	}
	testPushPullAndIndex(t, &Client{HTTP: &http.Client{}}, ns)
}
