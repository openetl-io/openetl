package pkgcatalog

import "testing"

func TestCovers(t *testing.T) {
	for _, c := range []struct {
		source, url string
		want        bool
	}{
		{"oci://ghcr.io/acme/connectors", "oci://ghcr.io/acme/connectors/postgres@sha256:aa", true},
		{"oci://ghcr.io/acme/connectors", "oci://ghcr.io/acme/connectors:1.0.0", true},
		{"oci://ghcr.io/acme/connectors", "oci://ghcr.io/acme/connectors", true},
		{"oci://ghcr.io/acme/connectors", "oci://ghcr.io/acme/connectorsx/postgres", false},
		{"oci://ghcr.io/acme", "oci://ghcr.io/acme/connectors/postgres", true},
		{"https://github.com/acme/conn", "https://github.com/acme/conn/releases/download/v1/x", true},
		{"https://github.com/acme/conn", "https://github.com/acme/connx/releases/download/v1/x", false},
	} {
		if got := Covers(c.source, c.url); got != c.want {
			t.Errorf("Covers(%s, %s) = %v", c.source, c.url, got)
		}
	}
}
