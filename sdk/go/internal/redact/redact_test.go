package redact

import "testing"

func TestURL(t *testing.T) {
	for raw, want := range map[string]string{
		"https://u:p@api.example.com:8443/v1/x?k=v#f":    "https://api.example.com:8443/v1/x",
		"https://api.example.com/v1/users/@me?token=abc": "https://api.example.com/v1/users/@me",
		"https://registry.example.com/@scope/pkg":        "https://registry.example.com/@scope/pkg",
		// A password with "#" or "?", whether url.Parse refuses it or reads
		// the userinfo as a host.
		"https://user:pa#ss@api.example.com/v1/me": "https://api.example.com/v1/me",
		"https://user:123#x@host":                  "https://host",
		"https://user:123?x@host/v1":               "https://host/v1",
		"https://tok#en@api.example.com":           "https://api.example.com",
		"user:123#x@host":                          "host",
		// One that doesn't parse, with a "/" or "@" in the password.
		"postgres://etl:p@ss@db:port/x?password=hunter2": "postgres://db:port/x",
		"https://user:pa/ss@host/path":                   "https://host/path",
	} {
		if got := URL(raw); got != want {
			t.Errorf("URL(%q) = %q, want %q", raw, got, want)
		}
	}
}
