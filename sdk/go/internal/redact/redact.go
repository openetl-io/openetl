// Package redact takes what can hold credentials out of text the SDK shows.
package redact

import (
	"net/url"
	"strings"
)

// URL is raw without its userinfo, query and fragment. A "?" or "#" in a
// password, which ends the authority for url.Parse, doesn't keep the
// userinfo: an "@" before the path ends it, wherever such a character is.
func URL(raw string) string {
	scheme, rest := "", raw
	if i := strings.Index(raw, "://"); i >= 0 {
		scheme, rest = raw[:i+3], raw[i+3:]
	}
	authority := rest
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		authority = rest[:i]
	}
	if u, err := url.Parse(raw); err == nil && (u.User != nil || !strings.Contains(authority, "@")) {
		u.User, u.RawQuery, u.ForceQuery, u.Fragment, u.RawFragment = nil, "", false, "", ""
		return u.String()
	}
	// Cut by hand: the userinfo ends at the last "@" before the path, or,
	// with none there, at the last "@" of all, as a "/" in a password puts
	// the "@" after it.
	if i := strings.LastIndexByte(authority, '@'); i >= 0 {
		rest = rest[i+1:]
	} else if i := strings.LastIndexByte(rest, '@'); i >= 0 {
		rest = rest[i+1:]
	}
	if i := strings.IndexAny(rest, "?#"); i >= 0 {
		rest = rest[:i]
	}
	return scheme + rest
}
