// Package github reads a repository's releases and their assets through
// GitHub's REST API, for connector packages released there.
package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/openetl-io/openetl/internal/netguard"
)

// DefaultAPI is GitHub's REST API.
const DefaultAPI = "https://api.github.com"

// Client reads releases, with a token for private repositories and a higher
// rate limit, or anonymously.
type Client struct {
	// Nil: a netguard client that reaches public addresses only.
	HTTP *http.Client
	// Empty: DefaultAPI.
	API   string
	Token string
}

// Release is a repository's release.
type Release struct {
	TagName    string  `json:"tag_name"`
	HTMLURL    string  `json:"html_url"`
	Draft      bool    `json:"draft"`
	Prerelease bool    `json:"prerelease"`
	Assets     []Asset `json:"assets"`
}

// Asset is a release's file.
type Asset struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
	// The API's, which serves the content with a token too.
	URL                string `json:"url"`
	BrowserDownloadURL string `json:"browser_download_url"`
	Size               int64  `json:"size"`
	UpdatedAt          string `json:"updated_at"`
}

// Asset returns the release's asset by name.
func (r *Release) Asset(name string) (Asset, bool) {
	for _, a := range r.Assets {
		if a.Name == name {
			return a, true
		}
	}
	return Asset{}, false
}

// ErrNotFound is a repository, release or asset that GitHub doesn't have, or
// doesn't show without a token.
var ErrNotFound = errors.New("not found")

const maxPage = 8 << 20

// Releases lists the repository's releases, newest first, at most pages
// pages of 100.
func (c *Client) Releases(ctx context.Context, owner, repo string, pages int) ([]Release, error) {
	var out []Release
	for page := 1; page <= pages; page++ {
		var batch []Release
		u := fmt.Sprintf("%s/repos/%s/%s/releases?per_page=100&page=%d", c.api(), url.PathEscape(owner), url.PathEscape(repo), page)
		if err := c.get(ctx, u, &batch); err != nil {
			return nil, err
		}
		out = append(out, batch...)
		if len(batch) < 100 {
			break
		}
	}
	return out, nil
}

// ReleaseByTag returns the repository's release of a tag.
func (c *Client) ReleaseByTag(ctx context.Context, owner, repo, tag string) (*Release, error) {
	var r Release
	u := fmt.Sprintf("%s/repos/%s/%s/releases/tags/%s", c.api(), url.PathEscape(owner), url.PathEscape(repo), url.PathEscape(tag))
	if err := c.get(ctx, u, &r); err != nil {
		return nil, fmt.Errorf("%s/%s's release %s: %w", owner, repo, tag, err)
	}
	return &r, nil
}

// Download writes an asset's content to w, failing past limit bytes.
func (c *Client) Download(ctx context.Context, a Asset, w io.Writer, limit int64) error {
	resp, err := c.do(ctx, a.URL, "application/octet-stream")
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	n, err := io.Copy(w, io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return fmt.Errorf("%s: %w", a.Name, err)
	}
	if n > limit {
		return fmt.Errorf("%s is more than %d bytes", a.Name, limit)
	}
	return nil
}

func (c *Client) api() string {
	if c.API == "" {
		return DefaultAPI
	}
	return strings.TrimSuffix(c.API, "/")
}

func (c *Client) get(ctx context.Context, u string, v any) error {
	resp, err := c.do(ctx, u, "application/vnd.github+json")
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxPage)).Decode(v); err != nil {
		return fmt.Errorf("%s: %w", u, err)
	}
	return nil
}

func (c *Client) do(ctx context.Context, u, accept string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", accept)
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	hc := c.HTTP
	if hc == nil {
		hc = (&netguard.Policy{}).Client()
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusOK {
		return resp, nil
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	_ = resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return nil, fmt.Errorf("%s: %w", u, ErrNotFound)
	case resp.StatusCode == http.StatusForbidden && resp.Header.Get("X-RateLimit-Remaining") == "0",
		resp.StatusCode == http.StatusTooManyRequests:
		return nil, fmt.Errorf("%s: GitHub's rate limit is spent until %s; a token raises it", u, resp.Header.Get("X-RateLimit-Reset"))
	}
	return nil, fmt.Errorf("%s: %s: %s", u, resp.Status, strings.TrimSpace(string(body)))
}

var (
	repoURL  = regexp.MustCompile(`^https://github\.com/([A-Za-z0-9-]{1,39})/([A-Za-z0-9._-]{1,100})$`)
	assetURL = regexp.MustCompile(`^https://github\.com/([A-Za-z0-9-]{1,39})/([A-Za-z0-9._-]{1,100})/releases/download/(.+)/([^/]+)$`)
	refPat   = regexp.MustCompile(`^github\.com/([A-Za-z0-9-]{1,39})/([A-Za-z0-9._-]{1,100})(/([a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?))?@([^@/\s]+)$`)
)

// ParseRepo reads https://github.com/OWNER/REPO.
func ParseRepo(u string) (owner, repo string, ok bool) {
	m := repoURL.FindStringSubmatch(u)
	if m == nil {
		return "", "", false
	}
	return m[1], m[2], true
}

// ParseAssetURL reads a release asset's download URL,
// https://github.com/OWNER/REPO/releases/download/TAG/ASSET.
func ParseAssetURL(u string) (owner, repo, tag, asset string, ok bool) {
	m := assetURL.FindStringSubmatch(u)
	if m == nil {
		return "", "", "", "", false
	}
	tag, err := url.PathUnescape(m[3])
	if err != nil {
		return "", "", "", "", false
	}
	return m[1], m[2], tag, m[4], true
}

// Ref is a package version released on GitHub: github.com/OWNER/REPO@VERSION
// for a repository's one package, tagged vVERSION, or
// github.com/OWNER/REPO/PACKAGE@VERSION, tagged PACKAGE/vVERSION.
type Ref struct {
	Owner, Repo string
	// The package's short name; empty for a repository's one package.
	Package string
	Version string
}

// ParseRef reads github.com/OWNER/REPO[/PACKAGE]@VERSION.
func ParseRef(s string) (Ref, bool) {
	m := refPat.FindStringSubmatch(s)
	if m == nil {
		return Ref{}, false
	}
	return Ref{Owner: m[1], Repo: m[2], Package: m[4], Version: m[6]}, true
}

// Tag is the release's git tag.
func (r Ref) Tag() string {
	if r.Package == "" {
		return "v" + r.Version
	}
	return r.Package + "/v" + r.Version
}

func (r Ref) String() string {
	s := "github.com/" + r.Owner + "/" + r.Repo
	if r.Package != "" {
		s += "/" + r.Package
	}
	return s + "@" + r.Version
}

// Repository is the repository's https URL.
func (r Ref) Repository() string { return "https://github.com/" + r.Owner + "/" + r.Repo }
