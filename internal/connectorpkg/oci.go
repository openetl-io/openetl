package connectorpkg

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
)

// Media types a registry serves an image's manifest as.
var manifestTypes = []string{
	"application/vnd.oci.image.index.v1+json",
	"application/vnd.docker.distribution.manifest.list.v2+json",
	"application/vnd.oci.image.manifest.v1+json",
	"application/vnd.docker.distribution.manifest.v2+json",
}

const maxManifest = 4 << 20

// Registry resolves image tags through registries' HTTP API, anonymously.
type Registry struct {
	Client *http.Client
	// Overrides the scheme for hosts, such as a local registry over http.
	Insecure map[string]bool
}

// Resolve returns the digest of what ref:tag names, and the platforms it
// holds. ref is an image repository, such as ghcr.io/acme/source.
func (r *Registry) Resolve(ctx context.Context, ref, tag string) (digest string, platforms []string, err error) {
	host, repo, ok := strings.Cut(ref, "/")
	if !ok {
		return "", nil, fmt.Errorf("image %s names no registry", ref)
	}
	if host == "docker.io" {
		host = "registry-1.docker.io"
		if !strings.Contains(repo, "/") {
			repo = "library/" + repo
		}
	}
	scheme := "https"
	if r.Insecure[host] {
		scheme = "http"
	}
	base := scheme + "://" + host + "/v2/" + repo
	body, mediaType, err := r.get(ctx, base+"/manifests/"+url.PathEscape(tag), repo, manifestTypes)
	if err != nil {
		return "", nil, fmt.Errorf("image %s:%s: %w", ref, tag, err)
	}
	sum := sha256.Sum256(body)
	digest = "sha256:" + hex.EncodeToString(sum[:])
	var m struct {
		MediaType string `json:"mediaType"`
		Manifests []struct {
			Platform *struct {
				OS           string `json:"os"`
				Architecture string `json:"architecture"`
			} `json:"platform"`
		} `json:"manifests"`
		Config struct {
			Digest string `json:"digest"`
		} `json:"config"`
	}
	if err := json.Unmarshal(body, &m); err != nil {
		return "", nil, fmt.Errorf("image %s:%s: %w", ref, tag, err)
	}
	mediaType = strings.TrimSpace(strings.Split(cmp.Or(mediaType, m.MediaType), ";")[0])
	switch mediaType {
	case manifestTypes[0], manifestTypes[1]:
		for _, d := range m.Manifests {
			if p := d.Platform; p != nil && p.OS != "unknown" && p.OS != "" {
				platforms = append(platforms, p.OS+"/"+p.Architecture)
			}
		}
	case manifestTypes[2], manifestTypes[3]:
		cfg, _, err := r.get(ctx, base+"/blobs/"+m.Config.Digest, repo, nil)
		if err != nil {
			return "", nil, fmt.Errorf("image %s:%s's config: %w", ref, tag, err)
		}
		var c struct {
			OS           string `json:"os"`
			Architecture string `json:"architecture"`
		}
		if err := json.Unmarshal(cfg, &c); err != nil || c.OS == "" {
			return "", nil, fmt.Errorf("image %s:%s's config names no platform", ref, tag)
		}
		platforms = []string{c.OS + "/" + c.Architecture}
	default:
		return "", nil, fmt.Errorf("image %s:%s is a %q, not an image", ref, tag, mediaType)
	}
	slices.Sort(platforms)
	return digest, slices.Compact(platforms), nil
}

// get fetches path, getting an anonymous token when the registry asks for
// one.
func (r *Registry) get(ctx context.Context, u, repo string, accept []string) ([]byte, string, error) {
	token := ""
	for range 2 {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return nil, "", err
		}
		if len(accept) > 0 {
			req.Header.Set("Accept", strings.Join(accept, ", "))
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := r.Client.Do(req)
		if err != nil {
			return nil, "", err
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, maxManifest))
		_ = resp.Body.Close()
		if err != nil {
			return nil, "", err
		}
		switch {
		case resp.StatusCode == http.StatusOK:
			return body, resp.Header.Get("Content-Type"), nil
		case resp.StatusCode == http.StatusUnauthorized && token == "":
			if token, err = r.token(ctx, resp.Header.Get("WWW-Authenticate"), repo); err != nil {
				return nil, "", err
			}
		default:
			return nil, "", fmt.Errorf("%s: %s", u, resp.Status)
		}
	}
	return nil, "", errors.New("the registry refused its own token")
}

// token gets an anonymous pull token from a Bearer challenge.
func (r *Registry) token(ctx context.Context, challenge, repo string) (string, error) {
	scheme, params, ok := strings.Cut(challenge, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return "", fmt.Errorf("the registry wants %q, and only anonymous pulls are supported", challenge)
	}
	p := map[string]string{}
	for part := range strings.SplitSeq(params, ",") {
		k, v, _ := strings.Cut(strings.TrimSpace(part), "=")
		p[k] = strings.Trim(v, `"`)
	}
	if p["realm"] == "" {
		return "", errors.New("the registry's challenge has no realm")
	}
	q := url.Values{"scope": {cmp.Or(p["scope"], "repository:"+repo+":pull")}}
	if p["service"] != "" {
		q.Set("service", p["service"])
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p["realm"]+"?"+q.Encode(), nil)
	if err != nil {
		return "", err
	}
	resp, err := r.Client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("token: %s", resp.Status)
	}
	var t struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&t); err != nil {
		return "", fmt.Errorf("token: %w", err)
	}
	return cmp.Or(t.Token, t.AccessToken), nil
}
