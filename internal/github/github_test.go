package github

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

func TestParse(t *testing.T) {
	if o, r, ok := ParseRepo("https://github.com/openetl-io/openetl-connectors"); !ok || o != "openetl-io" || r != "openetl-connectors" {
		t.Errorf("ParseRepo: %s %s %v", o, r, ok)
	}
	for _, bad := range []string{"https://github.com/a", "https://github.com/a/b/", "http://github.com/a/b", "https://gitlab.com/a/b"} {
		if _, _, ok := ParseRepo(bad); ok {
			t.Errorf("ParseRepo(%s) took it", bad)
		}
	}
	o, r, tag, asset, ok := ParseAssetURL("https://github.com/acme/conn/releases/download/postgres/v0.2.0/postgres-source-linux-amd64")
	if !ok || o != "acme" || r != "conn" || tag != "postgres/v0.2.0" || asset != "postgres-source-linux-amd64" {
		t.Errorf("ParseAssetURL: %s %s %s %s %v", o, r, tag, asset, ok)
	}
	for in, want := range map[string]Ref{
		"github.com/acme/conn@1.2.0":          {Owner: "acme", Repo: "conn", Version: "1.2.0"},
		"github.com/acme/conn/postgres@0.2.0": {Owner: "acme", Repo: "conn", Package: "postgres", Version: "0.2.0"},
	} {
		got, ok := ParseRef(in)
		if !ok || got != want || got.String() != in {
			t.Errorf("ParseRef(%s) = %+v, %v", in, got, ok)
		}
	}
	if r, _ := ParseRef("github.com/acme/conn/postgres@0.2.0"); r.Tag() != "postgres/v0.2.0" || r.Repository() != "https://github.com/acme/conn" {
		t.Errorf("tag %s, repository %s", r.Tag(), r.Repository())
	}
	if r, _ := ParseRef("github.com/acme/conn@1.2.0"); r.Tag() != "v1.2.0" {
		t.Errorf("tag %s", r.Tag())
	}
	for _, bad := range []string{"github.com/acme@1", "github.com/acme/conn", "github.com/acme/conn/Pg@1", "gitlab.com/a/b@1"} {
		if _, ok := ParseRef(bad); ok {
			t.Errorf("ParseRef(%s) took it", bad)
		}
	}
}

func TestReleases(t *testing.T) {
	var auth []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = append(auth, r.Header.Get("Authorization"))
		switch r.URL.Path {
		case "/repos/acme/conn/releases":
			page, _ := strconv.Atoi(r.URL.Query().Get("page"))
			n := 100
			if page == 2 {
				n = 3
			}
			out := make([]Release, n)
			for i := range out {
				out[i].TagName = fmt.Sprintf("v%d.%d.0", page, i)
			}
			_ = json.NewEncoder(w).Encode(out)
		case "/repos/acme/conn/releases/tags/postgres/v1.0.0":
			_ = json.NewEncoder(w).Encode(Release{TagName: "postgres/v1.0.0", Assets: []Asset{{ID: 7, Name: "x", URL: "http://" + r.Host + "/repos/acme/conn/releases/assets/7"}}})
		case "/repos/acme/conn/releases/assets/7":
			if r.Header.Get("Accept") != "application/octet-stream" {
				http.Error(w, "json", http.StatusBadRequest)
				return
			}
			_, _ = w.Write([]byte("content"))
		case "/repos/acme/limited/releases":
			w.Header().Set("X-RateLimit-Remaining", "0")
			http.Error(w, "rate limited", http.StatusForbidden)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	c := &Client{HTTP: srv.Client(), API: srv.URL, Token: "tok"}
	rels, err := c.Releases(t.Context(), "acme", "conn", 5)
	if err != nil || len(rels) != 103 {
		t.Fatalf("%d releases, %v", len(rels), err)
	}
	rel, err := c.ReleaseByTag(t.Context(), "acme", "conn", "postgres/v1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	a, ok := rel.Asset("x")
	if !ok {
		t.Fatal("no asset")
	}
	var buf bytes.Buffer
	if err := c.Download(t.Context(), a, &buf, 100); err != nil || buf.String() != "content" {
		t.Errorf("downloaded %q, %v", buf.String(), err)
	}
	if err := c.Download(t.Context(), a, &bytes.Buffer{}, 3); err == nil {
		t.Error("an asset over the limit was downloaded")
	}
	if _, err := c.ReleaseByTag(t.Context(), "acme", "conn", "nothing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("a missing release: %v", err)
	}
	if _, err := c.Releases(t.Context(), "acme", "limited", 1); err == nil {
		t.Error("a rate-limited listing succeeded")
	}
	for _, a := range auth {
		if a != "Bearer tok" {
			t.Errorf("sent %q", a)
		}
	}
}
