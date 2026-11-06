package auth

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestCredentials(t *testing.T) {
	key := regexp.MustCompile(`^oetl_[0-9a-zA-Z]{43}$`)
	token := regexp.MustCompile(`^oetl_dp_[0-9a-zA-Z]{43}$`)
	seen := map[string]bool{}
	for range 100 {
		k, tk := NewKey(), NewRunnerToken()
		if !key.MatchString(k.Secret) || !token.MatchString(tk.Secret) {
			t.Fatalf("key %q, token %q", k.Secret, tk.Secret)
		}
		if IsRunnerToken(k.Secret) || !IsRunnerToken(tk.Secret) {
			t.Fatalf("IsRunnerToken mixes up %q and %q", k.Secret, tk.Secret)
		}
		if !bytes.Equal(k.Hash, Hash(k.Secret)) || k.Shown != k.Secret[:12] || len(k.Hash) != 32 {
			t.Fatalf("hash or prefix of %q", k.Secret)
		}
		if seen[k.Secret] {
			t.Fatalf("%q twice", k.Secret)
		}
		seen[k.Secret] = true
	}
}

func TestBindings(t *testing.T) {
	b := ParseBindings([]string{"viewer", "editor/sales", "operator/sales", "nope", "operator/ops"})
	cases := []struct {
		workspace string
		want      Role
	}{
		{"", Viewer},
		{"sales", Editor},
		{"ops", Operator},
		{"other", Viewer},
	}
	for _, c := range cases {
		if got := b.In(c.workspace); got != c.want {
			t.Errorf("In(%q) = %s, want %s", c.workspace, got, c.want)
		}
	}
	if (Bindings{}).In("sales") != None {
		t.Error("no bindings grant a role")
	}
	if ParseBindings([]string{"owner"}).In("any") != Owner {
		t.Error("an org role covers every workspace")
	}
	for r := Viewer; r <= Owner; r++ {
		if p, ok := ParseRole(r.String()); !ok || p != r {
			t.Errorf("ParseRole(%s) = %s, %v", r, p, ok)
		}
	}
	if _, ok := ParseRole("none"); ok {
		t.Error(`"none" isn't a role`)
	}
}

func TestKeyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dir", "admin.key")
	for _, k := range []string{"oetl_first", "oetl_second"} {
		if err := WriteKeyFile(path, k); err != nil {
			t.Fatal(err)
		}
		got, err := ReadKeyFile(path)
		if err != nil || got != k {
			t.Fatalf("read %q, %v; want %q", got, err, k)
		}
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("mode %v", fi.Mode().Perm())
	}
	entries, _ := os.ReadDir(filepath.Dir(path))
	if len(entries) != 1 {
		t.Errorf("left %d files behind", len(entries))
	}
}

func TestWithBearer(t *testing.T) {
	var got string
	h := connect.NewUnaryHandler("/test.v1.T/M", func(_ context.Context, req *connect.Request[emptypb.Empty]) (*connect.Response[emptypb.Empty], error) {
		got = req.Header().Get("Authorization")
		return connect.NewResponse(&emptypb.Empty{}), nil
	})
	srv := httptest.NewServer(h)
	defer srv.Close()
	c := connect.NewClient[emptypb.Empty, emptypb.Empty](http.DefaultClient, srv.URL+"/test.v1.T/M", WithBearer("oetl_x"))
	if _, err := c.CallUnary(t.Context(), connect.NewRequest(&emptypb.Empty{})); err != nil {
		t.Fatal(err)
	}
	if got != "Bearer oetl_x" {
		t.Errorf("Authorization %q", got)
	}
}
