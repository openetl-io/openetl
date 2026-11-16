//go:build integration

package daemon_test

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/go-containerregistry/pkg/registry"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
	"github.com/openetl-io/openetl/internal/auth"
	"github.com/openetl-io/openetl/internal/daemon"
	"github.com/openetl-io/openetl/internal/oci"
	"github.com/openetl-io/openetl/internal/platform/config"
	"github.com/openetl-io/openetl/internal/store/storetest"
)

// A server that connects the official source to a new org lists it, so the
// catalog has the official packages with no orchestrator running and no
// refresh asked for.
func TestServerListsTheOfficialSource(t *testing.T) {
	reg := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	t.Cleanup(reg.Close)
	official, err := oci.Parse(oci.Scheme + strings.TrimPrefix(reg.URL, "http://") + "/openetl-io/openetl-connectors")
	if err != nil {
		t.Fatal(err)
	}
	card := oci.Card{Name: "openetl/postgres", Title: "PostgreSQL", Version: "1.0.0", Roles: []string{"source"}, Repository: official.Child("postgres").String()}
	if _, err := (&oci.Client{HTTP: reg.Client()}).UpdateIndex(t.Context(), official, card, nil); err != nil {
		t.Fatal(err)
	}

	cfg := config.Config{
		DatabaseURL: storetest.NewDatabase(t), DBOSDatabaseURL: storetest.NewDatabase(t), DataPlane: "default",
		EgressAllow: "127.0.0.1/32", BootstrapKeyFile: filepath.Join(t.TempDir(), "admin.key"), OfficialConnectors: official.String(),
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	ready, stopped := make(chan struct{}), make(chan struct{})
	var runErr error
	go func() {
		defer close(stopped)
		runErr = daemon.Run(ctx, daemon.Options{Config: cfg, Roles: daemon.Roles{Server: true}, Listener: ln, Ready: func(string) { close(ready) }})
	}()
	t.Cleanup(func() {
		cancel()
		<-stopped
		if runErr != nil && !errors.Is(runErr, context.Canceled) {
			t.Errorf("server: %v", runErr)
		}
	})
	select {
	case <-ready:
	case <-stopped:
		t.Fatalf("server: %v", runErr)
	case <-time.After(30 * time.Second):
		t.Fatal("the server didn't start")
	}
	key, err := auth.ReadKeyFile(cfg.BootstrapKeyFile)
	if err != nil {
		t.Fatal(err)
	}
	conns := apiv1connect.NewConnectorServiceClient(http.DefaultClient, "http://"+ln.Addr().String(), auth.WithBearer(key))

	deadline := time.Now().Add(30 * time.Second)
	for {
		list, err := conns.ListCatalog(ctx, connect.NewRequest(&apiv1.ListCatalogRequest{}))
		if err != nil {
			t.Fatal(err)
		}
		p := list.Msg.GetPackages()
		if len(p) == 1 && p[0].GetPackage() == "openetl/postgres" && p[0].GetOfficial() && p[0].GetVersion() == "1.0.0" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the catalog has %v, not the official package", p)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
