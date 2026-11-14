package dbosbackend

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLostCreateRace(t *testing.T) {
	for msg, want := range map[string]bool{
		"DBOS Error Initialization: Error initializing DBOS Transact: failed to run migrations: failed to execute migration 29: ERROR: deadlock detected (SQLSTATE 40P01)": true,
		`ERROR: duplicate key value violates unique constraint "pg_namespace_nspname_index" (SQLSTATE 23505)`:                                                              true,
		"failed to create database openetl_dbos": true,
		`DBOS Error Initialization: Error initializing DBOS Transact: failed to run migrations: failed to execute migration 12: ERROR: column "consumed" of relation "notifications" already exists (SQLSTATE 42701)`: true,
		`failed to execute migration 3: ERROR: relation "idx_workflow_status_created_at" already exists (SQLSTATE 42P07)`:                                                                                             true,
		`failed to execute migration 10: ERROR: constraint "notifications_pkey" for relation "notifications" already exists (SQLSTATE 42710)`:                                                                         true,
		"DBOS Error Initialization: Error initializing DBOS Transact: failed to run migrations: failed to execute migration 3: ERROR: permission denied (SQLSTATE 42501)":                                             false,
		"dial tcp 127.0.0.1:5432: connect: connection refused":     false,
		`ERROR: column "consumed" does not exist (SQLSTATE 42703)`: false,
	} {
		if got := lostCreateRace(errors.New(msg)); got != want {
			t.Errorf("lostCreateRace(%q) = %v, want %v", msg, got, want)
		}
	}
}

// Processes opening one system database take turns under a lock named after
// it, on the server's postgres database.
func TestOpeningLock(t *testing.T) {
	cfg, name, err := openingLock("postgres://u:p@db.example:6543/openetl_dbos?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Database != "postgres" || cfg.Host != "db.example" || cfg.Port != 6543 || cfg.User != "u" || cfg.Password != "p" {
		t.Errorf("the lock's connection: %s@%s:%d/%s", cfg.User, cfg.Host, cfg.Port, cfg.Database)
	}
	_, other, err := openingLock("postgres://u:p@db.example:6543/openetl_it_x")
	if err != nil {
		t.Fatal(err)
	}
	if name == other || name != "openetl: opening DBOS system database openetl_dbos" {
		t.Errorf("the locks' names: %q and %q", name, other)
	}
	if _, _, err := openingLock("postgres://%zz"); err == nil {
		t.Error("an unreadable URL gave a lock")
	}
	// A query value with a slash in it names no database.
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, selfSignedCA(t), 0o600); err != nil {
		t.Fatal(err)
	}
	system, err := SystemDatabaseURL("postgres://u:p@db.example:6543/openetl?sslmode=verify-full&application_name=openetl/it&sslrootcert=" + url.QueryEscape(ca))
	if err != nil {
		t.Fatal(err)
	}
	cfg, name, err = openingLock(system)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Database != "postgres" || cfg.Host != "db.example" || cfg.RuntimeParams["application_name"] != "openetl/it" || cfg.TLSConfig == nil || cfg.TLSConfig.RootCAs == nil ||
		name != "openetl: opening DBOS system database openetl_dbos" {
		t.Errorf("with a query: database %q on %s, params %v, TLS %v, lock %q", cfg.Database, cfg.Host, cfg.RuntimeParams, cfg.TLSConfig != nil, name)
	}
}

func selfSignedCA(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// A server that can't be reached gives an error, which Open passes over to
// let DBOS report on the server.
func TestLockOpeningUnreachable(t *testing.T) {
	if unlock, err := lockOpening(t.Context(), "postgres://u:p@127.0.0.1:1/openetl_dbos?connect_timeout=2"); err == nil {
		unlock()
		t.Error("took a lock on a server that isn't there")
	}
}
