package dbosbackend

import (
	"errors"
	"testing"
)

func TestLostCreateRace(t *testing.T) {
	for msg, want := range map[string]bool{
		"DBOS Error Initialization: Error initializing DBOS Transact: failed to run migrations: failed to execute migration 29: ERROR: deadlock detected (SQLSTATE 40P01)": true,
		`ERROR: duplicate key value violates unique constraint "pg_namespace_nspname_index" (SQLSTATE 23505)`:                                                              true,
		"failed to create database openetl_dbos": true,
		"DBOS Error Initialization: Error initializing DBOS Transact: failed to run migrations: failed to execute migration 3: ERROR: permission denied (SQLSTATE 42501)": false,
		"dial tcp 127.0.0.1:5432: connect: connection refused": false,
	} {
		if got := lostCreateRace(errors.New(msg)); got != want {
			t.Errorf("lostCreateRace(%q) = %v, want %v", msg, got, want)
		}
	}
}
