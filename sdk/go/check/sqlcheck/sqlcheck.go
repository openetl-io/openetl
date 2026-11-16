// Package sqlcheck checks a database through database/sql. A Kit's checks
// share one *sql.DB, opened by the first that runs and closed once the runs
// that used it have ended.
//
//	k := sqlcheck.New(func(ctx context.Context) (*sql.DB, error) { return sql.Open("pgx", dsn) })
//	usage := k.True("schema.usage", "Use schema {schema}",
//		check.PrivilegeMissing.Fail("principal", user, "privilege", "USAGE", "object", "schema "+schema),
//		check.NotFound.Fail("resource", "The schema "+schema),
//		`SELECT has_schema_privilege(oid, 'USAGE') FROM pg_namespace WHERE nspname = $1`, schema)
//	usage.Params = check.Params{"schema": schema} // fills the title's {schema}
//	checks := []check.Check{
//		k.Connect(), // db.connect
//		usage,
//	}
//
// Every check but Connect needs "db.connect", so Connect goes first in the
// list.
package sqlcheck

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"sync"

	"github.com/openetl-io/openetl/sdk/go/check"
)

// Kit makes checks that share one database handle. Its checks may run in
// any number of runs, one after another or at once: the handle opens when
// the first check needs it, and closes once every run that used it has
// ended. Set Classify before any check runs.
type Kit struct {
	open func(ctx context.Context) (*sql.DB, error)
	// Classifies the driver's errors before the kit does, such as by a
	// vendor's codes. Nil, or nil for an error: the kit's own classification.
	Classify func(error) *check.Failure

	mu sync.Mutex
	db *sql.DB
	// The checks that have the handle and whose runs haven't ended.
	users int
}

// New makes a Kit whose checks open their handle with open, which should
// stop when ctx ends. A *sql.DB opens connections lazily, so the handle
// opening proves nothing: Connect pings.
func New(open func(ctx context.Context) (*sql.DB, error)) *Kit {
	return &Kit{open: open}
}

// Failure is err classified: by Classify, then by the SQLSTATE of an error
// in err's chain with a SQLState() string method, as pgx's and lib/pq's
// errors have, and then by check.Classify. Nil for a nil err.
//
// By SQLSTATE: class 28 is check.CredentialsRejected; 42501
// check.PrivilegeMissing; 42P01 and 42S02 check.NotFound with resource "The
// table", 3D000 with "The database", and 3F000 with "The schema"; and class
// 08 check.Unreachable. Each sets the param sqlstate.
func (k *Kit) Failure(err error) *check.Failure {
	if err == nil {
		return nil
	}
	if k.Classify != nil {
		if f := k.Classify(err); f != nil {
			return f
		}
	}
	var state string
	if s, ok := errors.AsType[sqlStater](err); ok {
		state = s.SQLState()
	}
	if f := bySQLState(err, state); f != nil {
		return f
	}
	return check.Classify(err)
}

type sqlStater interface {
	error
	SQLState() string
}

// bySQLState is nil for a state without a reason of its own.
func bySQLState(err error, state string) *check.Failure {
	var f *check.Failure
	switch {
	case strings.HasPrefix(state, "28"):
		f = check.CredentialsRejected.Wrap(err)
	case state == "42501":
		f = check.PrivilegeMissing.Wrap(err)
		f.Message = "The database user lacks a privilege this needs."
		f.Fix = "Grant the connector's database user the privilege the detail names."
	case state == "42P01", state == "42S02":
		f = check.NotFound.Wrap(err, "resource", "The table")
	case state == "3D000":
		f = check.NotFound.Wrap(err, "resource", "The database")
	case state == "3F000":
		f = check.NotFound.Wrap(err, "resource", "The schema")
	case strings.HasPrefix(state, "08"):
		f = check.Unreachable.Wrap(err)
		f.Message = "Can't reach the database."
	default:
		return nil
	}
	return f.With("sqlstate", state)
}

func (k *Kit) fail(err error) error {
	if f := k.Failure(err); f != nil {
		return f
	}
	return nil
}

// acquire gives the shared handle, opening it if no check has it, and
// holds it until p's run ends.
func (k *Kit) acquire(ctx context.Context, p *check.Probe) (*sql.DB, error) {
	db, err := k.hold(ctx)
	if err != nil {
		return nil, err
	}
	p.Cleanup(func(context.Context) { k.release() })
	return db, nil
}

// hold gives the shared handle, opening it if no check has it, and counts
// one more user. The mutex is free again even if open panics.
func (k *Kit) hold(ctx context.Context) (*sql.DB, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.db == nil {
		db, err := k.open(ctx)
		if err != nil {
			return nil, err
		}
		k.db = db
	}
	k.users++
	return k.db, nil
}

func (k *Kit) release() {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.users--; k.users == 0 && k.db != nil {
		_ = k.db.Close()
		k.db = nil
	}
}

// Check is a check of kind that runs run with the kit's handle, for what the
// kit's other checks don't cover. It needs "db.connect". Return
// k.Failure(err) for a driver's error, so the kit classifies it.
func (k *Kit) Check(name string, kind check.Kind, title string, run func(ctx context.Context, p *check.Probe, db *sql.DB) error) check.Check {
	return check.Check{Name: name, Kind: kind, Title: title, Needs: []string{"db.connect"}, Run: func(ctx context.Context, p *check.Probe) error {
		db, err := k.acquire(ctx, p)
		if err != nil {
			return k.fail(err)
		}
		return run(ctx, p, db)
	}}
}

// Connect is "db.connect", a Connection check that the database answers a
// ping. It fails as Failure has it.
func (k *Kit) Connect() check.Check {
	c := k.Check("db.connect", check.Connection, "Connect to the database", func(ctx context.Context, _ *check.Probe, db *sql.DB) error {
		return k.fail(db.PingContext(ctx))
	})
	c.Needs = nil
	return c
}

// Query is a Permission check that query runs and its first row, if it has
// one, reads: some drivers report an error, such as a missing privilege,
// only then. It fails as Failure has it.
func (k *Kit) Query(name, title, query string, args ...any) check.Check {
	return k.Check(name, check.Permission, title, func(ctx context.Context, _ *check.Probe, db *sql.DB) error {
		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			return k.fail(err)
		}
		defer func() { _ = rows.Close() }()
		rows.Next()
		if err := rows.Err(); err != nil {
			return k.fail(err)
		}
		return k.fail(rows.Close())
	})
}

// noRow is the failure of a query that found nothing.
func noRow() *check.Failure {
	f := check.NotFound.Fail()
	f.Message = "The query found no row."
	return f
}

// truthy reports whether v, a value as text, is true: true in any case, t,
// or 1, as a boolean, its text or an integer reads.
func truthy(v string) bool {
	b, err := strconv.ParseBool(strings.TrimSpace(v))
	return err == nil && b
}

// True is a Permission check that query's first value is true: a boolean, a
// boolean's text such as "t", or the integer 1. Another value fails with
// denied, and no row or NULL with missing; nil denied is
// check.PermissionDenied, and nil missing check.NotFound with the message
// "The query found no row.". An error fails as Failure has it.
func (k *Kit) True(name, title string, denied, missing *check.Failure, query string, args ...any) check.Check {
	if denied == nil {
		denied = check.PermissionDenied.Fail()
	}
	if missing == nil {
		missing = noRow()
	}
	return k.Check(name, check.Permission, title, func(ctx context.Context, _ *check.Probe, db *sql.DB) error {
		var v sql.NullString
		switch err := db.QueryRowContext(ctx, query, args...).Scan(&v); {
		case errors.Is(err, sql.ErrNoRows):
			return missing
		case err != nil:
			return k.fail(err)
		case !v.Valid:
			return missing
		case !truthy(v.String):
			return denied
		}
		return nil
	})
}

// Readable is a Permission check that relation can be selected from. It
// sets the param relation to relation, and resource to "The table " and
// relation, as adbccheck's Kit.Table does. Relation is used in the query as
// written: quote it for the dialect, and never take it from untrusted input.
// It fails as Failure has it, so a relation that doesn't exist is
// check.NotFound naming it: "The table relation doesn't exist.".
func (k *Kit) Readable(name, relation string) check.Check {
	c := k.Query(name, "Read {relation}", "SELECT * FROM "+relation+" WHERE 1=0")
	c.Params = check.Params{"relation": relation, "resource": "The table " + relation}
	return c
}

// Value is a Compatibility check that passes query's first value, as text,
// to verify, whose error is the check's: for versions and settings, with
// check.UnsupportedVersion or check.SettingMismatch, whose {found} it sets
// to the value. No row fails as check.NotFound, NULL is the empty string,
// and an error fails as Failure has it.
func (k *Kit) Value(name, title string, verify func(value string) error, query string, args ...any) check.Check {
	return k.Check(name, check.Compatibility, title, func(ctx context.Context, p *check.Probe, db *sql.DB) error {
		var v sql.NullString
		switch err := db.QueryRowContext(ctx, query, args...).Scan(&v); {
		case errors.Is(err, sql.ErrNoRows):
			return noRow()
		case err != nil:
			return k.fail(err)
		}
		p.Param("found", v.String)
		return verify(v.String)
	})
}
