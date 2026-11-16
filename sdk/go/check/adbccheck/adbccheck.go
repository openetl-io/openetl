// Package adbccheck checks a database through ADBC. A Kit's checks share
// one database and connection, which they take turns with, opened by the
// first check that runs and closed once the runs that used them have ended.
//
//	k := adbccheck.New(func(ctx context.Context) (adbc.Database, error) { return drv.NewDatabase(opts) })
//	checks := []check.Check{
//		k.Connect(), // db.connect
//		k.Table("orders.table", nil, &schema, "orders"),
//	}
//
// Every check but Connect needs "db.connect", so Connect goes first in the
// list.
//
// It's a module of its own, so that the SDK doesn't require ADBC.
package adbccheck

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"

	"github.com/apache/arrow-adbc/go/adbc"

	"github.com/openetl-io/openetl/sdk/go/check"
)

// classify makes an ADBC error a failure by its status, or, for the
// statuses that say nothing more, by its SQLSTATE; nil for an error that
// isn't ADBC's.
func classify(err error) *check.Failure {
	e, ok := adbcError(err)
	if !ok {
		return nil
	}
	state := strings.TrimRight(string(e.SqlState[:]), "\x00")
	var f *check.Failure
	switch e.Code {
	case adbc.StatusOK, adbc.StatusUnknown, adbc.StatusInternal:
		f = bySQLState(err, state)
	}
	if f == nil {
		f = byStatus(err, e.Code)
	}
	if state != "" {
		f = f.With("sqlstate", state)
	}
	if e.VendorCode != 0 {
		f = f.With("vendor_code", strconv.Itoa(int(e.VendorCode)))
	}
	return f
}

// adbcError is the first adbc.Error in err's chain, which drivers give as a
// value or a pointer.
func adbcError(err error) (adbc.Error, bool) {
	if e, ok := errors.AsType[adbc.Error](err); ok {
		return e, true
	}
	if p, ok := errors.AsType[*adbc.Error](err); ok && p != nil {
		return *p, true
	}
	return adbc.Error{}, false
}

func byStatus(err error, s adbc.Status) *check.Failure {
	var f *check.Failure
	switch s {
	case adbc.StatusUnauthenticated:
		f = check.CredentialsRejected.Wrap(err)
	case adbc.StatusUnauthorized:
		f = check.PermissionDenied.Wrap(err)
	case adbc.StatusNotFound:
		f = check.NotFound.Wrap(err, "resource", "What the check looks for")
	case adbc.StatusTimeout:
		f = check.TimedOut.Wrap(err)
		f.Message = "The database didn't answer in time."
	case adbc.StatusIO:
		f = check.Unreachable.Wrap(err)
		f.Message = "Can't reach the database."
	case adbc.StatusInvalidArgument, adbc.StatusInvalidState:
		f = check.ConfigInvalid.Wrap(err)
	default:
		f = check.Unknown.Wrap(err)
	}
	return f
}

// bySQLState is nil for a state without a reason of its own. It's
// sqlcheck's.
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
	}
	return f
}

// Kit makes checks that share one ADBC connection. Its checks may run in
// any number of runs, one after another or at once, but they use the
// connection one at a time, opening it included: a check that needs it
// while another check has it waits until its own context ends, and then
// fails as timed_out without using it. The connection opens when the first
// check needs it, and closes, with its database, once every run that used it
// has ended and no check is using it. A check left running inside the driver
// keeps it: if that call never returns, the connection is never closed, and
// the kit's later checks fail waiting for it. Set Classify before any check
// runs.
type Kit struct {
	open func(ctx context.Context) (adbc.Database, error)
	// Classifies the driver's errors before the kit does. Nil, or nil for an
	// error: the kit's own classification.
	Classify func(error) *check.Failure

	// Holds a value while a check opens, uses or closes the connection. Only
	// its holder reads or changes db and conn.
	turn chan struct{}
	db   adbc.Database
	conn adbc.Connection

	mu sync.Mutex
	// The checks that have the connection and whose runs haven't ended.
	users int
}

// New makes a Kit whose checks open their database with open, which should
// stop when ctx ends.
func New(open func(ctx context.Context) (adbc.Database, error)) *Kit {
	return &Kit{open: open, turn: make(chan struct{}, 1)}
}

// Failure is err classified: by Classify, then, for an adbc.Error, by its
// status, or by its SQLSTATE as sqlcheck's Kit.Failure has it when the
// status is Unknown or Internal, and then by check.Classify. It sets the
// params sqlstate and vendor_code where the error has them. By status:
// Unauthenticated is check.CredentialsRejected, Unauthorized
// check.PermissionDenied, NotFound check.NotFound, Timeout check.TimedOut,
// IO check.Unreachable, InvalidArgument and InvalidState
// check.ConfigInvalid, and the others check.Unknown. Nil for a nil err.
func (k *Kit) Failure(err error) *check.Failure {
	if err == nil {
		return nil
	}
	if k.Classify != nil {
		if f := k.Classify(err); f != nil {
			return f
		}
	}
	if f := classify(err); f != nil {
		return f
	}
	return check.Classify(err)
}

func (k *Kit) fail(err error) error {
	if f := k.Failure(err); f != nil {
		return f
	}
	return nil
}

// inUse is the failure of a check whose context ended while it waited for
// the connection.
func inUse(err error) *check.Failure {
	f := check.TimedOut.Wrap(err)
	f.Message, f.Fix = "The connection to the database was still in use by a check that didn't finish.", ""
	return f
}

// acquire takes the kit's turn with the connection, opening the connection
// if no check has it, and holds the connection until p's run ends. The
// caller gives the turn back with put once it's done with the connection.
func (k *Kit) acquire(ctx context.Context, p *check.Probe) (adbc.Connection, error) {
	select {
	case k.turn <- struct{}{}:
	default:
		select {
		case k.turn <- struct{}{}:
		case <-ctx.Done():
			return nil, inUse(ctx.Err())
		}
	}
	if k.conn == nil {
		db, err := k.open(ctx)
		if err != nil {
			k.put()
			return nil, k.fail(err)
		}
		conn, err := db.Open(ctx)
		if err != nil {
			_ = db.Close()
			k.put()
			return nil, k.fail(err)
		}
		k.db, k.conn = db, conn
	}
	conn := k.conn
	k.mu.Lock()
	k.users++
	k.mu.Unlock()
	p.Cleanup(func(context.Context) { k.release() })
	return conn, nil
}

// put gives back the turn, first closing the connection if the runs that
// used it have all ended, as they may have while a check was left running
// with it.
func (k *Kit) put() {
	k.mu.Lock()
	unused := k.users == 0 && k.conn != nil
	if !unused {
		<-k.turn
	}
	k.mu.Unlock()
	if unused {
		_ = k.conn.Close()
		_ = k.db.Close()
		k.db, k.conn = nil, nil
		<-k.turn
	}
}

// release ends a check's hold on the connection, and after the last one
// closes it, unless a check is using it: that check's put closes it.
func (k *Kit) release() {
	k.mu.Lock()
	if k.users--; k.users > 0 {
		k.mu.Unlock()
		return
	}
	select {
	case k.turn <- struct{}{}:
		k.mu.Unlock()
		k.put()
	default:
		k.mu.Unlock()
	}
}

// Check is a check of kind that runs run with the kit's connection, for
// what the kit's other checks don't cover. It needs "db.connect". Return
// k.Failure(err) for a driver's error, so the kit classifies it. No other
// check uses the connection until run returns, so close what run opens on
// it before returning, not in a cleanup.
func (k *Kit) Check(name string, kind check.Kind, title string, run func(ctx context.Context, p *check.Probe, conn adbc.Connection) error) check.Check {
	return check.Check{Name: name, Kind: kind, Title: title, Needs: []string{"db.connect"}, Run: func(ctx context.Context, p *check.Probe) error {
		conn, err := k.acquire(ctx, p)
		if err != nil {
			return err
		}
		defer k.put()
		return run(ctx, p, conn)
	}}
}

// Connect is "db.connect", a Connection check that the database opens a
// connection and answers a request for its vendor's name, which a driver
// that connects lazily must make a round trip for. It fails as Failure has
// it.
func (k *Kit) Connect() check.Check {
	c := k.Check("db.connect", check.Connection, "Connect to the database", func(ctx context.Context, _ *check.Probe, conn adbc.Connection) error {
		rr, err := conn.GetInfo(ctx, []adbc.InfoCode{adbc.InfoVendorName})
		if err != nil {
			return k.fail(err)
		}
		defer rr.Release()
		rr.Next()
		return k.fail(rr.Err())
	})
	c.Needs = nil
	return c
}

// first runs query and gives its first value as text, and whether it has a
// first row; null is true for NULL.
func first(ctx context.Context, conn adbc.Connection, query string) (v string, ok, null bool, err error) {
	st, err := conn.NewStatement()
	if err != nil {
		return "", false, false, err
	}
	defer func() { _ = st.Close() }()
	if err := st.SetSqlQuery(query); err != nil {
		return "", false, false, err
	}
	rr, _, err := st.ExecuteQuery(ctx)
	if err != nil {
		return "", false, false, err
	}
	defer rr.Release()
	for rr.Next() {
		rec := rr.RecordBatch()
		if rec.NumRows() > 0 && rec.NumCols() > 0 {
			col := rec.Column(0)
			if col.IsNull(0) {
				return "", true, true, nil
			}
			return col.ValueStr(0), true, false, nil
		}
	}
	return "", false, false, rr.Err()
}

// Query is a Permission check that query runs and its first row, if it has
// one, reads. It fails as Failure has it.
func (k *Kit) Query(name, title, query string) check.Check {
	return k.Check(name, check.Permission, title, func(ctx context.Context, _ *check.Probe, conn adbc.Connection) error {
		_, _, _, err := first(ctx, conn, query)
		return k.fail(err)
	})
}

// noRow is the failure of a query that found nothing.
func noRow() *check.Failure {
	f := check.NotFound.Fail()
	f.Message = "The query found no row."
	return f
}

// truthy reports whether v, a value as text, is true: true in any case, t,
// or 1, as a boolean, its text or an integer reads. It's sqlcheck's.
func truthy(v string) bool {
	b, err := strconv.ParseBool(strings.TrimSpace(v))
	return err == nil && b
}

// True is a Permission check that query's first value is true: a boolean, a
// boolean's text such as "t", or the integer 1, as sqlcheck's Kit.True has
// it. Another value fails with denied, and no row or NULL with missing; nil
// denied is check.PermissionDenied, and nil missing check.NotFound with the
// message "The query found no row.". An error fails as Failure has it.
func (k *Kit) True(name, title string, denied, missing *check.Failure, query string) check.Check {
	if denied == nil {
		denied = check.PermissionDenied.Fail()
	}
	if missing == nil {
		missing = noRow()
	}
	return k.Check(name, check.Permission, title, func(ctx context.Context, _ *check.Probe, conn adbc.Connection) error {
		v, ok, null, err := first(ctx, conn, query)
		switch {
		case err != nil:
			return k.fail(err)
		case !ok, null:
			return missing
		case !truthy(v):
			return denied
		}
		return nil
	})
}

// Value is a Compatibility check that passes query's first value, as text,
// to verify, whose error is the check's: for versions and settings, with
// check.UnsupportedVersion or check.SettingMismatch, whose {found} it sets
// to the value. No row fails as check.NotFound, NULL is the empty string,
// and an error fails as Failure has it.
func (k *Kit) Value(name, title string, verify func(value string) error, query string) check.Check {
	return k.Check(name, check.Compatibility, title, func(ctx context.Context, p *check.Probe, conn adbc.Connection) error {
		v, ok, _, err := first(ctx, conn, query)
		switch {
		case err != nil:
			return k.fail(err)
		case !ok:
			return noRow()
		}
		p.Param("found", v)
		return verify(v)
	})
}

// Table is a Resource check that the table exists and its schema can be
// read; nil catalog or schema is the connection's default. It sets the
// params table, schema.table or table, and resource, "The table" and that.
// A table that doesn't exist fails as check.NotFound naming it, another
// error as Failure has it, and a driver that can't read a table's schema
// skips it.
func (k *Kit) Table(name string, catalog, schema *string, table string) check.Check {
	qualified := table
	if schema != nil {
		qualified = *schema + "." + table
	}
	c := k.Check(name, check.Resource, "Find the table {table}", func(ctx context.Context, _ *check.Probe, conn adbc.Connection) error {
		_, err := conn.GetTableSchema(ctx, catalog, schema, table)
		if e, ok := adbcError(err); ok && e.Code == adbc.StatusNotImplemented {
			return check.Skip("the driver can't read a table's schema")
		}
		return k.fail(err)
	})
	c.Params = check.Params{"table": qualified, "resource": "The table " + qualified}
	return c
}
