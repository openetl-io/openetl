package adbccheck

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/check/netcheck"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// null is an answer of one row whose value is NULL.
type null struct{}

// fakeDB answers each query with one row of its value, no rows for nil, or
// the error.
type fakeDB struct {
	adbc.Database
	answers   map[string]any
	tables    map[string]bool
	openErr   error
	infoErr   error
	noSchemas bool
	delay     time.Duration
	// The query "hold" blocks until gate closes, whatever its context, as a
	// driver that doesn't stop on a cancelled context does; entered gets a
	// value when it starts.
	gate, entered chan struct{}
	// Of the connection and the database.
	closed atomic.Int32
	// Gets a value when the database closes, which the kit does after the
	// connection.
	dbClosed chan struct{}
	// GetInfo's readers released.
	released atomic.Int32
	// The connection's calls, statements and readers open now; a call that
	// starts while another is open overlaps.
	busy     atomic.Int32
	overlaps atomic.Int32
	// The connection closed while something on it was open.
	closedUnderCall atomic.Int32
}

func (d *fakeDB) enter() {
	if d.busy.Add(1) > 1 {
		d.overlaps.Add(1)
	}
}

func (d *fakeDB) leave() { d.busy.Add(-1) }

func (d *fakeDB) Open(context.Context) (adbc.Connection, error) {
	if d.openErr != nil {
		return nil, d.openErr
	}
	return &fakeConn{db: d}, nil
}
func (d *fakeDB) Close() error {
	d.closed.Add(1)
	if d.dbClosed != nil {
		d.dbClosed <- struct{}{}
	}
	return nil
}

type fakeConn struct {
	adbc.Connection
	db *fakeDB
}

func (c *fakeConn) Close() error {
	if c.db.busy.Load() > 0 {
		c.db.closedUnderCall.Add(1)
	}
	c.db.closed.Add(1)
	return nil
}

func (c *fakeConn) NewStatement() (adbc.Statement, error) {
	c.db.enter()
	return &fakeStmt{db: c.db}, nil
}

func (c *fakeConn) GetInfo(context.Context, []adbc.InfoCode) (array.RecordReader, error) {
	c.db.enter()
	if c.db.infoErr != nil {
		c.db.leave()
		return nil, c.db.infoErr
	}
	rr, err := reader("PostgreSQL")
	return &released{RecordReader: rr, db: c.db}, err
}

func (c *fakeConn) GetTableSchema(_ context.Context, _, schema *string, table string) (*arrow.Schema, error) {
	c.db.enter()
	defer c.db.leave()
	if c.db.noSchemas {
		return nil, &adbc.Error{Code: adbc.StatusNotImplemented, Msg: "GetTableSchema not implemented"}
	}
	if c.db.tables[*schema+"."+table] {
		return arrow.NewSchema(nil, nil), nil
	}
	return nil, adbc.Error{Code: adbc.StatusNotFound, Msg: "no table " + table}
}

type released struct {
	array.RecordReader
	db *fakeDB
}

func (r *released) Release() { r.db.released.Add(1); r.db.leave(); r.RecordReader.Release() }

type fakeStmt struct {
	adbc.Statement
	db    *fakeDB
	query string
}

func (s *fakeStmt) Close() error               { s.db.leave(); return nil }
func (s *fakeStmt) SetSqlQuery(q string) error { s.query = q; return nil }
func (s *fakeStmt) ExecuteQuery(context.Context) (array.RecordReader, int64, error) {
	if s.query == "hold" {
		s.db.entered <- struct{}{}
		<-s.db.gate
	}
	time.Sleep(s.db.delay)
	a := s.db.answers[s.query]
	if err, ok := a.(error); ok {
		return nil, 0, err
	}
	rr, err := reader(a)
	return rr, 0, err
}

// reader is a reader of one row of v, of no rows for nil, or of NULL for
// null.
func reader(v any) (array.RecordReader, error) {
	var col arrow.Array
	switch v := v.(type) {
	case bool:
		b := array.NewBooleanBuilder(memory.DefaultAllocator)
		defer b.Release()
		b.Append(v)
		col = b.NewArray()
	case string:
		b := array.NewStringBuilder(memory.DefaultAllocator)
		defer b.Release()
		b.Append(v)
		col = b.NewArray()
	case int64:
		b := array.NewInt64Builder(memory.DefaultAllocator)
		defer b.Release()
		b.Append(v)
		col = b.NewArray()
	case null:
		b := array.NewInt64Builder(memory.DefaultAllocator)
		defer b.Release()
		b.AppendNull()
		col = b.NewArray()
	default:
		sch := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.BinaryTypes.String}}, nil)
		return array.NewRecordReader(sch, nil)
	}
	defer col.Release()
	sch := arrow.NewSchema([]arrow.Field{{Name: "v", Type: col.DataType(), Nullable: true}}, nil)
	rec := array.NewRecordBatch(sch, []arrow.Array{col}, 1)
	defer rec.Release()
	return array.NewRecordReader(sch, []arrow.RecordBatch{rec})
}

func kit(t *testing.T, db *fakeDB) *Kit {
	t.Cleanup(func() {
		if n := db.overlaps.Load(); n > 0 {
			t.Errorf("%d calls overlapped on the connection", n)
		}
		if n := db.closedUnderCall.Load(); n > 0 {
			t.Errorf("the connection closed %d times under a call", n)
		}
	})
	var opened atomic.Int32
	return New(func(context.Context) (adbc.Database, error) {
		if opened.Add(1) > 1 {
			t.Error("the kit opened the database twice")
		}
		return db, nil
	})
}

func statuses(r check.Report) string {
	var s []string
	for _, it := range r.Items {
		s = append(s, it.Name+"="+it.Status.String())
	}
	return strings.Join(s, " ")
}

// rendered is the report's items rendered by name, each checked for
// placeholders left unfilled.
func rendered(t *testing.T, r check.Report) map[string]check.Item {
	t.Helper()
	items := map[string]check.Item{}
	for _, it := range r.Items {
		it = it.Rendered()
		text := it.Title + it.Message
		if it.Failure != nil {
			text += it.Failure.Message + it.Failure.Fix
		}
		if strings.Contains(text, "{") {
			t.Errorf("%s has a placeholder unfilled: %q", it.Name, text)
		}
		items[it.Name] = it
	}
	return items
}

func TestKit(t *testing.T) {
	public := "public"
	db := &fakeDB{
		answers: map[string]any{
			"usage":   true,
			"create":  false,
			"slot":    nil,
			"version": "15.4",
			"denied":  adbc.Error{Code: adbc.StatusUnauthorized, Msg: "permission denied", SqlState: [5]byte{'4', '2', '5', '0', '1'}},
			"gone":    adbc.Error{Code: adbc.StatusUnknown, Msg: "relation gone doesn't exist", SqlState: [5]byte{'4', '2', 'P', '0', '1'}},
		},
		tables: map[string]bool{"public.orders": true},
	}
	k := kit(t, db)
	r := check.Run(t.Context(), []check.Check{
		k.Connect(),
		k.True("usage", "Use the schema", nil, nil, "usage"),
		k.True("create", "Create tables", nil, nil, "create"),
		k.True("slot", "Find the slot", nil, nil, "slot"),
		k.Query("denied", "Read", "denied"),
		k.Query("gone", "Read", "gone"),
		k.Table("orders", nil, &public, "orders"),
		k.Table("users", nil, &public, "users"),
		k.Value("version", "Run PostgreSQL 16", func(v string) error {
			if !strings.HasPrefix(v, "16") {
				return check.UnsupportedVersion.Fail("required", "16")
			}
			return nil
		}, "version"),
		k.Check("info", check.Compatibility, "Read the driver's info", func(ctx context.Context, _ *check.Probe, conn adbc.Connection) error {
			rr, err := conn.GetInfo(ctx, []adbc.InfoCode{adbc.InfoDriverName})
			if err != nil {
				return k.Failure(err)
			}
			rr.Release()
			return nil
		}),
	})
	if got := statuses(r); got != "db.connect=passed usage=passed create=failed slot=failed denied=failed gone=failed orders=passed users=failed version=failed info=passed" {
		t.Fatal(got)
	}
	res := rendered(t, r)
	if f := res["denied"].Failure; f.Reason != "permission_denied" || f.Params["sqlstate"] != "42501" {
		t.Errorf("denied: %+v", f)
	}
	if f := res["gone"].Failure; f.Reason != "not_found" || f.Message != "The table doesn't exist." || f.Params["sqlstate"] != "42P01" {
		t.Errorf("an Unknown status with a SQLSTATE: %+v", f)
	}
	if f := res["users"].Failure; f.Reason != "not_found" || f.Message != "The table public.users doesn't exist." {
		t.Errorf("users: %+v", f)
	}
	if f := res["version"].Failure; f.Message != "The upstream's version is 15.4, and the connector needs 16." {
		t.Errorf("version: %+v", f)
	}
	if db.closed.Load() != 2 {
		t.Errorf("closed %d of the connection and the database", db.closed.Load())
	}
	if db.released.Load() != 2 {
		t.Errorf("released %d of GetInfo's 2 readers", db.released.Load())
	}
}

func TestConnect(t *testing.T) {
	db := &fakeDB{openErr: adbc.Error{Code: adbc.StatusUnauthenticated, Msg: "bad password"}}
	r := check.Run(t.Context(), []check.Check{kit(t, db).Connect(), kit(t, db).True("usage", "Use", nil, nil, "usage")})
	if statuses(r) != "db.connect=failed usage=skipped" || r.Items[0].Failure.Code != protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID {
		t.Errorf("%s %+v", statuses(r), r.Items[0].Failure)
	}
	// A driver that opens lazily fails only when asked something.
	db = &fakeDB{infoErr: adbc.Error{Code: adbc.StatusIO, Msg: "connection refused"}}
	it := rendered(t, check.Run(t.Context(), []check.Check{kit(t, db).Connect()}))["db.connect"]
	if it.Status != check.Failed || it.Failure.Reason != "unreachable" || it.Message != "Can't reach the database." {
		t.Errorf("a lazy driver: %s %+v", it.Status, it.Failure)
	}
}

func TestTableWithoutSchemas(t *testing.T) {
	k := kit(t, &fakeDB{noSchemas: true})
	public := "public"
	it := check.Run(t.Context(), []check.Check{k.Connect(), k.Table("orders", nil, &public, "orders")}).Items[1]
	if it.Status != check.Skipped || !strings.Contains(it.Message, "can't read a table's schema") {
		t.Errorf("%s %q", it.Status, it.Message)
	}
}

// The cases sqlcheck's TestTrue has.
func TestTrue(t *testing.T) {
	for _, tc := range []struct {
		value  any
		reason string
	}{
		{true, ""}, {"true", ""}, {"t", ""}, {"TRUE", ""}, {int64(1), ""}, {"1", ""},
		{false, "permission_denied"}, {"f", "permission_denied"}, {int64(0), "permission_denied"}, {int64(2), "permission_denied"}, {"yes", "permission_denied"},
		{null{}, "not_found"}, {nil, "not_found"},
	} {
		k := kit(t, &fakeDB{answers: map[string]any{"q": tc.value}})
		it := check.Run(t.Context(), []check.Check{k.Connect(), k.True("t", "T", nil, nil, "q")}).Items[1]
		switch {
		case tc.reason == "" && it.Status != check.Passed,
			tc.reason != "" && (it.Status != check.Failed || it.Failure.Reason != tc.reason):
			t.Errorf("%#v: %s %+v", tc.value, it.Status, it.Failure)
		}
	}
}

func TestFailure(t *testing.T) {
	k := New(nil)
	for status, reason := range map[adbc.Status]string{
		adbc.StatusUnauthenticated: "credentials_rejected",
		adbc.StatusUnauthorized:    "permission_denied",
		adbc.StatusNotFound:        "not_found",
		adbc.StatusTimeout:         "timed_out",
		adbc.StatusIO:              "unreachable",
		adbc.StatusInvalidArgument: "config_invalid",
		adbc.StatusAlreadyExists:   "unknown",
		adbc.StatusUnknown:         "unknown",
	} {
		f := k.Failure(adbc.Error{Code: status, Msg: "x", VendorCode: 7})
		if f.Reason != reason || f.Params["vendor_code"] != "7" || strings.Contains(check.Render(f.Message, f.Params), "{") {
			t.Errorf("%s: %+v", status, f)
		}
	}
	for state, reason := range map[string]string{
		"28P01": "credentials_rejected", "42501": "privilege_missing", "42S02": "not_found",
		"3D000": "not_found", "3F000": "not_found", "08001": "unreachable", "22012": "unknown",
	} {
		var s [5]byte
		copy(s[:], state)
		for _, status := range []adbc.Status{adbc.StatusUnknown, adbc.StatusInternal} {
			f := k.Failure(&adbc.Error{Code: status, Msg: "x", SqlState: s})
			if f.Reason != reason || f.Params["sqlstate"] != state || strings.Contains(check.Render(f.Message, f.Params), "{") {
				t.Errorf("%s %s: %+v", status, state, f)
			}
		}
	}
	if f := k.Failure(adbc.Error{Code: adbc.StatusUnauthorized, SqlState: [5]byte{'2', '8', 'P', '0', '1'}}); f.Reason != "permission_denied" {
		t.Errorf("a SQLSTATE over a status that says more: %+v", f)
	}
	if k.Failure(nil) != nil {
		t.Error("a nil error failed")
	}
	if f := k.Failure(errors.New("x")); f.Reason != "unknown" {
		t.Errorf("not ADBC's: %+v", f)
	}
	k.Classify = func(error) *check.Failure { return check.QuotaExceeded.Fail() }
	if f := k.Failure(adbc.Error{Code: adbc.StatusUnauthorized}); f.Reason != "quota_exceeded" {
		t.Errorf("the hook didn't win: %+v", f)
	}
}

func TestWithNetcheck(t *testing.T) {
	k := kit(t, &fakeDB{})
	checks := append(netcheck.ReachTLS("db.example.com:5432", nil), k.Connect(), k.Table("orders.table", nil, nil, "orders"))
	if err := check.Validate(checks); err != nil {
		t.Error(err)
	}
}

// Runs that share a Kit, one after another or at once, each pass, and the
// connection closes once the last has ended.
func TestKitSharedByRuns(t *testing.T) {
	db := &fakeDB{answers: map[string]any{"ok": true}, delay: 50 * time.Millisecond}
	var opened atomic.Int32
	k := New(func(context.Context) (adbc.Database, error) { opened.Add(1); return db, nil })
	var wg sync.WaitGroup
	reports := make([]check.Report, 3)
	for i := range reports {
		wg.Go(func() {
			time.Sleep(time.Duration(i) * 30 * time.Millisecond)
			checks := []check.Check{k.Connect()}
			for _, n := range []string{"a", "b", "c", "d"}[:1+i] {
				checks = append(checks, k.Query(n, "Q", "ok"))
			}
			reports[i] = check.Run(context.Background(), checks)
		})
	}
	wg.Wait()
	for i, r := range reports {
		if err := r.Err(); err != nil {
			t.Errorf("run %d: %v", i, err)
		}
	}
	if db.closed.Load() != 2*opened.Load() || k.conn != nil || k.users != 0 {
		t.Errorf("opened %d, closed %d, users %d", opened.Load(), db.closed.Load(), k.users)
	}
	if db.overlaps.Load() > 0 || db.closedUnderCall.Load() > 0 {
		t.Errorf("%d calls overlapped, and the connection closed %d times under a call", db.overlaps.Load(), db.closedUnderCall.Load())
	}
}

func holding() *fakeDB {
	return &fakeDB{
		answers:  map[string]any{"ok": true},
		gate:     make(chan struct{}),
		entered:  make(chan struct{}, 1),
		dbClosed: make(chan struct{}, 1),
	}
}

// gaveUp checks that it is the failure of a check that gave up waiting for
// the connection.
func gaveUp(t *testing.T, it check.Item) {
	t.Helper()
	if it.Status != check.Failed || it.Failure.Reason != "timed_out" || it.Message != "The connection to the database was still in use by a check that didn't finish." || it.Failure.Fix != "" {
		t.Errorf("%s: %s %+v", it.Name, it.Status, it.Failure)
	}
}

// awaitClose waits for the connection and the database to close.
func awaitClose(t *testing.T, db *fakeDB) {
	t.Helper()
	select {
	case <-db.dbClosed:
	case <-time.After(5 * time.Second):
		t.Fatal("the database didn't close")
	}
	if n := db.closed.Load(); n != 2 {
		t.Errorf("closed %d of the connection and the database", n)
	}
}

// A run whose check waits for the connection while another run's check has
// it gives up when its own deadline passes, without touching the
// connection.
func TestKitRunsAtOnce(t *testing.T) {
	db := holding()
	k := kit(t, db)
	held := make(chan check.Report)
	go func() {
		held <- check.Run(context.Background(), []check.Check{k.Connect(), k.Query("hold", "Hold", "hold")})
	}()
	<-db.entered
	waiter := k.Connect()
	waiter.Timeout = 50 * time.Millisecond
	r := check.Run(context.Background(), []check.Check{waiter, k.Query("q", "Q", "ok")})
	if statuses(r) != "db.connect=failed q=skipped" {
		t.Error(statuses(r))
	}
	gaveUp(t, rendered(t, r)["db.connect"])
	close(db.gate)
	if r := <-held; r.Err() != nil {
		t.Errorf("the run that had the connection: %v", r.Err())
	}
	awaitClose(t, db)
}

// A check left running in the driver keeps the connection: the next check
// of the same run waits for it, and fails once its own deadline passes.
func TestKitCheckLeftRunning(t *testing.T) {
	db := holding()
	k := kit(t, db)
	hold := k.Query("hold", "Hold", "hold")
	hold.Timeout = 50 * time.Millisecond
	next := k.Query("next", "Next", "ok")
	next.Timeout = 50 * time.Millisecond
	r := check.Run(context.Background(), []check.Check{k.Connect(), hold, next})
	if statuses(r) != "db.connect=passed hold=failed next=failed" {
		t.Error(statuses(r))
	}
	items := rendered(t, r)
	if f := items["hold"].Failure; f.Reason != "timed_out" || f.Message != "The check didn't finish in time." {
		t.Errorf("hold: %+v", f)
	}
	gaveUp(t, items["next"])
	close(db.gate)
	awaitClose(t, db)
}

// The run's cleanups don't close the connection under a check still in the
// driver: it closes when that check returns.
func TestKitClosesAfterCallReturns(t *testing.T) {
	db := holding()
	k := kit(t, db)
	hold := k.Query("hold", "Hold", "hold")
	hold.Timeout = 50 * time.Millisecond
	check.Run(context.Background(), []check.Check{k.Connect(), hold})
	if n := db.closed.Load(); n != 0 {
		t.Errorf("closed %d of the connection and the database while a call ran", n)
	}
	close(db.gate)
	awaitClose(t, db)
}
