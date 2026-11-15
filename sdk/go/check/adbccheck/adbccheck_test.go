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
	// Of the connection and the database.
	closed atomic.Int32
	// GetInfo's readers released.
	released atomic.Int32
}

func (d *fakeDB) Open(context.Context) (adbc.Connection, error) {
	if d.openErr != nil {
		return nil, d.openErr
	}
	return &fakeConn{db: d}, nil
}
func (d *fakeDB) Close() error { d.closed.Add(1); return nil }

type fakeConn struct {
	adbc.Connection
	db *fakeDB
}

func (c *fakeConn) Close() error { c.db.closed.Add(1); return nil }
func (c *fakeConn) NewStatement() (adbc.Statement, error) {
	return &fakeStmt{db: c.db}, nil
}

func (c *fakeConn) GetInfo(context.Context, []adbc.InfoCode) (array.RecordReader, error) {
	if c.db.infoErr != nil {
		return nil, c.db.infoErr
	}
	rr, err := reader("PostgreSQL")
	return &released{RecordReader: rr, n: &c.db.released}, err
}

func (c *fakeConn) GetTableSchema(_ context.Context, _, schema *string, table string) (*arrow.Schema, error) {
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
	n *atomic.Int32
}

func (r *released) Release() { r.n.Add(1); r.RecordReader.Release() }

type fakeStmt struct {
	adbc.Statement
	db    *fakeDB
	query string
}

func (s *fakeStmt) Close() error               { return nil }
func (s *fakeStmt) SetSqlQuery(q string) error { s.query = q; return nil }
func (s *fakeStmt) ExecuteQuery(context.Context) (array.RecordReader, int64, error) {
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
}
