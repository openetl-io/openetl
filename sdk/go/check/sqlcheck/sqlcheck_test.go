package sqlcheck

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/check/netcheck"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// null is an answer of one row whose value is NULL.
type null struct{}

// late is an answer whose error comes with the first row, as streaming
// drivers give it.
type late struct{ err error }

// connector answers each query with one row of its value, no rows for nil,
// the error, or a late error. Closing it counts as closing the database.
type connector struct {
	answers map[string]any
	pingErr error
	delay   time.Duration
	closed  *atomic.Int32
}

func (d connector) Connect(context.Context) (driver.Conn, error) { return conn(d), nil }
func (d connector) Driver() driver.Driver                        { return nil }
func (d connector) Close() error                                 { d.closed.Add(1); return nil }

type conn connector

func (c conn) Prepare(string) (driver.Stmt, error) { return nil, errors.ErrUnsupported }
func (c conn) Close() error                        { return nil }
func (c conn) Begin() (driver.Tx, error)           { return nil, errors.ErrUnsupported }
func (c conn) Ping(context.Context) error          { return c.pingErr }

func (c conn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	time.Sleep(c.delay)
	a, ok := c.answers[query]
	if !ok {
		return nil, errors.New("no answer to " + query)
	}
	if err, ok := a.(error); ok {
		return nil, err
	}
	return &rows{v: a}, nil
}

type rows struct {
	v    any
	done bool
}

func (r *rows) Columns() []string { return []string{"v"} }
func (r *rows) Close() error      { return nil }
func (r *rows) Next(dest []driver.Value) error {
	switch v := r.v.(type) {
	case late:
		return v.err
	case nil:
		return io.EOF
	case null:
		dest[0] = nil
	default:
		dest[0] = v
	}
	if r.done {
		return io.EOF
	}
	r.done = true
	return nil
}

// pgError is a driver's error with a SQLSTATE, as pgx's is.
type pgError string

func (s pgError) Error() string    { return "ERROR: denied (SQLSTATE " + string(s) + ")" }
func (s pgError) SQLState() string { return string(s) }

func kit(t *testing.T, answers map[string]any, pingErr error) (*Kit, *atomic.Int32) {
	var opened, closed atomic.Int32
	k := New(func(context.Context) (*sql.DB, error) {
		if opened.Add(1) > 1 {
			t.Error("the kit opened the database twice")
		}
		return sql.OpenDB(connector{answers: answers, pingErr: pingErr, closed: &closed}), nil
	})
	return k, &closed
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
	k, closed := kit(t, map[string]any{
		"usage":                          true,
		"create":                         false,
		"slot":                           nil,
		"SELECT * FROM orders WHERE 1=0": pgError("42501"),
		"SELECT * FROM users WHERE 1=0":  nil,
		"SELECT * FROM gone WHERE 1=0":   pgError("42P01"),
		"broken":                         errors.New("syntax error"),
		"SHOW wal_level":                 "replica",
		"lsn":                            "0/16B3748",
	}, nil)
	r := check.Run(t.Context(), []check.Check{
		k.Connect(),
		k.True("usage", "Use the schema", nil, nil, "usage"),
		k.True("create", "Create tables", check.PrivilegeMissing.Fail("principal", "etl", "privilege", "CREATE", "object", "schema public"), nil, "create"),
		k.True("slot", "Find the slot", nil, nil, "slot"),
		k.Readable("orders", "orders"),
		k.Readable("users", "users"),
		k.Readable("gone", "gone"),
		k.Query("broken", "Run a query", "broken"),
		k.Value("wal", "Use logical WAL", func(v string) error {
			if v != "logical" {
				return check.SettingMismatch.Fail("setting", "wal_level", "required", "logical")
			}
			return nil
		}, "SHOW wal_level"),
		k.Check("lsn", check.Resource, "Read the LSN", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
			var lsn string
			if err := db.QueryRowContext(ctx, "lsn").Scan(&lsn); err != nil {
				return k.Failure(err)
			}
			p.Param("lsn", lsn)
			p.Found("At {lsn}.")
			return nil
		}),
	})
	if got := statuses(r); got != "db.connect=passed usage=passed create=failed slot=failed orders=failed users=passed gone=failed broken=failed wal=failed lsn=passed" {
		t.Fatal(got)
	}
	res := rendered(t, r)
	if f := res["create"].Failure; f.Reason != "privilege_missing" || f.Message != "etl has no CREATE on schema public." {
		t.Errorf("create: %+v", f)
	}
	if f := res["slot"].Failure; f.Reason != "not_found" || f.Message != "The query found no row." {
		t.Errorf("slot: %+v", f)
	}
	if it := res["orders"]; it.Failure.Reason != "privilege_missing" || it.Title != "Read orders" || it.Params["sqlstate"] != "42501" {
		t.Errorf("orders: %+v", it)
	}
	if f := res["gone"].Failure; f.Reason != "not_found" || f.Message != "The table gone doesn't exist." {
		t.Errorf("gone: %+v", f)
	}
	if f := res["broken"].Failure; f.Reason != "unknown" || f.Code != protocolv1.ErrorCode_ERROR_CODE_INTERNAL || f.Detail != "syntax error" {
		t.Errorf("an error the kit can't classify: %+v", f)
	}
	if f := res["wal"].Failure; f.Message != "wal_level is replica, and the connector needs logical." {
		t.Errorf("wal: %+v", f)
	}
	if it := res["lsn"]; it.Message != "At 0/16B3748." || len(r.Items[len(r.Items)-1].Params) != 1 {
		t.Errorf("lsn: %+v", it)
	}
	if closed.Load() != 1 {
		t.Errorf("the database was closed %d times", closed.Load())
	}
}

func TestQueryReadsARow(t *testing.T) {
	k, _ := kit(t, map[string]any{
		"denied": late{pgError("42501")},
		"empty":  nil,
	}, nil)
	r := check.Run(t.Context(), []check.Check{k.Connect(), k.Query("denied", "Read", "denied"), k.Query("empty", "Read", "empty")})
	if statuses(r) != "db.connect=passed denied=failed empty=passed" || r.Items[1].Failure.Reason != "privilege_missing" {
		t.Errorf("%s %+v", statuses(r), r.Items[1].Failure)
	}
}

// trueCases are the values a True check sees, and how it ends: the same
// for adbccheck's.
var trueCases = []struct {
	value  any
	reason string
}{
	{true, ""}, {"true", ""}, {"t", ""}, {"TRUE", ""}, {int64(1), ""}, {"1", ""},
	{false, "permission_denied"}, {"f", "permission_denied"}, {int64(0), "permission_denied"}, {int64(2), "permission_denied"}, {"yes", "permission_denied"},
	{null{}, "not_found"}, {nil, "not_found"},
}

func TestTrue(t *testing.T) {
	for _, tc := range trueCases {
		k, _ := kit(t, map[string]any{"q": tc.value}, nil)
		it := check.Run(t.Context(), []check.Check{k.Connect(), k.True("t", "T", nil, nil, "q")}).Items[1]
		switch {
		case tc.reason == "" && it.Status != check.Passed,
			tc.reason != "" && (it.Status != check.Failed || it.Failure.Reason != tc.reason):
			t.Errorf("%#v: %s %+v", tc.value, it.Status, it.Failure)
		}
	}
}

func TestSQLState(t *testing.T) {
	for _, tc := range []struct {
		state, reason, msg string
	}{
		{"28P01", "credentials_rejected", "The upstream rejected the credentials."},
		{"28000", "credentials_rejected", "The upstream rejected the credentials."},
		{"3D000", "not_found", "The database doesn't exist."},
		{"08006", "unreachable", "Can't reach the database."},
		{"57P03", "unknown", "The check failed."},
	} {
		k, _ := kit(t, nil, pgError(tc.state))
		r := check.Run(t.Context(), []check.Check{k.Connect(), k.True("usage", "Use", nil, nil, "usage")})
		it := rendered(t, r)["db.connect"]
		if statuses(r) != "db.connect=failed usage=skipped" || it.Failure.Reason != tc.reason || it.Message != tc.msg {
			t.Errorf("%s: %s %+v", tc.state, statuses(r), it.Failure)
		}
	}
	k, _ := kit(t, nil, pgError("28P01"))
	k.Classify = func(err error) *check.Failure {
		if s, ok := errors.AsType[pgError](err); ok && s == "28P01" {
			return check.CredentialsExpired.Wrap(err)
		}
		return nil
	}
	if f := check.Run(t.Context(), []check.Check{k.Connect()}).Items[0].Failure; f.Reason != "credentials_expired" {
		t.Errorf("the hook didn't win: %+v", f)
	}
	if k.Failure(nil) != nil {
		t.Error("a nil error failed")
	}
}

func TestWithNetcheck(t *testing.T) {
	k, _ := kit(t, nil, nil)
	checks := append(netcheck.ReachTLS("db.example.com:5432", nil), k.Connect(), k.Readable("orders.read", "orders"))
	if err := check.Validate(checks); err != nil {
		t.Error(err)
	}
}

// Runs that share a Kit, one after another or at once, each pass, and the
// handle closes once the last has ended.
func TestKitSharedByRuns(t *testing.T) {
	var opened, closed atomic.Int32
	k := New(func(context.Context) (*sql.DB, error) {
		opened.Add(1)
		return sql.OpenDB(connector{answers: map[string]any{"ok": true}, delay: 50 * time.Millisecond, closed: &closed}), nil
	})
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
	if opened.Load() != closed.Load() || k.db != nil || k.users != 0 {
		t.Errorf("opened %d, closed %d, users %d", opened.Load(), closed.Load(), k.users)
	}
	if r := check.Run(t.Context(), []check.Check{k.Connect(), k.Query("a", "Q", "ok")}); !r.OK() || k.db != nil {
		t.Errorf("a later run: %v", r.Err())
	}
}
