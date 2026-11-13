package sqlplan

import "testing"

func TestMentions(t *testing.T) {
	sql := `INSERT INTO contacts SELECT id FROM Public.Customers JOIN "HR.EMPLOYEES" ON true`
	for stream, want := range map[string]bool{
		"public.customers": true,
		"customers":        true,
		"hr.employees":     true,
		"HR.EMPLOYEES":     true,
		"orders":           false,
		"public.orders":    false,
		`odd"name`:         true,
		"trailing.":        true,
	} {
		if got := Mentions(sql, stream); got != want {
			t.Errorf("%s: %v", stream, got)
		}
	}
}

func TestErrorSaysWhere(t *testing.T) {
	at := Error{Kind: "bind", At: &Position{Line: 3, Column: 10}, Message: "there's no column nope"}
	if got := at.Error(); got != "transform.sql:3:10: there's no column nope" {
		t.Error(got)
	}
	if got := (Error{Kind: "unsupported", Message: "a transform writes at least one output"}).Error(); got != "transform.sql: a transform writes at least one output" {
		t.Error(got)
	}
}
