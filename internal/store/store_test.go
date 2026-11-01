package store

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var (
	queryName = regexp.MustCompile(`(?m)^-- name: (\w+)`)
	orgsTable = regexp.MustCompile(`(?i)\b(FROM|INTO|UPDATE)\s+orgs\b`)
)

// Looking up a credential comes before its org is known.
var unscoped = map[string]bool{"AuthenticateAPIKey": true, "AuthenticateRunnerToken": true}

func TestQueriesAreOrgScoped(t *testing.T) {
	files, err := filepath.Glob("queries/*.sql")
	if err != nil || len(files) == 0 {
		t.Fatalf("no query files: %v", err)
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		names := queryName.FindAllStringSubmatch(string(b), -1)
		for i, q := range queryName.Split(string(b), -1)[1:] {
			_, q, _ = strings.Cut(q, "\n") // the :one, :many or :exec annotation
			q = strings.TrimSpace(q)
			scoped := strings.Contains(q, "org_id = @org_id")
			if strings.HasPrefix(q, "INSERT") {
				scoped = strings.Contains(q, "@org_id")
			}
			if !scoped && !orgsTable.MatchString(q) && !unscoped[names[i][1]] {
				t.Errorf("%s: %s isn't scoped by org_id", f, names[i][1])
			}
		}
	}
}
