package workflows_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// A workflow runs again from its start after a crash, on either backend, so
// its code must take the same path each time. workflowcheck can't see this
// package, which has no workflow.Context, so this looks for what it would have.
func TestWorkflowsAreDeterministic(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("no files: %v", err)
	}
	fset := token.NewFileSet()
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			bad := ""
			switch n := n.(type) {
			case *ast.GoStmt:
				bad = "a goroutine"
			case *ast.SelectStmt:
				bad = "a select"
			case *ast.SendStmt:
				bad = "a channel send"
			case *ast.SelectorExpr:
				if x, ok := n.X.(*ast.Ident); ok && x.Name == "time" && slices.Contains(clocks, n.Sel.Name) {
					bad = "time." + n.Sel.Name
				}
			}
			if bad != "" {
				t.Errorf("%s: %s; use orchestration.Context", fset.Position(n.Pos()), bad)
			}
			return true
		})
	}
}

var clocks = []string{"Now", "Since", "Until", "Sleep", "After", "AfterFunc", "Tick", "NewTimer", "NewTicker"}
