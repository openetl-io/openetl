package declarative

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/ast"
	"cel.dev/cel-go/common/operators"
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
	"cel.dev/cel-go/common/types/traits"
	"cel.dev/cel-go/ext"
)

// Variables, and where expressions see them.
const (
	varConfig   = "config"
	varState    = "state"
	varPage     = "page"
	varParent   = "parent"
	varNow      = "now"
	varResponse = "response"
	varRecords  = "records"
	varRequest  = "request"
)

var varTypes = map[string]*cel.Type{
	varConfig:   cel.MapType(cel.StringType, cel.DynType),
	varState:    cel.MapType(cel.StringType, cel.DynType),
	varPage:     cel.MapType(cel.StringType, cel.DynType),
	varParent:   cel.MapType(cel.StringType, cel.DynType),
	varNow:      cel.TimestampType,
	varResponse: cel.MapType(cel.StringType, cel.DynType),
	varRecords:  cel.ListType(cel.DynType),
	varRequest:  cel.MapType(cel.StringType, cel.DynType),
}

// The fields of the variables whose fields are fixed.
var varFields = map[string][]string{
	varState:    {"cursor"},
	varPage:     {"token"},
	varResponse: {"status", "headers", "body"},
	varRequest:  {"method", "url", "path", "query", "headers", "body"},
}

// An expression's evaluation stops past this cost, about one operation each.
const costLimit = 10_000_000

type expr struct {
	where string
	prg   cel.Program
}

// compiler compiles a manifest's expressions. Each sees only the variables
// its place in the manifest has.
type compiler struct {
	// Config properties: true for secrets.
	props map[string]bool
	envs  map[string]*cel.Env
}

func (c *compiler) env(vars []string) (*cel.Env, error) {
	key := strings.Join(vars, ",")
	if e, ok := c.envs[key]; ok {
		return e, nil
	}
	opts := []cel.EnvOption{
		ext.Strings(), ext.Encoders(), ext.Lists(), ext.Math(), ext.Sets(),
		cel.CrossTypeNumericComparisons(true),
	}
	for _, v := range vars {
		opts = append(opts, cel.Variable(v, varTypes[v]))
	}
	e, err := cel.NewEnv(opts...)
	if err != nil {
		return nil, err
	}
	c.envs[key] = e
	return e, nil
}

// expr compiles src. want lists the output types it may have, besides dyn;
// none means any.
func (c *compiler) expr(where, src string, vars []string, want ...*cel.Type) (*expr, error) {
	env, err := c.env(vars)
	if err != nil {
		return nil, err
	}
	a, iss := env.Compile(src)
	if iss.Err() != nil {
		return nil, fmt.Errorf("%s: %w", where, iss.Err())
	}
	if err := c.checkRefs(a.NativeRep().Expr()); err != nil {
		return nil, fmt.Errorf("%s: %w", where, err)
	}
	if out := a.OutputType(); len(want) > 0 && !out.IsExactType(cel.DynType) && !slices.ContainsFunc(want, func(w *cel.Type) bool { return w.IsAssignableType(out) }) {
		names := make([]string, len(want))
		for i, w := range want {
			names[i] = w.String()
		}
		return nil, fmt.Errorf("%s: the expression is a %s, not a %s", where, out, strings.Join(names, " or "))
	}
	prg, err := env.Program(a, cel.CostLimit(costLimit))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", where, err)
	}
	return &expr{where: where, prg: prg}, nil
}

// checkRefs refuses references to secrets, to config properties that don't
// exist, and to fields the fixed variables don't have.
func (c *compiler) checkRefs(e ast.Expr) error {
	var err error
	ast.PreOrderVisit(e, ast.NewExprVisitor(func(e ast.Expr) {
		if err != nil {
			return
		}
		var v, field string
		switch e.Kind() {
		case ast.SelectKind:
			s := e.AsSelect()
			if s.Operand().Kind() != ast.IdentKind {
				return
			}
			v, field = s.Operand().AsIdent(), s.FieldName()
		case ast.CallKind:
			call := e.AsCall()
			args := call.Args()
			if call.FunctionName() != operators.Index || len(args) != 2 || args[0].Kind() != ast.IdentKind || args[1].Kind() != ast.LiteralKind {
				return
			}
			lit, ok := args[1].AsLiteral().(types.String)
			if !ok {
				return
			}
			v, field = args[0].AsIdent(), string(lit)
		default:
			return
		}
		err = c.checkField(v, field)
	}))
	return err
}

func (c *compiler) checkField(v, field string) error {
	if v == varConfig {
		secret, ok := c.props[field]
		switch {
		case !ok:
			return fmt.Errorf("config has no property %q", field)
		case secret:
			return fmt.Errorf("config.%s is a secret, which only auth and sign can reference", field)
		}
		return nil
	}
	if fs, ok := varFields[v]; ok && !slices.Contains(fs, field) {
		return fmt.Errorf("%s has no field %q; it has %s", v, field, strings.Join(fs, ", "))
	}
	return nil
}

func (e *expr) eval(vars map[string]any) (ref.Val, error) {
	v, _, err := e.prg.Eval(vars)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", e.where, err)
	}
	return v, nil
}

// template is text with {expression} holes.
type template struct {
	parts []part
}

// part is literal text, or a hole.
type part struct {
	text string
	hole *expr
}

func (c *compiler) template(where, src string, vars []string) (*template, error) {
	t := &template{}
	var lit strings.Builder
	for i := 0; i < len(src); {
		switch {
		case strings.HasPrefix(src[i:], "{{"):
			lit.WriteByte('{')
			i += 2
		case strings.HasPrefix(src[i:], "}}"):
			lit.WriteByte('}')
			i += 2
		case src[i] == '}':
			return nil, fmt.Errorf("%s: a } without its {; write }} for a literal one", where)
		case src[i] == '{':
			end, err := holeEnd(src, i)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", where, err)
			}
			e, err := c.expr(where, strings.TrimSpace(src[i+1:end]), vars)
			if err != nil {
				return nil, err
			}
			if lit.Len() > 0 {
				t.parts = append(t.parts, part{text: lit.String()})
				lit.Reset()
			}
			t.parts = append(t.parts, part{hole: e})
			i = end + 1
		default:
			lit.WriteByte(src[i])
			i++
		}
	}
	if lit.Len() > 0 || len(t.parts) == 0 {
		t.parts = append(t.parts, part{text: lit.String()})
	}
	return t, nil
}

// holeEnd finds the } that closes the hole opening at src[i], past nested
// braces and quoted strings.
func holeEnd(src string, i int) (int, error) {
	depth := 0
	for j := i; j < len(src); j++ {
		switch src[j] {
		case '\'', '"':
			q := src[j]
			for j++; j < len(src) && src[j] != q; j++ {
				if src[j] == '\\' {
					j++
				}
			}
			if j >= len(src) {
				return 0, errors.New("an unterminated string in a {hole}")
			}
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				if j == i+1 {
					return 0, errors.New("an empty {hole}; write {{ for a literal {")
				}
				return j, nil
			}
		}
	}
	return 0, errors.New("a { without its }; write {{ for a literal one")
}

// value is a lone hole's value, or else the template's text.
func (t *template) value(vars map[string]any) (ref.Val, error) {
	if len(t.parts) == 1 && t.parts[0].hole != nil {
		return t.parts[0].hole.eval(vars)
	}
	s, err := t.text(vars, nil)
	return types.String(s), err
}

// text renders the template, passing each hole's text through esc if it's
// set.
func (t *template) text(vars map[string]any, esc func(string) string) (string, error) {
	var b strings.Builder
	for _, p := range t.parts {
		if p.hole == nil {
			b.WriteString(p.text)
			continue
		}
		v, err := p.hole.eval(vars)
		if err != nil {
			return "", err
		}
		s, err := text(v)
		if err != nil {
			return "", fmt.Errorf("%s: %w", p.hole.where, err)
		}
		if esc != nil {
			s = esc(s)
		}
		b.WriteString(s)
	}
	return b.String(), nil
}

// text is a scalar's text. Timestamps are RFC 3339, in UTC.
func text(v ref.Val) (string, error) {
	switch v := v.(type) {
	case types.String:
		return string(v), nil
	case types.Int:
		return strconv.FormatInt(int64(v), 10), nil
	case types.Uint:
		return strconv.FormatUint(uint64(v), 10), nil
	case types.Double:
		return strconv.FormatFloat(float64(v), 'f', -1, 64), nil
	case types.Bool:
		return strconv.FormatBool(bool(v)), nil
	case types.Timestamp:
		return v.UTC().Format(time.RFC3339Nano), nil
	}
	return "", fmt.Errorf("a %s isn't text", v.Type().TypeName())
}

// native converts v to what encoding/json encodes. Values straight from a
// response stay as they were decoded, so their numbers keep their digits.
func native(v ref.Val) (any, error) {
	switch v := v.(type) {
	case types.Null:
		return nil, nil
	case types.Bool:
		return bool(v), nil
	case types.Int:
		return int64(v), nil
	case types.Uint:
		return uint64(v), nil
	case types.Double:
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return nil, errors.New("JSON has no NaN or infinity")
		}
		return float64(v), nil
	case types.String:
		return string(v), nil
	case types.Bytes:
		return []byte(v), nil
	case types.Timestamp:
		return v.UTC().Format(time.RFC3339Nano), nil
	case types.Duration:
		return v.String(), nil
	case traits.Mapper:
		if m, ok := v.Value().(map[string]any); ok {
			return m, nil
		}
		out := map[string]any{}
		for it := v.Iterator(); it.HasNext() == types.True; {
			k := it.Next()
			ks, ok := k.(types.String)
			if !ok {
				return nil, errors.New("JSON object keys are strings")
			}
			x, err := native(v.Get(k))
			if err != nil {
				return nil, err
			}
			out[string(ks)] = x
		}
		return out, nil
	case traits.Lister:
		if l, ok := v.Value().([]any); ok {
			return l, nil
		}
		n := int64(v.Size().(types.Int))
		out := make([]any, 0, n)
		for i := range n {
			x, err := native(v.Get(types.Int(i)))
			if err != nil {
				return nil, err
			}
			out = append(out, x)
		}
		return out, nil
	}
	return nil, fmt.Errorf("a %s isn't JSON", v.Type().TypeName())
}
