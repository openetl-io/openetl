package engineclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os/exec"
	"strings"

	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/sqlplan"
)

// The plan subcommand's exit codes: the SQL has errors, which its answer
// lists, or the request is one it can't plan, such as with a type it
// doesn't know.
const (
	planSQLErrors = 1
	planRefused   = 2
)

// Plan has the engine plan a SQL transform: `openetl-engine plan`, with the
// request on stdin and the answer on stdout. SQL with errors is an answer,
// and only a call that failed is an error.
func Plan(ctx context.Context, opts Options, req sqlplan.Request) (sqlplan.Answer, error) {
	var answer sqlplan.Answer
	bin, err := opts.binary()
	if err != nil {
		return answer, errs.Wrap(errs.Internal, err, "engineclient: find openetl-engine")
	}
	in, err := json.Marshal(req)
	if err != nil {
		return answer, errs.Wrap(errs.Internal, err, "engineclient: the plan request")
	}
	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, bin, "plan")
	cmd.Stdin, cmd.Stdout, cmd.Stderr = bytes.NewReader(in), &stdout, &stderr
	err = cmd.Run()
	exit, _ := errors.AsType[*exec.ExitError](err)
	switch {
	case err == nil, exit != nil && exit.ExitCode() == planSQLErrors:
	case ctx.Err() != nil:
		return answer, ctx.Err()
	case exit != nil && exit.ExitCode() == planRefused:
		return answer, errs.New(errs.ConfigInvalid, "%s", strings.TrimSpace(stderr.String()))
	default:
		return answer, errs.Wrap(errs.Internal, err, "engineclient: openetl-engine plan: "+strings.TrimSpace(stderr.String()))
	}
	if err := json.Unmarshal(stdout.Bytes(), &answer); err != nil {
		return answer, errs.Wrap(errs.Internal, err, "engineclient: openetl-engine plan's answer")
	}
	return answer, nil
}
