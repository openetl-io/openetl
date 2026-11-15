package activities

import (
	"bytes"
	"context"
	"errors"
	"strings"

	"github.com/google/uuid"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/platform/errs"
)

// Quarantine reads pipelines' quarantine files for the API.
type Quarantine struct {
	Store checkpoint.Store
}

// The most lines a read returns.
const maxQuarantineLines = 100

// ReadQuarantine returns the first lines of one of a pipeline's quarantine
// files: at most in.Lines, and no more bytes of them than
// contract.MaxQuarantineRead. A first line larger than that fails the read.
func (a *Quarantine) ReadQuarantine(ctx context.Context, in contract.ReadQuarantineInput) (contract.ReadQuarantineResult, error) {
	var res contract.ReadQuarantineResult
	for _, id := range []string{in.OrgID, in.PipelineID} {
		if u, err := uuid.Parse(id); err != nil || u.String() != id {
			return res, errs.New(errs.ConfigInvalid, "%q isn't an ID", id)
		}
	}
	if !quarantinePath(in.Path) {
		return res, errs.New(errs.ConfigInvalid, "%q isn't a path below a pipeline's quarantine directory", in.Path)
	}
	limit := in.Lines
	if limit <= 0 || limit > maxQuarantineLines {
		limit = maxQuarantineLines
	}
	// The lines a read returns, with their newlines, are shorter than window,
	// so a file that fills the window goes on past them.
	const window = contract.MaxQuarantineRead + maxQuarantineLines + 1
	data, err := a.Store.GetPrefix(ctx, in.OrgID+"/"+in.PipelineID+"/"+checkpoint.QuarantineDir+"/"+in.Path, window)
	if errors.Is(err, checkpoint.ErrNotFound) {
		res.Missing = true
		return res, nil
	}
	if err != nil {
		return res, errs.Wrap(errs.Transient, err, "read quarantine file "+in.Path)
	}
	whole := len(data) < window
	size := 0
	for len(data) > 0 {
		line, rest, ended := bytes.Cut(data, []byte("\n"))
		// A line without its newline in a read that stopped short is cut.
		cut := !ended && !whole
		if len(res.Lines) == limit || cut || size+len(line) > contract.MaxQuarantineRead {
			if len(res.Lines) == 0 {
				return res, errs.New(errs.ConfigInvalid, "quarantine file %s's first line is larger than %d KiB; read it from the data plane's storage", in.Path, contract.MaxQuarantineRead>>10)
			}
			res.More = true
			break
		}
		size += len(line)
		res.Lines = append(res.Lines, string(line))
		data = rest
	}
	return res, nil
}

// quarantinePath reports whether p is a path below a pipeline's quarantine
// directory: relative, without empty, . or .. parts.
func quarantinePath(p string) bool {
	if strings.ContainsAny(p, "\\\x00") {
		return false
	}
	for _, part := range strings.Split(p, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}
