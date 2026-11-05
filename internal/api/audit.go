package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
	"github.com/unsuitable001/openetl/internal/store/db"
)

// readsSecrets are the runner calls that return secrets' values.
var readsSecrets = map[string]bool{
	dataplanev1connect.DataPlaneServiceGetSyncSpecProcedure:        true,
	dataplanev1connect.DataPlaneServiceGetConnectorConfigProcedure: true,
	dataplanev1connect.DataPlaneServiceGetFlinkTeardownProcedure:   true,
}

// audited says whether a call goes into the audit log: every public call
// but reads, and the runner calls that read secrets.
func audited(procedure string, r rule) bool {
	if r.runner {
		return readsSecrets[procedure]
	}
	return !r.public() && !strings.HasSuffix(r.permission, ".get")
}

// An entry is what an audited call's handler adds to its audit log entry.
type entry struct {
	resources []string
	details   map[string]any
}

type entryKey struct{}

// audit is the call's audit entry; what's added to it is dropped if the call
// isn't audited.
func audit(ctx context.Context) *entry {
	if e, ok := ctx.Value(entryKey{}).(*entry); ok {
		return e
	}
	return &entry{}
}

// touched adds resources the call touched besides the one it names.
func (e *entry) touched(names ...string) {
	for _, n := range names {
		if n != "" && !slices.Contains(e.resources, n) {
			e.resources = append(e.resources, n)
		}
	}
}

// set adds a detail, which must never hold a secret's value.
func (e *entry) set(key string, value any) {
	if e.details == nil {
		e.details = map[string]any{}
	}
	e.details[key] = value
}

// named is the resource a request names, if any.
func named(m any) string {
	switch m := m.(type) {
	case interface{ GetName() string }:
		return m.GetName()
	case interface{ GetParent() string }:
		return m.GetParent()
	case *apiv1.UpdateSecretRequest:
		return m.GetSecret().GetName()
	}
	return ""
}

func (c *caller) actor() string {
	if c.dataPlane != "" {
		return "dataPlanes/" + c.dataPlane
	}
	return c.principal
}

func (c *caller) credential() string {
	switch {
	case c.dataPlane != "":
		return "dataPlanes/" + c.dataPlane + "/tokens/" + c.token.String()
	case c.session != uuid.Nil:
		return c.principal + "/sessions/" + c.session.String()
	case c.key != uuid.Nil:
		return c.principal + "/keys/" + c.key.String()
	}
	return ""
}

// Longer user agents and errors are cut.
const (
	maxUserAgent = 512
	maxError     = 2048
)

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return strings.ToValidUTF8(s[:n], "")
}

// audit writes the call's entry before it runs, so a call that changes
// something is never missing from the log, and completes it with how the
// call ended. A call whose entry can't be written doesn't run.
func (s *Server) audit(ctx context.Context, req connect.AnyRequest, call connect.UnaryFunc) (connect.AnyResponse, error) {
	c := callerOf(ctx)
	id, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	e := &entry{resources: []string{}}
	e.touched(named(req.Any()))
	err = s.Store.WriteAuditEntry(ctx, db.InsertAuditEntryParams{
		OrgID: c.org, ID: id, Time: time.Now(), Actor: c.actor(), Credential: c.credential(),
		Method: req.Spec().Procedure, Resources: e.resources, Details: []byte("{}"),
		Peer: req.Peer().Addr, UserAgent: truncate(req.Header().Get("User-Agent"), maxUserAgent),
	})
	if err != nil {
		return nil, fmt.Errorf("api: audit: %w", err)
	}
	res, err := call(context.WithValue(ctx, entryKey{}, e), req)
	err = s.connectError(req.Spec().Procedure, err)
	code, msg := "ok", ""
	if err != nil {
		code, msg = connect.CodeOf(err).String(), err.Error()
		if ce, ok := errors.AsType[*connect.Error](err); ok {
			msg = ce.Message()
		}
		msg = truncate(msg, maxError)
	}
	details, derr := json.Marshal(e.details)
	if derr != nil || e.details == nil {
		details = []byte("{}")
	}
	end := time.Now()
	// The call has happened, so its end is recorded even if the client left.
	n, cerr := s.Store.CompleteAuditEntry(context.WithoutCancel(ctx), db.CompleteAuditEntryParams{
		OrgID: c.org, ID: id, Resources: e.resources, Details: details, Code: &code, Error: msg, EndTime: &end,
	})
	if cerr != nil || derr != nil || n == 0 {
		s.log().Error("audit entry not completed", "id", id, "procedure", req.Spec().Procedure, "err", cerr, "details_err", derr)
		// Secrets' values go out only once the log names them.
		if err == nil && readsSecrets[req.Spec().Procedure] {
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("the audit log couldn't record which secrets this read; try again"))
		}
	}
	return res, err
}

func (s *Server) ListAuditEntries(ctx context.Context, req *connect.Request[apiv1.ListAuditEntriesRequest]) (*connect.Response[apiv1.ListAuditEntriesResponse], error) {
	after, n, err := page(req.Msg.GetPageSize(), req.Msg.GetPageToken())
	if err != nil {
		return nil, err
	}
	params := db.ListAuditEntriesParams{
		OrgID: s.Tenant.Org, Actor: req.Msg.GetActor(), Resource: req.Msg.GetResource(), Method: req.Msg.GetMethod(), MaxRows: n,
	}
	if after != "" {
		id, err := uuid.Parse(after)
		if err != nil {
			return nil, invalid("bad page_token")
		}
		params.Before = &id
	}
	if t := req.Msg.GetStartTime(); t != nil {
		v := t.AsTime()
		params.StartTime = &v
	}
	if t := req.Msg.GetEndTime(); t != nil {
		v := t.AsTime()
		params.EndTime = &v
	}
	rows, err := s.Store.ListAuditEntries(ctx, params)
	if err != nil {
		return nil, err
	}
	res := &apiv1.ListAuditEntriesResponse{}
	if len(rows) == int(n) {
		rows = rows[:n-1]
		res.NextPageToken = nextToken(rows[len(rows)-1].ID.String())
	}
	for _, r := range rows {
		e := &apiv1.AuditEntry{
			Name: "auditEntries/" + r.ID.String(), Time: timestamppb.New(r.Time), Actor: r.Actor, Credential: r.Credential,
			Method: r.Method, Resources: r.Resources, Details: &structpb.Struct{}, Peer: r.Peer, UserAgent: r.UserAgent, Error: r.Error,
		}
		if err := e.Details.UnmarshalJSON(r.Details); err != nil {
			return nil, err
		}
		if r.Code != nil {
			e.Code = *r.Code
		}
		if r.EndTime != nil {
			e.EndTime = timestamppb.New(*r.EndTime)
		}
		res.Entries = append(res.Entries, e)
	}
	return connect.NewResponse(res), nil
}
