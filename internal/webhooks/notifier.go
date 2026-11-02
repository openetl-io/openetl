package webhooks

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/encoding/protojson"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/internal/events"
	"github.com/unsuitable001/openetl/internal/netguard"
	"github.com/unsuitable001/openetl/internal/secrets"
	"github.com/unsuitable001/openetl/internal/store"
	"github.com/unsuitable001/openetl/internal/store/db"
)

// DefaultRetries is the wait before each retry of a delivery that failed
// in a way worth retrying: about a day in all.
var DefaultRetries = []time.Duration{5 * time.Second, 5 * time.Minute, 30 * time.Minute, 2 * time.Hour, 5 * time.Hour, 10 * time.Hour, 10 * time.Hour}

const (
	// How long one attempt may take, and how long a claimed delivery is
	// left to its notifier.
	attemptTimeout = 15 * time.Second
	lease          = time.Minute
	// How much of a response's body a delivery keeps.
	maxResponseBody = 1 << 10
	concurrency     = 8
)

var (
	meter       = otel.Meter("github.com/unsuitable001/openetl/internal/webhooks")
	attempts, _ = meter.Int64Counter("openetl.webhook.attempts", metric.WithUnit("{attempt}"),
		metric.WithDescription("Delivery attempts to webhooks, by outcome: succeeded, retried or failed"))
	latency, _ = meter.Float64Histogram("openetl.webhook.latency", metric.WithUnit("s"),
		metric.WithDescription("How long webhooks took to answer"))
)

// Notifier fans out the outbox's events to the webhooks they match, and
// delivers them. Any number of notifiers can share a database: each claims
// events and deliveries the others haven't.
type Notifier struct {
	Store *store.Store
	Org   uuid.UUID
	// Opens signing secrets. Nil: deliveries fail until the server has a
	// secrets key.
	Secrets *secrets.Keyring
	// netguard's client; redirects aren't followed.
	Client *http.Client
	// How often it looks for work. Zero: 1 s.
	Poll time.Duration
	// Nil: DefaultRetries.
	Retries []time.Duration
	// A webhook whose endpoint fails for this long is disabled. Zero: 3
	// days.
	DisableAfter time.Duration
	// How long events and their deliveries are kept. Zero: 7 days.
	Retention time.Duration
	Logger    *slog.Logger
}

// Run works until ctx ends.
func (n *Notifier) Run(ctx context.Context) error {
	t := time.NewTicker(cmp.Or(n.Poll, time.Second))
	defer t.Stop()
	var pruned time.Time
	for {
		if _, err := n.Dispatch(ctx); err != nil && ctx.Err() == nil {
			n.log().Warn("fanning out events failed", "err", err)
		}
		if _, err := n.Deliver(ctx); err != nil && ctx.Err() == nil {
			n.log().Warn("delivering events failed", "err", err)
		}
		if time.Since(pruned) >= time.Hour {
			if err := n.Prune(ctx); err != nil && ctx.Err() == nil {
				n.log().Warn("pruning events failed", "err", err)
			} else {
				pruned = time.Now()
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

func (n *Notifier) log() *slog.Logger {
	if n.Logger == nil {
		return slog.New(slog.DiscardHandler)
	}
	return n.Logger
}

type hook struct {
	row  db.Webhook
	spec *apiv1.WebhookSpec
}

func parseSpec(raw []byte) (*apiv1.WebhookSpec, error) {
	spec := &apiv1.WebhookSpec{}
	return spec, protojson.Unmarshal(raw, spec)
}

// Dispatch makes a delivery of each event not yet fanned out for each
// enabled webhook it matches, and returns how many events it fanned out.
func (n *Notifier) Dispatch(ctx context.Context) (int, error) {
	var count int
	err := n.Store.Tx(ctx, func(q *db.Queries) error {
		evs, err := q.ClaimEvents(ctx, db.ClaimEventsParams{OrgID: n.Org, MaxRows: 100})
		if err != nil || len(evs) == 0 {
			return err
		}
		var wss []uuid.UUID
		for _, e := range evs {
			if !slices.Contains(wss, e.WorkspaceID) {
				wss = append(wss, e.WorkspaceID)
			}
		}
		rows, err := q.ListEnabledWebhooks(ctx, db.ListEnabledWebhooksParams{OrgID: n.Org, Workspaces: wss})
		if err != nil {
			return err
		}
		var hooks []hook
		for _, r := range rows {
			spec, err := parseSpec(r.Spec)
			if err != nil {
				return fmt.Errorf("webhook %s: %w", r.Name, err)
			}
			hooks = append(hooks, hook{r, spec})
		}
		ids := make([]uuid.UUID, len(evs))
		for i, e := range evs {
			ids[i] = e.ID
			for _, h := range hooks {
				if !matches(h, e) {
					continue
				}
				id, err := uuid.NewV7()
				if err != nil {
					return err
				}
				if err := q.InsertDelivery(ctx, db.InsertDeliveryParams{OrgID: n.Org, ID: id, WebhookID: h.row.ID, EventID: e.ID}); err != nil {
					return err
				}
			}
		}
		count = len(evs)
		return q.MarkEventsDispatched(ctx, db.MarkEventsDispatchedParams{OrgID: n.Org, Ids: ids})
	})
	return count, err
}

func matches(h hook, e db.Outbox) bool {
	switch {
	case h.row.WorkspaceID != e.WorkspaceID:
		return false
	case e.WebhookID != nil:
		return *e.WebhookID == h.row.ID
	case !events.Matches(h.spec.GetEvents(), e.Type):
		return false
	case len(h.spec.GetPipelines()) == 0:
		return true
	}
	return e.Pipeline != nil && slices.Contains(h.spec.GetPipelines(), *e.Pipeline)
}

// Deliver makes an attempt at each due delivery, and returns how many it
// tried.
func (n *Notifier) Deliver(ctx context.Context) (int, error) {
	rows, err := n.Store.ClaimDeliveries(ctx, db.ClaimDeliveriesParams{OrgID: n.Org, LeaseSeconds: lease.Seconds(), MaxRows: 64})
	if err != nil {
		return 0, err
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)
	for _, r := range rows {
		g.Go(func() error {
			if err := n.attempt(gctx, r); err != nil && gctx.Err() == nil {
				n.log().Warn("recording a delivery attempt failed", "delivery", r.ID, "err", err)
			}
			return nil
		})
	}
	return len(rows), g.Wait()
}

// outcome is how an attempt went.
type outcome struct {
	status  int
	body    string
	err     string
	latency time.Duration
	// Worth another attempt, after at least retryAfter.
	retry      bool
	retryAfter time.Duration
	// The webhook's own fault, not its endpoint's, such as a missing
	// secret, so it doesn't count towards disabling it.
	local bool
	// Disable the webhook, for this reason.
	disable string
}

func (n *Notifier) attempt(ctx context.Context, r db.ClaimDeliveriesRow) error {
	o := n.send(ctx, r)
	if ctx.Err() != nil {
		// Shutting down: the lease runs out and another notifier tries.
		return nil
	}
	tries := n.Retries
	if tries == nil {
		tries = DefaultRetries
	}
	arg := db.RecordAttemptParams{OrgID: n.Org, ID: r.ID, Attempts: r.Attempts, State: "failed"}
	if o.status != 0 {
		s := int32(o.status) //nolint:gosec // an HTTP status
		arg.ResponseStatus = &s
		arg.ResponseBody = &o.body
	}
	if o.err != "" {
		arg.Error = &o.err
	}
	if o.latency > 0 {
		ms := int32(o.latency.Milliseconds()) //nolint:gosec // at most attemptTimeout
		arg.LatencyMs = &ms
		latency.Record(ctx, o.latency.Seconds(), metric.WithAttributes(attribute.String("openetl.webhook", r.WebhookID.String())))
	}
	result := "failed"
	switch {
	case o.status >= 200 && o.status < 300:
		arg.State, result = "succeeded", "succeeded"
	case o.retry && int(r.Attempts) < len(tries):
		wait := jitter(tries[r.Attempts])
		if o.retryAfter > wait {
			wait = min(o.retryAfter, slices.Max(tries))
		}
		arg.State, arg.WaitSeconds, result = "pending", wait.Seconds(), "retried"
	}
	attempts.Add(ctx, 1, metric.WithAttributes(attribute.String("openetl.webhook", r.WebhookID.String()), attribute.String("outcome", result)))
	recorded, err := n.Store.RecordAttempt(ctx, arg)
	if err != nil || recorded == 0 {
		return err
	}
	switch {
	case arg.State == "succeeded":
		if r.FailingSince != nil {
			return n.Store.WebhookSucceeded(ctx, db.WebhookSucceededParams{OrgID: n.Org, ID: r.WebhookID})
		}
		return nil
	case o.disable != "":
		return n.disable(ctx, r.WebhookID, o.disable)
	case o.local:
		return nil
	}
	failing, err := n.Store.WebhookFailed(ctx, db.WebhookFailedParams{OrgID: n.Org, ID: r.WebhookID})
	if err != nil {
		return err
	}
	if after := cmp.Or(n.DisableAfter, 72*time.Hour); failing >= after.Seconds() {
		return n.disable(ctx, r.WebhookID, fmt.Sprintf("its endpoint failed every delivery for %s", after))
	}
	return nil
}

func (n *Notifier) disable(ctx context.Context, id uuid.UUID, reason string) error {
	n.log().Warn("disabling a webhook", "webhook", id, "reason", reason)
	return n.Store.DisableWebhook(ctx, db.DisableWebhookParams{OrgID: n.Org, ID: id, Reason: &reason})
}

func (n *Notifier) send(ctx context.Context, r db.ClaimDeliveriesRow) outcome {
	spec, err := parseSpec(r.WebhookSpec)
	if err != nil {
		return outcome{err: err.Error(), retry: true, local: true}
	}
	keys, err := n.keys(ctx, r.WorkspaceID, spec.GetSecrets())
	if err != nil {
		return outcome{err: err.Error(), retry: true, local: true}
	}
	body, err := json.Marshal(events.Payload{Type: r.EventType, Timestamp: r.EventTime.UTC(), Data: r.EventData})
	if err != nil {
		return outcome{err: err.Error(), local: true}
	}
	ctx, cancel := context.WithTimeout(ctx, attemptTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, spec.GetUrl(), bytes.NewReader(body))
	if err != nil {
		return outcome{err: err.Error(), local: true}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "openetl-webhooks/1")
	SetHeaders(req.Header, r.EventID.String(), time.Now(), body, keys...)
	start := time.Now()
	res, err := n.Client.Do(req)
	o := outcome{latency: time.Since(start)}
	if err != nil {
		// An address the egress policy blocks stays blocked.
		_, blocked := errors.AsType[*netguard.BlockedError](err)
		o.err, o.retry = err.Error(), !blocked
		return o
	}
	defer func() { _ = res.Body.Close() }()
	b, _ := io.ReadAll(io.LimitReader(res.Body, maxResponseBody))
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 64<<10))
	o.status, o.body = res.StatusCode, strings.ToValidUTF8(strings.ReplaceAll(string(b), "\x00", ""), "�")
	switch c := res.StatusCode; {
	case c >= 200 && c < 300:
	case c == http.StatusGone:
		o.err, o.disable = "the endpoint answered 410 Gone", "its endpoint answered 410 Gone"
	case c == http.StatusRequestTimeout, c == http.StatusTooManyRequests, c >= 500:
		o.err, o.retry, o.retryAfter = res.Status, true, retryAfter(res.Header.Get("Retry-After"))
	case c >= 300 && c < 400:
		o.err = res.Status + ": webhooks don't follow redirects"
	default:
		o.err = res.Status
	}
	return o
}

// keys opens the webhook's signing secrets, in order.
func (n *Notifier) keys(ctx context.Context, ws uuid.UUID, names []string) ([][]byte, error) {
	if n.Secrets == nil {
		return nil, errors.New("the server has no secrets key (--secrets-key), so it can't open signing secrets")
	}
	rows, err := n.Store.GetSecretValues(ctx, db.GetSecretValuesParams{OrgID: n.Org, WorkspaceID: ws, Names: names})
	if err != nil {
		return nil, err
	}
	keys := make([][]byte, 0, len(names))
	for _, name := range names {
		i := slices.IndexFunc(rows, func(r db.GetSecretValuesRow) bool { return r.Name == name })
		if i < 0 {
			return nil, fmt.Errorf("signing secret %q doesn't exist", name)
		}
		r := rows[i]
		if r.Ref != nil {
			return nil, fmt.Errorf("signing secret %q is a reference to a cloud secret store, which only runners read", name)
		}
		v, err := n.Secrets.Open(ctx, n.Org, secrets.AAD(n.Org, ws, r.ID), r.Ciphertext)
		if err != nil {
			return nil, fmt.Errorf("signing secret %q: %w", name, err)
		}
		k, err := ParseSecret(string(v))
		if err != nil {
			return nil, fmt.Errorf("signing secret %q: %w", name, err)
		}
		keys = append(keys, k)
	}
	return keys, nil
}

// Prune deletes events, and their deliveries, past the retention.
func (n *Notifier) Prune(ctx context.Context) error {
	_, err := n.Store.PruneEvents(ctx, db.PruneEventsParams{OrgID: n.Org, Before: time.Now().Add(-cmp.Or(n.Retention, 7*24*time.Hour))})
	return err
}

// jitter is d give or take a tenth.
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return d - d/10 + rand.N(d/5+1) //nolint:gosec // spreading retries, not a secret
}

// retryAfter parses Retry-After: seconds or an HTTP date.
func retryAfter(v string) time.Duration {
	if v == "" {
		return 0
	}
	if s, err := strconv.Atoi(v); err == nil && s > 0 {
		return time.Duration(s) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		return max(time.Until(t), 0)
	}
	return 0
}

// Client is policy's client, which doesn't follow redirects: a webhook that
// answers with one fails.
func Client(policy netguard.Policy) *http.Client {
	c := policy.Client()
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return c
}
