package sim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/unsuitable001/openetl/sdk/go/destination"
)

// ---- Stage and apply: a warehouse or Postgres, or Parquet on object storage.

type marker struct {
	stream string
	gen, n uint64
}

type stagingKey struct {
	gen, n uint64
	stream string
}

// warehouse is the destination's durable state.
type warehouse struct {
	dedupeCapable bool // Postgres and warehouses can dedupe at apply; Parquet can't

	mu      sync.Mutex
	fence   uint64
	loads   map[marker]bool
	staging map[stagingKey][]row
	tables  map[string][]row          // append targets
	merged  map[string]map[string]row // merge targets, by key
	applies map[string]int            // how often each record version was applied
}

func newWarehouse(dedupeCapable bool) *warehouse {
	return &warehouse{
		dedupeCapable: dedupeCapable,
		loads:         map[marker]bool{},
		staging:       map[stagingKey][]row{},
		tables:        map[string][]row{},
		merged:        map[string]map[string]row{},
		applies:       map[string]int{},
	}
}

type stagedStream struct {
	Name   string `json:"name"`
	Mode   int    `json:"mode"`
	Dedupe bool   `json:"dedupe"`
}

type warehouseStaged struct {
	Gen     uint64         `json:"gen"`
	N       uint64         `json:"n"`
	Streams []stagedStream `json:"streams"`
}

var errStagingMissing = errors.New("sim: staging missing")

// applyTx is one destination transaction. It checks every precondition
// before changing anything.
func (wh *warehouse) applyTx(applierGen uint64, st warehouseStaged, streams []stagedStream) error {
	wh.mu.Lock()
	defer wh.mu.Unlock()
	if wh.fence > applierGen {
		return fmt.Errorf("%w: fence is %d, applier is %d", destination.ErrFenced, wh.fence, applierGen)
	}
	var todo []stagedStream
	for _, s := range streams {
		if wh.loads[marker{s.Name, st.Gen, st.N}] {
			continue
		}
		if _, ok := wh.staging[stagingKey{st.Gen, st.N, s.Name}]; !ok {
			return fmt.Errorf("%w for %s in %d-g%d", errStagingMissing, s.Name, st.N, st.Gen)
		}
		todo = append(todo, s)
	}
	for _, s := range todo {
		k := stagingKey{st.Gen, st.N, s.Name}
		wh.apply(s, wh.staging[k])
		wh.loads[marker{s.Name, st.Gen, st.N}] = true
		delete(wh.staging, k)
	}
	return nil
}

func (wh *warehouse) apply(s stagedStream, rows []row) {
	switch destination.WriteMode(s.Mode) {
	case destination.Merge:
		m := wh.merged[s.Name]
		if m == nil {
			m = map[string]row{}
			wh.merged[s.Name] = m
		}
		for _, r := range rows {
			if cur, ok := m[r.Key]; !ok || r.Seq >= cur.Seq { // the _openetl_seq guard
				m[r.Key] = r
				wh.applies[r.ID]++
			}
		}
	default:
		var have map[string]bool
		if s.Dedupe {
			have = map[string]bool{}
			for _, r := range wh.tables[s.Name] {
				have[r.ID] = true
			}
		}
		for _, r := range rows {
			if have != nil {
				if have[r.ID] {
					continue
				}
				have[r.ID] = true
			}
			wh.tables[s.Name] = append(wh.tables[s.Name], r)
			wh.applies[r.ID]++
		}
	}
}

func (wh *warehouse) missing(st warehouseStaged) (missing []stagedStream, staged int) {
	wh.mu.Lock()
	defer wh.mu.Unlock()
	for _, s := range st.Streams {
		if wh.loads[marker{s.Name, st.Gen, st.N}] {
			continue
		}
		missing = append(missing, s)
		if _, ok := wh.staging[stagingKey{st.Gen, st.N, s.Name}]; ok {
			staged++
		}
	}
	return missing, staged
}

// trim deletes the markers below an applied epoch, as Postgres does.
func (wh *warehouse) trim(applied uint64) {
	wh.mu.Lock()
	defer wh.mu.Unlock()
	for m := range wh.loads {
		if m.n < applied {
			delete(wh.loads, m)
		}
	}
}

type warehouseDest struct {
	w       *world
	wh      *warehouse
	attempt int
}

func (d *warehouseDest) Capabilities() destination.Capabilities {
	c := destination.Capabilities{Class: destination.StageAndApply, Scopes: []destination.Scope{destination.ScopeStream}, MinCommitInterval: time.Second}
	if d.wh.dedupeCapable {
		c.DedupeAtApply = true
		c.Scopes = append(c.Scopes, destination.ScopePipeline)
	}
	return c
}

func (d *warehouseDest) Open(ctx context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	if err := outcome(d.w.sched.point(ctx, d.attempt, "sink.open"), nop); err != nil {
		return nil, err
	}
	return &warehouseWriter{warehouseDest: d, cfg: cfg, buf: map[uint64]map[string][]row{}}, nil
}

type warehouseWriter struct {
	*warehouseDest
	cfg destination.OpenConfig
	buf map[uint64]map[string][]row // epoch → stream → rows; lost on a crash
}

func (w *warehouseWriter) Write(ctx context.Context, e destination.Epoch, stream string, batch arrow.RecordBatch) error {
	rows := rowsOf(batch)
	return outcome(w.w.sched.point(ctx, w.attempt, "sink.write"), func() error {
		if w.buf[e.N] == nil {
			w.buf[e.N] = map[string][]row{}
		}
		w.buf[e.N][stream] = append(w.buf[e.N][stream], rows...)
		return nil
	})
}

func (w *warehouseWriter) PreCommit(ctx context.Context, e destination.Epoch) (destination.Staged, error) {
	st := warehouseStaged{Gen: e.Generation, N: e.N}
	for _, s := range w.cfg.Streams {
		rows := w.buf[e.N][s.Name]
		if len(rows) == 0 {
			continue
		}
		err := outcome(w.w.sched.point(ctx, w.attempt, "sink.stage"), func() error {
			w.wh.mu.Lock()
			defer w.wh.mu.Unlock()
			w.wh.staging[stagingKey{e.Generation, e.N, s.Name}] = slices.Clone(rows)
			return nil
		})
		if err != nil {
			return nil, err
		}
		st.Streams = append(st.Streams, stagedStream{Name: s.Name, Mode: int(s.Mode), Dedupe: s.Dedupe})
	}
	delete(w.buf, e.N)
	return json.Marshal(st)
}

func (w *warehouseWriter) applyScoped(ctx context.Context, st warehouseStaged, streams []stagedStream) error {
	if w.cfg.Scope == destination.ScopePipeline {
		return outcome(w.w.sched.point(ctx, w.attempt, "sink.apply"), func() error { return w.wh.applyTx(w.cfg.Generation, st, streams) })
	}
	for _, s := range streams {
		err := outcome(w.w.sched.point(ctx, w.attempt, "sink.apply"), func() error { return w.wh.applyTx(w.cfg.Generation, st, []stagedStream{s}) })
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *warehouseWriter) Commit(ctx context.Context, _ destination.Epoch, s destination.Staged) error {
	var st warehouseStaged
	if err := json.Unmarshal(s, &st); err != nil {
		return err
	}
	return w.applyScoped(ctx, st, st.Streams)
}

func (w *warehouseWriter) Recover(ctx context.Context, gen uint64, e destination.Epoch, s destination.Staged) (destination.Recovery, error) {
	err := outcome(w.w.sched.point(ctx, w.attempt, "sink.recover"), func() error {
		w.wh.mu.Lock()
		defer w.wh.mu.Unlock()
		if w.wh.fence > gen {
			return fmt.Errorf("%w: fence is %d, recovering as %d", destination.ErrFenced, w.wh.fence, gen)
		}
		w.wh.fence = gen
		return nil
	})
	if err != nil {
		return 0, err
	}
	if e.IsZero() {
		return destination.Applied, nil
	}
	var st warehouseStaged
	if err := json.Unmarshal(s, &st); err != nil {
		return 0, err
	}
	missing, staged := w.wh.missing(st)
	var r destination.Recovery
	switch {
	case len(missing) == 0:
		r = destination.Applied
	case staged == len(missing):
		if err := w.applyScoped(ctx, st, missing); err != nil {
			return 0, err
		}
		r = destination.Redone
	case staged == 0 && len(missing) == len(st.Streams):
		return destination.Lost, nil
	default:
		return 0, fmt.Errorf("sim: epoch %d-g%d is partly applied and its remaining staging is gone", st.N, st.Gen)
	}
	// Best effort, as on Postgres: only a crash stops the attempt.
	err = outcome(w.w.sched.point(ctx, w.attempt, "sink.trim"), func() error { w.wh.trim(st.N); return nil })
	if errors.Is(err, errCrashed) || errors.Is(err, context.Canceled) {
		return 0, err
	}
	return r, nil
}

func (w *warehouseWriter) Close(context.Context) error { return nil }

// ---- Transactional stream: Kafka, with a pool of transactional producers.

const producerSlots = 3

type txn struct {
	gen, n uint64
	rows   []streamRow
}

type streamRow struct {
	stream string
	row
}

type broker struct {
	mu       sync.Mutex
	epochs   [producerSlots]uint64 // producer epoch per transactional ID; init raises it
	open     [producerSlots]*txn
	visible  []streamRow
	markers  map[[2]uint64]bool // (g, n) markers on the control topic
	applies  map[string]int
	aborted  int
	commitsN int
}

func newBroker() *broker { return &broker{markers: map[[2]uint64]bool{}, applies: map[string]int{}} }

type kafkaDest struct {
	w       *world
	b       *broker
	attempt int
}

func (d *kafkaDest) Capabilities() destination.Capabilities {
	return destination.Capabilities{Class: destination.TransactionalStream, Scopes: []destination.Scope{destination.ScopePipeline}}
}

func (d *kafkaDest) Open(ctx context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	kw := &kafkaWriter{kafkaDest: d, cfg: cfg}
	for slot := range producerSlots {
		// initTransactions fences older producers with this transactional ID
		// and aborts their open transaction.
		err := outcome(d.w.sched.point(ctx, d.attempt, "kafka.init"), func() error {
			d.b.mu.Lock()
			defer d.b.mu.Unlock()
			d.b.epochs[slot]++
			if d.b.open[slot] != nil {
				d.b.open[slot] = nil
				d.b.aborted++
			}
			kw.epochs[slot] = d.b.epochs[slot]
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return kw, nil
}

type kafkaWriter struct {
	*kafkaDest
	cfg    destination.OpenConfig
	epochs [producerSlots]uint64
}

type kafkaStaged struct {
	Slot  int    `json:"slot"`
	Epoch uint64 `json:"producer_epoch"`
	Gen   uint64 `json:"gen"`
	N     uint64 `json:"n"`
}

// txnFor returns the open transaction for epoch e on its slot, under b.mu.
func (w *kafkaWriter) txnFor(e destination.Epoch) (*txn, error) {
	slot := int(e.N % producerSlots)
	if w.b.epochs[slot] != w.epochs[slot] {
		return nil, fmt.Errorf("%w: producer %d", destination.ErrFenced, slot)
	}
	t := w.b.open[slot]
	if t == nil {
		t = &txn{gen: e.Generation, n: e.N}
		w.b.open[slot] = t
	}
	if t.n != e.N || t.gen != e.Generation {
		return nil, fmt.Errorf("sim: producer %d still has epoch %d open, writing %d", slot, t.n, e.N)
	}
	return t, nil
}

func (w *kafkaWriter) Write(ctx context.Context, e destination.Epoch, stream string, batch arrow.RecordBatch) error {
	rows := rowsOf(batch)
	return outcome(w.w.sched.point(ctx, w.attempt, "kafka.produce"), func() error {
		w.b.mu.Lock()
		defer w.b.mu.Unlock()
		t, err := w.txnFor(e)
		if err != nil {
			return err
		}
		for _, r := range rows {
			t.rows = append(t.rows, streamRow{stream, r})
		}
		return nil
	})
}

func (w *kafkaWriter) PreCommit(ctx context.Context, e destination.Epoch) (destination.Staged, error) {
	slot := int(e.N % producerSlots)
	err := outcome(w.w.sched.point(ctx, w.attempt, "kafka.flush"), func() error {
		w.b.mu.Lock()
		defer w.b.mu.Unlock()
		_, err := w.txnFor(e)
		return err
	})
	if err != nil {
		return nil, err
	}
	return json.Marshal(kafkaStaged{Slot: slot, Epoch: w.epochs[slot], Gen: e.Generation, N: e.N})
}

func (w *kafkaWriter) Commit(ctx context.Context, _ destination.Epoch, s destination.Staged) error {
	var st kafkaStaged
	if err := json.Unmarshal(s, &st); err != nil {
		return err
	}
	return outcome(w.w.sched.point(ctx, w.attempt, "kafka.commit"), func() error {
		w.b.mu.Lock()
		defer w.b.mu.Unlock()
		if w.b.markers[[2]uint64{st.Gen, st.N}] {
			return nil // committed by an earlier try
		}
		if w.b.epochs[st.Slot] != st.Epoch {
			return fmt.Errorf("%w: producer %d", destination.ErrFenced, st.Slot)
		}
		t := w.b.open[st.Slot]
		if t == nil || t.n != st.N || t.gen != st.Gen {
			return fmt.Errorf("sim: no open transaction for epoch %d-g%d", st.N, st.Gen)
		}
		w.b.visible = append(w.b.visible, t.rows...)
		for _, r := range t.rows {
			w.b.applies[r.ID]++
		}
		w.b.markers[[2]uint64{st.Gen, st.N}] = true
		w.b.open[st.Slot] = nil
		w.b.commitsN++
		return nil
	})
}

func (w *kafkaWriter) Recover(ctx context.Context, _ uint64, e destination.Epoch, _ destination.Staged) (destination.Recovery, error) {
	if e.IsZero() {
		return destination.Applied, nil
	}
	var visible bool
	err := outcome(w.w.sched.point(ctx, w.attempt, "kafka.recover"), func() error {
		w.b.mu.Lock()
		defer w.b.mu.Unlock()
		visible = w.b.markers[[2]uint64{e.Generation, e.N}]
		return nil
	})
	if err != nil {
		return 0, err
	}
	if visible {
		return destination.Applied, nil
	}
	return destination.Lost, nil // Open fenced the producer, which aborted it
}

func (w *kafkaWriter) Close(context.Context) error { return nil }

// ---- Deliver and acknowledge: a webhook receiver.

type receiver struct {
	mu        sync.Mutex
	delivered []streamRow
	applies   map[string]int
}

type webhookDest struct {
	w       *world
	r       *receiver
	attempt int
}

func (d *webhookDest) Capabilities() destination.Capabilities {
	return destination.Capabilities{Class: destination.DeliverAndAck, Scopes: []destination.Scope{destination.ScopeStream}}
}

func (d *webhookDest) Open(context.Context, destination.OpenConfig) (destination.Writer, error) {
	return d, nil
}

func (d *webhookDest) Write(ctx context.Context, _ destination.Epoch, stream string, batch arrow.RecordBatch) error {
	rows := rowsOf(batch)
	return outcome(d.w.sched.point(ctx, d.attempt, "hook.deliver"), func() error {
		d.r.mu.Lock()
		defer d.r.mu.Unlock()
		for _, r := range rows {
			d.r.delivered = append(d.r.delivered, streamRow{stream, r})
			d.r.applies[r.ID]++
		}
		return nil
	})
}

// Write returns once the receiver has acknowledged. PreCommit still yields,
// so runs stay reproducible.
func (d *webhookDest) PreCommit(ctx context.Context, _ destination.Epoch) (destination.Staged, error) {
	if err := outcome(d.w.sched.point(ctx, d.attempt, "hook.precommit"), nop); err != nil {
		return nil, err
	}
	return destination.Staged("{}"), nil
}

func (d *webhookDest) Commit(context.Context, destination.Epoch, destination.Staged) error {
	return nil
}

func (d *webhookDest) Recover(context.Context, uint64, destination.Epoch, destination.Staged) (destination.Recovery, error) {
	return destination.Applied, nil
}

func (d *webhookDest) Close(context.Context) error { return nil }
