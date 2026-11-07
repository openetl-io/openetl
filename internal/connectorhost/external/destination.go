package external

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/openetl-io/openetl/internal/connectorhost/hostservices"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

const closeTimeout = 30 * time.Second

// Destination is an external destination.
type Destination struct{ instance }

var (
	_ destination.Destination  = (*Destination)(nil)
	_ destination.Checker      = (*Destination)(nil)
	_ destination.ReadTracker  = (*writer)(nil)
	_ destination.StagedWriter = (*writer)(nil)
)

// NewDestination binds the destination at exe to config, a JSON object. It
// does no I/O.
func NewDestination(exe string, config []byte, opts Options) *Destination {
	return &Destination{instance{exe: exe, kind: protocolv1.ConnectorKind_CONNECTOR_KIND_DESTINATION, config: config, opts: opts}}
}

func (d *Destination) Check(ctx context.Context) error { return d.check(ctx) }

// SetStaging serves st's files and cursors to the destination's host
// services, now and in any process started later.
func (d *Destination) SetStaging(st hostservices.Staging) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.opts.Staging = st
	if d.p != nil {
		if hs := d.p.hs.Load(); hs != nil {
			hs.SetStaging(st)
		}
	}
}

// Capabilities come from the spec: the descriptor's, or else the running
// connector's, so they're zero until Start.
func (d *Destination) Capabilities() destination.Capabilities {
	spec := d.opts.Spec
	if spec == nil {
		d.mu.Lock()
		if d.p != nil {
			spec = d.p.spec
		}
		d.mu.Unlock()
	}
	return Capabilities(spec)
}

// Capabilities are what a destination's spec declares; zero for nil.
func Capabilities(spec *protocolv1.SpecResponse) destination.Capabilities {
	if spec == nil {
		return destination.Capabilities{}
	}
	pc := spec.GetDestination()
	c := destination.Capabilities{
		Class:                 destination.Class(pc.GetSinkClass()),
		DedupeAtApply:         pc.GetDedupeAtApply(),
		DefaultCommitInterval: pc.GetDefaultCommitInterval().AsDuration(),
		MinCommitInterval:     pc.GetMinCommitInterval().AsDuration(),
		MaxCommitInterval:     pc.GetMaxCommitInterval().AsDuration(),
		MaxIdentifierLength:   int(pc.GetMaxIdentifierLength()),
		NanosecondTimestamps:  pc.GetNanosecondTimestamps(),
		Delivery:              destination.Delivery(pc.GetDelivery()),
		WriterLanes:           int(pc.GetMaxWriterLanes()),
	}
	if h := pc.GetHolds(); h != nil {
		c.Holds = destination.Holds{ByKey: h.GetByKey(), ByID: h.GetById(), Last: h.GetLast(), Guarded: h.GetGuarded()}
	}
	for _, s := range pc.GetScopes() {
		c.Scopes = append(c.Scopes, destination.Scope(s))
	}
	for _, m := range pc.GetWriteModes() {
		c.WriteModes = append(c.WriteModes, destination.WriteMode(m))
	}
	return c
}

func (d *Destination) Open(ctx context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	p, err := d.proc(ctx)
	if err != nil {
		return nil, err
	}
	open := &protocolv1.WriteOpen{
		Config:     p.config,
		Pipeline:   cfg.Pipeline,
		Namespace:  cfg.Namespace,
		Generation: cfg.Generation,
		Scope:      protocolv1.CommitScope(cfg.Scope), //nolint:gosec // small
		Lane:       uint32(cfg.Lane),                  //nolint:gosec // small
	}
	if cfg.CommitInterval > 0 {
		open.CommitInterval = durationpb.New(cfg.CommitInterval)
	}
	for _, s := range cfg.Streams {
		open.Streams = append(open.Streams, &protocolv1.WriteStream{
			Name: s.Name, Mode: protocolv1.WriteMode(s.Mode), PrimaryKey: s.PrimaryKey, //nolint:gosec // small
			Dedupe: protocolv1.Dedupe(s.Dedupe), Deletes: protocolv1.DeleteMode(s.Deletes), //nolint:gosec // small
		})
	}
	// The stream lives until Close, or until ctx is done.
	sctx, cancel := context.WithCancel(ctx)
	stream, err := protocolv1.NewDestinationServiceClient(p.conn).Write(sctx)
	if err != nil {
		cancel()
		return nil, p.failure(errs.FromStatus(err))
	}
	w := &writer{
		p:         p,
		stream:    stream,
		cancel:    cancel,
		opened:    make(chan *protocolv1.WriteOpened, 1),
		recovered: make(chan *protocolv1.WriteRecovered, 1),
		staged:    make(chan *protocolv1.WriteStaged, 1),
		committed: make(chan *protocolv1.WriteCommitted, 1),
		applied:   make(chan *protocolv1.WriteSchemaApplied, 1),
		done:      make(chan struct{}),
	}
	go w.receive()
	opened, err := call(ctx, w, &protocolv1.WriteRequest{Msg: &protocolv1.WriteRequest_Open{Open: open}}, w.opened)
	if err != nil {
		w.abort()
		return nil, err
	}
	if opened.GetAppliesSchema() {
		return &schemaWriter{w}, nil
	}
	return w, nil
}

// writer is one Write stream. Each kind of answer has one caller waiting at
// most: Recover and PreCommit run on one goroutine, ApplySchema and Commit
// on another.
type writer struct {
	p      *process
	stream grpc.BidiStreamingClient[protocolv1.WriteRequest, protocolv1.WriteResponse]
	cancel context.CancelFunc
	sendMu sync.Mutex

	opened    chan *protocolv1.WriteOpened
	recovered chan *protocolv1.WriteRecovered
	staged    chan *protocolv1.WriteStaged
	committed chan *protocolv1.WriteCommitted
	applied   chan *protocolv1.WriteSchemaApplied
	// Closed when the stream ends, with err set: nil if the connector ended
	// it cleanly.
	done chan struct{}
	err  error
}

func (w *writer) receive() {
	defer close(w.done)
	for {
		m, err := w.stream.Recv()
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			w.err = w.p.failure(errs.FromStatus(err))
			return
		}
		switch x := m.GetMsg().(type) {
		case *protocolv1.WriteResponse_Opened:
			w.opened <- x.Opened
		case *protocolv1.WriteResponse_Recovered:
			w.recovered <- x.Recovered
		case *protocolv1.WriteResponse_Staged:
			w.staged <- x.Staged
		case *protocolv1.WriteResponse_Committed:
			w.committed <- x.Committed
		case *protocolv1.WriteResponse_SchemaApplied:
			w.applied <- x.SchemaApplied
		default:
			w.err = errs.New(errs.Internal, "connector: write: unexpected %T", x)
			w.cancel()
			return
		}
	}
}

func (w *writer) send(m *protocolv1.WriteRequest) error {
	w.sendMu.Lock()
	err := w.stream.Send(m)
	w.sendMu.Unlock()
	if err != nil {
		// Send reports io.EOF when the stream ended; Recv has the reason.
		<-w.done
		return w.ended()
	}
	return nil
}

func (w *writer) ended() error {
	if w.err != nil {
		return w.err
	}
	return errs.New(errs.Internal, "connector: write: the destination ended the stream")
}

func call[T any](ctx context.Context, w *writer, req *protocolv1.WriteRequest, answer chan T) (T, error) {
	var zero T
	if err := w.send(req); err != nil {
		return zero, err
	}
	select {
	case a := <-answer:
		return a, nil
	case <-w.done:
		select {
		case a := <-answer:
			return a, nil
		default:
			return zero, w.ended()
		}
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}

func epochProto(e destination.Epoch) *protocolv1.Epoch {
	return &protocolv1.Epoch{Generation: e.Generation, N: e.N}
}

func committables(staged []destination.Staged) [][]byte {
	out := make([][]byte, len(staged))
	for i, s := range staged {
		out[i] = s
	}
	return out
}

func (w *writer) Recover(ctx context.Context, _ uint64, e destination.Epoch, staged []destination.Staged) (destination.Recovery, error) {
	r, err := call(ctx, w, &protocolv1.WriteRequest{Msg: &protocolv1.WriteRequest_Recover{Recover: &protocolv1.WriteRecover{Epoch: epochProto(e), Staged: committables(staged)}}}, w.recovered)
	if err != nil {
		return 0, fenced(err)
	}
	return destination.Recovery(r.GetRecovery()), nil
}

func (w *writer) Write(_ context.Context, e destination.Epoch, stream string, batch arrow.RecordBatch) error {
	b, err := arrowipc.EncodeBatch(batch)
	if err != nil {
		return errs.Wrap(errs.Internal, err, "connector: encode a batch for "+stream)
	}
	return w.send(&protocolv1.WriteRequest{Msg: &protocolv1.WriteRequest_Batch{Batch: &protocolv1.WriteBatch{Epoch: epochProto(e), Stream: stream, Arrow: b}}})
}

// Patch is for destinations whose capabilities take holds.
func (w *writer) Patch(_ context.Context, e destination.Epoch, stream string, batch arrow.RecordBatch) error {
	b, err := arrowipc.EncodeBatch(batch)
	if err != nil {
		return errs.Wrap(errs.Internal, err, "connector: encode patches for "+stream)
	}
	return w.send(&protocolv1.WriteRequest{Msg: &protocolv1.WriteRequest_Batch{Batch: &protocolv1.WriteBatch{Epoch: epochProto(e), Stream: stream, Arrow: b, Patch: true}}})
}

// StartReads and EndRead go unanswered, like batches; a connector without a
// ReadTracker ignores them.
func (w *writer) StartReads(_ context.Context, starts map[string]time.Time) error {
	m := &protocolv1.WriteStartReads{Starts: make(map[string]*timestamppb.Timestamp, len(starts))}
	for name, t := range starts {
		m.Starts[name] = timestamppb.New(t)
	}
	return w.send(&protocolv1.WriteRequest{Msg: &protocolv1.WriteRequest_StartReads{StartReads: m}})
}

func (w *writer) EndRead(_ context.Context, e destination.Epoch, stream string) error {
	return w.send(&protocolv1.WriteRequest{Msg: &protocolv1.WriteRequest_EndRead{EndRead: &protocolv1.WriteEndRead{Epoch: epochProto(e), Stream: stream}}})
}

// WriteStaged goes unanswered, like a batch.
func (w *writer) WriteStaged(_ context.Context, e destination.Epoch, stream string, files []destination.StagedFile) error {
	m := &protocolv1.WriteStagedFiles{Epoch: epochProto(e), Stream: stream, Files: make([]*protocolv1.StagedFile, len(files))}
	for i, f := range files {
		m.Files[i] = &protocolv1.StagedFile{Path: f.Path, Rows: f.Rows, Bytes: f.Bytes}
	}
	return w.send(&protocolv1.WriteRequest{Msg: &protocolv1.WriteRequest_StagedFiles{StagedFiles: m}})
}

func (w *writer) PreCommit(ctx context.Context, e destination.Epoch) (destination.Staged, error) {
	st, err := call(ctx, w, &protocolv1.WriteRequest{Msg: &protocolv1.WriteRequest_PreCommit{PreCommit: &protocolv1.WritePreCommit{Epoch: epochProto(e)}}}, w.staged)
	if err != nil {
		return nil, err
	}
	return st.GetStaged(), nil
}

func (w *writer) Commit(ctx context.Context, e destination.Epoch, staged []destination.Staged) (destination.Committed, error) {
	c, err := call(ctx, w, &protocolv1.WriteRequest{Msg: &protocolv1.WriteRequest_Commit{Commit: &protocolv1.WriteCommit{Epoch: epochProto(e), Staged: committables(staged)}}}, w.committed)
	if err != nil {
		return destination.Committed{}, err
	}
	if err := fenced(errs.FromProto(c.GetError())); err != nil {
		return destination.Committed{}, err
	}
	return destination.Committed{Collapsed: c.GetCollapsedRows()}, nil
}

// Close ends the stream and waits for the destination to close its writer.
func (w *writer) Close(ctx context.Context) error {
	w.sendMu.Lock()
	err := w.stream.CloseSend()
	w.sendMu.Unlock()
	defer w.cancel()
	if err != nil {
		return nil
	}
	t := time.NewTimer(closeTimeout)
	defer t.Stop()
	select {
	case <-w.done:
		return w.err
	case <-t.C:
		return errs.New(errs.Internal, "connector: write: the destination didn't close within %v", closeTimeout)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *writer) abort() {
	w.cancel()
	<-w.done
}

// fenced makes a FENCED error match destination.ErrFenced.
func fenced(err error) error {
	if err != nil && errs.CodeOf(err) == errs.Fenced {
		return fmt.Errorf("%w: %w", destination.ErrFenced, err)
	}
	return err
}

// schemaWriter is a writer whose destination has a schema.
type schemaWriter struct{ *writer }

var _ destination.SchemaApplier = (*schemaWriter)(nil)

func (w *schemaWriter) ApplySchema(ctx context.Context, streams []destination.StreamSchema) (destination.SchemaApplied, error) {
	m := &protocolv1.WriteApplySchema{}
	for _, s := range streams {
		b, err := arrowipc.EncodeSchema(s.Schema)
		if err != nil {
			return destination.SchemaApplied{}, errs.Wrap(errs.Internal, err, "connector: encode the schema of "+s.Name)
		}
		m.Streams = append(m.Streams, &protocolv1.StreamSchema{Name: s.Name, ArrowSchema: b})
	}
	a, err := call(ctx, w.writer, &protocolv1.WriteRequest{Msg: &protocolv1.WriteRequest_ApplySchema{ApplySchema: m}}, w.applied)
	if err != nil {
		return destination.SchemaApplied{}, err
	}
	if err := errs.FromProto(a.GetError()); err != nil {
		return destination.SchemaApplied{}, err
	}
	var out destination.SchemaApplied
	for _, r := range a.GetRefused() {
		out.Refused = append(out.Refused, destination.RefusedColumn{
			Stream: r.GetStream(), Column: r.GetColumn(), Reason: r.GetReason(), FromType: r.GetFromType(), ToType: r.GetToType(), Fix: r.GetFix(),
		})
	}
	if len(a.GetLimits()) > 0 {
		out.Limits = make(map[string]destination.TableLimits, len(a.GetLimits()))
		for name, l := range a.GetLimits() {
			out.Limits[name] = destination.TableLimits{NotNull: l.GetNotNull(), Unholdable: l.GetUnholdable(), Fixed: l.GetFixed(), Columns: int(l.GetColumns()), MaxColumns: int(l.GetMaxColumns())}
		}
	}
	return out, nil
}
