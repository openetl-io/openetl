package conformance

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/internal/rpcerr"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// writer is one Write stream, driven one message at a time.
type writer struct {
	stream grpc.BidiStreamingClient[protocolv1.WriteRequest, protocolv1.WriteResponse]
	cancel context.CancelFunc
	opened *protocolv1.WriteOpened
	// Its lane, of the attempt's lanes.
	lane, lanes int
}

// openWriter's stream lives until close or crash, not just ctx.
func openWriter(ctx context.Context, conn grpc.ClientConnInterface, open *protocolv1.WriteOpen) (*writer, error) {
	sctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stream, err := protocolv1.NewDestinationServiceClient(conn).Write(sctx)
	if err != nil {
		cancel()
		return nil, rpcerr.FromStatus(err)
	}
	w := &writer{stream: stream, cancel: cancel}
	resp, err := w.call(&protocolv1.WriteRequest{Msg: &protocolv1.WriteRequest_Open{Open: open}})
	if err != nil {
		cancel()
		return nil, err
	}
	if w.opened = resp.GetOpened(); w.opened == nil {
		cancel()
		return nil, fmt.Errorf("open was answered with %T, not opened", resp.GetMsg())
	}
	return w, nil
}

func (w *writer) send(req *protocolv1.WriteRequest) error {
	if err := w.stream.Send(req); err != nil {
		// The stream ended; Recv has why.
		if _, rerr := w.stream.Recv(); rerr != nil && !errors.Is(rerr, io.EOF) {
			return rpcerr.FromStatus(rerr)
		}
		return rpcerr.FromStatus(err)
	}
	return nil
}

func (w *writer) call(req *protocolv1.WriteRequest) (*protocolv1.WriteResponse, error) {
	if err := w.send(req); err != nil {
		return nil, err
	}
	resp, err := w.stream.Recv()
	if errors.Is(err, io.EOF) {
		return nil, errors.New("the destination ended the write stream")
	}
	if err != nil {
		return nil, rpcerr.FromStatus(err)
	}
	return resp, nil
}

// lane0 is the committables of epoch e, staged on one writer lane; none for
// a zero e.
func lane0(e *protocolv1.Epoch, staged []byte) [][]byte {
	if e.GetN() == 0 {
		return nil
	}
	return [][]byte{staged}
}

func (w *writer) recover(e *protocolv1.Epoch, staged []byte) (protocolv1.Recovery, error) {
	return w.recoverLanes(e, lane0(e, staged))
}

func (w *writer) recoverLanes(e *protocolv1.Epoch, staged [][]byte) (protocolv1.Recovery, error) {
	resp, err := w.call(&protocolv1.WriteRequest{Msg: &protocolv1.WriteRequest_Recover{Recover: &protocolv1.WriteRecover{Epoch: e, Staged: staged}}})
	if err != nil {
		return 0, err
	}
	if resp.GetRecovered() == nil {
		return 0, fmt.Errorf("recover was answered with %T, not recovered", resp.GetMsg())
	}
	return resp.GetRecovered().GetRecovery(), nil
}

func (w *writer) startReads(streams []string, at time.Time) error {
	starts := map[string]*timestamppb.Timestamp{}
	for _, s := range streams {
		starts[s] = timestamppb.New(at)
	}
	return w.send(&protocolv1.WriteRequest{Msg: &protocolv1.WriteRequest_StartReads{StartReads: &protocolv1.WriteStartReads{Starts: starts}}})
}

func (w *writer) batch(e *protocolv1.Epoch, stream string, rec arrow.RecordBatch) error {
	b, err := arrowipc.EncodeBatch(rec)
	if err != nil {
		return err
	}
	return w.send(&protocolv1.WriteRequest{Msg: &protocolv1.WriteRequest_Batch{Batch: &protocolv1.WriteBatch{Epoch: e, Stream: stream, Arrow: b}}})
}

func (w *writer) stagedFiles(e *protocolv1.Epoch, stream string, files []*protocolv1.StagedFile) error {
	return w.send(&protocolv1.WriteRequest{Msg: &protocolv1.WriteRequest_StagedFiles{StagedFiles: &protocolv1.WriteStagedFiles{Epoch: e, Stream: stream, Files: files}}})
}

func (w *writer) endRead(e *protocolv1.Epoch, stream string) error {
	return w.send(&protocolv1.WriteRequest{Msg: &protocolv1.WriteRequest_EndRead{EndRead: &protocolv1.WriteEndRead{Epoch: e, Stream: stream}}})
}

func (w *writer) preCommit(e *protocolv1.Epoch) ([]byte, error) {
	resp, err := w.call(&protocolv1.WriteRequest{Msg: &protocolv1.WriteRequest_PreCommit{PreCommit: &protocolv1.WritePreCommit{Epoch: e}}})
	if err != nil {
		return nil, err
	}
	st := resp.GetStaged()
	if st == nil {
		return nil, fmt.Errorf("pre-commit was answered with %T, not staged", resp.GetMsg())
	}
	if st.GetEpoch().GetN() != e.GetN() || st.GetEpoch().GetGeneration() != e.GetGeneration() {
		return nil, fmt.Errorf("pre-commit %s was answered for epoch %s", epochString(e), epochString(st.GetEpoch()))
	}
	return st.GetStaged(), nil
}

func (w *writer) applySchema(schemas []*protocolv1.StreamSchema) error {
	_, err := w.schemaApplied(schemas)
	return err
}

func (w *writer) schemaApplied(schemas []*protocolv1.StreamSchema) (*protocolv1.WriteSchemaApplied, error) {
	resp, err := w.call(&protocolv1.WriteRequest{Msg: &protocolv1.WriteRequest_ApplySchema{ApplySchema: &protocolv1.WriteApplySchema{Streams: schemas}}})
	if err != nil {
		return nil, err
	}
	a := resp.GetSchemaApplied()
	if a == nil {
		return nil, fmt.Errorf("apply_schema was answered with %T, not schema_applied", resp.GetMsg())
	}
	if a.GetError() != nil {
		return nil, rpcerr.FromProto(a.GetError())
	}
	return a, nil
}

func (w *writer) commit(e *protocolv1.Epoch, staged []byte) error {
	return w.commitLanes(e, lane0(e, staged))
}

func (w *writer) commitLanes(e *protocolv1.Epoch, staged [][]byte) error {
	resp, err := w.call(&protocolv1.WriteRequest{Msg: &protocolv1.WriteRequest_Commit{Commit: &protocolv1.WriteCommit{Epoch: e, Staged: staged}}})
	if err != nil {
		return err
	}
	c := resp.GetCommitted()
	if c == nil {
		return fmt.Errorf("commit was answered with %T, not committed", resp.GetMsg())
	}
	if c.GetError() != nil {
		return rpcerr.FromProto(c.GetError())
	}
	return nil
}

// close ends the stream as the platform does at the end of an attempt.
func (w *writer) close() error {
	defer w.cancel()
	if err := w.stream.CloseSend(); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() {
		for {
			if _, err := w.stream.Recv(); err != nil {
				if errors.Is(err, io.EOF) {
					err = nil
				}
				done <- err
				return
			}
		}
	}()
	select {
	case err := <-done:
		return rpcerr.FromStatus(err)
	case <-time.After(time.Minute):
		return errors.New("the destination didn't end the write stream within a minute of its close")
	}
}

// crash drops the stream, as a runner that dies does.
func (w *writer) crash() { w.cancel() }

func epochString(e *protocolv1.Epoch) string {
	return fmt.Sprintf("(%d, %d)", e.GetGeneration(), e.GetN())
}

// The kit's streams hold an id and a value, with the engine's metadata
// columns.
var dataFields = []arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	{Name: "v", Type: arrow.BinaryTypes.String, Nullable: true},
}

// A column schemas add.
var wField = arrow.Field{Name: "w", Type: arrow.BinaryTypes.String, Nullable: true}

func metadataFields() []arrow.Field {
	ts := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	return []arrow.Field{
		{Name: "_openetl_id", Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}, Metadata: arrow.NewMetadata(
			[]string{"ARROW:extension:name", "ARROW:extension:metadata"}, []string{"arrow.uuid", ""})},
		{Name: "_openetl_op", Type: arrow.BinaryTypes.String},
		{Name: "_openetl_seq", Type: ts},
		{Name: "_openetl_synced_at", Type: ts},
	}
}

// streamSchema is a stream's destination schema, as the engine shapes it.
func streamSchema(stream string, data []arrow.Field) *arrow.Schema {
	md := arrow.NewMetadata([]string{"openetl.table"}, []string{stream})
	return arrow.NewSchema(append(append([]arrow.Field{}, data...), metadataFields()...), &md)
}

// record is one row of a kit stream.
type record struct {
	id int64
	v  string
}

func records(from, to int64, tag string) []record {
	out := make([]record, 0, to-from)
	for i := from; i < to; i++ {
		out = append(out, record{id: i, v: tag + strconv.FormatInt(i, 10)})
	}
	return out
}

func buildBatch(stream string, recs []record, op string) arrow.RecordBatch {
	return build(stream, recs, op, false)
}

// buildWideBatch's records also have the column w.
func buildWideBatch(stream string, recs []record) arrow.RecordBatch {
	return build(stream, recs, "insert", true)
}

func build(stream string, recs []record, op string, wide bool) arrow.RecordBatch {
	data := dataFields
	if wide {
		data = append(append([]arrow.Field{}, dataFields...), wField)
	}
	return buildData(stream, data, recs, op)
}

// buildData's records have the columns data, of the kit's data columns, w
// and n: w is "w" and the id, and n the id.
func buildData(stream string, data []arrow.Field, recs []record, op string) arrow.RecordBatch {
	b := array.NewRecordBuilder(memory.DefaultAllocator, streamSchema(stream, data))
	defer b.Release()
	now := arrow.Timestamp(time.Now().UnixMicro())
	m := len(data)
	for _, r := range recs {
		b.Field(0).(*array.Int64Builder).Append(r.id)
		b.Field(1).(*array.StringBuilder).Append(r.v)
		for i := 2; i < m; i++ {
			switch c := b.Field(i).(type) {
			case *array.StringBuilder:
				c.Append("w" + strconv.FormatInt(r.id, 10))
			case *array.Int32Builder:
				c.Append(int32(r.id & math.MaxInt32))
			case *array.Int64Builder:
				c.Append(r.id)
			case *array.Float64Builder:
				c.Append(float64(r.id))
			}
		}
		b.Field(m).(*array.FixedSizeBinaryBuilder).Append(rowID(stream, r))
		b.Field(m + 1).(*array.StringBuilder).Append(op)
		b.Field(m + 2).(*array.TimestampBuilder).Append(now)
		b.Field(m + 3).(*array.TimestampBuilder).Append(now)
	}
	return b.NewRecordBatch()
}

// rowID is the same for the same version of a record, as the engine's is.
func rowID(stream string, r record) []byte {
	h := sha256.Sum256(fmt.Appendf(nil, "%s\x00%d\x00%s", stream, r.id, r.v))
	return h[:16]
}

func randomName() string {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	b := make([]byte, 10)
	_, _ = rand.Read(b)
	for i := range b {
		b[i] = letters[int(b[i])%len(letters)]
	}
	return "conformance_" + string(b)
}
