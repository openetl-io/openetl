package io.openetl.flink;

import com.google.protobuf.UnsafeByteOperations;
import io.grpc.ManagedChannel;
import io.openetl.bridge.v1.BarrierType;
import io.openetl.bridge.v1.BridgeServiceGrpc;
import io.openetl.bridge.v1.SinkBarrier;
import io.openetl.bridge.v1.SinkHello;
import io.openetl.bridge.v1.WriteRequest;
import io.openetl.bridge.v1.WriteResponse;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.Collections;

/**
 * Writes one output to the runner. At each checkpoint or savepoint it sends what it holds, then
 * the barrier with its type, and its snapshot returns once the runner acknowledges it; a refused
 * barrier fails the snapshot.
 */
final class OpenEtlSink extends AbstractStreamOperator<Void> implements OneInputStreamOperator<RowData, Void> {
    private static final long serialVersionUID = 1L;
    private static final int BATCH_ROWS = 4096;

    private final BridgeConfig bridge;
    private final String output;
    private final RowType type;
    private final boolean changelog;

    private transient ListState<Long> checkpoints;
    private transient long restored;
    private transient ManagedChannel channel;
    private transient BridgeClient.AckedStream<WriteRequest, WriteResponse> call;
    private transient BufferAllocator alloc;
    private transient ArrowRows.Writer writer;

    OpenEtlSink(BridgeConfig bridge, String output, RowType type, boolean changelog) {
        this.bridge = bridge;
        this.output = output;
        this.type = type;
        this.changelog = changelog;
    }

    /** Flink tells an operator without state no restored checkpoint, so the sink keeps its own. */
    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        checkpoints =
                context.getOperatorStateStore()
                        .getUnionListState(new ListStateDescriptor<>("openetl-checkpoint", Types.LONG));
        restored = 0;
        if (context.isRestored()) {
            for (Long c : checkpoints.get()) {
                restored = Math.max(restored, c);
            }
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        checkpoints.update(Collections.singletonList(context.getCheckpointId()));
    }

    @Override
    public void open() throws Exception {
        super.open();
        alloc = Memory.child("openetl sink " + output);
        writer = new ArrowRows.Writer(output, type, changelog, alloc);
        channel = BridgeClient.open(bridge);
        call =
                BridgeClient.AckedStream.open(
                        "output " + output + "'s call to the runner",
                        o -> BridgeServiceGrpc.newStub(channel).write(o),
                        null);
        call.send(
                WriteRequest.newBuilder()
                        .setHello(
                                SinkHello.newBuilder()
                                        .setOutput(output)
                                        .setSubtask(getRuntimeContext().getTaskInfo().getIndexOfThisSubtask())
                                        .setParallelism(getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks())
                                        .setRestoredCheckpoint(restored))
                        .build());
        call.await();
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        if (call.error() != null) {
            throw BridgeClient.failure("output " + output + "'s call to the runner", call.error());
        }
        if (writer.add(element.getValue()) && writer.rows() >= BATCH_ROWS) {
            flush();
        }
    }

    private void flush() throws IOException, InterruptedException {
        if (writer.rows() == 0) {
            return;
        }
        call.send(WriteRequest.newBuilder().setBatch(UnsafeByteOperations.unsafeWrap(writer.finish())).build());
    }

    @Override
    public OperatorSnapshotFutures snapshotState(
            long checkpoint, long timestamp, CheckpointOptions options, CheckpointStreamFactory factory)
            throws Exception {
        flush();
        call.send(
                WriteRequest.newBuilder()
                        .setBarrier(SinkBarrier.newBuilder().setCheckpoint(checkpoint).setType(barrierType(options)))
                        .build());
        long acked = call.await().getAck().getCheckpoint();
        if (acked != checkpoint) {
            throw new IOException(
                    "openetl: the runner acknowledged output " + output + "'s barrier " + acked + ", not " + checkpoint);
        }
        return super.snapshotState(checkpoint, timestamp, options, factory);
    }

    static BarrierType barrierType(CheckpointOptions options) {
        SnapshotType t = options.getCheckpointType();
        if (!t.isSavepoint()) {
            return BarrierType.BARRIER_TYPE_CHECKPOINT;
        }
        return t instanceof SavepointType && ((SavepointType) t).isSynchronous()
                ? BarrierType.BARRIER_TYPE_SYNC_SAVEPOINT
                : BarrierType.BARRIER_TYPE_SAVEPOINT;
    }

    @Override
    public void close() throws Exception {
        try {
            super.close();
        } finally {
            if (call != null) {
                call.close();
            }
            BridgeClient.close(channel);
            if (writer != null) {
                writer.close();
            }
            Memory.close(alloc);
        }
    }
}
