package io.openetl.flink;

import io.grpc.ManagedChannel;
import io.openetl.bridge.v1.BridgeServiceGrpc;
import io.openetl.bridge.v1.CheckpointOutcome;
import io.openetl.bridge.v1.CheckpointStarted;
import io.openetl.bridge.v1.CoordinateRequest;
import io.openetl.bridge.v1.CoordinateResponse;
import io.openetl.bridge.v1.EnumeratorHello;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Assigns the streams round-robin by name, and tells the runner each checkpoint's start and
 * outcome, returning from each callback once the runner has it. The runner fails the job by
 * ending the call, as it does after an abort unless it's stopping the job, when Flink aborts the
 * checkpoint in flight. A subtask of the source restarting fails the job too.
 */
final class OpenEtlEnumerator implements SplitEnumerator<StreamSplit, EnumeratorState> {
    private final SplitEnumeratorContext<StreamSplit> context;
    private final BridgeConfig bridge;
    private final List<String> streams;
    private final Set<String> assigned;
    private final long restored;
    /** Started and not decided; 0 for none. */
    private long pending;

    private ManagedChannel channel;
    private BridgeClient.AckedStream<CoordinateRequest, CoordinateResponse> call;
    private volatile boolean closed;
    private boolean resetting;

    OpenEtlEnumerator(
            SplitEnumeratorContext<StreamSplit> context,
            BridgeConfig bridge,
            List<String> streams,
            EnumeratorState state) {
        this.context = context;
        this.bridge = bridge;
        this.streams = streams;
        this.assigned = new TreeSet<>(state.assigned);
        this.restored = state.checkpoint;
    }

    @Override
    public void start() {
        channel = BridgeClient.open(bridge);
        call =
                BridgeClient.AckedStream.open(
                        "the enumerator's call to the runner",
                        o -> BridgeServiceGrpc.newStub(channel).coordinate(o),
                        this::ended);
        String job = context.metricGroup().getAllVariables().getOrDefault("<job_id>", "");
        try {
            call.send(
                    CoordinateRequest.newBuilder()
                            .setHello(EnumeratorHello.newBuilder().setJobId(job).setRestoredCheckpoint(restored))
                            .build());
            call.await();
        } catch (IOException e) {
            throw new FlinkRuntimeException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlinkRuntimeException("openetl: interrupted saying hello to the runner", e);
        }
    }

    private void ended(Throwable t) {
        if (closed) {
            return;
        }
        IOException e = BridgeClient.failure("the enumerator's call to the runner ended", t);
        context.runInCoordinatorThread(
                () -> {
                    throw new FlinkRuntimeException(e);
                });
    }

    @Override
    public void handleSplitRequest(int subtask, String host) {}

    /**
     * Flink calls this when it restarts subtasks from a checkpoint without restarting the
     * enumerator. The job then fails as a whole, so every restart begins with a new enumerator,
     * whose hello tells the runner the checkpoint it restored.
     */
    @Override
    public void addSplitsBack(List<StreamSplit> splits, int subtask) {
        for (StreamSplit s : splits) {
            assigned.remove(s.stream);
        }
        if (!resetting) {
            resetting = true;
            context.runInCoordinatorThread(
                    () -> {
                        throw new FlinkRuntimeException(
                                "openetl: subtask " + subtask + " of the source restarted, so the job restarts as a whole");
                    });
        }
    }

    @Override
    public void addReader(int subtask) {
        int n = context.currentParallelism();
        List<StreamSplit> mine = new ArrayList<>();
        for (int i = 0; i < streams.size(); i++) {
            String s = streams.get(i);
            if (i % n == subtask && !assigned.contains(s)) {
                mine.add(new StreamSplit(s, restored));
            }
        }
        for (StreamSplit s : mine) {
            context.assignSplit(s, subtask);
            assigned.add(s.stream);
        }
        if (pending != 0) {
            context.sendEventToSourceReader(subtask, new CheckpointEvent(pending));
        }
    }

    @Override
    public EnumeratorState snapshotState(long checkpoint) throws Exception {
        call.send(
                CoordinateRequest.newBuilder()
                        .setStarted(CheckpointStarted.newBuilder().setCheckpoint(checkpoint))
                        .build());
        expect(checkpoint, call.await());
        pending = checkpoint;
        for (int r : context.registeredReaders().keySet()) {
            context.sendEventToSourceReader(r, new CheckpointEvent(checkpoint));
        }
        return new EnumeratorState(checkpoint, assigned);
    }

    @Override
    public void notifyCheckpointComplete(long checkpoint) throws Exception {
        outcome(checkpoint, true);
    }

    @Override
    public void notifyCheckpointAborted(long checkpoint) throws Exception {
        outcome(checkpoint, false);
    }

    private void outcome(long checkpoint, boolean completed) throws IOException, InterruptedException {
        call.send(
                CoordinateRequest.newBuilder()
                        .setOutcome(CheckpointOutcome.newBuilder().setCheckpoint(checkpoint).setCompleted(completed))
                        .build());
        expect(checkpoint, call.await());
        if (pending == checkpoint || !completed) {
            pending = 0;
        }
    }

    private static void expect(long checkpoint, CoordinateResponse r) throws IOException {
        if (r.getAck().getCheckpoint() != checkpoint) {
            throw new IOException(
                    "openetl: the runner acknowledged checkpoint " + r.getAck().getCheckpoint() + ", not " + checkpoint);
        }
    }

    @Override
    public void close() {
        closed = true;
        if (call != null) {
            call.close();
        }
        BridgeClient.close(channel);
    }
}
