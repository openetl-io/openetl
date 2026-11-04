package io.openetl.flink;

import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.openetl.bridge.v1.BridgeServiceGrpc;
import io.openetl.bridge.v1.ReadRequest;
import io.openetl.bridge.v1.ReadResponse;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.api.connector.source.ExternallyInducedSourceReader;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;

/**
 * Reads its splits' streams from the runner, each on its own thread. A split stops at each
 * barrier the runner sends; once every split is at checkpoint C's barrier, and the enumerator has
 * said C started, the reader induces C, and the splits go on after its snapshot. A barrier of a
 * checkpoint that was aborted, or that a later one has started after, is passed over: its rows
 * belong to the next checkpoint.
 */
final class OpenEtlReader implements ExternallyInducedSourceReader<TaggedRow, StreamSplit> {
    private static final int QUEUED = 4;

    private final OpenEtlSource source;
    private final SourceReaderContext context;
    private final List<Split> splits = new ArrayList<>();
    private final Object lock = new Object();
    private CompletableFuture<Void> available = new CompletableFuture<>();
    private volatile Throwable failure;
    private volatile boolean closing;
    private final ManagedChannel channel;
    private final BufferAllocator alloc;
    private int cursor;
    /** The latest checkpoint the enumerator said started, until the reader takes it. */
    private long event;
    /** Induced and not yet snapshot. */
    private long triggered;
    private long snapshot;
    /** The latest checkpoint Flink said was aborted. */
    private long abandoned;

    OpenEtlReader(OpenEtlSource source, SourceReaderContext context) {
        this.source = source;
        this.context = context;
        // Restored splits are added before start.
        this.channel = BridgeClient.open(source.bridge);
        this.alloc = Memory.child("openetl reader " + context.getIndexOfSubtask());
    }

    @Override
    public void start() {}

    @Override
    public InputStatus pollNext(ReaderOutput<TaggedRow> out) throws Exception {
        Throwable f = failure;
        if (f != null) {
            throw f instanceof IOException ? (IOException) f : new IOException(f.getMessage(), f);
        }
        if (triggered != 0) {
            return InputStatus.NOTHING_AVAILABLE;
        }
        int n = splits.size();
        for (int k = 0; k < n; k++) {
            cursor = (cursor + 1) % n;
            Split s = splits.get(cursor);
            if (s.barrier != 0) {
                continue;
            }
            Item it = s.queue.poll();
            if (it == null) {
                continue;
            }
            if (it.rows != null) {
                for (RowData r : it.rows) {
                    out.collect(new TaggedRow(s.index, r));
                }
            } else if (it.barrier != 0) {
                if (it.barrier > abandoned && it.barrier >= event) {
                    s.barrier = it.barrier;
                }
            } else {
                splits.remove(s);
                s.close();
            }
            return InputStatus.MORE_AVAILABLE;
        }
        return InputStatus.NOTHING_AVAILABLE;
    }

    @Override
    public Optional<Long> shouldTriggerCheckpoint() {
        long c = ready();
        if (c == 0) {
            return Optional.empty();
        }
        triggered = c;
        return Optional.of(c);
    }

    /** The checkpoint to induce now; 0 for none. */
    private long ready() {
        if (triggered != 0 || event == 0) {
            return 0;
        }
        for (Split s : splits) {
            if (s.barrier != event) {
                return 0;
            }
        }
        return event;
    }

    @Override
    public List<StreamSplit> snapshotState(long checkpoint) {
        if (checkpoint != triggered) {
            throw new IllegalStateException(
                    "openetl: checkpoint "
                            + checkpoint
                            + " isn't at the runner's barrier; the reader induced "
                            + (triggered == 0 ? "none" : String.valueOf(triggered)));
        }
        List<StreamSplit> state = new ArrayList<>(splits.size());
        for (Split s : splits) {
            // A split assigned after the reader induced the checkpoint hasn't reached its barrier.
            state.add(s.barrier == checkpoint ? new StreamSplit(s.split.stream, checkpoint) : s.split);
            s.barrier = 0;
        }
        triggered = 0;
        snapshot = checkpoint;
        if (event <= checkpoint) {
            event = 0;
        }
        signal();
        return state;
    }

    @Override
    public void handleSourceEvents(SourceEvent e) {
        if (e instanceof CheckpointEvent) {
            long c = ((CheckpointEvent) e).checkpoint;
            if (c > snapshot && c > event && c > abandoned) {
                event = c;
                // One checkpoint runs at a time, so an older one still held here was aborted.
                pass(c - 1);
                signal();
            }
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpoint) {
        abandoned = Math.max(abandoned, checkpoint);
        if (event <= checkpoint) {
            event = 0;
        }
        pass(checkpoint);
        signal();
    }

    /** Lets the splits held at checkpoints up to c read on. */
    private void pass(long c) {
        if (triggered != 0 && triggered <= c) {
            triggered = 0;
        }
        for (Split s : splits) {
            if (s.barrier != 0 && s.barrier <= c) {
                s.barrier = 0;
            }
        }
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        CompletableFuture<Void> f;
        synchronized (lock) {
            if (available.isDone()) {
                available = new CompletableFuture<>();
            }
            f = available;
        }
        if (hasWork()) {
            f.complete(null);
        }
        return f;
    }

    private boolean hasWork() {
        if (failure != null) {
            return true;
        }
        if (triggered != 0) {
            return false;
        }
        if (ready() != 0) {
            return true;
        }
        for (Split s : splits) {
            if (s.barrier == 0 && !s.queue.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    private void signal() {
        CompletableFuture<Void> f;
        synchronized (lock) {
            f = available;
        }
        f.complete(null);
    }

    @Override
    public void addSplits(List<StreamSplit> added) {
        for (StreamSplit s : added) {
            int i = source.streams.indexOf(s.stream);
            Split split;
            try {
                split = new Split(s, i, i < 0 ? null : ArrowTypes.schema(source.schemas.get(i)));
            } catch (IOException e) {
                failure = e;
                signal();
                return;
            }
            splits.add(split);
            split.start();
        }
        signal();
    }

    @Override
    public void notifyNoMoreSplits() {}

    @Override
    public void close() throws Exception {
        closing = true;
        for (Split s : splits) {
            s.close();
        }
        for (Split s : splits) {
            s.thread.join(5_000);
        }
        BridgeClient.close(channel);
        Memory.close(alloc);
    }

    private void fail(Throwable t) {
        if (closing) {
            return;
        }
        if (failure == null) {
            failure = t;
        }
        signal();
    }

    private static final class Item {
        static final Item GONE = new Item(null, 0);

        final List<RowData> rows;
        final long barrier;

        Item(List<RowData> rows, long barrier) {
            this.rows = rows;
            this.barrier = barrier;
        }
    }

    private final class Split implements Runnable {
        final StreamSplit split;
        final int index;
        final Schema schema;
        final BlockingQueue<Item> queue = new ArrayBlockingQueue<>(QUEUED);
        final Context.CancellableContext call = Context.current().withCancellation();
        final Thread thread;
        /** The checkpoint whose barrier the split stopped at; 0 while it reads. */
        long barrier;

        Split(StreamSplit split, int index, Schema schema) {
            this.split = split;
            this.index = index;
            this.schema = schema;
            this.thread = new Thread(this, "openetl read " + split.stream);
            this.thread.setDaemon(true);
        }

        void start() {
            if (index < 0) {
                // A stream the job wasn't built with: the runner's pipeline no longer has it.
                queue.add(Item.GONE);
                return;
            }
            thread.start();
        }

        @Override
        public void run() {
            try {
                call.call(
                        () -> {
                            Iterator<ReadResponse> it =
                                    BridgeServiceGrpc.newBlockingStub(channel)
                                            .read(
                                                    ReadRequest.newBuilder()
                                                            .setStream(split.stream)
                                                            .setSubtask(context.getIndexOfSubtask())
                                                            .setRestoredCheckpoint(split.checkpoint)
                                                            .build());
                            while (it.hasNext()) {
                                ReadResponse r = it.next();
                                Item item =
                                        r.hasBarrier()
                                                ? new Item(null, r.getBarrier().getCheckpoint())
                                                : new Item(
                                                        ArrowRows.read(
                                                                r.getBatch().newInput(), alloc, schema, split.stream),
                                                        0);
                                if (item.rows == null && item.barrier == 0) {
                                    throw new IOException("openetl: the runner sent a barrier of checkpoint 0");
                                }
                                queue.put(item);
                                signal();
                            }
                            throw new IOException("openetl: the runner ended stream " + split.stream + "'s read");
                        });
            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                    try {
                        queue.put(Item.GONE);
                        signal();
                    } catch (InterruptedException ignored) {
                        // Closing.
                    }
                } else {
                    fail(BridgeClient.failure("reading stream " + split.stream, e));
                }
            } catch (InterruptedException e) {
                // Closing.
            } catch (Exception e) {
                fail(e);
            }
        }

        void close() {
            call.cancel(null);
            thread.interrupt();
        }
    }
}
