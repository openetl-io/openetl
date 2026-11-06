package io.openetl.flink;

import com.google.protobuf.ByteString;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.openetl.bridge.v1.Ack;
import io.openetl.bridge.v1.Barrier;
import io.openetl.bridge.v1.BarrierType;
import io.openetl.bridge.v1.BridgeServiceGrpc;
import io.openetl.bridge.v1.CoordinateRequest;
import io.openetl.bridge.v1.CoordinateResponse;
import io.openetl.bridge.v1.DescribeRequest;
import io.openetl.bridge.v1.DescribeResponse;
import io.openetl.bridge.v1.ReadRequest;
import io.openetl.bridge.v1.ReadResponse;
import io.openetl.bridge.v1.WriteRequest;
import io.openetl.bridge.v1.WriteResponse;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.function.BooleanSupplier;

/**
 * A runner's bridge for tests. Each stream is a log of batches and barriers: rows are generated
 * as readers ask, row k of a stream has id k, and started(C) puts C's barrier at the end of every
 * stream's log. Outputs are expected to be the streams of the same names, passed through.
 */
final class FakeRunner extends BridgeServiceGrpc.BridgeServiceImplBase implements AutoCloseable {
    static final String TOKEN = "t0k3n";
    static final int BATCH = 20;

    final Map<String, Log> streams = new TreeMap<>();
    final Map<String, Boolean> outputs = new TreeMap<>();
    final BufferAllocator alloc = new RootAllocator();
    final List<Long> hellos = new CopyOnWriteArrayList<>();
    final List<Long> started = new CopyOnWriteArrayList<>();
    final List<Long> completed = new CopyOnWriteArrayList<>();
    final List<Long> aborted = new CopyOnWriteArrayList<>();
    final List<String> reads = new CopyOnWriteArrayList<>();
    final List<String> sinkHellos = new CopyOnWriteArrayList<>();
    final List<Metadata> headers = new CopyOnWriteArrayList<>();
    final Map<Long, BarrierType> types = new ConcurrentHashMap<>();
    /** By checkpoint and output: the ids the sinks sent before the barrier, since the one before. */
    final Map<Long, Map<String, Set<Long>>> epochs = new ConcurrentHashMap<>();
    /** By checkpoint: how many sink subtasks sent its barrier. */
    final Map<Long, Integer> barriers = new ConcurrentHashMap<>();
    /** By output: each id's op as written. */
    final Map<String, Map<Long, String>> ops = new ConcurrentHashMap<>();

    /** Streams described without a schema, as a runner that hasn't seen their rows does. */
    final Set<String> unknown = ConcurrentHashMap.newKeySet();

    /** Barriers of this checkpoint and later reach readers only once releaseBarriers opens. */
    volatile long holdBarriersFrom;
    volatile CountDownLatch releaseBarriers = new CountDownLatch(0);
    volatile long refuse;
    volatile long holdFrom;
    volatile CountDownLatch release = new CountDownLatch(0);
    volatile String failRead;
    volatile boolean failCoordinate;
    /** As a runner that isn't stopping the job does. */
    volatile boolean failOnAbort = true;
    volatile long maxRows = Long.MAX_VALUE;

    private final Server server;
    private final String fingerprint;
    private final List<StreamObserver<?>> coordinators = new CopyOnWriteArrayList<>();

    FakeRunner(List<String> names, Set<String> changelogs) throws Exception {
        for (String n : names) {
            streams.put(n, new Log(n, changelogs.contains(n)));
            outputs.put(n, changelogs.contains(n));
        }
        KeyPairGenerator g = KeyPairGenerator.getInstance("EC");
        g.initialize(256);
        KeyPair kp = g.generateKeyPair();
        X500Name name = new X500Name("CN=openetl-test");
        long now = System.currentTimeMillis();
        X509Certificate cert =
                new JcaX509CertificateConverter()
                        .getCertificate(
                                new JcaX509v3CertificateBuilder(
                                                name,
                                                BigInteger.ONE,
                                                new Date(now - 3_600_000L),
                                                new Date(now + 86_400_000L),
                                                name,
                                                kp.getPublic())
                                        .build(new JcaContentSignerBuilder("SHA256withECDSA").build(kp.getPrivate())));
        fingerprint = BridgeClient.PinnedTrustManager.fingerprint(cert);
        ServerInterceptor auth =
                new ServerInterceptor() {
                    @Override
                    public <Q, R> ServerCall.Listener<Q> interceptCall(
                            ServerCall<Q, R> call, Metadata h, ServerCallHandler<Q, R> next) {
                        headers.add(h);
                        if (!("Bearer " + TOKEN).equals(h.get(BridgeClient.AUTHORIZATION))
                                || !"v1".equals(h.get(BridgeClient.PROTOCOL_HEADER))) {
                            call.close(Status.UNAUTHENTICATED, new Metadata());
                            return new ServerCall.Listener<Q>() {};
                        }
                        return next.startCall(call, h);
                    }
                };
        server =
                NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
                        .sslContext(
                                GrpcSslContexts.configure(SslContextBuilder.forServer(kp.getPrivate(), cert), SslProvider.JDK)
                                        .build())
                        .maxInboundMessageSize(BridgeClient.MAX_MESSAGE)
                        .addService(ServerInterceptors.intercept(this, auth))
                        .build()
                        .start();
    }

    BridgeConfig config() {
        return new BridgeConfig("127.0.0.1:" + server.getPort(), fingerprint, TOKEN);
    }

    BridgeConfig config(String fingerprint) {
        return new BridgeConfig("127.0.0.1:" + server.getPort(), fingerprint, TOKEN);
    }

    @Override
    public void close() throws Exception {
        release.countDown();
        releaseBarriers.countDown();
        server.shutdownNow();
        server.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
    }

    static Schema schema() {
        return new Schema(
                Arrays.asList(
                        new Field("id", FieldType.notNullable(new ArrowType.Int(64, true)), null),
                        new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
                        new Field(
                                "at",
                                FieldType.notNullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")),
                                null),
                        new Field(ArrowTypes.OP, FieldType.notNullable(ArrowType.Utf8.INSTANCE), null)));
    }

    static String op(String stream, long id, boolean changelog) {
        if (!changelog) {
            return "insert";
        }
        return id % 3 == 0 ? "insert" : id % 3 == 1 ? "update" : "delete";
    }

    @Override
    public void describe(DescribeRequest req, StreamObserver<DescribeResponse> out) {
        DescribeResponse.Builder b = DescribeResponse.newBuilder().setPipeline("fake");
        try {
            ByteArrayOutputStream s = new ByteArrayOutputStream();
            MessageSerializer.serialize(new WriteChannel(Channels.newChannel(s)), schema());
            for (Log l : streams.values()) {
                b.addStreams(
                        io.openetl.bridge.v1.Schema.newBuilder()
                                .setName(l.name)
                                .setArrow(unknown.contains(l.name) ? ByteString.EMPTY : ByteString.copyFrom(s.toByteArray()))
                                .setChangelog(l.changelog));
            }
        } catch (Exception e) {
            out.onError(e);
            return;
        }
        outputs.forEach((n, c) -> b.addOutputs(io.openetl.bridge.v1.Schema.newBuilder().setName(n).setChangelog(c)));
        out.onNext(b.build());
        out.onCompleted();
    }

    @Override
    public StreamObserver<CoordinateRequest> coordinate(StreamObserver<CoordinateResponse> out) {
        coordinators.add(out);
        return new StreamObserver<CoordinateRequest>() {
            @Override
            public void onNext(CoordinateRequest r) {
                long c = 0;
                if (r.hasHello()) {
                    hellos.add(r.getHello().getRestoredCheckpoint());
                } else if (r.hasStarted()) {
                    c = r.getStarted().getCheckpoint();
                    started.add(c);
                    for (Log l : streams.values()) {
                        l.barrier(c);
                    }
                } else {
                    c = r.getOutcome().getCheckpoint();
                    (r.getOutcome().getCompleted() ? completed : aborted).add(c);
                    if (!r.getOutcome().getCompleted() && failOnAbort) {
                        out.onError(Status.ABORTED.withDescription("checkpoint " + c + " was aborted").asRuntimeException());
                        return;
                    }
                    if (failCoordinate) {
                        failCoordinate = false;
                        out.onError(Status.UNAVAILABLE.withDescription("dropped").asRuntimeException());
                        return;
                    }
                }
                out.onNext(CoordinateResponse.newBuilder().setAck(Ack.newBuilder().setCheckpoint(c)).build());
            }

            @Override
            public void onError(Throwable t) {}

            @Override
            public void onCompleted() {
                out.onCompleted();
            }
        };
    }

    @Override
    public void read(ReadRequest req, StreamObserver<ReadResponse> out) {
        ServerCallStreamObserver<ReadResponse> o = (ServerCallStreamObserver<ReadResponse>) out;
        Log l = streams.get(req.getStream());
        reads.add(req.getStream() + "/" + req.getSubtask() + "/" + req.getRestoredCheckpoint());
        if (l == null) {
            o.onError(Status.NOT_FOUND.withDescription("no stream " + req.getStream()).asRuntimeException());
            return;
        }
        int from = l.from(req.getRestoredCheckpoint());
        Thread t =
                new Thread(
                        () -> {
                            int sent = 0;
                            try {
                                for (int i = from; !o.isCancelled(); i++) {
                                    long[] item = l.get(i, o);
                                    if (item == null) {
                                        return;
                                    }
                                    if (item[0] == 1 && holdBarriersFrom != 0 && item[1] >= holdBarriersFrom) {
                                        releaseBarriers.await();
                                    }
                                    while (!o.isReady() && !o.isCancelled()) {
                                        Thread.sleep(1);
                                    }
                                    if (l.name.equals(failRead) && ++sent > 3) {
                                        failRead = null;
                                        o.onError(Status.UNAVAILABLE.withDescription("failed").asRuntimeException());
                                        return;
                                    }
                                    o.onNext(
                                            item[0] == 1
                                                    ? ReadResponse.newBuilder()
                                                            .setBarrier(Barrier.newBuilder().setCheckpoint(item[1]))
                                                            .build()
                                                    : ReadResponse.newBuilder()
                                                            .setBatch(ByteString.copyFrom(l.batch(item[1], item[2])))
                                                            .build());
                                }
                            } catch (Exception e) {
                                if (!o.isCancelled()) {
                                    o.onError(Status.INTERNAL.withDescription(e.toString()).asRuntimeException());
                                }
                            }
                        },
                        "fake read " + req.getStream());
        t.setDaemon(true);
        t.start();
    }

    @Override
    public StreamObserver<WriteRequest> write(StreamObserver<WriteResponse> out) {
        return new StreamObserver<WriteRequest>() {
            String output;
            final Set<Long> ids = new HashSet<>();

            @Override
            public void onNext(WriteRequest r) {
                switch (r.getMessageCase()) {
                    case HELLO:
                        output = r.getHello().getOutput();
                        sinkHellos.add(
                                output
                                        + "/"
                                        + r.getHello().getSubtask()
                                        + "/"
                                        + r.getHello().getParallelism()
                                        + "/"
                                        + r.getHello().getRestoredCheckpoint());
                        ack(0);
                        return;
                    case BATCH:
                        try (ArrowStreamReader rd = new ArrowStreamReader(r.getBatch().newInput(), alloc)) {
                            VectorSchemaRoot root = rd.getVectorSchemaRoot();
                            while (rd.loadNextBatch()) {
                                BigIntVector id = (BigIntVector) root.getVector("id");
                                VarCharVector op = (VarCharVector) root.getVector(ArrowTypes.OP);
                                for (int i = 0; i < root.getRowCount(); i++) {
                                    ids.add(id.get(i));
                                    if (op != null) {
                                        ops.computeIfAbsent(output, k -> new ConcurrentHashMap<>())
                                                .put(id.get(i), new String(op.get(i), StandardCharsets.UTF_8));
                                    }
                                }
                            }
                        } catch (Exception e) {
                            out.onError(Status.INTERNAL.withDescription(e.toString()).asRuntimeException());
                        }
                        return;
                    case BARRIER:
                        long c = r.getBarrier().getCheckpoint();
                        types.put(c, r.getBarrier().getType());
                        epochs.computeIfAbsent(c, k -> new ConcurrentHashMap<>())
                                .computeIfAbsent(output, k -> ConcurrentHashMap.newKeySet())
                                .addAll(ids);
                        ids.clear();
                        barriers.merge(c, 1, Integer::sum);
                        if (c == refuse) {
                            out.onError(Status.ABORTED.withDescription("refused").asRuntimeException());
                            return;
                        }
                        if (holdFrom != 0 && c >= holdFrom) {
                            CountDownLatch l = release;
                            Thread t =
                                    new Thread(
                                            () -> {
                                                try {
                                                    l.await();
                                                    ack(c);
                                                } catch (Exception ignored) {
                                                    // The call ended.
                                                }
                                            });
                            t.setDaemon(true);
                            t.start();
                            return;
                        }
                        ack(c);
                        return;
                    default:
                }
            }

            private synchronized void ack(long c) {
                out.onNext(WriteResponse.newBuilder().setAck(Ack.newBuilder().setCheckpoint(c)).build());
            }

            @Override
            public void onError(Throwable t) {}

            @Override
            public void onCompleted() {
                out.onCompleted();
            }
        };
    }

    /** Checkpoint c's rows at the sinks are exactly each stream's rows between its barrier and the one before. */
    Map<String, String> mismatches(long c) {
        Map<String, String> bad = new TreeMap<>();
        for (Log l : streams.values()) {
            Set<Long> want = l.epoch(c);
            Set<Long> got = epochs.getOrDefault(c, Collections.emptyMap()).getOrDefault(l.name, Collections.emptySet());
            if (want == null || !want.equals(got)) {
                bad.put(l.name, "want " + summary(want) + ", got " + summary(got));
            }
        }
        return bad;
    }

    private static String summary(Set<Long> s) {
        if (s == null) {
            return "no barrier";
        }
        if (s.isEmpty()) {
            return "none";
        }
        List<Long> l = new ArrayList<>(s);
        Collections.sort(l);
        return l.size() + " rows " + l.get(0) + ".." + l.get(l.size() - 1);
    }

    static void until(String what, long ms, BooleanSupplier cond) throws InterruptedException {
        long deadline = System.currentTimeMillis() + ms;
        while (!cond.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("timed out waiting for " + what);
            }
            Thread.sleep(20);
        }
    }

    /** A stream's log: batches [from, to) and barriers. */
    final class Log {
        final String name;
        final boolean changelog;
        private final List<long[]> items = new ArrayList<>();
        /** Each barrier's position, and the position of the barrier before it. */
        private final Map<Long, long[]> positions = new HashMap<>();
        private long next;
        private long last;

        Log(String name, boolean changelog) {
            this.name = name;
            this.changelog = changelog;
        }

        synchronized void barrier(long c) {
            items.add(new long[] {1, c, 0});
            positions.put(c, new long[] {next, last});
            last = next;
            notifyAll();
        }

        /** Where a read restored from checkpoint c starts; the log after c's barrier is gone. */
        synchronized int from(long c) {
            if (c == 0) {
                return 0;
            }
            for (int i = 0; i < items.size(); i++) {
                long[] it = items.get(i);
                if (it[0] == 1 && it[1] == c) {
                    items.subList(i + 1, items.size()).clear();
                    next = positions.get(c)[0];
                    last = next;
                    notifyAll();
                    return i + 1;
                }
            }
            throw new IllegalStateException(name + " has no barrier " + c);
        }

        /** Item i, generating rows if the log ends there; null if the call was cancelled. */
        synchronized long[] get(int i, ServerCallStreamObserver<?> o) throws InterruptedException {
            while (i >= items.size()) {
                if (o.isCancelled()) {
                    return null;
                }
                if (next < maxRows) {
                    long to = Math.min(next + BATCH, maxRows);
                    items.add(new long[] {0, next, to});
                    next = to;
                } else {
                    wait(10);
                }
            }
            long[] it = items.get(i);
            if (it[0] == 0) {
                // Slows the source down, so checkpoints are frequent compared to rows.
                wait(2);
            }
            return it;
        }

        synchronized Set<Long> epoch(long c) {
            long[] p = positions.get(c);
            if (p == null) {
                return null;
            }
            Set<Long> s = new HashSet<>();
            for (long k = p[1]; k < p[0]; k++) {
                s.add(k);
            }
            return s;
        }

        byte[] batch(long from, long to) throws Exception {
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema(), alloc)) {
                root.allocateNew();
                BigIntVector id = (BigIntVector) root.getVector("id");
                VarCharVector nm = (VarCharVector) root.getVector("name");
                TimeStampMicroTZVector at = (TimeStampMicroTZVector) root.getVector("at");
                VarCharVector op = (VarCharVector) root.getVector(ArrowTypes.OP);
                int n = (int) (to - from);
                for (int i = 0; i < n; i++) {
                    long k = from + i;
                    id.setSafe(i, k);
                    if (k % 7 == 0) {
                        nm.setNull(i);
                    } else {
                        nm.setSafe(i, (name + "-" + k).getBytes(StandardCharsets.UTF_8));
                    }
                    at.setSafe(i, 1_700_000_000_000_000L + k);
                    op.setSafe(i, op(name, k, changelog).getBytes(StandardCharsets.UTF_8));
                }
                root.setRowCount(n);
                ByteArrayOutputStream b = new ByteArrayOutputStream();
                try (ArrowStreamWriter w = new ArrowStreamWriter(root, null, Channels.newChannel(b))) {
                    w.start();
                    w.writeBatch();
                    w.end();
                }
                return b.toByteArray();
            }
        }
    }
}
