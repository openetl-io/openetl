package io.openetl.flink;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import io.openetl.bridge.v1.BridgeServiceGrpc;
import io.openetl.bridge.v1.DescribeRequest;
import io.openetl.bridge.v1.DescribeResponse;
import io.openetl.flink.examples.Passthrough;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BridgeClientTest {
    @Test
    void describesThePipelineWithTheTokenAndProtocol() throws Exception {
        try (FakeRunner fake = new FakeRunner(Arrays.asList("b", "a"), Collections.singleton("a"))) {
            OpenEtl o = OpenEtl.connect(StreamExecutionEnvironment.getExecutionEnvironment(), fake.config(), new String[] {"x"});
            assertEquals(Arrays.asList("a", "b"), o.streamNames());
            assertEquals(Arrays.asList("a", "b"), o.outputNames());
            assertEquals(Arrays.asList("id", "name", "at"), o.rowType("a").getFieldNames());
            assertArrayEquals(new String[] {"x"}, o.args());
            assertEquals("Bearer " + FakeRunner.TOKEN, fake.headers.get(0).get(BridgeClient.AUTHORIZATION));
            assertEquals("v1", fake.headers.get(0).get(BridgeClient.PROTOCOL_HEADER));
        }
    }

    @Test
    void aStreamWithoutASchemaYetHasNoColumns() throws Exception {
        try (FakeRunner fake = new FakeRunner(Arrays.asList("a", "b"), Collections.emptySet())) {
            fake.unknown.add("b");
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            OpenEtl o = OpenEtl.connect(env, fake.config(), new String[0]);
            assertEquals(0, o.rowType("b").getFieldCount());
            Passthrough.build(o, o.args());
            assertEquals(4, env.getStreamGraph().getStreamNodes().size());
        }
    }

    @Test
    void trustsOnlyThePinnedCertificate() throws Exception {
        try (FakeRunner fake = new FakeRunner(Collections.singletonList("a"), Collections.emptySet())) {
            StringBuilder other = new StringBuilder();
            for (int i = 0; i < 64; i++) {
                other.append('a');
            }
            IllegalStateException e =
                    assertThrows(
                            IllegalStateException.class,
                            () ->
                                    OpenEtl.connect(
                                            StreamExecutionEnvironment.getExecutionEnvironment(),
                                            fake.config(other.toString()),
                                            new String[0]));
            assertTrue(e.getMessage().contains("UNAVAILABLE"), e.getMessage());
            assertTrue(fake.headers.isEmpty());
        }
    }

    @Test
    void readsItsArguments() throws Exception {
        try (FakeRunner fake = new FakeRunner(Collections.singletonList("a"), Collections.emptySet())) {
            BridgeConfig c = fake.config();
            OpenEtl o =
                    OpenEtl.fromArgs(
                            StreamExecutionEnvironment.getExecutionEnvironment(),
                            new String[] {
                                "--mine", "1", OpenEtl.BRIDGE, c.host + ":" + c.port, OpenEtl.FINGERPRINT + "=" + c.fingerprint,
                                OpenEtl.TOKEN, c.token, "--outputs=a=a"
                            });
            assertArrayEquals(new String[] {"--mine", "1", "--outputs=a=a"}, o.args());
            IllegalArgumentException e =
                    assertThrows(
                            IllegalArgumentException.class,
                            () -> OpenEtl.fromArgs(StreamExecutionEnvironment.getExecutionEnvironment(), new String[] {"--mine"}));
            assertTrue(e.getMessage().contains(OpenEtl.BRIDGE), e.getMessage());
        }
    }

    /** A call's callbacks run on the channel's own threads, which stop before close returns, even when the closing thread is interrupted, as Flink's cancels do. */
    @Test
    void aClosedChannelLeavesNoThreads() throws Exception {
        try (FakeRunner fake = new FakeRunner(Collections.singletonList("a"), Collections.emptySet())) {
            for (boolean interrupt : new boolean[] {false, true}) {
                ManagedChannel ch = BridgeClient.open(fake.config());
                CompletableFuture<String> ran = new CompletableFuture<>();
                BridgeServiceGrpc.newStub(ch)
                        .describe(
                                DescribeRequest.getDefaultInstance(),
                                new StreamObserver<DescribeResponse>() {
                                    @Override
                                    public void onNext(DescribeResponse r) {
                                        ran.complete(Thread.currentThread().getName());
                                    }

                                    @Override
                                    public void onError(Throwable t) {
                                        ran.completeExceptionally(t);
                                    }

                                    @Override
                                    public void onCompleted() {}
                                });
                String name = ran.get(30, TimeUnit.SECONDS);
                assertTrue(name.startsWith(BridgeClient.THREADS), name);
                if (interrupt) {
                    Thread.currentThread().interrupt();
                }
                BridgeClient.close(ch);
                assertEquals(interrupt, Thread.interrupted());
                List<String> left = new ArrayList<>();
                for (Thread t : Thread.getAllStackTraces().keySet()) {
                    if (t.getName().startsWith(BridgeClient.THREADS)) {
                        t.join(1_000);
                        if (t.isAlive()) {
                            left.add(t.getName());
                        }
                    }
                }
                assertTrue(left.isEmpty(), "still running: " + left);
                assertTrue(ch.isTerminated());
            }
        }
    }
}
