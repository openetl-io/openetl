package io.openetl.flink;

import io.openetl.bridge.v1.BarrierType;
import io.openetl.flink.examples.Passthrough;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import static io.openetl.flink.FakeRunner.until;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OpenEtlJobTest {
    @TempDir Path dir;

    private MiniCluster cluster(int slots) throws Exception {
        Configuration c = new Configuration();
        c.set(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "full");
        c.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, dir.resolve("checkpoints").toUri().toString());
        c.set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 2);
        MiniCluster mc =
                new MiniCluster(
                        new MiniClusterConfiguration.Builder()
                                .setConfiguration(c)
                                .setNumTaskManagers(1)
                                .setNumSlotsPerTaskManager(slots)
                                .build());
        mc.start();
        return mc;
    }

    private static JobGraph job(FakeRunner fake, int parallelism, long timeout, int tolerable) {
        Configuration c = new Configuration();
        c.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        c.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
        c.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(100));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(c);
        env.setParallelism(parallelism);
        env.enableCheckpointing(200, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig cc = env.getCheckpointConfig();
        cc.setMaxConcurrentCheckpoints(1);
        cc.setTolerableCheckpointFailureNumber(tolerable);
        cc.setCheckpointTimeout(timeout);
        cc.setExternalizedCheckpointRetention(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        OpenEtl o = OpenEtl.connect(env, fake.config(), new String[0]);
        Passthrough.build(o, o.args());
        return env.getStreamGraph().getJobGraph();
    }

    private static List<String> names(int n) {
        List<String> l = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            l.add("s" + i);
        }
        return l;
    }

    private static JobID run(MiniCluster mc, JobGraph g) throws Exception {
        mc.submitJob(g).get();
        until("running", 60_000, () -> status(mc, g.getJobID()) == JobStatus.RUNNING);
        return g.getJobID();
    }

    private static JobStatus status(MiniCluster mc, JobID id) {
        try {
            return mc.getJobStatus(id).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void cancel(MiniCluster mc, JobID id) throws Exception {
        mc.cancelJob(id).get();
        until("cancelled", 60_000, () -> status(mc, id).isGloballyTerminalState());
    }

    private static void assertExact(FakeRunner fake, long c, int barriers) {
        assertEquals(barriers, fake.barriers.get(c), "sink barriers of checkpoint " + c);
        assertEquals(Collections.emptyMap(), fake.mismatches(c), "checkpoint " + c);
    }

    private static long rows(FakeRunner fake) {
        return fake.epochs.values().stream().flatMap(m -> m.values().stream()).mapToLong(Set::size).sum();
    }

    @ParameterizedTest(name = "parallelism {0}, {1} streams")
    @CsvSource({"2, 3", "4, 2", "1, 1"})
    void checkpointsLandAtTheRunnersBarriers(int parallelism, int streams) throws Exception {
        try (FakeRunner fake = new FakeRunner(names(streams), Collections.singleton("s0"));
                MiniCluster mc = cluster(parallelism)) {
            JobID id = run(mc, job(fake, parallelism, 60_000, 0));
            until("5 checkpoints", 60_000, () -> fake.completed.size() >= 5);
            cancel(mc, id);

            assertEquals(Collections.singletonList(0L), fake.hellos);
            assertEquals(streams, fake.reads.size(), fake.reads.toString());
            assertTrue(fake.reads.stream().allMatch(r -> r.endsWith("/0")), fake.reads.toString());
            assertEquals(streams * parallelism, fake.sinkHellos.size(), fake.sinkHellos.toString());
            for (long c : fake.completed) {
                assertEquals(BarrierType.BARRIER_TYPE_CHECKPOINT, fake.types.get(c));
                assertExact(fake, c, streams * parallelism);
            }
            assertTrue(rows(fake) > 0);
            Map<Long, String> ops = fake.ops.get("s0");
            assertTrue(ops.containsValue("delete") && ops.containsValue("update"), String.valueOf(ops.size()));
            ops.forEach((row, op) -> assertEquals(FakeRunner.op("s0", row, true), op, "row " + row));
            for (int i = 1; i < streams; i++) {
                assertFalse(fake.ops.containsKey("s" + i));
            }
        }
    }

    @Test
    void aSavepointTakenByHandIsTyped() throws Exception {
        try (FakeRunner fake = new FakeRunner(names(2), Collections.emptySet()); MiniCluster mc = cluster(2)) {
            JobID id = run(mc, job(fake, 2, 60_000, 0));
            until("2 checkpoints", 60_000, () -> fake.completed.size() >= 2);
            mc.triggerSavepoint(id, dir.resolve("savepoints").toUri().toString(), false, SavepointFormatType.CANONICAL)
                    .get();
            List<Long> savepoints = of(fake, BarrierType.BARRIER_TYPE_SAVEPOINT);
            assertEquals(1, savepoints.size());
            long sp = savepoints.get(0);
            until("2 checkpoints after it", 60_000, () -> fake.completed.stream().filter(c -> c > sp).count() >= 2);
            cancel(mc, id);

            assertFalse(fake.completed.contains(sp));
            assertFalse(fake.aborted.contains(sp));
            assertExact(fake, sp, 4);
            for (long c : fake.completed) {
                assertExact(fake, c, 4);
            }
        }
    }

    @Test
    void aStopWithASavepointIsSynchronous() throws Exception {
        try (FakeRunner fake = new FakeRunner(names(2), Collections.emptySet()); MiniCluster mc = cluster(2)) {
            JobID id = run(mc, job(fake, 2, 60_000, 0));
            until("2 checkpoints", 60_000, () -> fake.completed.size() >= 2);
            // A checkpoint in flight, which the stop aborts; a runner that's stopping lets it go.
            fake.failOnAbort = false;
            fake.release = new CountDownLatch(1);
            long held = Collections.max(fake.started) + 1;
            fake.holdFrom = held;
            until("a held barrier", 60_000, () -> fake.barriers.getOrDefault(held, 0) > 0);
            CompletableFuture<String> stop =
                    mc.stopWithSavepoint(id, dir.resolve("savepoints").toUri().toString(), false, SavepointFormatType.CANONICAL);
            until("the abort", 60_000, () -> fake.aborted.contains(held));
            fake.holdFrom = 0;
            fake.release.countDown();
            stop.get();
            until("finished", 60_000, () -> status(mc, id) == JobStatus.FINISHED);

            List<Long> stops = of(fake, BarrierType.BARRIER_TYPE_SYNC_SAVEPOINT);
            assertEquals(1, stops.size());
            long s = stops.get(0);
            assertTrue(fake.completed.contains(s));
            assertEquals(Collections.singletonList(0L), fake.hellos);
            assertExact(fake, s, 4);
        }
    }

    @Test
    void aReaderPassesOverTheBarrierOfAnAbortedCheckpoint() throws Exception {
        try (FakeRunner fake = new FakeRunner(names(2), Collections.emptySet()); MiniCluster mc = cluster(2)) {
            JobID id = run(mc, job(fake, 2, 60_000, 0));
            until("2 checkpoints", 60_000, () -> fake.completed.size() >= 2);
            // The readers don't get the next barrier until the stop has aborted its checkpoint.
            fake.failOnAbort = false;
            CountDownLatch barrier = new CountDownLatch(1);
            fake.releaseBarriers = barrier;
            long held = Collections.max(fake.started) + 1;
            fake.holdBarriersFrom = held;
            until("the next checkpoint", 60_000, () -> fake.started.contains(held));
            CompletableFuture<String> stop =
                    mc.stopWithSavepoint(id, dir.resolve("savepoints").toUri().toString(), false, SavepointFormatType.CANONICAL);
            until("the abort", 60_000, () -> fake.aborted.contains(held));
            fake.holdBarriersFrom = 0;
            barrier.countDown();
            stop.get();
            until("finished", 60_000, () -> status(mc, id) == JobStatus.FINISHED);

            assertFalse(fake.barriers.containsKey(held), "no sink took the aborted checkpoint");
            long s = of(fake, BarrierType.BARRIER_TYPE_SYNC_SAVEPOINT).get(0);
            assertTrue(fake.completed.contains(s));
            // Its rows went on into the stop's.
            for (String n : fake.streams.keySet()) {
                Set<Long> want = new HashSet<>(fake.streams.get(n).epoch(held));
                want.addAll(fake.streams.get(n).epoch(s));
                assertEquals(want, fake.epochs.get(s).get(n), n);
            }
        }
    }

    @Test
    void anAbortedCheckpointFailsTheJob() throws Exception {
        try (FakeRunner fake = new FakeRunner(names(2), Collections.emptySet()); MiniCluster mc = cluster(2)) {
            // Flink tolerates the expiry itself; the enumerator doesn't.
            JobID id = run(mc, job(fake, 2, 1_500, 10));
            until("2 checkpoints", 60_000, () -> fake.completed.size() >= 2);
            fake.release = new CountDownLatch(1);
            fake.holdFrom = Collections.max(fake.started) + 1;
            until("an abort", 60_000, () -> !fake.aborted.isEmpty());
            fake.holdFrom = 0;
            fake.release.countDown();
            until("a restart", 60_000, () -> fake.hellos.size() >= 2);
            long restored = fake.hellos.get(1);
            assertTrue(fake.completed.contains(restored), restored + " in " + fake.completed);
            long aborted = fake.aborted.get(0);
            until("a checkpoint after it", 60_000, () -> fake.completed.stream().anyMatch(c -> c > aborted));
            cancel(mc, id);
        }
    }

    @Test
    void aFailoverRestoresEverySubtaskFromTheSameCheckpoint() throws Exception {
        try (FakeRunner fake = new FakeRunner(names(3), Collections.singleton("s1")); MiniCluster mc = cluster(2)) {
            JobID id = run(mc, job(fake, 2, 60_000, 0));
            until("3 checkpoints", 60_000, () -> fake.completed.size() >= 3);
            fake.failRead = "s1";
            until("a restart", 60_000, () -> fake.hellos.size() >= 2);
            long r = fake.hellos.get(1);
            assertTrue(r != 0 && fake.completed.contains(r), r + " in " + fake.completed);
            int before = fake.completed.size();
            until("3 checkpoints after it", 60_000, () -> fake.completed.size() >= before + 3);
            cancel(mc, id);

            // Every call after the failure, the enumerator's included, restored from the same checkpoint.
            assertTrue(fake.hellos.subList(1, fake.hellos.size()).stream().allMatch(h -> h == r), fake.hellos.toString());
            List<String> reads = fake.reads.subList(3, fake.reads.size());
            assertTrue(reads.size() >= 3 && reads.stream().allMatch(x -> x.endsWith("/" + r)), fake.reads.toString());
            List<String> sinks = fake.sinkHellos.subList(6, fake.sinkHellos.size());
            assertTrue(sinks.size() >= 6 && sinks.stream().allMatch(x -> x.endsWith("/" + r)), fake.sinkHellos.toString());
            for (long c : fake.completed.subList(before, fake.completed.size())) {
                assertExact(fake, c, 6);
            }
        }
    }

    @Test
    void aJobDeployedFromAnotherJobsCheckpointSaysSo() throws Exception {
        try (FakeRunner fake = new FakeRunner(names(2), Collections.singleton("s0")); MiniCluster mc = cluster(2)) {
            JobID a = run(mc, job(fake, 2, 60_000, 0));
            until("3 checkpoints", 60_000, () -> fake.completed.size() >= 3);
            cancel(mc, a);
            long r = Collections.max(fake.completed);
            Path chk = dir.resolve("checkpoints").resolve(a.toString()).resolve("chk-" + r);
            assertTrue(chk.resolve("_metadata").toFile().exists(), chk.toString());

            int hellos = fake.hellos.size(), reads = fake.reads.size(), sinks = fake.sinkHellos.size(), done = fake.completed.size();
            JobGraph b = job(fake, 2, 60_000, 0);
            b.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(chk.toUri().toString(), false));
            JobID id = run(mc, b);
            until("3 checkpoints of the new job", 60_000, () -> fake.completed.size() >= done + 3);
            cancel(mc, id);

            assertEquals(Collections.singletonList(r), fake.hellos.subList(hellos, fake.hellos.size()));
            List<String> rs = fake.reads.subList(reads, fake.reads.size());
            assertTrue(rs.size() == 2 && rs.stream().allMatch(x -> x.endsWith("/" + r)), rs.toString());
            List<String> ss = fake.sinkHellos.subList(sinks, fake.sinkHellos.size());
            assertTrue(ss.size() == 4 && ss.stream().allMatch(x -> x.endsWith("/" + r)), ss.toString());
            for (long c : fake.completed.subList(done, fake.completed.size())) {
                assertTrue(c > r);
                assertExact(fake, c, 4);
            }
        }
    }

    @Test
    void aRefusedBarrierFailsTheSnapshot() throws Exception {
        try (FakeRunner fake = new FakeRunner(names(2), Collections.emptySet()); MiniCluster mc = cluster(2)) {
            JobID id = run(mc, job(fake, 2, 60_000, 0));
            until("2 checkpoints", 60_000, () -> fake.completed.size() >= 2);
            long refused = Collections.max(fake.started) + 2;
            fake.refuse = refused;
            until("a restart", 60_000, () -> fake.hellos.size() >= 2);
            until("a checkpoint after it", 60_000, () -> fake.completed.stream().anyMatch(c -> c > refused));
            cancel(mc, id);
            assertFalse(fake.completed.contains(refused));
        }
    }

    @Test
    void cancellingInterruptsASnapshotTheRunnerHolds() throws Exception {
        try (FakeRunner fake = new FakeRunner(names(2), Collections.emptySet()); MiniCluster mc = cluster(2)) {
            JobID id = run(mc, job(fake, 2, 120_000, 0));
            until("a checkpoint", 60_000, () -> !fake.completed.isEmpty());
            fake.release = new CountDownLatch(1);
            long held = Collections.max(fake.started) + 1;
            fake.holdFrom = held;
            until("a held barrier", 60_000, () -> fake.barriers.getOrDefault(held, 0) > 0);
            long start = System.nanoTime();
            cancel(mc, id);
            long ms = (System.nanoTime() - start) / 1_000_000;
            assertTrue(ms < 20_000, "cancel took " + ms + " ms");
        }
    }

    @Test
    void theEnumeratorsCallEndingFailsTheJob() throws Exception {
        try (FakeRunner fake = new FakeRunner(names(1), Collections.emptySet()); MiniCluster mc = cluster(1)) {
            JobID id = run(mc, job(fake, 1, 60_000, 0));
            until("a checkpoint", 60_000, () -> !fake.completed.isEmpty());
            fake.failCoordinate = true;
            until("a restart", 60_000, () -> fake.hellos.size() >= 2);
            cancel(mc, id);
        }
    }

    private static List<Long> of(FakeRunner fake, BarrierType t) {
        Set<Long> s = new HashSet<>();
        fake.types.forEach((c, type) -> {
            if (type == t) {
                s.add(c);
            }
        });
        List<Long> l = new ArrayList<>(s);
        Collections.sort(l);
        return l;
    }
}
