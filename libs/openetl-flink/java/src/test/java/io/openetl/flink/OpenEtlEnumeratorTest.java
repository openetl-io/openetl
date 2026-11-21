package io.openetl.flink;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OpenEtlEnumeratorTest {
    /** A context with one subtask that records the streams assigned to it. */
    @SuppressWarnings("unchecked")
    private static SplitEnumeratorContext<StreamSplit> context(List<String> assigned) {
        SplitEnumeratorMetricGroup metrics =
                (SplitEnumeratorMetricGroup)
                        Proxy.newProxyInstance(
                                SplitEnumeratorMetricGroup.class.getClassLoader(),
                                new Class<?>[] {SplitEnumeratorMetricGroup.class},
                                (p, m, args) -> m.getName().equals("getAllVariables") ? Collections.emptyMap() : null);
        return (SplitEnumeratorContext<StreamSplit>)
                Proxy.newProxyInstance(
                        SplitEnumeratorContext.class.getClassLoader(),
                        new Class<?>[] {SplitEnumeratorContext.class},
                        (p, m, args) -> {
                            switch (m.getName()) {
                                case "metricGroup":
                                    return metrics;
                                case "currentParallelism":
                                    return 1;
                                case "registeredReaders":
                                    return Collections.emptyMap();
                                case "assignSplit":
                                    assigned.add(((StreamSplit) args[0]).stream);
                                    return null;
                                default:
                                    throw new UnsupportedOperationException(m.getName());
                            }
                        });
    }

    @Test
    void aStreamAddedBackIsReadAgain() throws Exception {
        try (FakeRunner fake = new FakeRunner(List.of("a", "b"), Collections.emptySet())) {
            List<String> assigned = new ArrayList<>();
            // Checkpoint 5 had a and b. The job is deployed without b, whose split its reader drops,
            // and checkpoints.
            OpenEtlEnumerator without =
                    new OpenEtlEnumerator(context(assigned), fake.config(), List.of("a"), new EnumeratorState(5, Set.of("a", "b")));
            without.start();
            EnumeratorState state = without.snapshotState(6);
            without.close();
            // Deployed with b again, from checkpoint 6: a's split is a reader's, and b's no one's.
            OpenEtlEnumerator with = new OpenEtlEnumerator(context(assigned), fake.config(), List.of("a", "b"), state);
            with.addReader(0);
            assertEquals(Set.of("a"), new TreeSet<>(state.assigned));
            assertEquals(List.of("b"), assigned);
        }
    }
}
