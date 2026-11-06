package io.openetl.flink;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

final class Memory {
    private static final BufferAllocator ROOT = new RootAllocator();

    private Memory() {}

    static BufferAllocator child(String name) {
        return ROOT.newChildAllocator(name, 0, Long.MAX_VALUE);
    }

    /** Closes a, without failing if what it allocated wasn't all released. */
    static void close(BufferAllocator a) {
        if (a == null) {
            return;
        }
        try {
            a.close();
        } catch (IllegalStateException ignored) {
            // A batch still converting when the task was cancelled.
        }
    }
}
