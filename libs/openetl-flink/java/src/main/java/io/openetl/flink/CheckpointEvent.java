package io.openetl.flink;

import org.apache.flink.api.connector.source.SourceEvent;

/** From the enumerator to every reader: checkpoint C started. */
final class CheckpointEvent implements SourceEvent {
    private static final long serialVersionUID = 1L;

    final long checkpoint;

    CheckpointEvent(long checkpoint) {
        this.checkpoint = checkpoint;
    }
}
