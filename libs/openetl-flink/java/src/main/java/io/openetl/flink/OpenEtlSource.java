package io.openetl.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Every stream of the pipeline, a split per stream. Its checkpoints are induced: a reader takes
 * checkpoint C where the runner put C's barrier in each of its splits.
 */
final class OpenEtlSource
        implements Source<TaggedRow, StreamSplit, EnumeratorState>, ResultTypeQueryable<TaggedRow> {
    private static final long serialVersionUID = 1L;

    final BridgeConfig bridge;
    /** Sorted. */
    final List<String> streams;
    /** Each stream's schema, as an Arrow IPC stream holding only it. */
    final List<byte[]> schemas;

    final RowType[] types;

    OpenEtlSource(BridgeConfig bridge, List<String> streams, List<byte[]> schemas, RowType[] types) {
        this.bridge = bridge;
        this.streams = Collections.unmodifiableList(new ArrayList<>(streams));
        this.schemas = Collections.unmodifiableList(new ArrayList<>(schemas));
        this.types = types;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<TaggedRow, StreamSplit> createReader(SourceReaderContext context) throws Exception {
        return new OpenEtlReader(this, context);
    }

    @Override
    public SplitEnumerator<StreamSplit, EnumeratorState> createEnumerator(
            SplitEnumeratorContext<StreamSplit> context) {
        return new OpenEtlEnumerator(context, bridge, streams, new EnumeratorState(0, Collections.emptySet()));
    }

    @Override
    public SplitEnumerator<StreamSplit, EnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<StreamSplit> context, EnumeratorState state) {
        return new OpenEtlEnumerator(context, bridge, streams, state);
    }

    @Override
    public SimpleVersionedSerializer<StreamSplit> getSplitSerializer() {
        return new StreamSplit.Serializer();
    }

    @Override
    public SimpleVersionedSerializer<EnumeratorState> getEnumeratorCheckpointSerializer() {
        return new EnumeratorState.Serializer();
    }

    @Override
    public TypeInformation<TaggedRow> getProducedType() {
        return new TaggedRow.Info(types);
    }
}
