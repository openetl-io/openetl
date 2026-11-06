package io.openetl.flink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.Arrays;

/** A row of one of the source's streams, by the stream's index. */
public final class TaggedRow {
    final int stream;
    final RowData row;

    TaggedRow(int stream, RowData row) {
        this.stream = stream;
        this.row = row;
    }

    static final class Info extends TypeInformation<TaggedRow> {
        private static final long serialVersionUID = 1L;

        private final RowType[] types;

        Info(RowType[] types) {
            this.types = types;
        }

        @Override
        public boolean isBasicType() {
            return false;
        }

        @Override
        public boolean isTupleType() {
            return false;
        }

        @Override
        public int getArity() {
            return 1;
        }

        @Override
        public int getTotalFields() {
            return 1;
        }

        @Override
        public Class<TaggedRow> getTypeClass() {
            return TaggedRow.class;
        }

        @Override
        public boolean isKeyType() {
            return false;
        }

        @Override
        public TypeSerializer<TaggedRow> createSerializer(SerializerConfig config) {
            return new Serializer(types);
        }

        @Override
        @SuppressWarnings("deprecation")
        public TypeSerializer<TaggedRow> createSerializer(ExecutionConfig config) {
            return new Serializer(types);
        }

        @Override
        public String toString() {
            return "TaggedRow" + Arrays.toString(types);
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Info && Arrays.equals(types, ((Info) o).types);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(types);
        }

        @Override
        public boolean canEqual(Object o) {
            return o instanceof Info;
        }
    }

    static final class Serializer extends TypeSerializer<TaggedRow> {
        private static final long serialVersionUID = 1L;

        private final RowType[] types;
        private transient RowDataSerializer[] rows;

        Serializer(RowType[] types) {
            this.types = types;
        }

        private RowDataSerializer rows(int stream) {
            if (rows == null) {
                rows = new RowDataSerializer[types.length];
                for (int i = 0; i < types.length; i++) {
                    rows[i] = new RowDataSerializer(types[i]);
                }
            }
            return rows[stream];
        }

        @Override
        public boolean isImmutableType() {
            return false;
        }

        @Override
        public TypeSerializer<TaggedRow> duplicate() {
            return new Serializer(types);
        }

        @Override
        public TaggedRow createInstance() {
            return null;
        }

        @Override
        public TaggedRow copy(TaggedRow from) {
            return new TaggedRow(from.stream, rows(from.stream).copy(from.row));
        }

        @Override
        public TaggedRow copy(TaggedRow from, TaggedRow reuse) {
            return copy(from);
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(TaggedRow r, DataOutputView out) throws IOException {
            out.writeInt(r.stream);
            rows(r.stream).serialize(r.row, out);
        }

        @Override
        public TaggedRow deserialize(DataInputView in) throws IOException {
            int s = in.readInt();
            return new TaggedRow(s, rows(s).deserialize(in));
        }

        @Override
        public TaggedRow deserialize(TaggedRow reuse, DataInputView in) throws IOException {
            return deserialize(in);
        }

        @Override
        public void copy(DataInputView in, DataOutputView out) throws IOException {
            int s = in.readInt();
            out.writeInt(s);
            rows(s).copy(in, out);
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Serializer && Arrays.equals(types, ((Serializer) o).types);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(types);
        }

        @Override
        public TypeSerializerSnapshot<TaggedRow> snapshotConfiguration() {
            return new Snapshot(types);
        }
    }

    /** Only for completeness: tagged rows never reach state. */
    public static final class Snapshot implements TypeSerializerSnapshot<TaggedRow> {
        private RowType[] types;

        public Snapshot() {}

        Snapshot(RowType[] types) {
            this.types = types;
        }

        @Override
        public int getCurrentVersion() {
            return 1;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            byte[] b = InstantiationUtil.serializeObject(types);
            out.writeInt(b.length);
            out.write(b);
        }

        @Override
        public void readSnapshot(int version, DataInputView in, ClassLoader loader) throws IOException {
            byte[] b = new byte[in.readInt()];
            in.readFully(b);
            try {
                types = InstantiationUtil.deserializeObject(b, loader);
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
        }

        @Override
        public TypeSerializer<TaggedRow> restoreSerializer() {
            return new Serializer(types);
        }

        @Override
        public TypeSerializerSchemaCompatibility<TaggedRow> resolveSchemaCompatibility(
                TypeSerializerSnapshot<TaggedRow> old) {
            return old instanceof Snapshot && Arrays.equals(types, ((Snapshot) old).types)
                    ? TypeSerializerSchemaCompatibility.compatibleAsIs()
                    : TypeSerializerSchemaCompatibility.incompatible();
        }
    }
}
