package io.openetl.flink;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseLargeVariableWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BaseVariableWidthViewVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ViewVarBinaryVector;
import org.apache.arrow.vector.ViewVarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Arrow record batches as Flink rows and back. */
final class ArrowRows {
    private static final long MILLIS_PER_DAY = 86_400_000L;

    private ArrowRows() {}

    /**
     * The rows of an Arrow IPC stream whose schema is expected's. _openetl_op, if there is one,
     * becomes each row's kind and leaves the row.
     */
    static List<RowData> read(InputStream ipc, BufferAllocator alloc, Schema expected, String stream)
            throws IOException {
        List<RowData> out = new ArrayList<>();
        try (ArrowStreamReader r = new ArrowStreamReader(ipc, alloc)) {
            VectorSchemaRoot root = r.getVectorSchemaRoot();
            if (!ArrowTypes.sameShape(root.getSchema().getFields(), expected.getFields())) {
                throw new IOException(
                        "openetl: a batch of stream "
                                + stream
                                + " has schema "
                                + root.getSchema()
                                + ", and the job was built for "
                                + expected);
            }
            while (r.loadNextBatch()) {
                Rows rows = new Rows(root);
                for (int i = 0; i < root.getRowCount(); i++) {
                    out.add(rows.row(i));
                }
            }
        }
        return out;
    }

    /** Reads one batch's rows. */
    static final class Rows {
        private final Getter[] getters;
        private final VarCharVector op;

        Rows(VectorSchemaRoot root) {
            List<Getter> gs = new ArrayList<>();
            VarCharVector o = null;
            for (FieldVector v : root.getFieldVectors()) {
                if (v.getName().equals(ArrowTypes.OP) && v instanceof VarCharVector) {
                    o = (VarCharVector) v;
                } else {
                    gs.add(getter(v));
                }
            }
            this.getters = gs.toArray(new Getter[0]);
            this.op = o;
        }

        RowData row(int i) {
            GenericRowData r = new GenericRowData(kind(i), getters.length);
            for (int c = 0; c < getters.length; c++) {
                r.setField(c, getters[c].get(i));
            }
            return r;
        }

        private RowKind kind(int i) {
            if (op == null || op.isNull(i)) {
                return RowKind.INSERT;
            }
            String s = new String(op.get(i), StandardCharsets.UTF_8);
            switch (s) {
                case "insert":
                    return RowKind.INSERT;
                case "update":
                    return RowKind.UPDATE_AFTER;
                case "delete":
                    return RowKind.DELETE;
                default:
                    throw new IllegalStateException("openetl: a row's " + ArrowTypes.OP + " is " + s);
            }
        }
    }

    interface Getter {
        /** The value in Flink's internal format, or null. */
        Object get(int i);
    }

    static Getter getter(FieldVector vector) {
        FieldVector v =
                vector instanceof ExtensionTypeVector
                        ? ((ExtensionTypeVector<?>) vector).getUnderlyingVector()
                        : vector;
        Getter g;
        if (v instanceof BitVector) {
            BitVector x = (BitVector) v;
            g = i -> x.get(i) != 0;
        } else if (v instanceof TinyIntVector) {
            TinyIntVector x = (TinyIntVector) v;
            g = x::get;
        } else if (v instanceof SmallIntVector) {
            SmallIntVector x = (SmallIntVector) v;
            g = x::get;
        } else if (v instanceof IntVector) {
            IntVector x = (IntVector) v;
            g = x::get;
        } else if (v instanceof BigIntVector) {
            BigIntVector x = (BigIntVector) v;
            g = x::get;
        } else if (v instanceof Float4Vector) {
            Float4Vector x = (Float4Vector) v;
            g = x::get;
        } else if (v instanceof Float8Vector) {
            Float8Vector x = (Float8Vector) v;
            g = x::get;
        } else if (v instanceof VarCharVector) {
            VarCharVector x = (VarCharVector) v;
            g = i -> StringData.fromBytes(x.get(i));
        } else if (v instanceof LargeVarCharVector) {
            LargeVarCharVector x = (LargeVarCharVector) v;
            g = i -> StringData.fromBytes(x.get(i));
        } else if (v instanceof ViewVarCharVector) {
            ViewVarCharVector x = (ViewVarCharVector) v;
            g = i -> StringData.fromBytes(x.get(i));
        } else if (v instanceof VarBinaryVector) {
            g = ((VarBinaryVector) v)::get;
        } else if (v instanceof LargeVarBinaryVector) {
            g = ((LargeVarBinaryVector) v)::get;
        } else if (v instanceof ViewVarBinaryVector) {
            g = ((ViewVarBinaryVector) v)::get;
        } else if (v instanceof FixedSizeBinaryVector) {
            g = ((FixedSizeBinaryVector) v)::get;
        } else if (v instanceof DecimalVector) {
            DecimalVector x = (DecimalVector) v;
            int p = x.getPrecision(), s = x.getScale();
            g = i -> DecimalData.fromBigDecimal(x.getObject(i), p, s);
        } else if (v instanceof DateDayVector) {
            g = ((DateDayVector) v)::get;
        } else if (v instanceof DateMilliVector) {
            DateMilliVector x = (DateMilliVector) v;
            g = i -> (int) Math.floorDiv(x.get(i), MILLIS_PER_DAY);
        } else if (v instanceof TimeSecVector) {
            TimeSecVector x = (TimeSecVector) v;
            g = i -> x.get(i) * 1000;
        } else if (v instanceof TimeMilliVector) {
            g = ((TimeMilliVector) v)::get;
        } else if (v instanceof TimeMicroVector) {
            TimeMicroVector x = (TimeMicroVector) v;
            g = i -> (int) Math.floorDiv(x.get(i), 1000L);
        } else if (v instanceof TimeNanoVector) {
            TimeNanoVector x = (TimeNanoVector) v;
            g = i -> (int) Math.floorDiv(x.get(i), 1_000_000L);
        } else if (v instanceof TimeStampVector) {
            TimeStampVector x = (TimeStampVector) v;
            TimeUnit u = ((ArrowType.Timestamp) x.getField().getType()).getUnit();
            g = i -> timestamp(x.get(i), u);
        } else if (v instanceof ListVector) {
            ListVector x = (ListVector) v;
            Getter item = getter(x.getDataVector());
            g =
                    i -> {
                        int from = x.getElementStartIndex(i), to = x.getElementEndIndex(i);
                        Object[] a = new Object[to - from];
                        for (int j = from; j < to; j++) {
                            a[j - from] = item.get(j);
                        }
                        return new GenericArrayData(a);
                    };
        } else if (v instanceof StructVector) {
            StructVector x = (StructVector) v;
            List<Getter> cs = new ArrayList<>();
            for (FieldVector c : x.getChildrenFromFields()) {
                cs.add(getter(c));
            }
            g =
                    i -> {
                        GenericRowData r = new GenericRowData(cs.size());
                        for (int c = 0; c < cs.size(); c++) {
                            r.setField(c, cs.get(c).get(i));
                        }
                        return r;
                    };
        } else {
            throw new IllegalArgumentException(
                    "openetl: column " + v.getName() + " is " + v.getField().getType() + ", which openetl-flink can't read");
        }
        Getter typed = g;
        return i -> v.isNull(i) ? null : typed.get(i);
    }

    static TimestampData timestamp(long v, TimeUnit u) {
        switch (u) {
            case SECOND:
                return TimestampData.fromEpochMillis(Math.multiplyExact(v, 1000L));
            case MILLISECOND:
                return TimestampData.fromEpochMillis(v);
            case MICROSECOND:
                return TimestampData.fromEpochMillis(
                        Math.floorDiv(v, 1000L), (int) Math.floorMod(v, 1000L) * 1000);
            default:
                return TimestampData.fromEpochMillis(
                        Math.floorDiv(v, 1_000_000L), (int) Math.floorMod(v, 1_000_000L));
        }
    }

    /** Builds an output's batches from its rows. */
    static final class Writer implements AutoCloseable {
        private final String output;
        private final Schema schema;
        private final RowType type;
        private final boolean changelog;
        private final BufferAllocator alloc;
        private final RowData.FieldGetter[] fields;
        private VectorSchemaRoot root;
        private Setter[] setters;
        private VarCharVector op;
        private int rows;

        Writer(String output, RowType type, boolean changelog, BufferAllocator alloc) {
            this.output = output;
            this.type = type;
            this.changelog = changelog;
            this.alloc = alloc;
            this.schema = ArrowTypes.arrow(type, changelog, "output " + output);
            this.fields = new RowData.FieldGetter[type.getFieldCount()];
            for (int i = 0; i < fields.length; i++) {
                fields[i] = RowData.createFieldGetter(type.getTypeAt(i), i);
            }
        }

        Schema schema() {
            return schema;
        }

        int rows() {
            return rows;
        }

        /**
         * Adds a row; false if its kind takes no row (an update's before-image). A delete on an
         * output that isn't a changelog fails.
         */
        boolean add(RowData r) {
            RowKind k = r.getRowKind();
            if (k == RowKind.UPDATE_BEFORE) {
                return false;
            }
            if (k == RowKind.DELETE && !changelog) {
                throw new IllegalStateException(
                        "openetl: output " + output + " isn't a changelog, and the job deleted a row from it");
            }
            if (root == null) {
                root = VectorSchemaRoot.create(schema, alloc);
                root.allocateNew();
                List<FieldVector> vs = root.getFieldVectors();
                setters = new Setter[fields.length];
                for (int i = 0; i < fields.length; i++) {
                    setters[i] = setter(vs.get(i), type.getTypeAt(i));
                }
                op = changelog ? (VarCharVector) vs.get(fields.length) : null;
            }
            int i = rows++;
            for (int c = 0; c < fields.length; c++) {
                setters[c].set(i, fields[c].getFieldOrNull(r));
            }
            if (op != null) {
                op.setSafe(i, (k == RowKind.INSERT ? "insert" : k == RowKind.DELETE ? "delete" : "update")
                        .getBytes(StandardCharsets.UTF_8));
            }
            return true;
        }

        /** The rows added since the last call, as an Arrow IPC stream holding one batch. */
        byte[] finish() throws IOException {
            root.setRowCount(rows);
            ByteArrayOutputStream b = new ByteArrayOutputStream();
            try (ArrowStreamWriter w = new ArrowStreamWriter(root, null, Channels.newChannel(b))) {
                w.start();
                w.writeBatch();
                w.end();
            }
            reset();
            return b.toByteArray();
        }

        private void reset() {
            if (root != null) {
                root.close();
                root = null;
            }
            rows = 0;
        }

        @Override
        public void close() {
            reset();
        }
    }

    interface Setter {
        /** Sets row i to v, Flink's internal value, or to null. */
        void set(int i, Object v);
    }

    static Setter setter(FieldVector v, LogicalType t) {
        Setter s;
        switch (t.getTypeRoot()) {
            case BOOLEAN:
                {
                    BitVector x = (BitVector) v;
                    s = (i, o) -> x.setSafe(i, (Boolean) o ? 1 : 0);
                    break;
                }
            case TINYINT:
                {
                    SmallIntVector x = (SmallIntVector) v;
                    s = (i, o) -> x.setSafe(i, (Byte) o);
                    break;
                }
            case SMALLINT:
                {
                    SmallIntVector x = (SmallIntVector) v;
                    s = (i, o) -> x.setSafe(i, (Short) o);
                    break;
                }
            case INTEGER:
                {
                    IntVector x = (IntVector) v;
                    s = (i, o) -> x.setSafe(i, (Integer) o);
                    break;
                }
            case BIGINT:
                {
                    BigIntVector x = (BigIntVector) v;
                    s = (i, o) -> x.setSafe(i, (Long) o);
                    break;
                }
            case FLOAT:
                {
                    Float4Vector x = (Float4Vector) v;
                    s = (i, o) -> x.setSafe(i, (Float) o);
                    break;
                }
            case DOUBLE:
                {
                    Float8Vector x = (Float8Vector) v;
                    s = (i, o) -> x.setSafe(i, (Double) o);
                    break;
                }
            case CHAR:
            case VARCHAR:
                {
                    VarCharVector x = (VarCharVector) v;
                    s = (i, o) -> x.setSafe(i, ((StringData) o).toBytes());
                    break;
                }
            case BINARY:
                if (v instanceof FixedSizeBinaryVector) {
                    FixedSizeBinaryVector x = (FixedSizeBinaryVector) v;
                    s = (i, o) -> x.setSafe(i, (byte[]) o);
                } else {
                    VarBinaryVector x = (VarBinaryVector) v;
                    s = (i, o) -> x.setSafe(i, (byte[]) o);
                }
                break;
            case VARBINARY:
                {
                    VarBinaryVector x = (VarBinaryVector) v;
                    s = (i, o) -> x.setSafe(i, (byte[]) o);
                    break;
                }
            case DECIMAL:
                {
                    DecimalVector x = (DecimalVector) v;
                    int scale = ((DecimalType) t).getScale();
                    s =
                            (i, o) -> {
                                BigDecimal d = ((DecimalData) o).toBigDecimal();
                                x.setSafe(i, d.scale() == scale ? d : d.setScale(scale));
                            };
                    break;
                }
            case DATE:
                {
                    DateDayVector x = (DateDayVector) v;
                    s = (i, o) -> x.setSafe(i, (Integer) o);
                    break;
                }
            case TIME_WITHOUT_TIME_ZONE:
                {
                    TimeMicroVector x = (TimeMicroVector) v;
                    s = (i, o) -> x.setSafe(i, (Integer) o * 1000L);
                    break;
                }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                {
                    TimeStampVector x = (TimeStampVector) v;
                    boolean micros = ((ArrowType.Timestamp) x.getField().getType()).getUnit() == TimeUnit.MICROSECOND;
                    s =
                            (i, o) -> {
                                TimestampData ts = (TimestampData) o;
                                long ms = ts.getMillisecond();
                                int nano = ts.getNanoOfMillisecond();
                                x.setSafe(
                                        i,
                                        micros
                                                ? Math.addExact(Math.multiplyExact(ms, 1000L), nano / 1000)
                                                : Math.addExact(Math.multiplyExact(ms, 1_000_000L), nano));
                            };
                    break;
                }
            case ARRAY:
                {
                    ListVector x = (ListVector) v;
                    LogicalType et = ((ArrayType) t).getElementType();
                    ArrayData.ElementGetter eg = ArrayData.createElementGetter(et);
                    Setter item = setter(x.getDataVector(), et);
                    s =
                            (i, o) -> {
                                ArrayData a = (ArrayData) o;
                                int start = x.startNewValue(i);
                                for (int j = 0; j < a.size(); j++) {
                                    item.set(start + j, eg.getElementOrNull(a, j));
                                }
                                x.endValue(i, a.size());
                            };
                    break;
                }
            case ROW:
                {
                    StructVector x = (StructVector) v;
                    RowType rt = (RowType) t;
                    List<FieldVector> cs = x.getChildrenFromFields();
                    RowData.FieldGetter[] gs = new RowData.FieldGetter[rt.getFieldCount()];
                    Setter[] ss = new Setter[gs.length];
                    for (int c = 0; c < gs.length; c++) {
                        gs[c] = RowData.createFieldGetter(rt.getTypeAt(c), c);
                        ss[c] = setter(cs.get(c), rt.getTypeAt(c));
                    }
                    s =
                            (i, o) -> {
                                RowData r = (RowData) o;
                                x.setIndexDefined(i);
                                for (int c = 0; c < gs.length; c++) {
                                    ss[c].set(i, gs[c].getFieldOrNull(r));
                                }
                            };
                    break;
                }
            default:
                throw new IllegalArgumentException("openetl: no Arrow writer for " + t);
        }
        Setter typed = s;
        return (i, o) -> {
            if (o == null) {
                setNull(v, i);
            } else {
                typed.set(i, o);
            }
        };
    }

    private static void setNull(FieldVector v, int i) {
        if (v instanceof BaseFixedWidthVector) {
            ((BaseFixedWidthVector) v).setNull(i);
        } else if (v instanceof BaseVariableWidthVector) {
            ((BaseVariableWidthVector) v).setNull(i);
        } else if (v instanceof BaseLargeVariableWidthVector) {
            ((BaseLargeVariableWidthVector) v).setNull(i);
        } else if (v instanceof BaseVariableWidthViewVector) {
            ((BaseVariableWidthViewVector) v).setNull(i);
        } else if (v instanceof ListVector) {
            ((ListVector) v).setNull(i);
        } else if (v instanceof StructVector) {
            ((StructVector) v).setNull(i);
        } else {
            throw new IllegalArgumentException("openetl: can't set a null in " + v.getField());
        }
    }
}
