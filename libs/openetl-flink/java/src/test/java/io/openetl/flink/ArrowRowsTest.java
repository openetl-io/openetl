package io.openetl.flink;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArrowRowsTest {
    private final BufferAllocator alloc = new RootAllocator();

    @AfterEach
    void close() {
        alloc.close();
    }

    private static Field f(String name, ArrowType t, boolean nullable, Field... children) {
        return new Field(name, new FieldType(nullable, t, null), Arrays.asList(children));
    }

    private static Field uuid(String name) {
        Map<String, String> md = new HashMap<>();
        md.put(ArrowTypes.EXTENSION_NAME, ArrowTypes.UUID);
        md.put(ArrowTypes.EXTENSION_METADATA, "");
        return new Field(name, new FieldType(false, new ArrowType.FixedSizeBinary(16), null, md), null);
    }

    /** What the engine's normalized batches hold, metadata columns included. */
    private static Schema everyType() {
        return new Schema(
                Arrays.asList(
                        f("b", ArrowType.Bool.INSTANCE, true),
                        f("i16", new ArrowType.Int(16, true), true),
                        f("i32", new ArrowType.Int(32, true), true),
                        f("i64", new ArrowType.Int(64, true), false),
                        f("f32", new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE), true),
                        f("f64", new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE), true),
                        f("s", ArrowType.Utf8.INSTANCE, true),
                        f("bin", ArrowType.Binary.INSTANCE, true),
                        f("dec", new ArrowType.Decimal(38, 9, 128), true),
                        f("d", new ArrowType.Date(DateUnit.DAY), true),
                        f("t", new ArrowType.Time(TimeUnit.MICROSECOND, 64), true),
                        f("ts", new ArrowType.Timestamp(TimeUnit.MICROSECOND, null), true),
                        f("tsz", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"), true),
                        f("tsn", new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC"), true),
                        f("list", ArrowType.List.INSTANCE, true, f("item", ArrowType.Utf8.INSTANCE, true)),
                        f("st", ArrowType.Struct.INSTANCE, true, f("x", new ArrowType.Int(32, true), true)),
                        uuid("_openetl_id")));
    }

    private static RowData row(RowKind k, int n) {
        GenericRowData r = new GenericRowData(k, 17);
        r.setField(0, n % 2 == 0);
        r.setField(1, (short) -n);
        r.setField(2, n * 1000);
        r.setField(3, (long) n << 40);
        r.setField(4, n / 4f);
        r.setField(5, n / 3d);
        r.setField(6, StringData.fromString("søme ☃ " + n));
        r.setField(7, new byte[] {1, 2, (byte) n});
        r.setField(8, DecimalData.fromBigDecimal(new BigDecimal("12345678901234567890.123456789").negate(), 38, 9));
        r.setField(9, -719162 + n);
        r.setField(10, 86_399_999 - n);
        r.setField(11, TimestampData.fromEpochMillis(-62_135_596_800_000L, 123_000));
        r.setField(12, TimestampData.fromEpochMillis(253_402_300_799_999L, 999_000));
        r.setField(13, TimestampData.fromEpochMillis(1_700_000_000_123L, 456_789));
        r.setField(14, new GenericArrayData(new Object[] {StringData.fromString("a"), null, StringData.fromString("c")}));
        r.setField(15, GenericRowData.of(n));
        r.setField(16, new byte[16]);
        return r;
    }

    private static GenericRowData nulls() {
        GenericRowData r = new GenericRowData(17);
        r.setField(3, 7L);
        r.setField(16, new byte[16]);
        return r;
    }

    @Test
    void everyTypeRoundTrips() throws Exception {
        Schema schema = everyType();
        RowType type = ArrowTypes.rowType(schema, "stream s");
        assertEquals(17, type.getFieldCount());
        assertEquals(new BinaryType(false, 16), type.getTypeAt(16));
        assertEquals(new TimeType(true, 6), type.getTypeAt(10));
        assertEquals(new TimestampType(true, 6), type.getTypeAt(11));
        assertEquals(new LocalZonedTimestampType(true, 6), type.getTypeAt(12));
        assertEquals(new LocalZonedTimestampType(true, 9), type.getTypeAt(13));

        List<RowData> rows = new ArrayList<>();
        for (int n = 0; n < 5; n++) {
            rows.add(row(RowKind.INSERT, n));
        }
        rows.add(nulls());
        byte[] ipc;
        try (ArrowRows.Writer w = new ArrowRows.Writer("o", type, false, alloc)) {
            for (RowData r : rows) {
                assertTrue(w.add(r));
            }
            assertTrue(ArrowTypes.sameShape(w.schema().getFields(), schema.getFields()), w.schema().toString());
            assertEquals(ArrowTypes.UUID, w.schema().getFields().get(16).getMetadata().get(ArrowTypes.EXTENSION_NAME));
            ipc = w.finish();
        }
        List<RowData> back = ArrowRows.read(new ByteArrayInputStream(ipc), alloc, schema, "s");
        assertEquals(rows.size(), back.size());
        for (int i = 0; i < rows.size(); i++) {
            for (int c = 0; c < 17; c++) {
                RowData.FieldGetter g = RowData.createFieldGetter(type.getTypeAt(c), c);
                Object want = g.getFieldOrNull(rows.get(i)), got = g.getFieldOrNull(back.get(i));
                if (want instanceof byte[]) {
                    assertTrue(Arrays.equals((byte[]) want, (byte[]) got), "row " + i + " column " + c);
                } else if (want instanceof GenericArrayData) {
                    assertEquals(Arrays.asList(((GenericArrayData) want).toObjectArray()), Arrays.asList(((GenericArrayData) got).toObjectArray()));
                } else {
                    assertEquals(want, got, "row " + i + " column " + c);
                }
            }
        }
    }

    @Test
    void timesKeepMillisecondsOnly() throws Exception {
        Schema s = new Schema(Collections.singletonList(f("t", new ArrowType.Time(TimeUnit.MICROSECOND, 64), true)));
        RowType type = ArrowTypes.rowType(s, "s");
        byte[] ipc;
        try (ArrowRows.Writer w = new ArrowRows.Writer("o", type, false, alloc)) {
            w.add(GenericRowData.of(1234));
            ipc = w.finish();
        }
        assertEquals(1234, ArrowRows.read(new ByteArrayInputStream(ipc), alloc, s, "s").get(0).getInt(0));
    }

    @Test
    void opsAreRowKinds() throws Exception {
        Schema s =
                new Schema(
                        Arrays.asList(
                                f("id", new ArrowType.Int(64, true), false),
                                f(ArrowTypes.OP, ArrowType.Utf8.INSTANCE, false)));
        RowType type = ArrowTypes.rowType(s, "s");
        assertEquals(Collections.singletonList("id"), type.getFieldNames());
        byte[] ipc;
        try (ArrowRows.Writer w = new ArrowRows.Writer("o", type, true, alloc)) {
            assertTrue(w.add(GenericRowData.ofKind(RowKind.INSERT, 1L)));
            assertFalse(w.add(GenericRowData.ofKind(RowKind.UPDATE_BEFORE, 2L)));
            assertTrue(w.add(GenericRowData.ofKind(RowKind.UPDATE_AFTER, 2L)));
            assertTrue(w.add(GenericRowData.ofKind(RowKind.DELETE, 3L)));
            assertEquals(ArrowTypes.OP, w.schema().getFields().get(1).getName());
            ipc = w.finish();
        }
        List<RowData> back = ArrowRows.read(new ByteArrayInputStream(ipc), alloc, s, "s");
        assertEquals(3, back.size());
        assertEquals(RowKind.INSERT, back.get(0).getRowKind());
        assertEquals(RowKind.UPDATE_AFTER, back.get(1).getRowKind());
        assertEquals(2L, back.get(1).getLong(0));
        assertEquals(RowKind.DELETE, back.get(2).getRowKind());
    }

    @Test
    void anOutputThatIsntAChangelogTakesNoDeletes() {
        RowType type = RowType.of(new BigIntType());
        try (ArrowRows.Writer w = new ArrowRows.Writer("o", type, false, alloc)) {
            assertTrue(w.add(GenericRowData.ofKind(RowKind.UPDATE_AFTER, 1L)));
            IllegalStateException e =
                    assertThrows(IllegalStateException.class, () -> w.add(GenericRowData.ofKind(RowKind.DELETE, 1L)));
            assertTrue(e.getMessage().contains("isn't a changelog"), e.getMessage());
        }
    }

    @Test
    void aChangelogOutputSetsTheOpItself() {
        RowType type = RowType.of(new VarCharType());
        RowType withOp =
                new RowType(Collections.singletonList(new RowType.RowField(ArrowTypes.OP, new VarCharType())));
        IllegalArgumentException e =
                assertThrows(IllegalArgumentException.class, () -> new ArrowRows.Writer("o", withOp, true, alloc));
        assertTrue(e.getMessage().contains("drop the column"), e.getMessage());
        new ArrowRows.Writer("o", withOp, false, alloc).close();
        new ArrowRows.Writer("o", type, true, alloc).close();
    }

    @Test
    void aBatchOfAnotherSchemaFails() throws Exception {
        Schema s = new Schema(Collections.singletonList(f("id", new ArrowType.Int(64, true), false)));
        Schema other = new Schema(Collections.singletonList(f("id", ArrowType.Utf8.INSTANCE, false)));
        byte[] ipc;
        try (ArrowRows.Writer w = new ArrowRows.Writer("o", ArrowTypes.rowType(other, "s"), false, alloc)) {
            w.add(GenericRowData.of(StringData.fromString("x")));
            ipc = w.finish();
        }
        Exception e =
                assertThrows(Exception.class, () -> ArrowRows.read(new ByteArrayInputStream(ipc), alloc, s, "s"));
        assertTrue(e.getMessage().contains("the job was built for"), e.getMessage());
    }

    @Test
    void anIntervalHasNoFlinkType() {
        Schema s =
                new Schema(Collections.singletonList(f("iv", new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO), true)));
        IllegalArgumentException e =
                assertThrows(IllegalArgumentException.class, () -> ArrowTypes.rowType(s, "stream s"));
        assertTrue(e.getMessage().contains("stream s, column iv"), e.getMessage());
    }

    @Test
    void aZonedTimestampMustBeUtc() {
        Schema s =
                new Schema(
                        Collections.singletonList(
                                f("ts", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "Asia/Kolkata"), true)));
        assertThrows(IllegalArgumentException.class, () -> ArrowTypes.rowType(s, "s"));
        assertInstanceOf(
                LocalZonedTimestampType.class,
                ArrowTypes.logical(f("ts", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "+00:00"), true), "s"));
    }
}
