package io.openetl.flink;

import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Arrow types as Flink's logical types and back. Each epoch's normalized types map one to one,
 * except that Flink keeps times of day in milliseconds, so a TIME loses its microseconds; JSON
 * columns become strings; and an interval has no Flink type.
 */
final class ArrowTypes {
    static final String OP = "_openetl_op";
    static final String EXTENSION_NAME = "ARROW:extension:name";
    static final String EXTENSION_METADATA = "ARROW:extension:metadata";
    static final String UUID = "arrow.uuid";

    private ArrowTypes() {}

    /**
     * An Arrow IPC stream holding only a schema. None, from a runner that hasn't seen the stream's
     * rows yet, is a schema without columns; the runner deploys the job again once it has one.
     */
    static Schema schema(byte[] ipc) throws IOException {
        if (ipc.length == 0) {
            return new Schema(Collections.emptyList());
        }
        return MessageSerializer.deserializeSchema(
                new ReadChannel(Channels.newChannel(new ByteArrayInputStream(ipc))));
    }

    /** A stream's row type: every column but _openetl_op, which becomes each row's kind. */
    static RowType rowType(Schema schema, String what) {
        List<RowType.RowField> fields = new ArrayList<>();
        for (Field f : schema.getFields()) {
            if (!f.getName().equals(OP)) {
                fields.add(new RowType.RowField(f.getName(), logical(f, what + ", column " + f.getName())));
            }
        }
        return new RowType(false, fields);
    }

    static LogicalType logical(Field f, String what) {
        ArrowType t = f.getType();
        boolean n = f.isNullable();
        if (t instanceof ArrowType.ExtensionType) {
            t = ((ArrowType.ExtensionType) t).storageType();
        }
        switch (t.getTypeID()) {
            case Bool:
                return new BooleanType(n);
            case Int:
                {
                    ArrowType.Int i = (ArrowType.Int) t;
                    if (i.getIsSigned()) {
                        switch (i.getBitWidth()) {
                            case 8:
                                return new TinyIntType(n);
                            case 16:
                                return new SmallIntType(n);
                            case 32:
                                return new IntType(n);
                            case 64:
                                return new BigIntType(n);
                            default:
                        }
                    }
                    break;
                }
            case FloatingPoint:
                {
                    FloatingPointPrecision p = ((ArrowType.FloatingPoint) t).getPrecision();
                    if (p == FloatingPointPrecision.SINGLE) {
                        return new FloatType(n);
                    }
                    if (p == FloatingPointPrecision.DOUBLE) {
                        return new DoubleType(n);
                    }
                    break;
                }
            case Utf8:
            case LargeUtf8:
            case Utf8View:
                return new VarCharType(n, VarCharType.MAX_LENGTH);
            case Binary:
            case LargeBinary:
            case BinaryView:
                return new VarBinaryType(n, VarBinaryType.MAX_LENGTH);
            case FixedSizeBinary:
                return new BinaryType(n, ((ArrowType.FixedSizeBinary) t).getByteWidth());
            case Decimal:
                {
                    ArrowType.Decimal d = (ArrowType.Decimal) t;
                    if (d.getBitWidth() == 128 && d.getPrecision() <= DecimalType.MAX_PRECISION) {
                        return new DecimalType(n, d.getPrecision(), d.getScale());
                    }
                    break;
                }
            case Date:
                return new DateType(n);
            case Time:
                return new TimeType(n, precision(((ArrowType.Time) t).getUnit()));
            case Timestamp:
                {
                    ArrowType.Timestamp ts = (ArrowType.Timestamp) t;
                    int p = precision(ts.getUnit());
                    if (ts.getTimezone() == null) {
                        return new TimestampType(n, p);
                    }
                    if (utc(ts.getTimezone())) {
                        return new LocalZonedTimestampType(n, p);
                    }
                    break;
                }
            case List:
                return new ArrayType(n, logical(f.getChildren().get(0), what + "'s items"));
            case Struct:
                {
                    List<RowType.RowField> fields = new ArrayList<>();
                    for (Field c : f.getChildren()) {
                        fields.add(new RowType.RowField(c.getName(), logical(c, what + "." + c.getName())));
                    }
                    return new RowType(n, fields);
                }
            default:
        }
        throw new IllegalArgumentException(
                "openetl: " + what + " is " + t + ", which has no Flink type openetl-flink maps it to");
    }

    private static boolean utc(String tz) {
        return tz.equals("UTC") || tz.equals("Etc/UTC") || tz.equals("+00:00") || tz.equals("Z");
    }

    private static int precision(TimeUnit u) {
        switch (u) {
            case SECOND:
                return 0;
            case MILLISECOND:
                return 3;
            case MICROSECOND:
                return 6;
            default:
                return 9;
        }
    }

    /** An output's Arrow schema; a changelog's last column is _openetl_op, from each row's kind. */
    static Schema arrow(RowType type, boolean changelog, String what) {
        List<Field> fields = new ArrayList<>();
        for (RowType.RowField f : type.getFields()) {
            if (changelog && f.getName().equals(OP)) {
                throw new IllegalArgumentException(
                        "openetl: "
                                + what
                                + " is a changelog, so the sink sets "
                                + OP
                                + " from each row's kind; drop the column");
            }
            fields.add(field(f.getName(), f.getType(), what + ", column " + f.getName()));
        }
        if (changelog) {
            fields.add(new Field(OP, FieldType.notNullable(ArrowType.Utf8.INSTANCE), null));
        }
        return new Schema(fields);
    }

    static Field field(String name, LogicalType t, String what) {
        boolean n = t.isNullable();
        Map<String, String> md = null;
        List<Field> children = null;
        ArrowType a;
        switch (t.getTypeRoot()) {
            case BOOLEAN:
                a = ArrowType.Bool.INSTANCE;
                break;
            case TINYINT:
            case SMALLINT:
                a = new ArrowType.Int(16, true);
                break;
            case INTEGER:
                a = new ArrowType.Int(32, true);
                break;
            case BIGINT:
                a = new ArrowType.Int(64, true);
                break;
            case FLOAT:
                a = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
                break;
            case DOUBLE:
                a = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
                break;
            case CHAR:
            case VARCHAR:
                a = ArrowType.Utf8.INSTANCE;
                break;
            case BINARY:
                if (((BinaryType) t).getLength() == 16) {
                    // What the platform's UUIDs read as.
                    a = new ArrowType.FixedSizeBinary(16);
                    md = new HashMap<>();
                    md.put(EXTENSION_NAME, UUID);
                    md.put(EXTENSION_METADATA, "");
                } else {
                    a = ArrowType.Binary.INSTANCE;
                }
                break;
            case VARBINARY:
                a = ArrowType.Binary.INSTANCE;
                break;
            case DECIMAL:
                {
                    DecimalType d = (DecimalType) t;
                    a = new ArrowType.Decimal(d.getPrecision(), d.getScale(), 128);
                    break;
                }
            case DATE:
                a = new ArrowType.Date(DateUnit.DAY);
                break;
            case TIME_WITHOUT_TIME_ZONE:
                a = new ArrowType.Time(TimeUnit.MICROSECOND, 64);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                a = new ArrowType.Timestamp(unit(((TimestampType) t).getPrecision()), null);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                a = new ArrowType.Timestamp(unit(((LocalZonedTimestampType) t).getPrecision()), "UTC");
                break;
            case ARRAY:
                a = ArrowType.List.INSTANCE;
                children =
                        Collections.singletonList(
                                field("item", ((ArrayType) t).getElementType(), what + "'s items"));
                break;
            case ROW:
                {
                    a = ArrowType.Struct.INSTANCE;
                    children = new ArrayList<>();
                    for (RowType.RowField c : ((RowType) t).getFields()) {
                        children.add(field(c.getName(), c.getType(), what + "." + c.getName()));
                    }
                    break;
                }
            default:
                throw new IllegalArgumentException(
                        "openetl: " + what + " is " + t + ", which has no Arrow type openetl-flink maps it to");
        }
        return new Field(name, new FieldType(n, a, null, md), children);
    }

    private static TimeUnit unit(int precision) {
        return precision <= 6 ? TimeUnit.MICROSECOND : TimeUnit.NANOSECOND;
    }

    /** The same columns in the same order, with the same types; metadata and nullability aside. */
    static boolean sameShape(List<Field> a, List<Field> b) {
        if (a.size() != b.size()) {
            return false;
        }
        for (int i = 0; i < a.size(); i++) {
            Field x = a.get(i), y = b.get(i);
            if (!x.getName().equals(y.getName())
                    || !Objects.equals(storage(x.getType()), storage(y.getType()))
                    || !sameShape(x.getChildren(), y.getChildren())) {
                return false;
            }
        }
        return true;
    }

    private static ArrowType storage(ArrowType t) {
        return t instanceof ArrowType.ExtensionType ? ((ArrowType.ExtensionType) t).storageType() : t;
    }
}
