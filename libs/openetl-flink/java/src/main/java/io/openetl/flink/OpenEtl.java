package io.openetl.flink;

import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.openetl.bridge.v1.BridgeServiceGrpc;
import io.openetl.bridge.v1.DescribeRequest;
import io.openetl.bridge.v1.DescribeResponse;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A continuous pipeline's streams and outputs, for building the Flink job that transforms them.
 *
 * <pre>{@code
 * StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 * OpenEtl openetl = OpenEtl.fromArgs(env, args);
 * DataStream<RowData> payments = openetl.stream("payments");
 * openetl.sink("scored_payments", payments.map(new Score(), payments.getType()));
 * env.execute("fraud");
 * }</pre>
 *
 * <p>Rows are {@link RowData} in each stream's {@link RowType}. A changelog's {@code _openetl_op}
 * is each row's {@link org.apache.flink.types.RowKind}: insert, update-after and delete. Every
 * stream of the pipeline is read, whether the job uses it or not.
 */
public final class OpenEtl {
    public static final String BRIDGE = "--openetl.bridge";
    public static final String FINGERPRINT = "--openetl.fingerprint";
    public static final String TOKEN = "--openetl.token";

    private final StreamExecutionEnvironment env;
    private final BridgeConfig bridge;
    private final String[] args;
    private final String pipeline;
    private final Map<String, RowType> streams = new TreeMap<>();
    private final Map<String, byte[]> schemas = new TreeMap<>();
    private final Map<String, Boolean> outputs = new TreeMap<>();
    private Map<String, DataStream<RowData>> built;

    private OpenEtl(StreamExecutionEnvironment env, BridgeConfig bridge, String[] args, DescribeResponse d) {
        this.env = env;
        this.bridge = bridge;
        this.args = args;
        this.pipeline = d.getPipeline();
        for (io.openetl.bridge.v1.Schema s : d.getStreamsList()) {
            byte[] ipc = s.getArrow().toByteArray();
            try {
                streams.put(s.getName(), ArrowTypes.rowType(ArrowTypes.schema(ipc), "stream " + s.getName()));
            } catch (IOException e) {
                throw new IllegalStateException("openetl: stream " + s.getName() + "'s schema", e);
            }
            schemas.put(s.getName(), ipc);
        }
        for (io.openetl.bridge.v1.Schema s : d.getOutputsList()) {
            outputs.put(s.getName(), s.getChangelog());
        }
    }

    /**
     * Reads the bridge's address, fingerprint and token from the arguments the runner gave the
     * job ({@value #BRIDGE}, {@value #FINGERPRINT} and {@value #TOKEN}, each followed by its value
     * or joined to it by "="), and asks the runner for the pipeline's streams and outputs.
     */
    public static OpenEtl fromArgs(StreamExecutionEnvironment env, String[] args) {
        Map<String, String> ours = new TreeMap<>();
        List<String> rest = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            String key = a, value = null;
            int eq = a.indexOf('=');
            if (eq > 0) {
                key = a.substring(0, eq);
                value = a.substring(eq + 1);
            }
            if (!key.equals(BRIDGE) && !key.equals(FINGERPRINT) && !key.equals(TOKEN)) {
                rest.add(a);
                continue;
            }
            if (value == null) {
                if (i + 1 == args.length) {
                    throw new IllegalArgumentException("openetl: " + key + " has no value");
                }
                value = args[++i];
            }
            ours.put(key, value);
        }
        for (String k : new String[] {BRIDGE, FINGERPRINT, TOKEN}) {
            if (!ours.containsKey(k)) {
                throw new IllegalArgumentException(
                        "openetl: the job's arguments have no " + k + ", which the runner passes when it deploys the job");
            }
        }
        return connect(
                env, new BridgeConfig(ours.get(BRIDGE), ours.get(FINGERPRINT), ours.get(TOKEN)), rest.toArray(new String[0]));
    }

    static OpenEtl connect(StreamExecutionEnvironment env, BridgeConfig bridge, String[] rest) {
        ManagedChannel ch = BridgeClient.open(bridge);
        try {
            DescribeResponse d = BridgeServiceGrpc.newBlockingStub(ch).describe(DescribeRequest.getDefaultInstance());
            return new OpenEtl(env, bridge, rest, d);
        } catch (StatusRuntimeException e) {
            throw new IllegalStateException(BridgeClient.failure("asking the runner for the pipeline", e).getMessage(), e);
        } finally {
            ch.shutdownNow();
        }
    }

    /** The job's arguments without openetl's. */
    public String[] args() {
        return args.clone();
    }

    public String pipeline() {
        return pipeline;
    }

    /** The pipeline's streams, sorted. */
    public List<String> streamNames() {
        return Collections.unmodifiableList(new ArrayList<>(streams.keySet()));
    }

    /** The outputs the job may write, sorted. */
    public List<String> outputNames() {
        return Collections.unmodifiableList(new ArrayList<>(outputs.keySet()));
    }

    /** A stream's row type: its columns but {@code _openetl_op}, which is each row's kind. */
    public RowType rowType(String stream) {
        RowType t = streams.get(stream);
        if (t == null) {
            throw new IllegalArgumentException("openetl: the pipeline has no stream " + stream + "; it has " + streams.keySet());
        }
        return t;
    }

    /** One stream's rows. */
    public DataStream<RowData> stream(String name) {
        rowType(name);
        return streams().get(name);
    }

    /** Every stream's rows, by name. One source reads them all, so asking again returns the same streams. */
    public Map<String, DataStream<RowData>> streams() {
        if (built != null) {
            return built;
        }
        List<String> names = new ArrayList<>(streams.keySet());
        List<byte[]> ipc = new ArrayList<>();
        RowType[] types = new RowType[names.size()];
        List<OutputTag<RowData>> tags = new ArrayList<>();
        for (int i = 0; i < names.size(); i++) {
            ipc.add(schemas.get(names.get(i)));
            types[i] = streams.get(names.get(i));
            tags.add(new OutputTag<>("openetl-" + names.get(i), InternalTypeInfo.of(types[i])));
        }
        SingleOutputStreamOperator<Void> split =
                env.fromSource(new OpenEtlSource(bridge, names, ipc, types), WatermarkStrategy.noWatermarks(), "openetl source")
                        .uid("openetl-source")
                        .process(new Splitter(tags), Types.VOID)
                        .name("openetl streams")
                        .uid("openetl-streams");
        Map<String, DataStream<RowData>> out = new LinkedHashMap<>();
        for (int i = 0; i < names.size(); i++) {
            out.put(names.get(i), split.getSideOutput(tags.get(i)));
        }
        built = Collections.unmodifiableMap(out);
        return built;
    }

    /** Writes rows to an output; their type must be an {@link InternalTypeInfo} of a row type. */
    public SingleOutputStreamOperator<Void> sink(String output, DataStream<RowData> rows) {
        if (!(rows.getType() instanceof InternalTypeInfo)) {
            throw new IllegalArgumentException(
                    "openetl: output " + output + "'s rows have type " + rows.getType() + "; give their RowType");
        }
        LogicalType t = ((InternalTypeInfo<?>) rows.getType()).toLogicalType();
        if (!(t instanceof RowType)) {
            throw new IllegalArgumentException("openetl: output " + output + "'s rows are " + t + ", not rows");
        }
        return sink(output, rows, (RowType) t);
    }

    /** Writes rows of the given type to an output. */
    public SingleOutputStreamOperator<Void> sink(String output, DataStream<RowData> rows, RowType type) {
        Boolean changelog = outputs.get(output);
        if (changelog == null) {
            throw new IllegalArgumentException(
                    "openetl: the pipeline has no output " + output + "; it has " + outputs.keySet());
        }
        // Checks the type maps to Arrow before the job runs.
        ArrowTypes.arrow(type, changelog, "output " + output);
        return rows.transform("openetl sink " + output, Types.VOID, new OpenEtlSink(bridge, output, type, changelog))
                .uid("openetl-sink-" + output);
    }

    private static final class Splitter extends ProcessFunction<TaggedRow, Void> {
        private static final long serialVersionUID = 1L;

        private final List<OutputTag<RowData>> tags;

        Splitter(List<OutputTag<RowData>> tags) {
            this.tags = tags;
        }

        @Override
        public void processElement(TaggedRow r, Context ctx, Collector<Void> out) {
            ctx.output(tags.get(r.stream), r.row);
        }
    }
}
