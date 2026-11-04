package io.openetl.flink.examples;

import io.openetl.flink.OpenEtl;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;

/**
 * Writes each stream to an output unchanged: to the output of the same name, or as {@code
 * --outputs stream=output,...} maps them. Every output gets a stream.
 */
public final class Passthrough {
    private Passthrough() {}

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        OpenEtl openetl = OpenEtl.fromArgs(env, args);
        build(openetl, openetl.args());
        env.execute("openetl passthrough " + openetl.pipeline());
    }

    public static void build(OpenEtl openetl, String[] args) {
        Map<String, String> outputs = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            String list = null;
            if (a.startsWith("--outputs=")) {
                list = a.substring("--outputs=".length());
            } else if (a.equals("--outputs") && i + 1 < args.length) {
                list = args[++i];
            }
            if (list == null) {
                continue;
            }
            for (String pair : list.split(",")) {
                String[] kv = pair.split("=", 2);
                if (kv.length != 2) {
                    throw new IllegalArgumentException("--outputs takes stream=output pairs, not " + pair);
                }
                outputs.put(kv[0], kv[1]);
            }
        }
        Map<String, String> unwritten = new HashMap<>();
        for (String o : openetl.outputNames()) {
            unwritten.put(o, o);
        }
        for (String s : openetl.streamNames()) {
            String o = outputs.getOrDefault(s, openetl.outputNames().contains(s) ? s : null);
            if (o != null) {
                openetl.sink(o, openetl.stream(s));
                unwritten.remove(o);
            }
        }
        if (!unwritten.isEmpty()) {
            throw new IllegalArgumentException("openetl: no stream is written to outputs " + unwritten.keySet());
        }
    }
}
