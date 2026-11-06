package io.openetl.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** The shaded jar's own classes, with their relocated gRPC, protobuf and Arrow, against a runner. */
class ShadedJarIT {
    private static File jar() {
        File[] jars =
                new File("target").listFiles((d, n) -> n.startsWith("openetl-flink-") && n.endsWith(".jar"));
        assertNotNull(jars);
        assertEquals(1, jars.length, Arrays.toString(jars));
        return jars[0];
    }

    /** Loads openetl's classes from the jar first, everything else from the test's classpath. */
    private static final class Shaded extends URLClassLoader {
        Shaded(File jar) throws Exception {
            super(new URL[] {jar.toURI().toURL()}, ShadedJarIT.class.getClassLoader());
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (!name.startsWith("io.openetl.")) {
                return super.loadClass(name, resolve);
            }
            synchronized (getClassLoadingLock(name)) {
                Class<?> c = findLoadedClass(name);
                return c != null ? c : findClass(name);
            }
        }
    }

    @Test
    void describesThePipelineAndConvertsRows() throws Exception {
        try (FakeRunner fake = new FakeRunner(Arrays.asList("a", "b"), Collections.singleton("a"));
                Shaded cl = new Shaded(jar())) {
            BridgeConfig c = fake.config();
            Class<?> config = cl.loadClass("io.openetl.flink.BridgeConfig");
            Constructor<?> ctor = config.getDeclaredConstructor(String.class, String.class, String.class);
            ctor.setAccessible(true);
            Object shadedConfig = ctor.newInstance(c.host + ":" + c.port, c.fingerprint, c.token);

            Class<?> openetl = cl.loadClass("io.openetl.flink.OpenEtl");
            assertFalse(openetl == OpenEtl.class);
            Method connect = openetl.getDeclaredMethod("connect", StreamExecutionEnvironment.class, config, String[].class);
            connect.setAccessible(true);
            Object o = connect.invoke(null, StreamExecutionEnvironment.getExecutionEnvironment(), shadedConfig, new String[0]);
            assertEquals(Arrays.asList("a", "b"), openetl.getMethod("streamNames").invoke(o));
            RowType type = (RowType) openetl.getMethod("rowType", String.class).invoke(o, "a");

            Class<?> memory = cl.loadClass("io.openetl.flink.Memory");
            Method child = memory.getDeclaredMethod("child", String.class);
            child.setAccessible(true);
            Object alloc = child.invoke(null, "shaded");
            Class<?> writer = cl.loadClass("io.openetl.flink.ArrowRows$Writer");
            Constructor<?> wc = writer.getDeclaredConstructor(
                    String.class, RowType.class, boolean.class,
                    cl.loadClass("io.openetl.flink.shaded.org.apache.arrow.memory.BufferAllocator"));
            wc.setAccessible(true);
            Object w = wc.newInstance("a", type, true, alloc);
            Method add = writer.getDeclaredMethod("add", RowData.class);
            add.setAccessible(true);
            add.invoke(w, GenericRowData.of(1L, null, org.apache.flink.table.data.TimestampData.fromEpochMillis(1)));
            Method finish = writer.getDeclaredMethod("finish");
            finish.setAccessible(true);
            byte[] ipc = (byte[]) finish.invoke(w);

            Method schema = writer.getDeclaredMethod("schema");
            schema.setAccessible(true);
            Class<?> rows = cl.loadClass("io.openetl.flink.ArrowRows");
            Method read = null;
            for (Method m : rows.getDeclaredMethods()) {
                if (m.getName().equals("read")) {
                    read = m;
                }
            }
            assertNotNull(read);
            read.setAccessible(true);
            @SuppressWarnings("unchecked")
            List<RowData> back =
                    (List<RowData>) read.invoke(null, new ByteArrayInputStream(ipc), alloc, schema.invoke(w), "a");
            assertEquals(1, back.size());
            assertEquals(1L, back.get(0).getLong(0));
        }
    }
}
