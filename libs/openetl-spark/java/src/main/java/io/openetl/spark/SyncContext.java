package io.openetl.spark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * What a post-sync Spark application learns of the sync that ran before it: the records the sync
 * committed, and each stream's table and rows.
 *
 * <pre>{@code
 * SyncContext ctx = SyncContext.load(spark);
 * for (long id : ctx.snapshots(spark, "lake.shop.orders")) { ... }
 * }</pre>
 */
public final class SyncContext {
  /** The Spark property that names the context document. */
  public static final String CONF = "spark.openetl.context";

  /** The document's protocol version this library reads. */
  public static final int VERSION = 1;

  private static final ObjectMapper JSON = new ObjectMapper();

  private final String pipeline;
  private final String sync;
  private final String connector;
  private final String namespace;
  private final Ref after;
  private final Ref through;
  private final List<Stream> streams;

  private SyncContext(
      String pipeline,
      String sync,
      String connector,
      String namespace,
      Ref after,
      Ref through,
      List<Stream> streams) {
    this.pipeline = pipeline;
    this.sync = sync;
    this.connector = connector;
    this.namespace = namespace;
    this.after = after;
    this.through = through;
    this.streams = Collections.unmodifiableList(streams);
  }

  /** Reads the document that {@link #CONF} names, through Hadoop's file systems. */
  public static SyncContext load(SparkSession spark) throws IOException {
    String uri = spark.conf().get(CONF, "");
    if (uri.isEmpty()) {
      throw new IllegalStateException(CONF + " isn't set: this application isn't an openetl post-sync step");
    }
    Path path = new Path(uri);
    FileSystem fs = path.getFileSystem(spark.sparkContext().hadoopConfiguration());
    try (InputStream in = fs.open(path)) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] buf = new byte[8192];
      for (int n; (n = in.read(buf)) > 0; ) {
        out.write(buf, 0, n);
      }
      return parse(new String(out.toByteArray(), StandardCharsets.UTF_8));
    }
  }

  public static SyncContext parse(String json) throws IOException {
    JsonNode root = JSON.readTree(json);
    int version = root.path("version").asInt();
    if (version != VERSION) {
      throw new IOException("the sync's context is version " + version + ", and this openetl-spark reads " + VERSION);
    }
    List<Stream> streams = new ArrayList<>();
    for (JsonNode s : root.path("streams")) {
      streams.add(new Stream(s.path("name").asText(), s.path("table").asText(""), s.path("rows").asLong()));
    }
    JsonNode dst = root.path("destination");
    return new SyncContext(
        root.path("pipeline").asText(),
        root.path("sync").asText(),
        dst.path("connector").asText(),
        dst.path("namespace").asText(""),
        Ref.of(root.get("after")),
        Ref.of(root.get("through")),
        streams);
  }

  public String pipeline() {
    return pipeline;
  }

  public String sync() {
    return sync;
  }

  /** The destination's connector, such as postgres or iceberg. */
  public String destinationConnector() {
    return connector;
  }

  /** The schema or namespace the pipeline writes to; empty if it has none. */
  public String destinationNamespace() {
    return namespace;
  }

  /** The record the sync started after; null if the checkpoint log doesn't say. */
  public Ref after() {
    return after;
  }

  /** The last record the sync committed; equal to {@link #after()} if it committed none. */
  public Ref through() {
    return through;
  }

  public List<Stream> streams() {
    return streams;
  }

  /**
   * Whether this sync committed the snapshot with this summary: one of the pipeline's whose epoch
   * is after {@link #after()} and through {@link #through()}. destination-iceberg writes the
   * openetl.pipeline, openetl.generation and openetl.epoch keys it reads.
   */
  public boolean committed(Map<String, String> summary) {
    if (summary == null || !pipeline.equals(summary.get("openetl.pipeline"))) {
      return false;
    }
    Ref r;
    try {
      r = new Ref(Long.parseLong(summary.get("openetl.epoch")), Long.parseLong(summary.get("openetl.generation")));
    } catch (NumberFormatException e) {
      return false;
    }
    return (after == null || r.compareTo(after) > 0) && r.compareTo(through) <= 0;
  }

  /** The IDs of an Iceberg table's snapshots this sync committed, from its snapshots table. */
  public List<Long> snapshots(SparkSession spark, String table) {
    List<Long> ids = new ArrayList<>();
    for (Row row : spark.sql("SELECT snapshot_id, summary FROM " + table + ".snapshots").collectAsList()) {
      if (committed(row.<String, String>getJavaMap(1))) {
        ids.add(row.getLong(0));
      }
    }
    return ids;
  }

  /** A checkpoint record: an epoch, and the generation that wrote it. */
  public static final class Ref implements Comparable<Ref> {
    private final long epoch;
    private final long generation;

    public Ref(long epoch, long generation) {
      this.epoch = epoch;
      this.generation = generation;
    }

    static Ref of(JsonNode n) {
      if (n == null || n.isNull()) {
        return null;
      }
      return new Ref(n.path("epoch").asLong(), n.path("generation").asLong());
    }

    public long epoch() {
      return epoch;
    }

    public long generation() {
      return generation;
    }

    @Override
    public int compareTo(Ref o) {
      int c = Long.compare(epoch, o.epoch);
      return c != 0 ? c : Long.compare(generation, o.generation);
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof Ref && compareTo((Ref) o) == 0;
    }

    @Override
    public int hashCode() {
      return Long.hashCode(epoch) * 31 + Long.hashCode(generation);
    }

    @Override
    public String toString() {
      return epoch + "-g" + generation;
    }
  }

  /** One of the pipeline's streams, with what the sync read of it. */
  public static final class Stream {
    private final String name;
    private final String table;
    private final long rows;

    Stream(String name, String table, long rows) {
      this.name = name;
      this.table = table;
      this.rows = rows;
    }

    public String name() {
      return name;
    }

    /** The destination's table; empty if the checkpoint log doesn't name it. */
    public String table() {
      return table;
    }

    /** The rows the source emitted in the sync, quarantined ones included. */
    public long rows() {
      return rows;
    }
  }
}
