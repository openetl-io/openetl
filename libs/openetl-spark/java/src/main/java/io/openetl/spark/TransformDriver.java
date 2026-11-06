package io.openetl.spark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The driver of a pipeline's Spark transform: the main class the runner submits, once per sync
 * attempt. It transforms each epoch the runner stages below {@link #ROOT}, in order, until the
 * runner writes {@code _end} or stops rewriting {@code _alive}.
 *
 * <p>Below the root: {@code {N}/in/_ready.json} lists epoch N's inputs; the driver writes each
 * output to {@code {N}/out/{k}/} and then lists the files in {@code {N}/out/_done.json}. A
 * failure of the user's code goes to {@code _failed.json}.
 */
public final class TransformDriver {
  /** The attempt's directory, ending in a slash. */
  public static final String ROOT = "spark.openetl.root";
  /** The class implementing {@link Transform}. */
  public static final String TRANSFORM = "spark.openetl.transform";
  /** The protocol the runner speaks. */
  public static final String PROTOCOL = "spark.openetl.protocol";
  /** The protocol's major version this library speaks. */
  public static final int VERSION = 1;

  private static final ObjectMapper JSON = new ObjectMapper();
  private static final Logger LOG = LoggerFactory.getLogger(TransformDriver.class);

  private final SparkSession spark;
  private final String root;
  private final FileSystem fs;
  private final long pollMillis;
  private final long staleMillis;
  private final Set<Long> finished = new HashSet<>();

  public TransformDriver(SparkSession spark, String root) throws IOException {
    this(spark, root, 1000, 5 * 60 * 1000);
  }

  TransformDriver(SparkSession spark, String root, long pollMillis, long staleMillis) throws IOException {
    this.spark = spark;
    this.root = root.endsWith("/") ? root : root + "/";
    this.fs = new Path(this.root).getFileSystem(spark.sparkContext().hadoopConfiguration());
    this.pollMillis = pollMillis;
    this.staleMillis = staleMillis;
  }

  public static void main(String[] args) throws Exception {
    SparkSession spark = SparkSession.builder().getOrCreate();
    try {
      run(spark);
    } finally {
      spark.stop();
    }
  }

  /** Runs the transform that the session's properties name. */
  static void run(SparkSession spark) throws Exception {
    String root = spark.conf().get(ROOT, "");
    if (root.isEmpty()) {
      throw new IllegalStateException(ROOT + " isn't set: this application isn't an openetl transform");
    }
    TransformDriver driver = new TransformDriver(spark, root);
    Transform transform;
    try {
      checkProtocol(spark.conf().get(PROTOCOL, ""));
      transform = load(spark.conf().get(TRANSFORM, ""));
    } catch (Exception | LinkageError e) {
      driver.fail(0, e);
      throw e;
    }
    driver.run(transform);
  }

  static void checkProtocol(String protocol) {
    String major = protocol.split("\\.", 2)[0];
    if (!major.equals(Integer.toString(VERSION))) {
      throw new IllegalStateException(
          "the runner speaks protocol " + (protocol.isEmpty() ? "(none)" : protocol) + ", and this openetl-spark speaks " + VERSION);
    }
  }

  static Transform load(String name) throws ReflectiveOperationException {
    if (name.isEmpty()) {
      throw new IllegalStateException(TRANSFORM + " isn't set: it names the class that implements io.openetl.spark.Transform");
    }
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    if (loader == null) {
      loader = TransformDriver.class.getClassLoader();
    }
    Class<?> c = Class.forName(name, true, loader);
    if (!Transform.class.isAssignableFrom(c)) {
      throw new IllegalStateException(name + " doesn't implement io.openetl.spark.Transform");
    }
    return (Transform) c.getDeclaredConstructor().newInstance();
  }

  /**
   * Transforms each epoch as it's ready, until {@code _end} exists or {@code _alive} hasn't
   * changed for the staleness timeout. A failure of the user's code is written to {@code
   * _failed.json} and thrown.
   */
  public void run(Transform transform) throws Exception {
    boolean looked = false;
    long alive = 0;
    long changed = 0;
    while (true) {
      if (fs.exists(path("_end"))) {
        LOG.info("the runner ended the attempt");
        return;
      }
      long modified = modified(path("_alive"));
      long now = System.nanoTime() / 1_000_000;
      if (!looked || modified != alive) {
        looked = true;
        alive = modified;
        changed = now;
      } else if (now - changed >= staleMillis) {
        LOG.warn("_alive hasn't changed for {} ms, so the runner is gone; exiting", now - changed);
        return;
      }
      Long n = next();
      if (n != null) {
        epoch(transform, n);
        finished.add(n);
        continue;
      }
      Thread.sleep(pollMillis);
    }
  }

  /** The first epoch that's ready and not done, in epoch order; null if none. */
  private Long next() throws IOException {
    FileStatus[] children;
    try {
      children = fs.listStatus(new Path(root));
    } catch (FileNotFoundException e) {
      return null;
    }
    TreeSet<Long> epochs = new TreeSet<>();
    for (FileStatus c : children) {
      String name = c.getPath().getName();
      if (c.isDirectory() && digits(name)) {
        epochs.add(Long.parseLong(name));
      }
    }
    for (long n : epochs) {
      if (!finished.contains(n) && fs.exists(path(n + "/in/_ready.json")) && !fs.exists(path(n + "/out/_done.json"))) {
        return n;
      }
    }
    return null;
  }

  private static boolean digits(String name) {
    if (name.isEmpty() || name.length() > 18) {
      return false;
    }
    for (int i = 0; i < name.length(); i++) {
      if (name.charAt(i) < '0' || name.charAt(i) > '9') {
        return false;
      }
    }
    return true;
  }

  private void epoch(Transform transform, long n) throws Exception {
    LOG.info("transforming epoch {}", n);
    Ready ready;
    try {
      ready = Ready.parse(read(path(n + "/in/_ready.json")), n);
    } catch (RuntimeException | com.fasterxml.jackson.core.JsonProcessingException e) {
      fail(n, e);
      throw e;
    }
    Map<String, Dataset<Row>> inputs = new LinkedHashMap<>();
    for (Input in : ready.inputs) {
      inputs.put(in.name, read(n, in));
    }
    EpochContext ctx = new EpochContext(ready.generation, n, ready.outputs);
    ArrayNode outputs = JSON.createArrayNode();
    try {
      Map<String, Dataset<Row>> out = transform.apply(spark, Collections.unmodifiableMap(inputs), ctx);
      checkOutputs(ready.outputs, out);
      for (int k = 0; k < ready.outputs.size(); k++) {
        out.get(ready.outputs.get(k)).write().mode(SaveMode.Overwrite).parquet(uri(n + "/out/" + k));
      }
    } catch (Exception | LinkageError e) {
      fail(n, e);
      throw e;
    }
    for (int k = 0; k < ready.outputs.size(); k++) {
      ObjectNode o = outputs.addObject();
      o.put("name", ready.outputs.get(k));
      ArrayNode files = o.putArray("files");
      for (String f : parts(n + "/out/" + k)) {
        files.add("out/" + k + "/" + f);
      }
    }
    ObjectNode done = JSON.createObjectNode();
    done.put("version", VERSION);
    done.put("epoch", n);
    done.set("outputs", outputs);
    write(path(n + "/out/_done.json"), JSON.writeValueAsString(done));
    LOG.info("epoch {} done", n);
  }

  private Dataset<Row> read(long n, Input in) {
    if (!in.files.isEmpty()) {
      DataFrameReader r = spark.read();
      if (in.schema != null) {
        r = r.schema(in.schema);
      }
      String[] uris = new String[in.files.size()];
      for (int i = 0; i < uris.length; i++) {
        uris[i] = uri(n + "/" + in.files.get(i));
      }
      return r.parquet(uris);
    }
    if (in.schema == null) {
      return spark.emptyDataFrame();
    }
    return spark.createDataFrame(Collections.<Row>emptyList(), in.schema);
  }

  private static void checkOutputs(List<String> declared, Map<String, Dataset<Row>> out) {
    List<String> missing = new ArrayList<>();
    for (String name : declared) {
      if (out == null || out.get(name) == null) {
        missing.add(name);
      }
    }
    List<String> extra = new ArrayList<>();
    if (out != null) {
      for (String name : out.keySet()) {
        if (!declared.contains(name)) {
          extra.add(name);
        }
      }
    }
    if (!missing.isEmpty() || !extra.isEmpty()) {
      StringBuilder msg = new StringBuilder("the transform's outputs are " + declared);
      if (!missing.isEmpty()) {
        msg.append(", and it returned none for ").append(missing);
      }
      if (!extra.isEmpty()) {
        msg.append(missing.isEmpty() ? ", and" : ", and also").append(" returned undeclared ").append(extra);
      }
      throw new IllegalStateException(msg.toString());
    }
  }

  /** The names of the Parquet files Spark wrote in rel, sorted. */
  private List<String> parts(String rel) throws IOException {
    List<String> names = new ArrayList<>();
    FileStatus[] files;
    try {
      files = fs.listStatus(path(rel));
    } catch (FileNotFoundException e) {
      return names;
    }
    for (FileStatus f : files) {
      String name = f.getPath().getName();
      if (f.isFile() && name.startsWith("part-") && name.endsWith(".parquet")) {
        names.add(name);
      }
    }
    Collections.sort(names);
    return names;
  }

  /** Writes the failure to _failed.json. */
  void fail(long epoch, Throwable e) {
    StringWriter trace = new StringWriter();
    e.printStackTrace(new PrintWriter(trace));
    ObjectNode doc = JSON.createObjectNode();
    doc.put("version", VERSION);
    doc.put("epoch", epoch);
    doc.put("error", e.toString());
    doc.put("trace", trace.toString());
    try {
      write(path("_failed.json"), JSON.writeValueAsString(doc));
    } catch (IOException w) {
      LOG.error("writing _failed.json failed", w);
      e.addSuppressed(w);
    }
  }

  private String uri(String rel) {
    return root + rel;
  }

  private Path path(String rel) {
    return new Path(uri(rel));
  }

  private long modified(Path p) throws IOException {
    try {
      return fs.getFileStatus(p).getModificationTime();
    } catch (FileNotFoundException e) {
      return -1;
    }
  }

  private String read(Path p) throws IOException {
    try (InputStream in = fs.open(p)) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] buf = new byte[8192];
      for (int n; (n = in.read(buf)) > 0; ) {
        out.write(buf, 0, n);
      }
      return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }
  }

  private void write(Path p, String text) throws IOException {
    try (OutputStream o = fs.create(p, true)) {
      o.write(text.getBytes(StandardCharsets.UTF_8));
    }
  }

  /** An epoch's _ready.json. */
  private static final class Ready {
    final long generation;
    final List<Input> inputs;
    final List<String> outputs;

    private Ready(long generation, List<Input> inputs, List<String> outputs) {
      this.generation = generation;
      this.inputs = inputs;
      this.outputs = outputs;
    }

    static Ready parse(String json, long n) throws IOException {
      JsonNode doc = JSON.readTree(json);
      int version = doc.path("version").asInt();
      if (version != VERSION) {
        throw new IllegalStateException("epoch " + n + "'s _ready.json is version " + version + ", and this openetl-spark reads " + VERSION);
      }
      if (doc.path("epoch").asLong() != n) {
        throw new IllegalStateException("epoch " + n + "'s _ready.json is for epoch " + doc.path("epoch").asLong());
      }
      List<Input> inputs = new ArrayList<>();
      for (JsonNode in : doc.path("inputs")) {
        String name = in.path("name").asText();
        StructType schema = null;
        JsonNode s = in.get("schema");
        if (s != null && !s.isNull()) {
          DataType t = DataType.fromJson(s.toString());
          if (!(t instanceof StructType)) {
            throw new IllegalStateException("input " + name + "'s schema isn't a struct: " + t.simpleString());
          }
          schema = (StructType) t;
        }
        List<String> files = new ArrayList<>();
        for (JsonNode f : in.path("files")) {
          files.add(f.asText());
        }
        inputs.add(new Input(name, schema, files));
      }
      List<String> outputs = new ArrayList<>();
      for (JsonNode o : doc.path("outputs")) {
        outputs.add(o.asText());
      }
      return new Ready(doc.path("generation").asLong(), inputs, outputs);
    }
  }

  private static final class Input {
    final String name;
    final StructType schema;
    final List<String> files;

    Input(String name, StructType schema, List<String> files) {
      this.name = name;
      this.schema = schema;
      this.files = files;
    }
  }
}
