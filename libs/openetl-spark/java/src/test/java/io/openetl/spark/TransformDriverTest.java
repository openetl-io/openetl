package io.openetl.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openetl.spark.e2e.EnrichCheck;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(value = 2, unit = TimeUnit.MINUTES)
class TransformDriverTest {
  static final ObjectMapper JSON = new ObjectMapper();
  static final StructType ORDERS =
      new StructType().add("id", DataTypes.LongType, false).add("total", DataTypes.StringType);
  static final StructType USERS = new StructType().add("id", DataTypes.LongType).add("name", DataTypes.StringType);

  static SparkSession spark;

  @TempDir Path tmp;

  @BeforeAll
  static void start() {
    spark =
        SparkSession.builder()
            .master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate();
  }

  @AfterAll
  static void stop() {
    spark.stop();
  }

  String root() {
    return tmp.toUri().toString();
  }

  TransformDriver driver() throws IOException {
    // Without _alive, a loop that never sees _end still stops.
    return new TransformDriver(spark, root(), 10, 5000);
  }

  /** Writes rows as epoch n's files of stream, and returns their paths relative to n. */
  List<String> stage(long n, String stream, StructType schema, Row... rows) throws IOException {
    Path dir = tmp.resolve(n + "/in/" + stream);
    spark.createDataFrame(Arrays.asList(rows), schema).coalesce(1).write().parquet(dir.toUri().toString());
    try (Stream<Path> files = Files.list(dir)) {
      return files
          .map(p -> p.getFileName().toString())
          .filter(f -> f.startsWith("part-") && f.endsWith(".parquet"))
          .sorted()
          .map(f -> "in/" + stream + "/" + f)
          .collect(Collectors.toList());
    }
  }

  static Map<String, Object> input(String name, StructType schema, List<String> files) throws IOException {
    Map<String, Object> in = new LinkedHashMap<>();
    in.put("name", name);
    in.put("schema", schema == null ? null : JSON.readTree(schema.json()));
    in.put("files", files);
    in.put("rows", 0);
    return in;
  }

  void ready(long n, long generation, List<Map<String, Object>> inputs, String... outputs) throws IOException {
    Map<String, Object> doc = new LinkedHashMap<>();
    doc.put("version", 1);
    doc.put("generation", generation);
    doc.put("epoch", n);
    doc.put("inputs", inputs);
    doc.put("outputs", Arrays.asList(outputs));
    write(n + "/in/_ready.json", JSON.writeValueAsString(doc));
  }

  void write(String rel, String text) throws IOException {
    Path p = tmp.resolve(rel);
    Files.createDirectories(p.getParent());
    Files.write(p, text.getBytes(StandardCharsets.UTF_8));
  }

  JsonNode json(String rel) throws IOException {
    return JSON.readTree(tmp.resolve(rel).toFile());
  }

  /** Ends the attempt, so the loop stops once the epoch it's in is done. */
  void end() {
    try {
      write("_end", "");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  Dataset<Row> output(long n, JsonNode files) {
    List<String> uris = new ArrayList<>();
    for (JsonNode f : files) {
      uris.add(tmp.resolve(n + "/" + f.asText()).toUri().toString());
    }
    return spark.read().parquet(uris.toArray(new String[0]));
  }

  @Test
  void transformsAnEpoch() throws Exception {
    List<String> files = stage(7, "orders", ORDERS, RowFactory.create(1L, "9.50"), RowFactory.create(2L, "3.00"), RowFactory.create(3L, null));
    ready(7, 3, Arrays.asList(input("orders", ORDERS, files), input("users", USERS, Collections.emptyList()), input("events", null, Collections.emptyList())),
        "enriched", "totals");
    List<String> seen = new ArrayList<>();
    driver()
        .run(
            (s, inputs, ctx) -> {
              seen.addAll(inputs.keySet());
              assertEquals(3, ctx.generation());
              assertEquals(7, ctx.epoch());
              assertEquals(Arrays.asList("enriched", "totals"), ctx.outputs());
              assertEquals(0, inputs.get("users").count());
              assertEquals(Arrays.asList("id", "name"), Arrays.asList(inputs.get("users").columns()));
              assertEquals(0, inputs.get("events").columns().length);
              Map<String, Dataset<Row>> out = new LinkedHashMap<>();
              out.put("totals", inputs.get("orders").groupBy().count());
              out.put("enriched", inputs.get("orders").withColumn("users", functions.lit(inputs.get("users").count())));
              end();
              return out;
            });
    assertEquals(Arrays.asList("orders", "users", "events"), seen);
    JsonNode done = json("7/out/_done.json");
    assertEquals(1, done.path("version").asInt());
    assertEquals(7, done.path("epoch").asLong());
    JsonNode outs = done.path("outputs");
    assertEquals("enriched", outs.get(0).path("name").asText());
    assertEquals("totals", outs.get(1).path("name").asText());
    for (int k = 0; k < 2; k++) {
      for (JsonNode f : outs.get(k).path("files")) {
        assertTrue(f.asText().startsWith("out/" + k + "/part-") && f.asText().endsWith(".parquet"), f.asText());
      }
    }
    Dataset<Row> enriched = output(7, outs.get(0).path("files"));
    assertEquals(3, enriched.count());
    assertEquals(Arrays.asList("id", "total", "users"), Arrays.asList(enriched.columns()));
    assertEquals(3L, output(7, outs.get(1).path("files")).first().getLong(0));
    assertFalse(Files.exists(tmp.resolve("_failed.json")));
  }

  @Test
  void takesReadyEpochsInOrderAndSkipsDoneOnes() throws Exception {
    for (long n : new long[] {2, 5, 9}) {
      ready(n, 1, Collections.singletonList(input("a", USERS, Collections.emptyList())), "o");
    }
    write("2/out/_done.json", "{\"from\":\"an earlier driver\"}");
    stage(11, "a", USERS, RowFactory.create(1L, "x"));
    List<Long> epochs = new ArrayList<>();
    driver()
        .run(
            (s, inputs, ctx) -> {
              epochs.add(ctx.epoch());
              if (ctx.epoch() == 9) {
                end();
              }
              return Collections.singletonMap("o", inputs.get("a"));
            });
    assertEquals(Arrays.asList(5L, 9L), epochs);
    assertEquals("an earlier driver", json("2/out/_done.json").path("from").asText());
    assertTrue(Files.exists(tmp.resolve("5/out/_done.json")));
    assertFalse(Files.exists(tmp.resolve("11/out/_done.json")), "epoch 11 isn't ready");
  }

  @Test
  void stopsAtEnd() throws Exception {
    ready(1, 1, Collections.singletonList(input("a", USERS, Collections.emptyList())), "o");
    write("_end", "");
    driver().run((s, inputs, ctx) -> { throw new AssertionError("transformed after _end"); });
    assertFalse(Files.exists(tmp.resolve("1/out/_done.json")));
  }

  @Test
  void stopsOnceAliveIsStale() throws Exception {
    write("_alive", "");
    long start = System.nanoTime();
    new TransformDriver(spark, root(), 10, 300).run((s, inputs, ctx) -> Collections.emptyMap());
    long millis = (System.nanoTime() - start) / 1_000_000;
    assertTrue(millis >= 300 && millis < 10_000, millis + " ms");
  }

  @Test
  void writesTheUsersFailure() throws Exception {
    ready(4, 2, Collections.singletonList(input("a", USERS, Collections.emptyList())), "o");
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> driver().run((s, inputs, ctx) -> { throw new IllegalStateException("boom"); }));
    assertEquals("boom", e.getMessage());
    JsonNode failed = json("_failed.json");
    assertEquals(1, failed.path("version").asInt());
    assertEquals(4, failed.path("epoch").asLong());
    assertEquals("java.lang.IllegalStateException: boom", failed.path("error").asText());
    assertTrue(failed.path("trace").asText().contains("TransformDriverTest"));
    assertFalse(Files.exists(tmp.resolve("4/out/_done.json")));
  }

  @Test
  void failsAMissingOrUndeclaredOutput() throws Exception {
    ready(1, 1, Collections.singletonList(input("a", USERS, Collections.emptyList())), "o", "p");
    assertThrows(IllegalStateException.class, () -> driver().run((s, inputs, ctx) -> Collections.singletonMap("o", inputs.get("a"))));
    String error = json("_failed.json").path("error").asText();
    assertTrue(error.contains("returned none for [p]"), error);

    Files.delete(tmp.resolve("_failed.json"));
    Map<String, Dataset<Row>> out = new LinkedHashMap<>();
    assertThrows(
        IllegalStateException.class,
        () ->
            driver()
                .run(
                    (s, inputs, ctx) -> {
                      out.put("o", inputs.get("a"));
                      out.put("p", inputs.get("a"));
                      out.put("q", inputs.get("a"));
                      return out;
                    }));
    error = json("_failed.json").path("error").asText();
    assertTrue(error.contains("returned undeclared [q]"), error);
  }

  @Test
  void refusesAnotherVersion() throws Exception {
    ready(1, 1, Collections.singletonList(input("a", USERS, Collections.emptyList())), "o");
    write("1/in/_ready.json", new String(Files.readAllBytes(tmp.resolve("1/in/_ready.json")), StandardCharsets.UTF_8).replace("\"version\":1", "\"version\":2"));
    assertThrows(IllegalStateException.class, () -> driver().run((s, inputs, ctx) -> Collections.singletonMap("o", inputs.get("a"))));
    assertTrue(json("_failed.json").path("error").asText().contains("version 2"));
  }

  @Test
  void runsTheTransformTheConfNames() throws Exception {
    try {
      spark.conf().set(TransformDriver.ROOT, root());
      spark.conf().set(TransformDriver.PROTOCOL, "2");
      spark.conf().set(TransformDriver.TRANSFORM, EnrichCheck.class.getName());
      assertThrows(IllegalStateException.class, () -> TransformDriver.run(spark));
      JsonNode failed = json("_failed.json");
      assertEquals(0, failed.path("epoch").asLong());
      assertTrue(failed.path("error").asText().contains("protocol 2"), failed.path("error").asText());

      Files.delete(tmp.resolve("_failed.json"));
      spark.conf().set(TransformDriver.PROTOCOL, "1");
      spark.conf().set(TransformDriver.TRANSFORM, "io.openetl.spark.Nope");
      assertThrows(ClassNotFoundException.class, () -> TransformDriver.run(spark));
      assertTrue(json("_failed.json").path("error").asText().contains("io.openetl.spark.Nope"));

      Files.delete(tmp.resolve("_failed.json"));
      spark.conf().set(TransformDriver.TRANSFORM, EnrichCheck.class.getName());
      write("_end", "");
      TransformDriver.run(spark);
      assertFalse(Files.exists(tmp.resolve("_failed.json")));
    } finally {
      spark.conf().unset(TransformDriver.ROOT);
      spark.conf().unset(TransformDriver.PROTOCOL);
      spark.conf().unset(TransformDriver.TRANSFORM);
    }
  }

  @Test
  void enrichCheck() throws Exception {
    StructType schema = new StructType().add("id", DataTypes.LongType).add("fail", DataTypes.BooleanType);
    ready(1, 1, Collections.singletonList(input("orders", schema, stage(1, "orders", schema, RowFactory.create(1L, false)))), "orders");
    ready(2, 1, Collections.singletonList(input("orders", schema, stage(2, "orders", schema, RowFactory.create(2L, true)))), "orders");
    IllegalStateException e = assertThrows(IllegalStateException.class, () -> driver().run(new EnrichCheck()));
    assertEquals("asked to fail", e.getMessage());
    assertEquals(2, json("_failed.json").path("epoch").asLong());
    Row row = output(1, json("1/out/_done.json").path("outputs").get(0).path("files")).first();
    assertEquals(1L, row.getLong(0));
    assertEquals(1L, row.getLong(row.fieldIndex("spark_epoch")));
  }
}
