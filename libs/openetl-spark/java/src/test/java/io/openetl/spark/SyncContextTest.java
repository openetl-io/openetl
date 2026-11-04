package io.openetl.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SyncContextTest {
  // As the runner writes it (internal/spark.Context).
  static final String DOC =
      "{\"version\":1,\"pipeline\":\"p1\",\"sync\":\"s1\",\"destination\":{\"connector\":\"iceberg\",\"namespace\":\"shop\"},"
          + "\"after\":{\"epoch\":3,\"generation\":1},\"through\":{\"epoch\":5,\"generation\":2},"
          + "\"streams\":[{\"name\":\"customers\",\"rows\":2},{\"name\":\"orders\",\"table\":\"shop_orders\",\"rows\":7}]}";

  static Map<String, String> summary(String pipeline, long epoch, long generation) {
    Map<String, String> s = new HashMap<>();
    s.put("openetl.pipeline", pipeline);
    s.put("openetl.epoch", Long.toString(epoch));
    s.put("openetl.generation", Long.toString(generation));
    return s;
  }

  @Test
  void parses() throws IOException {
    SyncContext c = SyncContext.parse(DOC);
    assertEquals("p1", c.pipeline());
    assertEquals("s1", c.sync());
    assertEquals("iceberg", c.destinationConnector());
    assertEquals("shop", c.destinationNamespace());
    assertEquals(new SyncContext.Ref(3, 1), c.after());
    assertEquals(new SyncContext.Ref(5, 2), c.through());
    assertEquals(2, c.streams().size());
    assertEquals("", c.streams().get(0).table());
    assertEquals("shop_orders", c.streams().get(1).table());
    assertEquals(7, c.streams().get(1).rows());
  }

  @Test
  void committedIsTheSyncsRecords() throws IOException {
    SyncContext c = SyncContext.parse(DOC);
    assertFalse(c.committed(summary("p1", 3, 1)), "the record the sync started after");
    assertTrue(c.committed(summary("p1", 3, 2)), "a later generation's epoch 3");
    assertTrue(c.committed(summary("p1", 4, 1)));
    assertTrue(c.committed(summary("p1", 5, 2)));
    assertFalse(c.committed(summary("p1", 5, 3)), "past the sync's last record");
    assertFalse(c.committed(summary("p2", 4, 1)), "another pipeline's");
    assertFalse(c.committed(new HashMap<>()), "a snapshot openetl didn't write");
  }

  @Test
  void anUnknownStartTakesEverythingThroughTheEnd() throws IOException {
    SyncContext c = SyncContext.parse(DOC.replace("{\"epoch\":3,\"generation\":1}", "null"));
    assertNull(c.after());
    assertTrue(c.committed(summary("p1", 1, 1)));
    assertFalse(c.committed(summary("p1", 6, 1)));
  }

  @Test
  void refusesAnotherVersion() {
    IOException e = assertThrows(IOException.class, () -> SyncContext.parse(DOC.replace("\"version\":1", "\"version\":2")));
    assertTrue(e.getMessage().contains("version 2"));
  }
}
