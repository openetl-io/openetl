package io.openetl.spark;

import java.io.Serializable;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * The user's code in a pipeline's Spark transform. {@link TransformDriver} calls it once for each
 * epoch of a sync, with a DataFrame for every input stream, empty if the stream has no rows in
 * the epoch, and writes the DataFrame it returns for each of {@link EpochContext#outputs()}.
 *
 * <p>Each call sees one epoch, so a join across streams misses rows that land in different
 * epochs. Inputs carry the {@code _openetl_*} metadata columns; outputs keep the ones they
 * don't drop.
 *
 * <pre>{@code
 * public final class Enrich implements Transform {
 *   public Map<String, Dataset<Row>> apply(SparkSession spark, Map<String, Dataset<Row>> inputs, EpochContext ctx) {
 *     return Collections.singletonMap("orders_enriched", inputs.get("orders").join(...));
 *   }
 * }
 * }</pre>
 */
public interface Transform extends Serializable {
  /**
   * Returns a DataFrame for each output, by name.
   *
   * @param inputs every input stream's DataFrame, by name, in the pipeline's order
   */
  Map<String, Dataset<Row>> apply(SparkSession spark, Map<String, Dataset<Row>> inputs, EpochContext ctx);
}
