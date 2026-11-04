package io.openetl.spark.e2e;

import io.openetl.spark.EpochContext;
import io.openetl.spark.Transform;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * The e2e suite's transform (test/e2e): each input becomes the output of its name, with every
 * column and spark_epoch, the epoch. An input with a true value in a column named fail throws.
 */
public final class EnrichCheck implements Transform {
  private static final long serialVersionUID = 1L;

  @Override
  public Map<String, Dataset<Row>> apply(SparkSession spark, Map<String, Dataset<Row>> inputs, EpochContext ctx) {
    Map<String, Dataset<Row>> out = new LinkedHashMap<>();
    for (Map.Entry<String, Dataset<Row>> e : inputs.entrySet()) {
      Dataset<Row> df = e.getValue();
      if (Arrays.asList(df.columns()).contains("fail") && !df.filter(functions.col("fail").equalTo(true)).isEmpty()) {
        throw new IllegalStateException("asked to fail");
      }
      out.put(e.getKey(), df.withColumn("spark_epoch", functions.lit(ctx.epoch())));
    }
    return out;
  }
}
