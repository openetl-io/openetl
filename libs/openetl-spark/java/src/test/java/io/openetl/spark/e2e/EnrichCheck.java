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
 * column and spark_epoch, the epoch. It throws when spark.e2e.fail is true, or an input has a true
 * value in a column named fail.
 */
public final class EnrichCheck implements Transform {
  private static final long serialVersionUID = 1L;

  @Override
  public Map<String, Dataset<Row>> apply(SparkSession spark, Map<String, Dataset<Row>> inputs, EpochContext ctx) {
    if (Boolean.parseBoolean(spark.conf().get("spark.e2e.fail", "false"))) {
      throw new IllegalStateException("asked to fail");
    }
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
