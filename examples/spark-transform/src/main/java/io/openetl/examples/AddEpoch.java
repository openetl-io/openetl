package io.openetl.examples;

import io.openetl.spark.EpochContext;
import io.openetl.spark.Transform;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public final class AddEpoch implements Transform {
  private static final long serialVersionUID = 1L;

  @Override
  public Map<String, Dataset<Row>> apply(
      SparkSession spark, Map<String, Dataset<Row>> inputs, EpochContext context) {
    Map<String, Dataset<Row>> outputs = new LinkedHashMap<>();
    for (Map.Entry<String, Dataset<Row>> input : inputs.entrySet()) {
      outputs.put(
          input.getKey(),
          input.getValue().withColumn("transform_epoch", functions.lit(context.epoch())));
    }
    return outputs;
  }
}
