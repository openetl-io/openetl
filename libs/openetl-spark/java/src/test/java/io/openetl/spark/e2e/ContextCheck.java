package io.openetl.spark.e2e;

import io.openetl.spark.SyncContext;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;

/**
 * The e2e suite's post-sync application (test/e2e/spark_test.go). It reads the sync's context,
 * runs a job on the executors, and writes what it read to the path in its first argument. With
 * --fail as its second, it throws instead.
 */
public final class ContextCheck {
  public static void main(String[] args) throws Exception {
    SparkSession spark = SparkSession.builder().getOrCreate();
    try {
      SyncContext ctx = SyncContext.load(spark);
      if (args.length > 1 && args[1].equals("--fail")) {
        throw new IllegalStateException("asked to fail by sync " + ctx.sync());
      }
      long rows = 0;
      for (SyncContext.Stream s : ctx.streams()) {
        rows += s.rows();
      }
      long counted = spark.range(rows).count();
      String out =
          String.format(
              "{\"sync\":\"%s\",\"pipeline\":\"%s\",\"rows\":%d,\"counted\":%d,\"through\":%d}",
              ctx.sync(), ctx.pipeline(), rows, counted, ctx.through().epoch());
      Path path = new Path(args[0]);
      FileSystem fs = path.getFileSystem(spark.sparkContext().hadoopConfiguration());
      try (OutputStream o = fs.create(path, true)) {
        o.write(out.getBytes(StandardCharsets.UTF_8));
      }
    } finally {
      spark.stop();
    }
  }
}
