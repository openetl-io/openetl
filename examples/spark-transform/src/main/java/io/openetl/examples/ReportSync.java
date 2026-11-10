package io.openetl.examples;

import io.openetl.spark.SyncContext;
import org.apache.spark.sql.SparkSession;

public final class ReportSync {
  private ReportSync() {}

  public static void main(String[] args) throws Exception {
    SparkSession spark = SparkSession.builder().getOrCreate();
    try {
      SyncContext context = SyncContext.load(spark);
      long rows = 0;
      for (SyncContext.Stream stream : context.streams()) {
        rows += stream.rows();
        System.out.println("OpenETL stream " + stream.name() + ": " + stream.rows() + " source rows");
      }
      System.out.println("OpenETL sync " + context.sync() + ": " + rows + " source rows");
    } finally {
      spark.stop();
    }
  }
}
