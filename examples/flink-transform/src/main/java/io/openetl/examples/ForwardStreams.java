package io.openetl.examples;

import io.openetl.flink.OpenEtl;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public final class ForwardStreams {
  private ForwardStreams() {}

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
    OpenEtl openetl = OpenEtl.fromArgs(environment, args);
    for (String name : openetl.streamNames()) {
      openetl.sink(name, openetl.stream(name));
    }
    environment.execute("OpenETL forward streams");
  }
}
