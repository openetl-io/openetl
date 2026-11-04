package io.openetl.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** The epoch a {@link Transform} is called for. */
public final class EpochContext implements Serializable {
  private static final long serialVersionUID = 1L;

  private final long generation;
  private final long epoch;
  private final List<String> outputs;

  public EpochContext(long generation, long epoch, List<String> outputs) {
    this.generation = generation;
    this.epoch = epoch;
    this.outputs = Collections.unmodifiableList(new ArrayList<>(outputs));
  }

  /** The sync attempt's generation. */
  public long generation() {
    return generation;
  }

  public long epoch() {
    return epoch;
  }

  /** The outputs the transform returns, by name, as the pipeline declares them. */
  public List<String> outputs() {
    return outputs;
  }
}
