package com.example.livestream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Receiver for a {@link PartitionedStream}. Per partition the contract of {@link EventListener}
 * holds (strictly increasing, gap-free, exactly once). ACROSS partitions there is no order, and
 * callbacks for different partitions run on different dispatcher threads, so an implementation must
 * be thread-safe (or hand off to a queue).
 */
public interface PartitionedListener {
  Logger LOG = LoggerFactory.getLogger(PartitionedListener.class);

  /**
   * {@code event.seq()} is the position inside {@code partition}; it is not comparable across
   * partitions.
   */
  void onEvent(int partition, Event event, EventListener.Source source);

  /** Once, after EVERY partition has delivered what existed when the subscription was created. */
  default void onCaughtUp() {}

  /** Called once for the first failure in any partition; all partitions are cancelled. */
  default void onError(Throwable t) {
    LOG.error("Partitioned subscription terminated by an error", t);
  }
}
