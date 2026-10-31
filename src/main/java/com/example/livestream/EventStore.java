package com.example.livestream;

import java.util.stream.Stream;

/**
 * The durable history (DB, Cassandra, S3 segment files...), written by independent writer services.
 * This library only reads it. Must be safe for concurrent use.
 */
public interface EventStore {
  /**
   * Up to {@code limit} events starting EXACTLY at {@code fromSeq} and contiguous, as a stream in
   * seq order. The stream is empty if {@code fromSeq} is not (yet) visible in the store. The caller
   * closes it.
   */
  Stream<Event> read(long fromSeq, int limit);

  /**
   * Smallest visible seq that is &gt;= {@code fromSeq}, or -1 if there is none. Lets the {@link
   * Tailer} tell "nothing new yet" from "a hole at fromSeq (an in-flight or rolled-back
   * transaction)". The default (-1) disables hole detection: the tailer then simply waits at a hole
   * forever.
   */
  default long nextVisibleSeq(long fromSeq) {
    return -1;
  }

  /** Highest seq such that every seq <= it is visible; 0 if empty. Used at start-up. */
  long lastSeq();

  /** First visible seq whose timestamp >= ts, or lastSeq()+1 if there is none. */
  long firstSeqAtOrAfter(long timestampMillis);
}
