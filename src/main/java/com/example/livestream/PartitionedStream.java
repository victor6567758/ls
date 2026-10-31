package com.example.livestream;

import java.util.List;
import java.util.function.Function;

/**
 * Scale-out without a global order: N independent streams, each with its OWN store (data source),
 * its own seq counter and its own watermark. Nothing is shared between partitions, so nothing
 * serializes them: each partition has its own tailer and its own order.
 *
 * <p>A partition is a {@link TailingStream} over its own store. The subscriber's position is a
 * vector cursor {@code {partition -> next seq}}; every entry is exactly the single cursor of one
 * stream, so the seam between history and live is crossed per partition by the unchanged state
 * machine.
 *
 * <p>Guarantee: per partition, in order, gap-free, exactly once. Across partitions: none. Events
 * that must stay ordered relative to each other must share a key (so they share a partition).
 */
public final class PartitionedStream implements AutoCloseable {
  private final List<TailingStream> partitions;
  private final Subscriber[] subscribers;
  private final int bufferPerSubscription;

  /**
   * @param subscriberFor creates the subscriber of one partition: {@code SharedSubscriber} or
   *     {@code DedicatedSubscriber}, with its own settings. The partition's subscriber is closed by
   *     {@link #close()}.
   * @param bufferPerSubscription events each subscription may hold before it is cancelled (see
   *     {@link Subscription})
   */
  public PartitionedStream(
      List<TailingStream> partitions,
      Function<TailingStream, Subscriber> subscriberFor,
      int bufferPerSubscription) {
    if (partitions.isEmpty()) {
      throw new IllegalArgumentException("need at least one partition");
    }
    this.partitions = List.copyOf(partitions);
    this.bufferPerSubscription = bufferPerSubscription;
    this.subscribers = new Subscriber[partitions.size()];
    for (int p = 0; p < subscribers.length; p++) {
      subscribers[p] = subscriberFor.apply(partitions.get(p));
    }
  }

  public int partitionCount() {
    return partitions.size();
  }

  /**
   * Stable routing: the same key always lands in the same partition (as long as the count is
   * unchanged). Writers must use the same rule to choose the data source they insert into.
   */
  public int partitionFor(String key) {
    int h = key.hashCode();
    return Math.floorMod(h ^ (h >>> 16), partitions.size());
  }

  /** The partition itself, e.g. to call {@link TailingStream#notifyChange()} on it. */
  public TailingStream partition(int p) {
    return partitions.get(p);
  }

  /** Resume: {@code next[p]} is the first seq wanted from partition p (1 = from its beginning). */
  public PartitionedSubscription subscribeFromCursor(long[] next, PartitionedListener l) {
    if (next.length != partitions.size()) {
      throw new IllegalArgumentException(
          "cursor has " + next.length + " entries, expected " + partitions.size());
    }
    PartitionedSubscription sub = new PartitionedSubscription(next, l);
    for (int p = 0; p < next.length; p++) {
      sub.attach(p, subscribers[p].subscribe(next[p], sub.adapter(p), bufferPerSubscription));
    }
    return sub;
  }

  /** Everything from every partition's beginning, then live. */
  public PartitionedSubscription subscribeFromStart(PartitionedListener l) {
    long[] next = new long[partitions.size()];
    java.util.Arrays.fill(next, 1);
    return subscribeFromCursor(next, l);
  }

  /** Only what is published after this call (each partition's head is read separately). */
  public PartitionedSubscription subscribeFromLatest(PartitionedListener l) {
    long[] next = new long[partitions.size()];
    for (int p = 0; p < next.length; p++) {
      next[p] = partitions.get(p).lastPublishedSeq() + 1;
    }
    return subscribeFromCursor(next, l);
  }

  /**
   * Per partition: from its first event with timestamp &gt;= ts, then live. Clocks are per data
   * source, so this is approximate.
   */
  public PartitionedSubscription subscribeFromTimestamp(long ts, PartitionedListener l) {
    long[] next = new long[partitions.size()];
    for (int p = 0; p < next.length; p++) {
      next[p] = partitions.get(p).firstSeqAtOrAfter(ts);
    }
    return subscribeFromCursor(next, l);
  }

  /**
   * Closes the partitions' subscribers; closing the partitions (streams and stores) stays with
   * whoever created them.
   */
  @Override
  public void close() {
    for (Subscriber d : subscribers) {
      d.close();
    }
  }
}
