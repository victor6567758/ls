package com.example.livestream;

/**
 * Where subscribers run. A subscriber is attached to a {@link TailingStream} and calls its
 * listeners on consumer threads. Two implementations, chosen by the caller:
 *
 * <ul>
 *   <li>{@link SharedSubscriber}: a fixed pool of shard threads shared by all its subscriptions.
 *       Cheap with many subscribers; a blocking listener delays the others on its shard.
 *   <li>{@link DedicatedSubscriber}: one thread per subscription. A blocking listener delays nobody
 *       else; costs a thread.
 * </ul>
 */
public interface Subscriber extends AutoCloseable {
  /** {@code fromSeq} value meaning "only events published after the subscription". */
  long LATEST = -1;

  /** Events a subscription may hold before it is cancelled, when the caller does not choose. */
  int DEFAULT_CAPACITY = 1024;

  /**
   * History from {@code fromSeq} (inclusive), then live events.
   *
   * @param capacity events the subscription may hold before it is cancelled ({@link Subscription})
   * @throws IllegalArgumentException if {@code fromSeq} is older than the stream's window
   */
  Subscription subscribe(long fromSeq, EventListener l, int capacity);

  /** As {@link #subscribe(long, EventListener, int)} with {@link #DEFAULT_CAPACITY}. */
  default Subscription subscribe(long fromSeq, EventListener l) {
    return subscribe(fromSeq, l, DEFAULT_CAPACITY);
  }

  /** Stops this subscriber's threads and cancels what it started. */
  @Override
  void close();
}
