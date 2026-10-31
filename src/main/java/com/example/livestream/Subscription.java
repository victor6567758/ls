package com.example.livestream;

/**
 * A registered subscriber, as its user sees it. The delivery machinery (history, buffer, pump) is
 * in the package-private {@code SubscriptionImpl}; the public API only needs this interface.
 */
public interface Subscription {
  /**
   * Stops the subscriber. Its listener is not called again; no {@code onError} is sent for a plain
   * cancel.
   */
  void cancel();

  /**
   * True once the subscriber has been cancelled, by the caller or because its buffer overflowed.
   */
  boolean isCancelled();

  /** The seq to re-subscribe from: the last delivered seq + 1. */
  long nextSeq();

  /** Number of events delivered to the listener so far. */
  long deliveredCount();
}
