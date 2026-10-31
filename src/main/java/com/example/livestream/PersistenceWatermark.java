package com.example.livestream;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Highest seq W such that ALL events with seq <= W are visible in the store (contiguously, or
 * declared dead). Monotonic, advanced only by the {@link Tailer}, and only after the ring slots are
 * written. For subscribers it means: every seq <= W can be read from the store, and every seq > W
 * is not visible to anyone yet, so there is nothing to deliver.
 */
public final class PersistenceWatermark {
  private final AtomicLong value;

  public PersistenceWatermark(long initial) {
    value = new AtomicLong(initial);
  }

  public long get() {
    return value.get();
  }

  public void advance(long v) {
    long cur;
    while ((cur = value.get()) < v && !value.compareAndSet(cur, v)) {
      /* retry */
    }
  }
}
