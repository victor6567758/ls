package com.example.livestream;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * One {@link Subscription} per partition; its position is the vector cursor {@code {partition ->
 * next seq}}.
 */
public final class PartitionedSubscription {
  private final long[] initial;
  private final AtomicReferenceArray<Subscription> subs;
  private final PartitionedListener listener;
  private final AtomicInteger pendingCatchUp;
  private final AtomicBoolean failed = new AtomicBoolean();
  private volatile boolean cancelled;

  PartitionedSubscription(long[] initial, PartitionedListener listener) {
    this.initial = initial.clone();
    this.subs = new AtomicReferenceArray<>(initial.length);
    this.listener = listener;
    this.pendingCatchUp = new AtomicInteger(initial.length);
  }

  public EventListener adapter(int partition) {
    return new EventListener() {
      @Override
      public void onEvent(Event e, Source source) {
        listener.onEvent(partition, e, source);
      }

      @Override
      public void onCaughtUp() {
        if (pendingCatchUp.decrementAndGet() == 0) {
          listener.onCaughtUp();
        }
      }

      @Override
      public void onError(Throwable t) {
        cancel();
        if (failed.compareAndSet(false, true)) {
          listener.onError(t);
        }
      }
    };
  }

  public void attach(int partition, Subscription s) {
    subs.set(partition, s);
    if (cancelled) { // cancelled while we were still attaching the other partitions
      s.cancel();
    }
  }

  /** Stops all partitions. Safe to call from any thread, any number of times. */
  public void cancel() {
    cancelled = true;
    for (int p = 0; p < subs.length(); p++) {
      Subscription s = subs.get(p);
      if (s != null) {
        s.cancel();
      }
    }
  }

  public boolean isCancelled() {
    return cancelled;
  }

  /**
   * The seq each partition will deliver next. Store it on the client and pass it to {@link
   * PartitionedStream#subscribeFromCursor} to resume. Each entry is exact for its own partition;
   * the entries are read one after another, not at one instant (nobody needs that, there is no
   * global order).
   */
  @VisibleForTesting
  long[] cursor() {
    long[] c = new long[subs.length()];
    for (int p = 0; p < c.length; p++) {
      Subscription s = subs.get(p);
      c[p] = s == null ? initial[p] : s.nextSeq();
    }
    return c;
  }

  public Subscription partition(int p) {
    return subs.get(p);
  }
}
