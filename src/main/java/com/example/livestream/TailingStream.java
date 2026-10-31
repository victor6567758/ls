package com.example.livestream;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

/**
 * Stream over a store that is the source of truth, merged with a realtime feed. Writer services
 * insert into the {@link EventStore} themselves and may call {@link #notifyChange()} as a latency
 * hint. The store is read only by the Tailer; subscribers read the merged window and their own
 * buffers, never the store.
 *
 * <p>Subscribers attach through a {@link Subscriber}: {@link SharedSubscriber} (shared consumer
 * threads) or {@link DedicatedSubscriber} (one thread per subscription).
 */
public final class TailingStream implements AutoCloseable {

  private final EventStore store;
  private final PersistenceWatermark watermark;
  private final LiveRing ring;
  private final SkippedSeqs skipped = new SkippedSeqs();
  private final MergeBuffer buffer;
  private final Object lock =
      new Object(); // guards the window, the watermark and the subscriber list
  private final List<SubscriptionImpl> subscribers = new ArrayList<>(); // guarded by lock
  private final Tailer tailer;
  private final Thread tailerThread;

  /**
   * @param ringCapacity power of two; size of the window of merged events that new subscribers can
   *     start from
   * @param pollMillis upper bound on the delay when no notification arrives
   * @param gapTimeoutMillis how long a hole may stay before it is declared dead; must exceed the
   *     longest writer transaction
   */
  public TailingStream(
      EventStore store,
      int ringCapacity,
      int readBatch,
      long pollMillis,
      long gapTimeoutMillis,
      GapListener gapListener) {
    this(
        store,
        ringCapacity,
        readBatch,
        pollMillis,
        gapTimeoutMillis,
        gapListener,
        MergePolicies.PREFER_STORE);
  }

  /**
   * @param mergePolicy folds every version of one seq (duplicates from the store and the realtime
   *     feed) into the one event that is delivered; see {@link MergePolicies}
   */
  public TailingStream(
      EventStore store,
      int ringCapacity,
      int readBatch,
      long pollMillis,
      long gapTimeoutMillis,
      GapListener gapListener,
      BinaryOperator<Version> mergePolicy) {
    this.store = store;
    this.buffer = new MergeBuffer(mergePolicy);
    long last = store.lastSeq();
    this.ring = new LiveRing(ringCapacity);
    warmUp(last, ringCapacity, readBatch);
    this.watermark = new PersistenceWatermark(last);
    this.tailer =
        new Tailer(
            new Output(),
            store,
            watermark,
            skipped,
            gapListener,
            buffer,
            readBatch,
            TimeUnit.MILLISECONDS.toNanos(pollMillis),
            TimeUnit.MILLISECONDS.toNanos(gapTimeoutMillis));
    this.tailerThread = new Thread(tailer, "tailer");
    this.tailerThread.setDaemon(true);
    this.tailerThread.start();
  }

  /** The Tailer's side of the stream: the window and the fan-out, both under the lock. */
  private final class Output implements Tailer.Output {
    @Override
    public void deliver(Event merged) {
      synchronized (lock) {
        ring.put(merged);
        watermark.advance(merged.seq());
        subscribers.removeIf(s -> !s.offer(merged) && s.isCancelled());
      }
    }

    @Override
    public void skipTo(long seq) {
      synchronized (lock) {
        watermark.advance(seq);
      }
    }
  }

  /**
   * Realtime feed: an event that may come before, with, or after its store row, and may come more
   * than once. It is queued, and merged with the other versions of that seq when the seq is sealed
   * (see {@link MergePolicies}). A seq is sealed when a higher seq has been seen, or by {@link
   * #publishTombstone}. An event for a seq that was already delivered is ignored.
   */
  public void publish(Event e) {
    if (e.seq() > watermark.get()) {
      buffer.add(new Version(e, Version.Origin.REALTIME));
    }
  }

  /**
   * The feed's "time switched" marker: no realtime version at or below {@code seq} will follow.
   * This seals those seqs without waiting for a higher seq, so a quiet stream does not hold its
   * last event back. The store must already hold every row at or below {@code seq}; a row that
   * arrives later is a hole and is declared dead.
   */
  public void publishTombstone(long seq) {
    buffer.tombstone(seq);
  }

  /** Optional hint from a writer (LISTEN/NOTIFY, CDC message...): "look at the store now". */
  public void notifyChange() {
    tailer.wakeUp();
  }

  /** Highest seq visible (and contiguous, or skipped) in the store as seen by this node. */
  public long visibleSeq() {
    return watermark.get();
  }

  /** Highest seq merged and visible so far. */
  public long lastPublishedSeq() {
    return watermark.get();
  }

  /**
   * First seq whose timestamp is at or after {@code ts} (see {@link EventStore#firstSeqAtOrAfter}).
   */
  public long firstSeqAtOrAfter(long ts) {
    return store.firstSeqAtOrAfter(ts);
  }

  /**
   * Registers a subscriber under the lock, so its history and the live events join without a gap or
   * a duplicate. History is copied from the window now; live events are offered by the merger from
   * now on.
   */
  /**
   * Used by {@link Subscriber} implementations only. Call {@code subscribe} on a subscriber
   * instead.
   */
  SubscriptionImpl register(long fromSeq, EventListener l, int capacity) {
    synchronized (lock) {
      long joinSeq = watermark.get();
      long from = fromSeq == Subscriber.LATEST ? joinSeq + 1 : Math.max(1, fromSeq);
      List<Event> history = new ArrayList<>();
      for (long s = from; s <= joinSeq; ) {
        long r = skipped.resolve(s);
        if (r != s) { // a dead range: nothing to deliver there
          s = r;
          continue;
        }
        Event e = ring.read(s);
        if (e == null || e == LiveRing.OVERRUN) {
          throw new IllegalArgumentException(
              "seq "
                  + s
                  + " is older than the retained window; the oldest retained seq is "
                  + oldestRetained(joinSeq));
        }
        history.add(e);
        s++;
      }
      SubscriptionImpl sub = new SubscriptionImpl(l, capacity, history, from, joinSeq);
      subscribers.add(sub);
      return sub;
    }
  }

  @Override
  public void close() {
    tailer.stop();
    try {
      tailerThread.join(2000);
    } catch (InterruptedException ignored) {
      Thread.currentThread().interrupt();
    }
    synchronized (lock) {
      for (SubscriptionImpl s : subscribers) {
        s.cancel();
      }
      subscribers.clear();
    }
  }

  /**
   * Restart: fills the window with the last {@code capacity} rows of the store, before any
   * subscriber or the Tailer exists. Subscribers then get the same history after a restart as
   * before it, without reading the store. The store is contiguous up to {@code last}, so the rows
   * have no holes.
   */
  private void warmUp(long last, int capacity, int batch) {
    long s = Math.max(1, last - capacity + 1);
    while (s <= last) {
      try (Stream<Event> rows = store.read(s, batch)) {
        Iterator<Event> it = rows.iterator();
        if (!it.hasNext()) {
          throw new IllegalStateException(
              "store has no row at seq " + s + ", below its lastSeq " + last);
        }
        while (s <= last && it.hasNext()) {
          Event e = it.next();
          if (e.seq() > last) {
            break;
          }
          if (e.seq() != s) {
            throw new IllegalStateException(
                "store read from seq "
                    + s
                    + " returned seq "
                    + e.seq()
                    + "; it must be contiguous");
          }
          ring.put(e);
          s = e.seq() + 1;
        }
      }
    }
  }

  private long oldestRetained(long joinSeq) {
    long oldest = joinSeq;
    while (oldest > 1
        && ring.read(oldest - 1) != LiveRing.OVERRUN
        && ring.read(oldest - 1) != null) {
      oldest--;
    }
    return oldest;
  }
}
