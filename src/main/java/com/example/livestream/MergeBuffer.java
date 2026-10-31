package com.example.livestream;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.BinaryOperator;

/**
 * Versions of seqs that are not delivered yet, from the store and from the realtime feed.
 *
 * <p>Two queues, one per input. Each is in seq order: the store yields its rows in seq order, and
 * the feed promises non-decreasing seqs. So the next seq is always at the head of one of the
 * queues, and no map is needed.
 *
 * <p>A seq is <em>sealed</em> when no further version of it is expected: a higher seq has been seen
 * from either input, or a tombstone at or above it has arrived. The Tailer delivers sealed seqs in
 * increasing order. When a seq is taken, its versions are folded into one event: the realtime
 * versions first, then the store versions, each in order. The two inputs have no order between
 * them, so the policy must not rely on one.
 *
 * <p>The lock guards both queues and the counters. The feed thread calls {@link #add} and {@link
 * #tombstone}, the {@link Tailer} calls the rest. The merge policy runs under the lock, so it must
 * be quick and must not call back into the stream.
 */
public final class MergeBuffer {
  private final Object lock = new Object();
  private final Deque<Version> store = new ArrayDeque<>(); // guarded by lock
  private final Deque<Version> realtime = new ArrayDeque<>(); // guarded by lock
  private final BinaryOperator<Version> policy;
  private long highest; // guarded by lock
  private long tombstone; // guarded by lock
  private long lastRealtimeSeq; // guarded by lock

  public MergeBuffer(BinaryOperator<Version> policy) {
    this.policy = policy;
  }

  /**
   * Queues one version on the queue of its origin. Realtime versions must come in non-decreasing
   * seq order; a store version always does, since the Tailer reads in seq order.
   */
  public void add(Version v) {
    synchronized (lock) {
      long seq = v.event().seq();
      if (v.origin() == Version.Origin.REALTIME) {
        if (seq < lastRealtimeSeq) {
          throw new IllegalArgumentException(
              "realtime seq "
                  + seq
                  + " came after seq "
                  + lastRealtimeSeq
                  + "; the feed must be non-decreasing");
        }
        lastRealtimeSeq = seq;
        realtime.addLast(v);
      } else {
        store.addLast(v);
      }
      highest = Math.max(highest, seq);
    }
  }

  /** Records that {@code seq} exists, without buffering an event for it. */
  public void observe(long seq) {
    synchronized (lock) {
      highest = Math.max(highest, seq);
    }
  }

  /**
   * The feed promises that no realtime version at or below {@code seq} follows; the store must
   * already hold them.
   */
  public void tombstone(long seq) {
    synchronized (lock) {
      tombstone = Math.max(tombstone, seq);
    }
  }

  public boolean sealed(long seq) {
    synchronized (lock) {
      return seq < highest || seq <= tombstone;
    }
  }

  /**
   * Folds the versions of {@code seq} and removes them. Returns {@code null} if no version of it
   * arrived. Versions of older seqs that are still queued (late duplicates) are dropped here.
   */
  public Version take(long seq) {
    synchronized (lock) {
      dropUpTo(seq - 1);
      Version folded = foldHead(realtime, seq, null);
      return foldHead(store, seq, folded);
    }
  }

  /** The smallest queued seq above {@code seq}, or {@link Long#MAX_VALUE} if there is none. */
  public long nextAbove(long seq) {
    synchronized (lock) {
      return Math.min(firstAbove(realtime, seq), firstAbove(store, seq));
    }
  }

  /** Drops everything at or below {@code seq}: those seqs are delivered or declared dead. */
  public void discardUpTo(long seq) {
    synchronized (lock) {
      dropUpTo(seq);
    }
  }

  private void dropUpTo(long seq) {
    while (!realtime.isEmpty() && realtime.peekFirst().event().seq() <= seq) {
      realtime.pollFirst();
    }
    while (!store.isEmpty() && store.peekFirst().event().seq() <= seq) {
      store.pollFirst();
    }
  }

  /**
   * Pops the versions of {@code seq} from the head of {@code queue} and folds them into {@code
   * folded}.
   */
  private Version foldHead(Deque<Version> queue, long seq, Version folded) {
    Version result = folded;
    while (!queue.isEmpty() && queue.peekFirst().event().seq() == seq) {
      Version v = queue.pollFirst();
      result = result == null ? v : policy.apply(result, v);
    }
    return result;
  }

  private static long firstAbove(Deque<Version> queue, long seq) {
    for (Version v : queue) {
      if (v.event().seq() > seq) {
        return v.event().seq();
      }
    }
    return Long.MAX_VALUE;
  }
}
