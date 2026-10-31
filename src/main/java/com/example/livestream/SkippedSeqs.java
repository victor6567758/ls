package com.example.livestream;

import java.util.Map;
import java.util.TreeMap;

/**
 * Seq ranges that the {@link Tailer} declared permanently missing (a rolled-back transaction, or a
 * writer that never committed within the gap timeout). Every subscription consults it so that its
 * cursor jumps over a dead range instead of waiting for events that will never appear.
 */
public final class SkippedSeqs {
  private final Object lock = new Object();
  private final TreeMap<Long, Long> ranges =
      new TreeMap<>(); // from -> to (inclusive), guarded by lock

  /** Must be called BEFORE the watermark is advanced past {@code toSeq}. */
  public void add(long fromSeq, long toSeq) {
    synchronized (lock) {
      ranges.put(fromSeq, toSeq);
    }
  }

  /**
   * @return {@code seq}, or the first seq after the dead range that contains it.
   */
  public long resolve(long seq) {
    synchronized (lock) {
      while (true) {
        Map.Entry<Long, Long> e = ranges.floorEntry(seq);
        if (e == null || e.getValue() < seq) {
          return seq;
        }
        seq = e.getValue() + 1;
      }
    }
  }
}
