package com.example.livestream;

import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Stream;

/** In-memory store for tests and examples. Writers call {@link #insert}, in any order. */
public final class InMemoryEventStore implements EventStore {
  private final Object lock = new Object();
  private final TreeMap<Long, Event> map = new TreeMap<>(); // guarded by lock
  private long lastSeq; // guarded by lock

  /**
   * What an independent writer service does: make one event visible, in any order. {@link
   * #lastSeq()} only advances over a contiguous prefix, so an id that becomes visible before its
   * predecessor does not move it.
   */
  public void insert(Event e) {
    synchronized (lock) {
      map.put(e.seq(), e);
      long last = lastSeq;
      while (map.containsKey(last + 1)) {
        last++;
      }
      lastSeq = last;
    }
  }

  @Override
  public long nextVisibleSeq(long fromSeq) {
    synchronized (lock) {
      Long k = map.ceilingKey(fromSeq);
      return k == null ? -1 : k;
    }
  }

  @Override
  public Stream<Event> read(long fromSeq, int limit) {
    // lazy, like a cursor: each row is looked up under the lock as the Tailer asks for it
    return Stream.iterate(fromSeq, s -> s + 1)
        .map(this::get)
        .takeWhile(Objects::nonNull)
        .limit(limit);
  }

  private Event get(long seq) {
    synchronized (lock) {
      return map.get(seq);
    }
  }

  @Override
  public long lastSeq() {
    synchronized (lock) {
      return lastSeq;
    }
  }

  @Override
  public long firstSeqAtOrAfter(long ts) { // a real DB would use an index on (ts)
    synchronized (lock) {
      for (Event e : map.values()) {
        if (e.timestampMillis() >= ts) {
          return e.seq();
        }
      }
      return lastSeq + 1;
    }
  }
}
