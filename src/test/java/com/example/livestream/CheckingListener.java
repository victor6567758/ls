package com.example.livestream;

import java.util.concurrent.locks.LockSupport;

/**
 * Stand-in for a websocket session: verifies the delivery contract (consecutive seqs) and counts.
 */
final class CheckingListener implements EventListener {
  final String name;
  final long slowNanos;
  volatile long expected;
  volatile long count;
  volatile long history;
  volatile long live;
  volatile long last;
  volatile boolean ok = true, caughtUp;

  CheckingListener(String name, long firstExpected, long slowNanos) {
    this.name = name;
    this.expected = firstExpected;
    this.slowNanos = slowNanos;
  }

  @Override
  public void onEvent(Event e, Source src) {
    if (e.seq() != expected) {
      if (ok) {
        System.out.printf("!! %s: expected seq %d but got %d%n", name, expected, e.seq());
      }
      ok = false;
    }
    expected = e.seq() + 1;
    last = e.seq();
    count++;
    if (src == Source.HISTORY) {
      history++;
    } else {
      live++;
    }
    if (slowNanos > 0) {
      LockSupport.parkNanos(slowNanos); // a slow websocket client
    }
  }

  @Override
  public void onCaughtUp() {
    caughtUp = true;
  }
}
