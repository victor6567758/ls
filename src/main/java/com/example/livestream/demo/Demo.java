package com.example.livestream.demo;

import com.example.livestream.DedicatedSubscriber;
import com.example.livestream.Event;
import com.example.livestream.EventListener;
import com.example.livestream.InMemoryEventStore;
import com.example.livestream.SharedSubscriber;
import com.example.livestream.Subscriber;
import com.example.livestream.Subscription;
import com.example.livestream.TailingStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Self-checking demo of the library, kept in its own package so that it is clearly not part of it.
 *
 * <p>The scene: several independent writers take ids from one sequence and insert events into a
 * store, each after a short random delay (a transaction in flight), so ids become visible out of
 * order. Every {@value #ROLLED_BACK_EVERY}th id is rolled back and never appears (a permanent
 * hole), which the Tailer has to give up on after the gap timeout. Subscribers join at different
 * moments: one from the start, one slow one on its own dispatcher, one live-only, and one that
 * joins mid-run asking for everything.
 *
 * <p>The demo verifies that every subscriber saw every event exactly once and in order, and that
 * the only seqs nobody saw are exactly the rolled-back ones that the Tailer reported as skipped.
 * Exit code 1 on a violation.
 */
public final class Demo {

  /** Every this-many-th id (after the preloaded history) is a rolled-back transaction. */
  static final long ROLLED_BACK_EVERY = 5_000;

  private static final long GAP_TIMEOUT_MILLIS = 200;

  /**
   * Buffer of each fan-out client: a burst of writers can run far ahead of a shard, so it must hold
   * the whole run.
   */
  private static final int BUFFER = 1 << 16;

  private Demo() {}

  public static void main(String[] args) throws Exception {
    System.exit(run(4, 10_000, 5_000) ? 0 : 1);
  }

  /** Stand-in for a websocket session: records what it receives and checks that seqs only go up. */
  static final class Client implements EventListener {
    final String name;
    final long slowNanos;
    private long[] seqs = new long[1 << 12];
    private long last;
    volatile int count;
    volatile long history, live;
    volatile boolean increasing = true, caughtUp;

    Client(String name, long slowNanos) {
      this.name = name;
      this.slowNanos = slowNanos;
    }

    @Override
    public void onEvent(Event e, Source source) {
      if (e.seq() <= last) {
        increasing = false;
      }
      last = e.seq();
      int n = count;
      if (n == seqs.length) {
        seqs = java.util.Arrays.copyOf(seqs, n * 2);
      }
      seqs[n] = e.seq();
      if (source == Source.HISTORY) {
        history++;
      } else {
        live++;
      }
      count = n + 1; // volatile write publishes the array slot
      if (slowNanos > 0) {
        LockSupport.parkNanos(slowNanos); // a slow websocket client
      }
    }

    @Override
    public void onCaughtUp() {
      caughtUp = true;
    }

    long lastSeen() {
      int n = count;
      return n == 0 ? -1 : seqs[n - 1];
    }

    /**
     * Walks the received seqs: each must be the next id that was not rolled back. Returns null if
     * fine.
     */
    String verify(long expectedFirst, long preload, long total) {
      int n = count;
      if (n == 0) {
        return "received nothing";
      }
      if (expectedFirst > 0 && seqs[0] != expectedFirst) {
        return "first seq " + seqs[0] + ", expected " + expectedFirst;
      }
      if (!increasing) {
        return "seqs were not strictly increasing";
      }
      long next = seqs[0];
      for (int i = 0; i < n; i++) {
        while (rolledBack(next, preload, total)) {
          next++;
        }
        if (seqs[i] != next) {
          return "expected seq " + next + " but got " + seqs[i] + " (position " + i + ")";
        }
        next++;
      }
      if (seqs[n - 1] != total) {
        return "stopped at " + seqs[n - 1] + ", expected " + total;
      }
      return null;
    }
  }

  static boolean rolledBack(long id, long preload, long total) {
    return id > preload && id < total && id % ROLLED_BACK_EVERY == 0;
  }

  /**
   * @param writers independent writer threads
   * @param perWriter ids each writer takes
   * @param preload events already in the store before the stream starts (history)
   * @return true if every check passed
   */
  public static boolean run(int writers, int perWriter, int preload) throws Exception {
    long total = (long) preload + (long) writers * perWriter;
    InMemoryEventStore store = new InMemoryEventStore();
    List<long[]> gaps = Collections.synchronizedList(new ArrayList<>());
    AtomicLong nextId = new AtomicLong(preload);
    Client fromStart = new Client("A from-start", 0);
    Client slow = new Client("B slow-from-start", 20_000);
    Client liveOnly = new Client("D live-only (joined 1/3 in)", 0);
    Client midRun = new Client("C from-start (joined 2/3 in)", 0);
    Subscription[] subs = new Subscription[4];

    long startedNanos = System.nanoTime();
    // the window must hold the whole run: a subscriber can only start from merged events still in
    // the window
    try (TailingStream stream =
            new TailingStream(
                store,
                65_536,
                256,
                1,
                GAP_TIMEOUT_MILLIS,
                (from, to) -> gaps.add(new long[] {from, to}));
        SharedSubscriber fanout = new SharedSubscriber(stream, 2, 128);
        DedicatedSubscriber dedicated = new DedicatedSubscriber(stream)) {

      // the preloaded events enter through the stream like any other row, so they are in the window
      for (long id = 1; id <= preload; id++) {
        store.insert(event(id));
      }
      awaitVisible(stream, preload - 1); // seq preload-1 is sealed once preload is seen

      subs[0] = fanout.subscribe(1, fromStart, BUFFER);
      subs[1] = dedicated.subscribe(1, slow, BUFFER); // slow listener, own thread, own buffer

      List<Thread> threads = new ArrayList<>();
      for (int w = 0; w < writers; w++) {
        Thread t = new Thread(() -> write(store, nextId, preload, total), "writer-" + w);
        threads.add(t);
        t.start();
      }

      awaitIds(nextId, preload + (total - preload) / 3);
      subs[2] =
          fanout.subscribe(Subscriber.LATEST, liveOnly, BUFFER); // only what happens from now on
      awaitIds(nextId, preload + 2 * (total - preload) / 3);
      subs[3] = fanout.subscribe(1, midRun, BUFFER); // all history from the window, then live

      for (Thread t : threads) {
        t.join();
      }
      stream.publishTombstone(
          total); // the store is complete: the last id is not waiting for a higher one
      Client[] all = {fromStart, slow, liveOnly, midRun};
      long deadline = System.nanoTime() + 60_000_000_000L;
      boolean done = false;
      while (!done && System.nanoTime() < deadline) {
        done = stream.visibleSeq() >= total;
        for (Client c : all) {
          done &= c.lastSeen() == total;
        }
        Thread.sleep(10);
      }

      return report(
          all, subs, gaps, preload, total, done, (System.nanoTime() - startedNanos) / 1_000_000);
    }
  }

  private static void write(InMemoryEventStore store, AtomicLong nextId, long preload, long total) {
    ThreadLocalRandom rnd = ThreadLocalRandom.current();
    long id;
    while ((id = nextId.incrementAndGet()) <= total) {
      if (rolledBack(id, preload, total)) {
        continue; // this transaction rolls back: the id is taken, the event never appears
      }
      if (rnd.nextInt(8) == 0) {
        LockSupport.parkNanos(rnd.nextInt(1_500) * 1_000L); // a transaction still in flight
      }
      store.insert(event(id));
    }
  }

  private static Event event(long id) {
    return new Event(id, System.currentTimeMillis(), "key" + (id % 16), "payload-" + id);
  }

  private static void awaitVisible(TailingStream stream, long seq) {
    while (stream.visibleSeq() < seq) {
      LockSupport.parkNanos(1_000_000);
    }
  }

  private static void awaitIds(AtomicLong nextId, long target) {
    while (nextId.get() < target) {
      LockSupport.parkNanos(1_000_000);
    }
  }

  private static boolean report(
      Client[] all,
      Subscription[] subs,
      List<long[]> gaps,
      long preload,
      long total,
      boolean done,
      long millis) {
    long expectedDead = 0;
    for (long id = preload + 1; id < total; id++) {
      if (rolledBack(id, preload, total)) {
        expectedDead++;
      }
    }
    long skipped = 0;
    boolean gapsOk = true;
    synchronized (gaps) {
      for (long[] g : gaps) {
        for (long id = g[0]; id <= g[1]; id++) {
          skipped++;
          gapsOk &= rolledBack(id, preload, total);
        }
      }
    }
    gapsOk &= skipped == expectedDead;

    System.out.printf(
        "ids 1..%d (%d preloaded), %d rolled back, finished in %d ms%n",
        total, preload, expectedDead, millis);
    System.out.printf(
        "%-30s %8s %8s %8s %10s  %s%n",
        "subscriber", "events", "history", "live", "cancelled", "result");
    boolean ok = done;
    for (int i = 0; i < all.length; i++) {
      Client c = all[i];
      String problem = c.verify(c == all[2] ? -1 : 1, preload, total);
      ok &= problem == null;
      System.out.printf(
          "%-30s %8d %8d %8d %10s  %s%n",
          c.name,
          c.count,
          c.history,
          c.live,
          subs[i].isCancelled() ? "yes" : "no",
          problem == null ? "OK" : "FAILED: " + problem);
    }
    System.out.printf(
        "gap timeout %d ms: Tailer skipped %d dead id(s) in %d range(s), expected %d: %s%n",
        GAP_TIMEOUT_MILLIS, skipped, gaps.size(), expectedDead, gapsOk ? "OK" : "FAILED");
    ok &= gapsOk;
    if (!done) {
      System.out.println("!! timed out before every subscriber reached the end");
    }
    System.out.println(ok ? "ALL SUBSCRIBERS CONSISTENT" : "FAILED");
    return ok;
  }
}
