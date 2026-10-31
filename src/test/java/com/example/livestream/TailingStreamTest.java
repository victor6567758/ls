package com.example.livestream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.google.common.util.concurrent.Uninterruptibles;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/** Independent writers, the store is the truth: a Tailer feeds the ring. */
class TailingStreamTest {

  /** Records every seq it receives, in order. */
  static final class Collector implements EventListener {
    final List<Long> seqs = Collections.synchronizedList(new ArrayList<>());
    volatile boolean caughtUp;

    @Override
    public void onEvent(Event e, Source src) {
      seqs.add(e.seq());
    }

    @Override
    public void onCaughtUp() {
      caughtUp = true;
    }
  }

  @Test
  void outOfOrderVisibilityWithNotifications() throws Exception {
    runWriters(true);
  }

  @Test
  void lostNotificationsAreCoveredByPolling() throws Exception {
    runWriters(false);
  }

  /** 1,2 visible, then 4: nobody may see 4 before 3 shows up. */
  @Test
  void stopsAtAHoleUntilThePredecessorBecomesVisible() throws Exception {
    InMemoryEventStore store = new InMemoryEventStore();
    try (TailingStream stream = TestStreams.storeOnly(store, 16, 16, 1, 60_000, (a, b) -> {});
        SharedSubscriber fanout = new SharedSubscriber(stream, 1, 16)) {
      Collector c = new Collector();
      fanout.subscribe(1, c);
      store.insert(ev(1));
      store.insert(ev(2));
      store.insert(ev(4));
      Awaitility.await()
          .alias("1 and 2")
          .atMost(Duration.ofSeconds(20))
          .until(() -> c.seqs.size() >= 2);
      Thread.sleep(100); // give the tailer every chance to (wrongly) deliver 4
      assertThat(new ArrayList<>(c.seqs)).isEqualTo(List.of(1L, 2L));
      assertThat(stream.visibleSeq()).isEqualTo(2);

      store.insert(ev(3));
      Awaitility.await()
          .alias("3 and 4")
          .atMost(Duration.ofSeconds(20))
          .until(() -> c.seqs.size() >= 4);
      assertThat(new ArrayList<>(c.seqs)).isEqualTo(List.of(1L, 2L, 3L, 4L));
    }
  }

  /**
   * 3 is a rolled-back transaction and never appears: after the gap timeout the stream moves on.
   */
  @Test
  void permanentHoleIsSkippedAfterTimeoutForLiveAndLateSubscribers() throws Exception {
    InMemoryEventStore store = new InMemoryEventStore();
    List<String> skippedRanges = Collections.synchronizedList(new ArrayList<>());
    try (TailingStream stream =
            TestStreams.storeOnly(
                store, 16, 16, 1, 150, (a, b) -> skippedRanges.add(a + ".." + b));
        SharedSubscriber fanout = new SharedSubscriber(stream, 1, 16)) {
      Collector early = new Collector();
      fanout.subscribe(1, early);
      for (long id : new long[] {1, 2, 4, 5}) {
        store.insert(ev(id));
      }
      Awaitility.await()
          .alias("1,2,4,5 after the gap timeout")
          .atMost(Duration.ofSeconds(20))
          .until(() -> early.seqs.size() >= 4);
      assertThat(new ArrayList<>(early.seqs)).isEqualTo(List.of(1L, 2L, 4L, 5L));
      assertThat(skippedRanges).isEqualTo(List.of("3..3"));

      // a subscriber that arrives later reads history from the store and must jump over 3 too
      Collector late = new Collector();
      fanout.subscribe(1, late);
      Awaitility.await()
          .alias("late subscriber to catch up")
          .atMost(Duration.ofSeconds(20))
          .until(() -> late.seqs.size() >= 4);
      assertThat(new ArrayList<>(late.seqs)).isEqualTo(List.of(1L, 2L, 4L, 5L));
      assertThat(late.caughtUp).isTrue();
    }
  }

  /**
   * Restart: the stream starts with 500 rows already in the store. Its window is filled from the
   * store before it starts, so a subscriber gets the last 64 events as history, and rows older than
   * that are rejected.
   */
  @Test
  void restartServesTheWindowHistoryThenLive() throws Exception {
    InMemoryEventStore store = new InMemoryEventStore();
    for (long i = 1; i <= 500; i++) {
      store.insert(ev(i));
    }
    try (TailingStream stream = TestStreams.storeOnly(store, 64, 32, 1, 60_000, (a, b) -> {});
        SharedSubscriber fanout = new SharedSubscriber(stream, 1, 64)) {
      assertThatExceptionOfType(IllegalArgumentException.class)
          .isThrownBy(() -> fanout.subscribe(436, new CheckingListener("too-old", 436, 0)))
          .withMessageContaining("the oldest retained seq is 437");

      CheckingListener l = new CheckingListener("restart", 437, 0);
      fanout.subscribe(437, l);
      for (long i = 501; i <= 600; i++) {
        store.insert(ev(i));
        stream.notifyChange();
      }
      Awaitility.await()
          .alias("600 events")
          .atMost(Duration.ofSeconds(20))
          .until(() -> l.last == 600);
      assertThat(l.ok).isTrue();
      assertThat(l.history).isEqualTo(64); // 437..500 from the window filled at start-up
      assertThat(l.count).isEqualTo(164);
    }
  }

  private static Event ev(long seq) {
    return new Event(seq, System.currentTimeMillis(), "w", "v" + seq);
  }

  /** Writers take ids, then become visible in random order; no hints, polling only; tiny ring. */
  private void runWriters(boolean withNotifications) throws Exception {
    int writers = 4;
    int perWriter = 5_000;
    int total = writers * perWriter;
    InMemoryEventStore store = new InMemoryEventStore();
    AtomicLong ids = new AtomicLong();
    ConcurrentLinkedQueue<String> gaps = new ConcurrentLinkedQueue<>();
    try (TailingStream stream =
            TestStreams.storeOnly(store, 4096, 128, 1, 60_000, (a, b) -> gaps.add(a + ".." + b));
        SharedSubscriber fanout = new SharedSubscriber(stream, 2, 64)) {

      CheckingListener fromStart = new CheckingListener("from-start", 1, 0);
      fanout.subscribe(1, fromStart);
      CheckingListener slow = new CheckingListener("slow", 1, 20_000);
      fanout.subscribe(1, slow, 1 << 15); // slow, but its buffer holds every event: never cancelled

      List<Thread> threads = new ArrayList<>();
      for (int w = 0; w < writers; w++) {
        Thread t =
            new Thread(
                () -> {
                  for (int i = 0; i < perWriter; i++) {
                    long id = ids.incrementAndGet(); // like a DB sequence ...
                    LockSupport.parkNanos(
                        ThreadLocalRandom.current().nextInt(50_000)); // ... commit later
                    store.insert(ev(id)); // visible out of order
                    if (withNotifications) {
                      stream.notifyChange();
                    }
                  }
                });
        threads.add(t);
        t.start();
      }
      Awaitility.await()
          .alias("half of the events")
          .atMost(Duration.ofSeconds(20))
          .until(() -> stream.visibleSeq() >= total / 2);
      long midFrom =
          stream.visibleSeq() - 64; // a late subscriber starts inside the window, not at seq 1
      CheckingListener midRun = new CheckingListener("mid-run", midFrom, 0);
      fanout.subscribe(midFrom, midRun);

      for (Thread t : threads) {
        Uninterruptibles.joinUninterruptibly(t);
      }
      Awaitility.await()
          .alias("all subscribers to reach " + total)
          .atMost(Duration.ofSeconds(20))
          .until(() -> fromStart.last == total && slow.last == total && midRun.last == total);
      assertThat(fromStart.ok && slow.ok && midRun.ok)
          .as("every subscriber must see 1..N in order")
          .isTrue();
      assertThat(fromStart.count).isEqualTo(total);
      assertThat(midRun.count).isEqualTo(total - midFrom + 1);
      assertThat(gaps.isEmpty())
          .as("no hole may be skipped when every writer commits: " + gaps)
          .isTrue();
    }
  }
}
