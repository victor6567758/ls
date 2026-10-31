package com.example.livestream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/** Edge cases of the independent-writers model: failures, late commits, cancellation, overruns. */
class TailingStreamEdgeCasesTest {

  /** Delegates to a real store, but fails the first few reads. */
  private static class FlakyStore implements EventStore {
    final InMemoryEventStore inner = new InMemoryEventStore();
    final AtomicInteger failuresLeft;

    FlakyStore(int failures) {
      failuresLeft = new AtomicInteger(failures);
    }

    @Override
    public Stream<Event> read(long fromSeq, int limit) {
      if (failuresLeft.getAndDecrement() > 0) {
        throw new IllegalStateException("simulated store outage");
      }
      return inner.read(fromSeq, limit);
    }

    @Override
    public long nextVisibleSeq(long fromSeq) {
      return inner.nextVisibleSeq(fromSeq);
    }

    @Override
    public long lastSeq() {
      return inner.lastSeq();
    }

    @Override
    public long firstSeqAtOrAfter(long ts) {
      return inner.firstSeqAtOrAfter(ts);
    }
  }

  @Test
  void tailerSurvivesTransientStoreErrorsWithoutLosingEvents() throws Exception {
    FlakyStore store = new FlakyStore(5);
    try (TailingStream stream = TestStreams.storeOnly(store, 16, 16, 1, 60_000, (a, b) -> {});
        SharedSubscriber fanout = new SharedSubscriber(stream, 1, 16)) {
      CheckingListener l = new CheckingListener("flaky", 1, 0);
      fanout.subscribe(1, l);
      for (long i = 1; i <= 50; i++) {
        store.inner.insert(ev(i));
      }
      Awaitility.await()
          .alias("all 50 events after the outage")
          .atMost(Duration.ofSeconds(20))
          .until(() -> l.last == 50);
      assertThat(l.ok).isTrue();
      assertThat(l.count).isEqualTo(50);
    }
  }

  /**
   * Documented trade-off: a transaction that commits after the gap timeout is missed, consistently
   * for everyone.
   */
  @Test
  void lateCommitAfterTheTimeoutIsMissedByLiveAndLateSubscribers() throws Exception {
    InMemoryEventStore store = new InMemoryEventStore();
    List<String> skipped = Collections.synchronizedList(new ArrayList<>());
    try (TailingStream stream =
            TestStreams.storeOnly(store, 16, 16, 1, 100, (a, b) -> skipped.add(a + ".." + b));
        SharedSubscriber fanout = new SharedSubscriber(stream, 1, 16)) {
      TailingStreamTest.Collector live = new TailingStreamTest.Collector();
      fanout.subscribe(1, live);
      store.insert(ev(1));
      store.insert(ev(2));
      store.insert(ev(4));
      Awaitility.await()
          .alias("1,2,4 after the timeout")
          .atMost(Duration.ofSeconds(20))
          .until(() -> snapshot(live).size() >= 3);

      store.insert(ev(3)); // the slow transaction finally commits: too late
      store.insert(ev(5));
      Awaitility.await()
          .alias("5")
          .atMost(Duration.ofSeconds(20))
          .until(() -> snapshot(live).size() >= 4);
      assertThat(snapshot(live)).isEqualTo(List.of(1L, 2L, 4L, 5L));

      TailingStreamTest.Collector late = new TailingStreamTest.Collector();
      fanout.subscribe(1, late);
      Awaitility.await()
          .alias("late subscriber")
          .atMost(Duration.ofSeconds(20))
          .until(() -> snapshot(late).size() >= 4);
      assertThat(snapshot(late))
          .as("everybody must agree, even though 3 is in the store")
          .isEqualTo(List.of(1L, 2L, 4L, 5L));
      assertThat(skipped).isEqualTo(List.of("3..3"));
    }
  }

  @Test
  void severalHolesAreSkippedOneAfterTheOther() throws Exception {
    InMemoryEventStore store = new InMemoryEventStore();
    List<String> skipped = Collections.synchronizedList(new ArrayList<>());
    try (TailingStream stream =
            TestStreams.storeOnly(store, 16, 16, 1, 80, (a, b) -> skipped.add(a + ".." + b));
        SharedSubscriber fanout = new SharedSubscriber(stream, 1, 16)) {
      TailingStreamTest.Collector c = new TailingStreamTest.Collector();
      fanout.subscribe(1, c);
      for (long id : new long[] {1, 3, 5, 12}) {
        store.insert(ev(id));
      }
      Awaitility.await()
          .alias("1,3,5,12")
          .atMost(Duration.ofSeconds(20))
          .until(() -> snapshot(c).size() >= 4);
      assertThat(snapshot(c)).isEqualTo(List.of(1L, 3L, 5L, 12L));
      assertThat(skipped).isEqualTo(List.of("2..2", "4..4", "6..11"));
      assertThat(stream.visibleSeq()).isEqualTo(12);
    }
  }

  /**
   * Ids are assigned in time order, so every id below a visible one was taken before it committed.
   * Skipping a whole hole range after one timeout is therefore safe: a transaction shorter than the
   * timeout always makes it. Here 3 never commits, 4 commits well inside the timeout, 5 was visible
   * first.
   */
  @Test
  void aTransactionShorterThanTheTimeoutSurvivesInsideAHoleRange() throws Exception {
    InMemoryEventStore store = new InMemoryEventStore();
    List<String> skipped = Collections.synchronizedList(new ArrayList<>());
    try (TailingStream stream =
            TestStreams.storeOnly(store, 16, 16, 1, 400, (a, b) -> skipped.add(a + ".." + b));
        SharedSubscriber fanout = new SharedSubscriber(stream, 1, 16)) {
      TailingStreamTest.Collector c = new TailingStreamTest.Collector();
      fanout.subscribe(1, c);
      store.insert(ev(1));
      store.insert(ev(2));
      store.insert(ev(5)); // 3 and 4 were taken before 5 committed: both are holes now
      Thread.sleep(
          150); // writer of 4 is still inside its transaction, well under the 400 ms timeout
      store.insert(ev(4));
      Awaitility.await()
          .alias("1,2,4,5")
          .atMost(Duration.ofSeconds(20))
          .until(() -> snapshot(c).size() >= 4);
      assertThat(snapshot(c)).isEqualTo(List.of(1L, 2L, 4L, 5L));
      Awaitility.await()
          .alias("the permanent hole at 3 to be skipped")
          .atMost(Duration.ofSeconds(20))
          .until(() -> !skipped.isEmpty());
      assertThat(skipped).as("only the dead id may be skipped, not 4").isEqualTo(List.of("3..3"));
    }
  }

  /**
   * A store that cannot report later ids (nextVisibleSeq = -1) gives the Tailer nothing above the
   * hole, so it cannot tell a hole from a seq not written yet: it waits and skips nothing.
   */
  @Test
  void withoutHoleDetectionTheTailerWaitsForever() throws Exception {
    FlakyStore noDetection =
        new FlakyStore(0) {
          @Override
          public long nextVisibleSeq(long fromSeq) {
            return -1;
          }
        };
    List<String> skipped = Collections.synchronizedList(new ArrayList<>());
    try (TailingStream stream =
            TestStreams.storeOnly(noDetection, 16, 16, 1, 30, (a, b) -> skipped.add(a + ".." + b));
        SharedSubscriber fanout = new SharedSubscriber(stream, 1, 16)) {
      TailingStreamTest.Collector c = new TailingStreamTest.Collector();
      fanout.subscribe(1, c);
      noDetection.inner.insert(ev(1));
      noDetection.inner.insert(ev(3));
      Awaitility.await()
          .alias("1")
          .atMost(Duration.ofSeconds(20))
          .until(() -> snapshot(c).size() >= 1);
      Thread.sleep(300); // ten times the gap timeout
      assertThat(snapshot(c)).isEqualTo(List.of(1L));
      assertThat(skipped.isEmpty()).isTrue();
    }
  }

  @Test
  void subscribeFromLatestSeesOnlyNewEvents() throws Exception {
    InMemoryEventStore store = new InMemoryEventStore();
    for (long i = 1; i <= 10; i++) {
      store.insert(ev(i));
    }
    try (TailingStream stream = TestStreams.storeOnly(store, 16, 16, 1, 60_000, (a, b) -> {});
        SharedSubscriber fanout = new SharedSubscriber(stream, 1, 16)) {
      TailingStreamTest.Collector c = new TailingStreamTest.Collector();
      fanout.subscribe(Subscriber.LATEST, c);
      store.insert(ev(11));
      store.insert(ev(12));
      stream.notifyChange();
      Awaitility.await()
          .alias("11 and 12")
          .atMost(Duration.ofSeconds(20))
          .until(() -> snapshot(c).size() >= 2);
      Thread.sleep(50);
      assertThat(snapshot(c)).isEqualTo(List.of(11L, 12L));
    }
  }

  @Test
  void subscribeFromSeqInTheMiddleStartsExactlyThere() throws Exception {
    InMemoryEventStore store = new InMemoryEventStore();
    try (TailingStream stream = TestStreams.storeOnly(store, 16, 16, 1, 60_000, (a, b) -> {});
        SharedSubscriber fanout = new SharedSubscriber(stream, 1, 16)) {
      for (long i = 1; i <= 100; i++) {
        store.insert(ev(i));
      }
      Awaitility.await()
          .alias("the Tailer to merge 1..100")
          .atMost(Duration.ofSeconds(20))
          .until(() -> stream.visibleSeq() == 100);
      TailingStreamTest.Collector c = new TailingStreamTest.Collector();
      fanout.subscribe(95, c); // the window holds the last 16: 85..100
      Awaitility.await()
          .alias("95..100")
          .atMost(Duration.ofSeconds(20))
          .until(() -> snapshot(c).size() >= 6);
      assertThat(snapshot(c)).isEqualTo(List.of(95L, 96L, 97L, 98L, 99L, 100L));
    }
  }

  @Test
  void cancelledSubscriptionStopsReceiving() throws Exception {
    InMemoryEventStore store = new InMemoryEventStore();
    try (TailingStream stream = TestStreams.storeOnly(store, 64, 16, 1, 60_000, (a, b) -> {});
        SharedSubscriber fanout = new SharedSubscriber(stream, 1, 16)) {
      TailingStreamTest.Collector c = new TailingStreamTest.Collector();
      Subscription sub = fanout.subscribe(1, c);
      for (long i = 1; i <= 10; i++) {
        store.insert(ev(i));
      }
      Awaitility.await()
          .alias("first 10")
          .atMost(Duration.ofSeconds(20))
          .until(() -> snapshot(c).size() >= 10);
      sub.cancel();
      Thread.sleep(50); // let an in-flight pump finish
      int before = snapshot(c).size();
      for (long i = 11; i <= 30; i++) {
        store.insert(ev(i));
        stream.notifyChange();
      }
      Awaitility.await()
          .alias("the tailer to load 30")
          .atMost(Duration.ofSeconds(20))
          .until(() -> stream.visibleSeq() == 30);
      Thread.sleep(100);
      assertThat(snapshot(c).size()).isEqualTo(before);
      assertThat(sub.isCancelled()).isTrue();
    }
  }

  @Test
  void listenerExceptionTerminatesOnlyThatSubscription() throws Exception {
    InMemoryEventStore store = new InMemoryEventStore();
    try (TailingStream stream = TestStreams.storeOnly(store, 64, 16, 1, 60_000, (a, b) -> {});
        SharedSubscriber fanout = new SharedSubscriber(stream, 1, 16)) { // same shard for both
      AtomicInteger errors = new AtomicInteger();
      Subscription bad =
          fanout.subscribe(
              1,
              new EventListener() {
                @Override
                public void onEvent(Event e, Source s) {
                  throw new IllegalStateException("boom");
                }

                @Override
                public void onError(Throwable t) {
                  errors.incrementAndGet();
                }
              });
      CheckingListener good = new CheckingListener("good", 1, 0);
      fanout.subscribe(1, good);
      for (long i = 1; i <= 20; i++) {
        store.insert(ev(i));
      }
      Awaitility.await()
          .alias("the healthy subscriber to finish")
          .atMost(Duration.ofSeconds(20))
          .until(() -> good.last == 20);
      assertThat(good.ok).isTrue();
      assertThat(bad.isCancelled()).isTrue();
      assertThat(errors.get()).as("onError exactly once").isEqualTo(1);
    }
  }

  /**
   * A slow live subscriber whose buffer fills is cancelled, and only it. It re-subscribes from its
   * next seq and gets the rest from the window, with nothing lost.
   */
  @Test
  void slowSubscriberIsCancelledAndResumesFromItsNextSeq() throws Exception {
    InMemoryEventStore store = new InMemoryEventStore();
    try (TailingStream stream = TestStreams.storeOnly(store, 4_096, 64, 1, 60_000, (a, b) -> {});
        SharedSubscriber fanout = new SharedSubscriber(stream, 1, 4)) {
      CheckingListener slow = new CheckingListener("slow", 1, 100_000);
      Subscription sub = fanout.subscribe(1, slow, 16); // 16 events of buffer, far fewer than 2,000
      for (long i = 1; i <= 2_000; i++) {
        store.insert(ev(i));
      }
      stream.notifyChange();
      Awaitility.await()
          .alias("the slow subscriber to be cancelled")
          .atMost(Duration.ofSeconds(20))
          .until(sub::isCancelled);
      assertThat(stream.visibleSeq())
          .as("the Tailer is not held back by the slow subscriber")
          .isEqualTo(2_000);

      long resumeAt = sub.nextSeq();
      CheckingListener resumed = new CheckingListener("resumed", resumeAt, 0);
      fanout.subscribe(resumeAt, resumed);
      Awaitility.await()
          .alias("the resumed subscriber to finish")
          .atMost(Duration.ofSeconds(20))
          .until(() -> resumed.last == 2_000);
      assertThat(resumed.ok).isTrue();
      assertThat(resumed.count).isEqualTo(2_000 - resumeAt + 1);
    }
  }

  @Test
  void manyIndependentSubscribersAllSeeTheSameStream() throws Exception {
    InMemoryEventStore store = new InMemoryEventStore();
    int subs = 200;
    int total = 3_000;
    try (TailingStream stream = TestStreams.storeOnly(store, 64, 64, 1, 60_000, (a, b) -> {});
        SharedSubscriber fanout = new SharedSubscriber(stream, 3, 32)) {
      List<CheckingListener> all = new ArrayList<>();
      for (int i = 0; i < subs; i++) {
        CheckingListener l = new CheckingListener("s" + i, 1, 0);
        all.add(l);
        fanout.subscribe(1, l, 1 << 12); // 200 subscribers on 3 shards can lag 3,000 events
      }
      for (long i = 1; i <= total; i++) {
        store.insert(ev(i));
        if (i % 100 == 0) {
          stream.notifyChange();
        }
      }
      Awaitility.await()
          .alias("all " + subs + " subscribers")
          .atMost(Duration.ofSeconds(20))
          .until(() -> all.stream().allMatch(l -> l.last == total));
      assertThat(all.stream().allMatch(l -> l.ok && l.count == total)).isTrue();
    }
  }

  /**
   * A store that breaks its read contract once the stream runs: a read from seq 2 returns rows from
   * 3.
   */
  @Test
  void aStoreThatSkipsARowInARead_isRefusedNotSilentlyLost() throws Exception {
    InMemoryEventStore inner = new InMemoryEventStore();
    AtomicBoolean broken = new AtomicBoolean();
    EventStore store =
        new EventStore() {
          @Override
          public Stream<Event> read(long fromSeq, int limit) {
            return broken.get() ? inner.read(fromSeq + 1, limit) : inner.read(fromSeq, limit);
          }

          @Override
          public long lastSeq() {
            return inner.lastSeq();
          }

          @Override
          public long firstSeqAtOrAfter(long ts) {
            return inner.firstSeqAtOrAfter(ts);
          }
        };
    try (TailingStream stream = TestStreams.storeOnly(store, 16, 16, 1, 60_000, (a, b) -> {});
        SharedSubscriber fanout = new SharedSubscriber(stream, 1, 16)) {
      TailingStreamTest.Collector c = new TailingStreamTest.Collector();
      fanout.subscribe(1, c);
      inner.insert(ev(1));
      Awaitility.await()
          .alias("seq 1 delivered")
          .atMost(Duration.ofSeconds(20))
          .until(() -> stream.visibleSeq() == 1);

      broken.set(true);
      inner.insert(ev(2));
      inner.insert(ev(3));
      Thread.sleep(300); // many polls: every one must be refused
      assertThat(snapshot(c)).isEqualTo(List.of(1L));
      assertThat(stream.visibleSeq()).isEqualTo(1);
    }
  }

  /**
   * A store whose read skips a row is refused at start-up too: the window is not loaded from it.
   */
  @Test
  void aStoreThatSkipsARowAtStartUpFailsTheConstructor() {
    InMemoryEventStore inner = new InMemoryEventStore();
    inner.insert(ev(1));
    inner.insert(ev(2));
    inner.insert(ev(3));
    EventStore skipping =
        new EventStore() {
          @Override
          public Stream<Event> read(long fromSeq, int limit) {
            return inner.read(fromSeq == 1 ? 3 : fromSeq, limit);
          }

          @Override
          public long lastSeq() {
            return inner.lastSeq();
          }

          @Override
          public long firstSeqAtOrAfter(long ts) {
            return inner.firstSeqAtOrAfter(ts);
          }
        };
    assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(() -> new TailingStream(skipping, 16, 16, 1, 60_000, (a, b) -> {}))
        .withMessageContaining("must be contiguous");
  }

  private static Event ev(long seq) {
    return new Event(seq, System.currentTimeMillis(), "w", "v" + seq);
  }

  private static List<Long> snapshot(TailingStreamTest.Collector c) {
    synchronized (c.seqs) {
      return new ArrayList<>(c.seqs);
    }
  }
}
