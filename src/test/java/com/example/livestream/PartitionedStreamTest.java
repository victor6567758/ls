package com.example.livestream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.google.common.util.concurrent.Uninterruptibles;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

class PartitionedStreamTest {

  /**
   * N partitions, each a TailingStream over its own store, plus writers that route like the readers
   * do.
   */
  private static final class Cluster implements AutoCloseable {
    final List<InMemoryEventStore> stores = new ArrayList<>();
    final List<TailingStream> streams = new ArrayList<>();
    final AtomicLong[] lastSeq;
    final PartitionedStream ps;

    Cluster(int partitions, int ring) {
      lastSeq = new AtomicLong[partitions];
      for (int p = 0; p < partitions; p++) {
        InMemoryEventStore store = new InMemoryEventStore();
        stores.add(store);
        streams.add(TestStreams.storeOnly(store, ring, 64, 1, 60_000, (a, b) -> {}));
        lastSeq[p] = new AtomicLong();
      }
      ps =
          new PartitionedStream(
              streams,
              s -> new SharedSubscriber(s, 1, 64),
              1 << 15); // a writer burst can run far ahead of a shard
    }

    /**
     * What a writer service does: choose the data source by key, take the next id there, insert.
     */
    long insert(String key) {
      int p = ps.partitionFor(key);
      long seq = lastSeq[p].incrementAndGet();
      stores.get(p).insert(new Event(seq, System.currentTimeMillis(), key, "v"));
      return seq;
    }

    void awaitVisible() {
      Awaitility.await()
          .alias("every partition to show its last inserted event")
          .atMost(Duration.ofSeconds(20))
          .until(
              () ->
                  IntStream.range(0, streams.size())
                      .allMatch(p -> streams.get(p).visibleSeq() >= lastSeq[p].get()));
    }

    long[] heads() {
      long[] h = new long[streams.size()];
      for (int p = 0; p < h.length; p++) {
        h[p] = lastSeq[p].get() + 1;
      }
      return h;
    }

    @Override
    public void close() {
      ps.close();
      streams.forEach(TailingStream::close);
    }
  }

  /** Checks per-partition order and routing; there is deliberately no cross-partition check. */
  private static final class Recorder implements PartitionedListener {
    final PartitionedStream ps;
    final AtomicLongArray expected;
    final AtomicInteger total = new AtomicInteger();
    final AtomicBoolean caughtUp = new AtomicBoolean();
    final AtomicReference<String> error = new AtomicReference<>();

    Recorder(PartitionedStream ps, long[] firstSeqPerPartition) {
      this.ps = ps;
      this.expected = new AtomicLongArray(firstSeqPerPartition);
    }

    @Override
    public void onEvent(int partition, Event e, EventListener.Source source) {
      // one partition = one dispatcher thread, so get/set per index is not racy
      long want = expected.get(partition);
      if (e.seq() != want) {
        error.compareAndSet(
            null, "partition " + partition + ": expected " + want + " got " + e.seq());
      }
      if (ps.partitionFor(e.key()) != partition) {
        error.compareAndSet(null, e.key() + " delivered by partition " + partition);
      }
      expected.set(partition, e.seq() + 1);
      total.incrementAndGet();
    }

    @Override
    public void onCaughtUp() {
      caughtUp.set(true);
    }

    void await(int events) {
      Awaitility.await()
          .alias(events + " events delivered")
          .atMost(Duration.ofSeconds(20))
          .until(() -> total.get() >= events);
      assertThat(error.get()).isNull();
      assertThat(total.get()).isEqualTo(events);
    }
  }

  @Test
  void separateDataSourcesEachOrderedAndGapFreeAndResumableByVectorCursor() throws Exception {
    int partitions = 3, writers = 4, perWriter = 5_000;
    try (Cluster c = new Cluster(partitions, 1 << 10)) {
      Recorder all = new Recorder(c.ps, ones(partitions));
      PartitionedSubscription sub = c.ps.subscribeFromStart(all);

      insertConcurrently(c, writers, perWriter, "a");
      all.await(writers * perWriter);

      // every data source holds exactly its own partition's events, gap-free
      c.awaitVisible();
      long sum = 0;
      for (int p = 0; p < partitions; p++) {
        assertThat(c.stores.get(p).lastSeq()).isEqualTo(c.lastSeq[p].get());
        sum += c.stores.get(p).lastSeq();
      }
      assertThat(sum).isEqualTo(writers * perWriter);
      assertThat(all.caughtUp.get()).isTrue();

      // resume from the vector cursor: exactly the new events, each partition continuing where it
      // stopped
      long[] cursor = sub.cursor();
      sub.cancel();
      insertConcurrently(c, 2, 500, "b");
      Recorder resumed = new Recorder(c.ps, cursor);
      c.ps.subscribeFromCursor(cursor, resumed);
      resumed.await(2 * 500);
    }
  }

  @Test
  void routingIsStableInRangeAndSpreadsKeys() {
    try (Cluster c = new Cluster(4, 64)) {
      int[] hits = new int[4];
      for (int i = 0; i < 4_000; i++) {
        String k = "user-" + i;
        int p = c.ps.partitionFor(k);
        assertThat(p >= 0 && p < 4).isTrue();
        assertThat(c.ps.partitionFor(k)).isEqualTo(p);
        hits[p]++;
      }
      for (int h : hits) {
        assertThat(h > 700).as("badly skewed routing: " + java.util.Arrays.toString(hits)).isTrue();
      }
    }
  }

  @Test
  void rejectsBadArguments() {
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                new PartitionedStream(
                    List.of(), s -> new SharedSubscriber(s, 1, 16), Subscriber.DEFAULT_CAPACITY));
    try (Cluster c = new Cluster(2, 64)) {
      assertThatExceptionOfType(IllegalArgumentException.class)
          .isThrownBy(
              () ->
                  c.ps.subscribeFromCursor(
                      new long[] {1},
                      new PartitionedListener() {
                        @Override
                        public void onEvent(int partition, Event e, EventListener.Source source) {}
                      }));
    }
  }

  @Test
  void subscribeFromLatestSeesOnlyNewEventsInEveryPartition() throws Exception {
    try (Cluster c = new Cluster(3, 256)) {
      for (int i = 0; i < 300; i++) {
        c.insert("old" + i);
      }
      c.awaitVisible();
      Recorder r = new Recorder(c.ps, c.heads());
      c.ps.subscribeFromLatest(r);
      for (int i = 0; i < 100; i++) {
        c.insert("new" + i);
      }
      r.await(100);
    }
  }

  @Test
  void caughtUpFiresOnceAfterAllPartitionsEvenWhenSomeAreEmpty() throws Exception {
    try (Cluster c = new Cluster(3, 256)) {
      String onlyPartition0 = keyFor(c.ps, 0, 0);
      for (int i = 0; i < 50; i++) {
        c.insert(onlyPartition0); // partitions 1 and 2 stay empty
      }
      c.awaitVisible();
      AtomicInteger caughtUp = new AtomicInteger();
      AtomicInteger events = new AtomicInteger();
      c.ps.subscribeFromStart(
          new PartitionedListener() {
            @Override
            public void onEvent(int partition, Event e, EventListener.Source source) {
              events.incrementAndGet();
            }

            @Override
            public void onCaughtUp() {
              caughtUp.incrementAndGet();
            }
          });
      Awaitility.await()
          .alias("onCaughtUp and 50 events")
          .atMost(Duration.ofSeconds(20))
          .until(() -> caughtUp.get() >= 1 && events.get() >= 50);
      Thread.sleep(100); // a second call, if there were one, would show up here
      assertThat(caughtUp.get()).isEqualTo(1);
      assertThat(events.get()).isEqualTo(50);
    }
  }

  @Test
  void aThrowingListenerCancelsEveryPartitionAndReportsOnce() throws Exception {
    try (Cluster c = new Cluster(3, 256)) {
      AtomicInteger errors = new AtomicInteger();
      AtomicInteger events = new AtomicInteger();
      PartitionedSubscription sub =
          c.ps.subscribeFromStart(
              new PartitionedListener() {
                @Override
                public void onEvent(int partition, Event e, EventListener.Source source) {
                  events.incrementAndGet();
                  throw new IllegalStateException("boom");
                }

                @Override
                public void onError(Throwable t) {
                  errors.incrementAndGet();
                }
              });
      for (int i = 0; i < 300; i++) {
        c.insert("k" + i);
      }
      Awaitility.await()
          .alias("the listener error to be reported")
          .atMost(Duration.ofSeconds(20))
          .until(() -> errors.get() > 0);
      Thread.sleep(100);
      assertThat(errors.get()).isEqualTo(1);
      assertThat(sub.isCancelled()).isTrue();
      for (int p = 0; p < 3; p++) {
        assertThat(sub.partition(p).isCancelled()).isTrue();
      }
      assertThat(events.get() <= 3)
          .as("each partition delivers at most the one event that threw")
          .isTrue();
    }
  }

  /** The point of separate data sources: a stalled store holds up only its own partition. */
  @Test
  void aStalledDataSourceDoesNotHoldUpOtherPartitions() throws Exception {
    CountDownLatch release = new CountDownLatch(1);
    InMemoryEventStore stalledInner = new InMemoryEventStore();
    EventStore stalledStore =
        new EventStore() {
          @Override
          public Stream<Event> read(long fromSeq, int limit) {
            Uninterruptibles.awaitUninterruptibly(release); // the database does not answer
            return stalledInner.read(fromSeq, limit);
          }

          @Override
          public long nextVisibleSeq(long fromSeq) {
            return stalledInner.nextVisibleSeq(fromSeq);
          }

          @Override
          public long lastSeq() {
            return stalledInner.lastSeq();
          }

          @Override
          public long firstSeqAtOrAfter(long ts) {
            return stalledInner.firstSeqAtOrAfter(ts);
          }
        };
    InMemoryEventStore healthyStore = new InMemoryEventStore();
    TailingStream stalled = TestStreams.storeOnly(stalledStore, 16, 16, 1, 60_000, (a, b) -> {});
    TailingStream healthy = TestStreams.storeOnly(healthyStore, 16, 16, 1, 60_000, (a, b) -> {});
    // the buffer holds the whole burst: this test is about the stall, not about overflow
    // (SlowSubscriberTest covers that)
    try (PartitionedStream ps =
        new PartitionedStream(
            List.of(stalled, healthy), s -> new SharedSubscriber(s, 1, 16), 4_096)) {
      Recorder r = new Recorder(ps, ones(2));
      ps.subscribeFromStart(r);

      String stalledKey = keyFor(ps, 0, 0);
      String healthyKey = keyFor(ps, 1, 0);
      for (int i = 1; i <= 2_000; i++) { // far more than the ring of 16 holds
        healthyStore.insert(new Event(i, System.currentTimeMillis(), healthyKey, "v"));
      }
      for (int i = 1; i <= 10; i++) {
        stalledInner.insert(new Event(i, System.currentTimeMillis(), stalledKey, "v"));
      }
      r.await(2_000); // all of them came from the healthy partition
      assertThat(r.expected.get(0)).as("the stalled partition delivered nothing").isEqualTo(1);
      assertThat(r.expected.get(1)).isEqualTo(2_001);

      release.countDown(); // the database comes back
      r.await(2_010);
      assertThat(r.expected.get(0)).isEqualTo(11);
    } finally {
      release.countDown();
      stalled.close();
      healthy.close();
    }
  }

  /**
   * The same partitioned model with one dedicated thread per partition's subscription instead of
   * shared shards.
   */
  @Test
  void dedicatedSubscribersDeliverEveryPartitionInOrder() throws Exception {
    int partitions = 3, perPartition = 500;
    List<InMemoryEventStore> stores = new ArrayList<>();
    List<TailingStream> streams = new ArrayList<>();
    for (int p = 0; p < partitions; p++) {
      InMemoryEventStore store = new InMemoryEventStore();
      stores.add(store);
      streams.add(TestStreams.storeOnly(store, 4_096, 64, 1, 60_000, (a, b) -> {}));
    }
    AtomicLongArray last = new AtomicLongArray(partitions);
    AtomicLongArray count = new AtomicLongArray(partitions);
    AtomicBoolean ordered = new AtomicBoolean(true);
    PartitionedListener listener =
        (partition, e, source) -> {
          if (e.seq() != last.get(partition) + 1) {
            ordered.set(false);
          }
          last.set(partition, e.seq());
          count.incrementAndGet(partition);
        };
    try (PartitionedStream ps = new PartitionedStream(streams, DedicatedSubscriber::new, 1 << 12)) {
      ps.subscribeFromStart(listener);
      for (int p = 0; p < partitions; p++) {
        for (long seq = 1; seq <= perPartition; seq++) {
          stores.get(p).insert(new Event(seq, 0, "k", "v"));
        }
      }
      Awaitility.await()
          .alias("every partition to deliver " + perPartition)
          .atMost(Duration.ofSeconds(20))
          .until(() -> IntStream.range(0, partitions).allMatch(p -> count.get(p) == perPartition));
      assertThat(ordered.get()).as("each partition in order, gap-free").isTrue();
    }
  }

  private static long[] ones(int n) {
    long[] a = new long[n];
    java.util.Arrays.fill(a, 1);
    return a;
  }

  private static String keyFor(PartitionedStream ps, int partition, int nth) {
    for (int i = 0, found = 0; ; i++) {
      String k = "key" + i;
      if (ps.partitionFor(k) == partition && found++ == nth) {
        return k;
      }
    }
  }

  private static void insertConcurrently(Cluster c, int threads, int each, String prefix) {
    List<Thread> ts = new ArrayList<>();
    for (int t = 0; t < threads; t++) {
      int id = t;
      Thread th =
          new Thread(
              () -> {
                for (int i = 0; i < each; i++) {
                  c.insert(prefix + (id * each + i));
                }
              });
      ts.add(th);
      th.start();
    }
    for (Thread th : ts) {
      Uninterruptibles.joinUninterruptibly(th);
    }
  }
}
