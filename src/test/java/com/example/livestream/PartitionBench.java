package com.example.livestream;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

/**
 * Write throughput versus the number of partitions. Each partition is a {@link TailingStream} over
 * its own {@link InMemoryEventStore}; the benchmarked operation is what a writer service does:
 * route the key, take the next id of that partition's data source and insert the event there, while
 * the partition's tailer and the subscribers keep running. The number of writer threads is set by
 * JMH's {@code -t}.
 *
 * <ul>
 *   <li>{@code partitions}: 1 is a single data source (one insert lock, one tailer); more
 *       partitions means more independent data sources and tailers.
 *   <li>{@code subscribers}: live subscribers per partition with an empty listener. They only read,
 *       so the writers' throughput should not change between 0 and 16.
 * </ul>
 *
 * The in-memory store keeps every event, so heap use grows with the throughput; the fork gets 16
 * GB.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 4, time = 2)
@Fork(
    value = 1,
    jvmArgsAppend = {"-Xmx16g"})
@TestMethodOrder(
    MethodOrderer.OrderAnnotation.class) // correctness test first, then the real JMH run
public class PartitionBench {

  @State(Scope.Benchmark)
  public static class Streams {
    @Param({"1", "2", "4", "8"})
    public int partitions;

    @Param({"0", "16"})
    public int subscribers;

    PartitionedStream ps;
    final List<InMemoryEventStore> stores = new ArrayList<>();
    final List<TailingStream> streams = new ArrayList<>();
    AtomicLong[] lastSeq;

    @Setup(Level.Trial)
    public void up() {
      lastSeq = new AtomicLong[partitions];
      for (int p = 0; p < partitions; p++) {
        InMemoryEventStore store = new InMemoryEventStore();
        stores.add(store);
        streams.add(TestStreams.storeOnly(store, 1 << 16, 512, 1, 60_000, (a, b) -> {}));
        lastSeq[p] = new AtomicLong();
      }
      ps =
          new PartitionedStream(
              streams, s -> new SharedSubscriber(s, 1, 256), Subscriber.DEFAULT_CAPACITY);
      PartitionedListener empty =
          new PartitionedListener() {
            @Override
            public void onEvent(int partition, Event e, EventListener.Source source) {
              // deliberately nothing shared: a subscriber that pays for a contended counter would
              // hide the effect
            }
          };
      for (int i = 0; i < subscribers; i++) {
        ps.subscribeFromLatest(empty);
      }
    }

    @TearDown(Level.Trial)
    public void down() {
      ps.close();
      streams.forEach(TailingStream::close);
      streams.clear();
      stores.clear();
    }

    /** What a writer service does. Returns the id (seq) inside the key's partition. */
    long insert(String key) {
      int p = ps.partitionFor(key);
      long seq = lastSeq[p].incrementAndGet();
      stores.get(p).insert(new Event(seq, System.currentTimeMillis(), key, "v"));
      return seq;
    }
  }

  @State(Scope.Thread)
  public static class Keys {
    static final int N = 1024; // power of two, spreads evenly over partitions
    final String[] keys = new String[N];
    int i;

    @Setup(Level.Trial)
    public void up() {
      long id = Thread.currentThread().getId();
      for (int k = 0; k < N; k++) {
        keys[k] = "t" + id + "-k" + k;
      }
    }

    String next() {
      return keys[i++ & (N - 1)];
    }
  }

  @Benchmark
  public long insert(Streams s, Keys k) {
    return s.insert(k.next());
  }

  /**
   * Correctness first: calls the benchmarked method directly, like a regular unit test. The first
   * call must return id 1 in its partition, further calls must return consecutive ids per
   * partition, and a subscriber must receive every event exactly once and in order per partition.
   * If this fails, the throughput numbers of the next test would be meaningless.
   */
  @Test
  @Order(1)
  void insertIsCorrectForEveryConfiguration() throws Exception {
    for (int partitions : new int[] {1, 4}) {
      for (int subscribers : new int[] {0, 2}) {
        String config = "partitions=" + partitions + "/subscribers=" + subscribers;
        Streams s = new Streams();
        s.partitions = partitions;
        s.subscribers = subscribers;
        s.up();
        try {
          AtomicReference<String> error = new AtomicReference<>();
          AtomicInteger received = new AtomicInteger();
          long[] expected =
              new long[partitions]; // each index is touched by one dispatcher thread only
          java.util.Arrays.fill(expected, 1);
          s.ps.subscribeFromStart(
              new PartitionedListener() {
                @Override
                public void onEvent(int partition, Event e, EventListener.Source source) {
                  if (e.seq() != expected[partition]++) {
                    error.compareAndSet(
                        null, config + ": partition " + partition + " got " + e.seq());
                  }
                  received.incrementAndGet();
                }
              });

          Keys k = new Keys();
          k.up();
          PartitionBench bench = new PartitionBench();

          String firstKey = k.keys[k.i & (Keys.N - 1)];
          assertThat(bench.insert(s, k))
              .as(config + ": first event of a partition must get id 1")
              .isEqualTo(1);

          long[] inserted = new long[partitions];
          inserted[s.ps.partitionFor(firstKey)] = 1;
          int total = 1;
          for (int i = 0; i < 2_000; i++) {
            int p = s.ps.partitionFor(k.keys[k.i & (Keys.N - 1)]);
            long seq = bench.insert(s, k);
            assertThat(seq)
                .as(config + ": partition " + p + " must be consecutive")
                .isEqualTo(++inserted[p]);
            total++;
          }

          int expectedTotal = total;
          Awaitility.await()
              .alias(config + ": events delivered")
              .atMost(Duration.ofSeconds(10))
              .until(() -> received.get() >= expectedTotal);
          assertThat(error.get()).isNull();
          assertThat(received.get())
              .as(config + ": every event must reach the subscriber")
              .isEqualTo(total);
          for (int p = 0; p < partitions; p++) {
            assertThat(s.streams.get(p).visibleSeq())
                .as(config + ": partition " + p)
                .isEqualTo(inserted[p]);
          }
        } finally {
          s.down();
        }
      }
    }
  }

  /**
   * Runs this benchmark from {@code mvn test} / the IDE with short settings (no fork, 1 s warm-up,
   * 2 s measurement, a reduced matrix), so it is a quick check that also prints a result table. For
   * a proper measurement run {@code org.openjdk.jmh.Main PartitionBench}, which forks a fresh JVM
   * and runs the full matrix.
   */
  @Test
  @Order(2)
  void runsAsRegularTest() throws RunnerException {
    Options options =
        new OptionsBuilder()
            .include(PartitionBench.class.getName() + ".insert")
            .forks(0) // same JVM: works under surefire without a classpath hand-over
            .warmupIterations(1)
            .warmupTime(TimeValue.seconds(1))
            .measurementIterations(1)
            .measurementTime(TimeValue.seconds(2))
            .threads(Math.min(8, Runtime.getRuntime().availableProcessors()))
            .param("partitions", "1", "4")
            .build();
    Collection<RunResult> results = new Runner(options).run();
    assertThat(results.size()).isEqualTo(2 * 2); // partitions x subscribers
    for (RunResult r : results) {
      assertThat(r.getPrimaryResult().getScore() > 0).isTrue();
    }
  }
}
