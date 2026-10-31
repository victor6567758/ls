package com.example.livestream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BinaryOperator;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/**
 * Duplicates are possible on both inputs. A seq is delivered once, after it is sealed. Every
 * arriving version is folded into the result by the merge policy as it comes, like a reduce.
 */
class RealtimeMergeTest {

  private static final long TS = 1_000L;

  /** Records the payload of every event it receives, in order. */
  private static final class Collector implements EventListener {
    private final List<String> payloads = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void onEvent(Event e, Source source) {
      payloads.add(e.payload());
    }

    int count() {
      return payloads.size();
    }

    List<String> payloads() {
      return List.copyOf(payloads);
    }
  }

  @Test
  void keepFirstDeliversTheFirstVersionThatArrived() {
    // the realtime version was published before the store row was inserted
    assertThat(deliveredPayloads(MergePolicies.KEEP_FIRST)).containsExactly("realtime 105/104");
  }

  /**
   * One candle, three arrivals: feed 105, feed 104, store 103; seq 2 seals seq 1. The policy folds
   * them in arrival order.
   */
  @Test
  void theSameCandleUnderEachBuiltInPolicy() {
    assertThat(mergedCandle(MergePolicies.KEEP_FIRST)).isEqualTo("feed 105");
    assertThat(mergedCandle(MergePolicies.KEEP_LAST)).isEqualTo("store 103");
    assertThat(mergedCandle(MergePolicies.PREFER_STORE)).isEqualTo("store 103");
  }

  @Test
  void preferStoreDeliversOnlyTheStoreVersion() {
    assertThat(deliveredPayloads(MergePolicies.PREFER_STORE)).containsExactly("store 103/102");
  }

  /** A custom fold: the result so far, then each later version appended, in arrival order. */
  private static final BinaryOperator<Version> APPEND =
      (sofar, next) ->
          new Version(
              new Event(
                  sofar.event().seq(),
                  TS,
                  "BTC",
                  sofar.event().payload() + " | " + next.origin() + ":" + next.event().payload()),
              next.origin());

  @Test
  void customMergeFoldsEveryVersionInArrivalOrder() {
    InMemoryEventStore store = new InMemoryEventStore();
    try (TailingStream stream = new TailingStream(store, 16, 16, 1, 60_000, (a, b) -> {}, APPEND);
        SharedSubscriber fanout = new SharedSubscriber(stream, 1, 16)) {
      stream.publish(candle(1, "realtime 104"));
      stream.publish(candle(1, "realtime 105")); // a later realtime version of the same seq
      store.insert(candle(1, "store 103"));
      store.insert(candle(2, "store 2"));
      Collector c = new Collector();
      fanout.subscribe(1, c);
      awaitDelivered(c, 1);
      // the first arrival is the starting result as it is; each later one is appended
      assertThat(c.payloads())
          .containsExactly("realtime 104 | REALTIME:realtime 105 | STORE:store 103");
    }
  }

  /**
   * Nothing is dropped before the fold: a repeated version is folded again, so a policy must give
   * the same result for a repeat (the built-ins do; APPEND shows the repeat).
   */
  @Test
  void aRepeatedVersionIsFoldedAgain() {
    InMemoryEventStore store = new InMemoryEventStore();
    try (TailingStream stream = new TailingStream(store, 16, 16, 1, 60_000, (a, b) -> {}, APPEND);
        SharedSubscriber fanout = new SharedSubscriber(stream, 1, 16)) {
      stream.publish(candle(1, "same"));
      stream.publish(candle(1, "same")); // the feed repeated itself
      store.insert(candle(2, "store 2"));
      store.insert(candle(1, "same"));
      Collector c = new Collector();
      fanout.subscribe(1, c);
      awaitDelivered(c, 1);
      assertThat(c.payloads()).containsExactly("same | REALTIME:same | STORE:same");
    }
  }

  @Test
  void aSealedSeqWaitsForAHigherSeqOrATombstone() throws Exception {
    InMemoryEventStore store = new InMemoryEventStore();
    try (TailingStream stream =
            new TailingStream(store, 16, 16, 1, 60_000, (a, b) -> {}, MergePolicies.PREFER_STORE);
        SharedSubscriber fanout = new SharedSubscriber(stream, 1, 16)) {
      stream.publish(candle(1, "realtime 1"));
      store.insert(candle(1, "store 1"));
      Collector c = new Collector();
      fanout.subscribe(1, c);
      Thread.sleep(100); // seq 1 is the newest seq: nothing says it is complete yet
      assertThat(c.count()).isZero();

      stream.publishTombstone(1); // the feed says: no more versions of seq 1
      awaitDelivered(c, 1);
      assertThat(c.payloads()).containsExactly("store 1");
    }
  }

  @Test
  void realtimeFillsAStoreHole() throws Exception {
    InMemoryEventStore store = new InMemoryEventStore();
    try (TailingStream stream =
            new TailingStream(store, 16, 16, 1, 60_000, (a, b) -> {}, MergePolicies.PREFER_STORE);
        SharedSubscriber fanout = new SharedSubscriber(stream, 1, 16)) {
      store.insert(candle(1, "store 1"));
      store.insert(candle(3, "store 3")); // seq 2 is missing from the store, but the feed has it
      stream.publish(candle(2, "realtime 2"));

      Collector c = new Collector();
      fanout.subscribe(1, c);
      awaitDelivered(c, 2);
      Thread.sleep(100);
      assertThat(c.payloads())
          .containsExactly("store 1", "realtime 2"); // seq 3 is still the newest
    }
  }

  /**
   * The realtime queue depends on the feed's order, so a lower seq after a higher one is refused,
   * not queued.
   */
  @Test
  void aRealtimeSeqBelowAnEarlierRealtimeSeqIsRefused() {
    InMemoryEventStore store = new InMemoryEventStore();
    try (TailingStream stream =
        new TailingStream(store, 16, 16, 1, 60_000, (a, b) -> {}, MergePolicies.PREFER_STORE)) {
      stream.publish(candle(2, "realtime 2"));
      assertThatExceptionOfType(IllegalArgumentException.class)
          .isThrownBy(() -> stream.publish(candle(1, "realtime 1")))
          .withMessageContaining("must be non-decreasing");
      stream.publish(candle(2, "realtime 2 again")); // a repeat of the same seq is allowed
    }
  }

  private static Event candle(long seq, String payload) {
    return new Event(seq, TS, "BTC", payload);
  }

  private static void awaitDelivered(Collector c, int n) {
    Awaitility.await()
        .alias(n + " event(s) delivered")
        .atMost(Duration.ofSeconds(20))
        .until(() -> c.count() >= n);
  }

  /**
   * Realtime versions of seq 1 first, then the store row for seq 1, then a row for seq 2, which
   * seals seq 1. Only seq 1 can be delivered: seq 2 is the newest seq, so it is not sealed yet.
   */
  private static List<String> deliveredPayloads(BinaryOperator<Version> policy) {
    InMemoryEventStore store = new InMemoryEventStore();
    try (TailingStream stream = new TailingStream(store, 16, 16, 1, 60_000, (a, b) -> {}, policy);
        SharedSubscriber fanout = new SharedSubscriber(stream, 1, 16)) {
      stream.publish(candle(1, "realtime 105/104"));
      store.insert(candle(1, "store 103/102"));
      store.insert(candle(2, "store 2"));
      Collector c = new Collector();
      fanout.subscribe(1, c);
      awaitDelivered(c, 1);
      return c.payloads();
    }
  }

  private static String mergedCandle(BinaryOperator<Version> policy) {
    InMemoryEventStore store = new InMemoryEventStore();
    try (TailingStream stream = new TailingStream(store, 16, 16, 1, 60_000, (a, b) -> {}, policy);
        SharedSubscriber fanout = new SharedSubscriber(stream, 1, 16)) {
      stream.publish(candle(1, "feed 105"));
      stream.publish(candle(1, "feed 104"));
      store.insert(candle(1, "store 103"));
      store.insert(candle(2, "store 2")); // seals seq 1
      Collector c = new Collector();
      fanout.subscribe(1, c);
      awaitDelivered(c, 1);
      return c.payloads().get(0);
    }
  }
}
