package com.example.livestream;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.util.concurrent.Uninterruptibles;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/**
 * What a slow subscriber does and does not affect. There is no back-pressure towards the writers or
 * the Tailer: the ring is overwritten freely and a slow reader recovers from the store. The only
 * thing a stuck listener holds up is the dispatcher shard thread that runs it.
 */
class SlowSubscriberTest {

  /**
   * A listener that is completely stuck until released (a client that stopped reading its socket).
   */
  private static final class BlockingListener implements EventListener {
    final CountDownLatch release;
    volatile long last, count;
    volatile boolean ok = true;
    private long expected = 1; // touched by the one dispatcher thread only

    BlockingListener(CountDownLatch release) {
      this.release = release;
    }

    @Override
    public void onEvent(Event e, Source source) {
      Uninterruptibles.awaitUninterruptibly(release);
      if (e.seq() != expected) {
        ok = false;
      }
      expected = e.seq() + 1;
      last = e.seq();
      count++;
    }
  }

  @Test
  void aStuckSubscriberDoesNotHoldBackTheTailerOrSubscribersOnOtherDispatchers() throws Exception {
    InMemoryEventStore store = new InMemoryEventStore();
    CountDownLatch release = new CountDownLatch(1);
    try (TailingStream stream = TestStreams.storeOnly(store, 8, 64, 1, 60_000, (a, b) -> {});
        SharedSubscriber slowDispatcher = new SharedSubscriber(stream, 1, 4);
        SharedSubscriber fastDispatcher = new SharedSubscriber(stream, 1, 4)) {
      BlockingListener slow = new BlockingListener(release);
      Subscription slowSub =
          slowDispatcher.subscribe(1, slow, 4_096); // holds all 2,000 while stuck
      CheckingListener fast = new CheckingListener("fast", 1, 0);
      fastDispatcher.subscribe(1, fast);

      for (long i = 1; i <= 2_000; i++) { // 250 times more than the ring of 8 holds
        store.insert(ev(i));
      }
      stream.notifyChange();

      Awaitility.await()
          .alias("the Tailer to reach the end although a subscriber is stuck")
          .atMost(Duration.ofSeconds(20))
          .until(() -> stream.visibleSeq() == 2_000);
      Awaitility.await()
          .alias("the fast subscriber on the other dispatcher")
          .atMost(Duration.ofSeconds(20))
          .until(() -> fast.last == 2_000);
      assertThat(fast.ok).isTrue();
      assertThat(fast.count).isEqualTo(2_000);
      assertThat(slow.count)
          .as("the stuck subscriber is still inside its first onEvent")
          .isEqualTo(0);

      release.countDown(); // the client wakes up
      Awaitility.await()
          .alias("the slow subscriber to catch up")
          .atMost(Duration.ofSeconds(20))
          .until(() -> slow.last == 2_000);
      assertThat(slow.ok).as("it must see 1..2000 in order, with nothing lost").isTrue();
      assertThat(slow.count).isEqualTo(2_000);
    } finally {
      release.countDown();
    }
  }

  @Test
  void aStuckSubscriberDoesHoldUpOthersOnTheSameShardThread() throws Exception {
    InMemoryEventStore store = new InMemoryEventStore();
    CountDownLatch release = new CountDownLatch(1);
    try (TailingStream stream = TestStreams.storeOnly(store, 64, 64, 1, 60_000, (a, b) -> {});
        SharedSubscriber shared = new SharedSubscriber(stream, 1, 4)) { // one shard thread for both
      BlockingListener slow = new BlockingListener(release);
      CheckingListener fast = new CheckingListener("fast", 1, 0);
      shared.subscribe(1, slow);
      shared.subscribe(1, fast);

      for (long i = 1; i <= 100; i++) {
        store.insert(ev(i));
      }
      stream.notifyChange();
      Awaitility.await()
          .alias("the Tailer")
          .atMost(Duration.ofSeconds(20))
          .until(() -> stream.visibleSeq() == 100);

      Thread.sleep(300);
      assertThat(fast.count < 100)
          .as("the shard thread is blocked inside the other listener, delivered: " + fast.count)
          .isTrue();

      release.countDown();
      Awaitility.await()
          .alias("both subscribers to finish after the release")
          .atMost(Duration.ofSeconds(20))
          .until(() -> fast.last == 100 && slow.last == 100);
      assertThat(fast.ok && slow.ok).isTrue();
    } finally {
      release.countDown();
    }
  }

  /**
   * The documented pattern for real websockets: a bounded outbound queue, cancel when it is full,
   * resume later.
   */
  @Test
  void aClientThatCannotKeepUpIsCancelledAndResumesFromItsLastSeq() throws Exception {
    InMemoryEventStore store = new InMemoryEventStore();
    try (TailingStream stream =
            TestStreams.storeOnly(store, 128, 64, 1, 60_000, (a, b) -> {}); // window holds 1..100
        SharedSubscriber fanout = new SharedSubscriber(stream, 1, 4)) {
      ArrayBlockingQueue<Event> outbound =
          new ArrayBlockingQueue<>(10); // nobody drains it: a dead connection
      AtomicReference<Subscription> self = new AtomicReference<>();
      AtomicLong lastQueued = new AtomicLong();
      Subscription sub =
          fanout.subscribe(
              1,
              new EventListener() {
                @Override
                public void onEvent(Event e, Source source) {
                  if (outbound.offer(e)) {
                    lastQueued.set(e.seq());
                  } else {
                    self.get().cancel(); // too slow: drop the client, it can reconnect
                  }
                }
              });
      self.set(sub); // no event exists yet, so onEvent cannot have run before this

      for (long i = 1; i <= 100; i++) {
        store.insert(ev(i));
      }
      stream.notifyChange();

      Awaitility.await()
          .alias("the subscription to be cancelled when the queue overflowed")
          .atMost(Duration.ofSeconds(20))
          .until(sub::isCancelled);
      assertThat(outbound.size()).isEqualTo(10);
      assertThat(lastQueued.get()).isEqualTo(10);

      // the client reconnects with lastSeq + 1 and gets exactly the rest
      CheckingListener resumed = new CheckingListener("resumed", lastQueued.get() + 1, 0);
      fanout.subscribe(lastQueued.get() + 1, resumed);
      Awaitility.await()
          .alias("the resumed subscription")
          .atMost(Duration.ofSeconds(20))
          .until(() -> resumed.last == 100);
      assertThat(resumed.ok).isTrue();
      assertThat(resumed.count).isEqualTo(90);
    }
  }

  private static Event ev(long seq) {
    return new Event(seq, seq, "k", "v");
  }
}
