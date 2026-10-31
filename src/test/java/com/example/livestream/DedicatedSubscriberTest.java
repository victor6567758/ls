package com.example.livestream;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

class DedicatedSubscriberTest {

  /** Subscribe and cancel many times: finished subscriptions must not stay in the registry. */
  @Test
  void finishedSubscriptionsAreForgotten() throws Exception {
    InMemoryEventStore store = new InMemoryEventStore();
    try (TailingStream stream = TestStreams.storeOnly(store, 16, 16, 1, 60_000, (a, b) -> {});
        DedicatedSubscriber dedicated = new DedicatedSubscriber(stream)) {
      store.insert(new Event(1, 1, "k", "v"));
      for (int i = 0; i < 200; i++) {
        Subscription s = dedicated.subscribe(1, new CheckingListener("c" + i, 1, 0));
        s.cancel();
      }
      Awaitility.await()
          .alias("every finished subscription to be forgotten")
          .atMost(Duration.ofSeconds(20))
          .until(() -> dedicated.liveSubscriptions() == 0);
      assertThat(dedicated.liveSubscriptions()).isZero();
    }
  }

  /** Live subscriptions are counted until they end, and close() ends the rest. */
  @Test
  void liveSubscriptionsAreCountedUntilTheyEnd() throws Exception {
    InMemoryEventStore store = new InMemoryEventStore();
    try (TailingStream stream = TestStreams.storeOnly(store, 16, 16, 1, 60_000, (a, b) -> {});
        DedicatedSubscriber dedicated = new DedicatedSubscriber(stream)) {
      Subscription a = dedicated.subscribe(Subscriber.LATEST, new CheckingListener("a", 1, 0));
      dedicated.subscribe(Subscriber.LATEST, new CheckingListener("b", 1, 0));
      assertThat(dedicated.liveSubscriptions()).isEqualTo(2);

      a.cancel();
      Awaitility.await()
          .alias("the cancelled subscription to be forgotten")
          .atMost(Duration.ofSeconds(20))
          .until(() -> dedicated.liveSubscriptions() == 1);
    }
    // try-with-resources closes the subscriber first, and its close() cancels the remaining
    // subscription
  }
}
