package com.example.livestream;

import java.util.HashSet;
import java.util.Set;

/**
 * Dedicated: one thread per subscription. A blocking listener delays nobody else, at the cost of
 * one thread each. Use it for slow or untrusted listeners; {@link SharedSubscriber} is the default
 * for everything else.
 */
public final class DedicatedSubscriber extends AbstractSubscriber {
  private static final int BATCH = 64;

  private final Object lock = new Object();
  private final Set<SubscriptionImpl> subscriptions =
      new HashSet<>(); // live subscriptions, guarded by lock

  public DedicatedSubscriber(TailingStream stream) {
    super(stream);
  }

  @Override
  protected void start(SubscriptionImpl s) {
    synchronized (lock) {
      subscriptions.add(s);
    }
    Thread t =
        new Thread(
            () -> {
              run(s);
              forget(s);
            },
            "subscriber");
    t.setDaemon(true);
    t.start();
  }

  @Override
  public void close() {
    synchronized (lock) {
      for (SubscriptionImpl s : subscriptions) {
        s.cancel();
      }
      subscriptions.clear();
    }
  }

  /** Number of subscriptions whose thread has not finished yet. For tests. */
  int liveSubscriptions() {
    synchronized (lock) {
      return subscriptions.size();
    }
  }

  /**
   * Called by the subscription's own thread when it is done, so finished subscriptions are not
   * kept.
   */
  private void forget(SubscriptionImpl s) {
    synchronized (lock) {
      subscriptions.remove(s);
    }
  }

  private static void run(SubscriptionImpl s) {
    try {
      while (!s.isDone()) {
        if (s.pump(BATCH) == 0) {
          s.awaitData(50);
        }
      }
    } catch (InterruptedException e) {
      s.cancel();
      Thread.currentThread().interrupt();
    }
  }
}
