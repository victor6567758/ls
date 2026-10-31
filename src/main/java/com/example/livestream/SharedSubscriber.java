package com.example.livestream;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * Shared: K shard threads, each pumping the subscriptions assigned to it round-robin. Many
 * subscriptions cost only their buffers, not threads. A listener that blocks stalls every
 * subscription on its shard; use {@link DedicatedSubscriber} for those.
 */
public final class SharedSubscriber extends AbstractSubscriber {
  private static final class Shard implements Runnable {
    final ConcurrentLinkedQueue<SubscriptionImpl> inbox = new ConcurrentLinkedQueue<>();
    final List<SubscriptionImpl> mine = new ArrayList<>();
    volatile boolean running = true;
    private final int batch;

    Shard(int batch) {
      this.batch = batch;
    }

    @Override
    public void run() {
      int idleRounds = 0;
      while (running) {
        for (SubscriptionImpl s; (s = inbox.poll()) != null; ) {
          mine.add(s);
        }
        int work = 0;
        for (int i = mine.size() - 1; i >= 0; i--) {
          SubscriptionImpl s = mine.get(i);
          work += s.pump(batch);
          if (s.isDone()) {
            mine.set(i, mine.get(mine.size() - 1));
            mine.remove(mine.size() - 1);
          }
        }
        idleRounds = work == 0 ? idleRounds + 1 : 0;
        if (idleRounds > 0) {
          pause(idleRounds);
        }
      }
    }

    /** Spin, then yield, then park up to 1 ms: low latency while busy, little CPU when idle. */
    private static void pause(int idleRounds) {
      if (idleRounds < 64) {
        Thread.onSpinWait();
      } else if (idleRounds < 256) {
        Thread.yield();
      } else {
        LockSupport.parkNanos(
            Math.min(1_000_000L, 1_000L << Math.min(10, (idleRounds - 256) / 16)));
      }
    }
  }

  private final Shard[] shards;
  private final AtomicInteger rr = new AtomicInteger();

  /**
   * @param shardThreads threads that pump the subscriptions; each serves many of them
   * @param batchPerSubscriptionPerRound events one subscription may deliver before the shard moves
   *     on
   */
  public SharedSubscriber(
      TailingStream stream, int shardThreads, int batchPerSubscriptionPerRound) {
    super(stream);
    this.shards = new Shard[shardThreads];
    for (int i = 0; i < shardThreads; i++) {
      shards[i] = new Shard(batchPerSubscriptionPerRound);
      Thread t = new Thread(shards[i], "shared-" + i);
      t.setDaemon(true);
      t.start();
    }
  }

  @Override
  protected void start(SubscriptionImpl s) {
    shards[Math.floorMod(rr.getAndIncrement(), shards.length)].inbox.add(s);
  }

  @Override
  public void close() {
    for (Shard s : shards) {
      s.running = false;
    }
  }
}
