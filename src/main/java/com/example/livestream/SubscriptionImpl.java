package com.example.livestream;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * The delivery state of one subscriber: a bounded buffer of merged events and the listener that
 * consumes them. Package-private: users hold the {@link Subscription} interface; the subscribers
 * call {@link #pump} and {@link #awaitData} from their consumer threads.
 *
 * <p>The merger calls {@link #offer} under the stream lock; it never blocks. A consumer thread (a
 * shard of a {@link SharedSubscriber}, or a {@link DedicatedSubscriber}'s thread) calls {@link
 * #pump} and is the only thread that calls the listener. History from the merged window is
 * delivered first, then {@code onCaughtUp}, then the buffer.
 *
 * <p>If the buffer is full, the subscriber is cancelled and its listener gets {@code onError}. The
 * other subscribers are not affected. The subscriber may re-subscribe from {@link #nextSeq()}.
 */
final class SubscriptionImpl implements Subscription {
  private final EventListener listener;
  private final List<Event> history;
  private final ArrayBlockingQueue<Event> buffer;
  private final long fromSeq;
  private final long joinSeq;

  // consumer-thread state: only the thread that calls pump() or awaitData() touches these
  private int historyPos;
  private boolean caughtUp;
  private Event held;

  private volatile boolean cancelled;
  private volatile boolean reported;
  private volatile Throwable error;
  private volatile long nextSeq;
  private volatile long delivered;

  /**
   * @param history merged events from {@code fromSeq} up to {@code joinSeq}, copied from the window
   *     under the lock
   * @param joinSeq the watermark when the subscriber was registered; events up to it are HISTORY,
   *     later ones LIVE
   */
  SubscriptionImpl(
      EventListener listener, int capacity, List<Event> history, long fromSeq, long joinSeq) {
    this.listener = listener;
    this.history = history;
    this.fromSeq = fromSeq;
    this.joinSeq = joinSeq;
    this.buffer = new ArrayBlockingQueue<>(capacity);
    this.nextSeq = fromSeq;
  }

  /**
   * Called by the merger under the stream lock. Never blocks. @return false if the subscriber is
   * cancelled.
   */
  boolean offer(Event e) {
    if (cancelled) {
      return false;
    }
    if (e.seq() < fromSeq) {
      return true; // not wanted by this subscriber
    }
    if (!buffer.offer(e)) {
      fail(
          new IllegalStateException(
              "subscriber too slow: buffer of "
                  + buffer.size()
                  + " events is full; re-subscribe from seq "
                  + nextSeq));
      return false;
    }
    return true;
  }

  /**
   * Stops the subscriber. Its listener is not called again; no {@code onError} is sent for a plain
   * cancel.
   */
  @Override
  public void cancel() {
    cancelled = true;
  }

  @Override
  public boolean isCancelled() {
    return cancelled;
  }

  /**
   * True once the subscriber has stopped and its listener has been told (the consumer may drop it).
   */
  boolean isDone() {
    return reported;
  }

  /** The seq to re-subscribe from: the last delivered seq + 1. */
  @Override
  public long nextSeq() {
    return nextSeq;
  }

  @Override
  public long deliveredCount() {
    return delivered;
  }

  /**
   * Delivers up to {@code max} events. Consumer thread only.
   *
   * @return the number of events delivered (0 means nothing to do)
   */
  int pump(int max) {
    int n = 0;
    try {
      while (n < max && !cancelled && historyPos < history.size()) {
        deliver(history.get(historyPos++));
        n++;
      }
      if (!caughtUp && !cancelled && historyPos == history.size()) {
        caughtUp = true;
        listener.onCaughtUp();
      }
      while (n < max && !cancelled) {
        Event e = held != null ? held : buffer.poll();
        held = null;
        if (e == null) {
          break;
        }
        deliver(e);
        n++;
      }
    } catch (Throwable t) {
      fail(t);
    }
    if (cancelled && !reported) {
      reported = true;
      if (error != null) {
        listener.onError(error);
      }
    }
    return n;
  }

  /**
   * Waits up to {@code millis} for an event. Consumer thread only; used by a subscriber's own
   * thread.
   */
  void awaitData(long millis) throws InterruptedException {
    if (held == null) {
      held = buffer.poll(millis, TimeUnit.MILLISECONDS);
    }
  }

  private void fail(Throwable t) {
    if (!cancelled) {
      error = t;
      cancelled = true;
    }
  }

  private void deliver(Event e) {
    listener.onEvent(
        e, e.seq() <= joinSeq ? EventListener.Source.HISTORY : EventListener.Source.LIVE);
    nextSeq = e.seq() + 1;
    delivered++;
  }
}
