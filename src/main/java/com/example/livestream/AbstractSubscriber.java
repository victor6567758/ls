package com.example.livestream;

/**
 * What the two subscribers have in common: they are attached to one {@link TailingStream}, and a
 * subscription is registered with the stream first and then handed to a consumer thread. The
 * subclass decides which thread, in {@link #start}, and what {@link #close} does.
 */
abstract class AbstractSubscriber implements Subscriber {
  protected final TailingStream stream;

  protected AbstractSubscriber(TailingStream stream) {
    this.stream = stream;
  }

  @Override
  public final Subscription subscribe(long fromSeq, EventListener l, int capacity) {
    SubscriptionImpl s = stream.register(fromSeq, l, capacity);
    start(s);
    return s;
  }

  /** Gives a registered subscription to a consumer thread. */
  protected abstract void start(SubscriptionImpl s);
}
