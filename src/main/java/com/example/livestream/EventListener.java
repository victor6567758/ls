package com.example.livestream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Receiver of the stream: this is your "websocket session" / onNewEvent stub. All callbacks for ONE
 * subscription are invoked sequentially from ONE dispatcher thread, so an implementation needs no
 * synchronisation for its own state. Callbacks MUST NOT block (hand off to a per-connection
 * outbound queue for real I/O).
 */
public interface EventListener {
  Logger LOG = LoggerFactory.getLogger(EventListener.class);

  enum Source {
    HISTORY,
    LIVE
  }

  /** seq numbers arrive strictly increasing, gap-free, exactly once. */
  void onEvent(Event event, Source source);

  /** Called once, when the last event that existed at subscribe time has been delivered. */
  default void onCaughtUp() {}

  /** The subscription is terminated after this call. */
  default void onError(Throwable t) {
    LOG.error("Subscription terminated by an error", t);
  }
}
