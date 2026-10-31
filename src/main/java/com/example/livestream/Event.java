package com.example.livestream;

/**
 * Immutable event. {@code seq} is the gap-free total order within one stream, assigned by the
 * store.
 */
public record Event(long seq, long timestampMillis, String key, String payload) {
  @Override
  public String toString() {
    return "Event#" + seq + "[" + key + "=" + payload + "]";
  }
}
