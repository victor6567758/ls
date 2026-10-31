package com.example.livestream;

/**
 * One copy of an event, as it arrived: from the store, or from the realtime feed. The same seq may
 * come several times from either input (at-least-once delivery), so a seq can have several versions
 * before it is delivered.
 */
public record Version(Event event, Origin origin) {

  public enum Origin {
    STORE,
    REALTIME
  }
}
