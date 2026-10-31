package com.example.livestream;

/** Test helpers for streams whose store is the only input. */
final class TestStreams {
  private TestStreams() {}

  /**
   * A stream with no realtime feed: the store is declared complete, so every row is delivered as
   * soon as the Tailer reads it (a tombstone at the top of the range seals everything). This is the
   * store-only timing.
   */
  static TailingStream storeOnly(
      EventStore store,
      int ringCapacity,
      int readBatch,
      long pollMillis,
      long gapTimeoutMillis,
      GapListener gapListener) {
    TailingStream stream =
        new TailingStream(
            store, ringCapacity, readBatch, pollMillis, gapTimeoutMillis, gapListener);
    stream.publishTombstone(Long.MAX_VALUE);
    return stream;
  }
}
