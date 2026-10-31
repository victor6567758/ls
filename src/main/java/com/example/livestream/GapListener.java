package com.example.livestream;

/**
 * Told when the {@link TailingStream} gives up on a hole in the store (a transaction that never
 * committed).
 */
@FunctionalInterface
public interface GapListener {
  void onGapSkipped(long fromSeq, long toSeqInclusive);
}
