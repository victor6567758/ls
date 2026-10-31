package com.example.livestream;

import java.util.function.BinaryOperator;

/**
 * Built-in merge policies. A policy is a {@link BinaryOperator}, the same shape as the remapping
 * function of {@code Map.merge}: it gets the result so far and the next version of the same seq,
 * and returns the new result. The {@link MergeBuffer} folds the versions of a seq this way when the
 * seq is taken: realtime first, then store.
 *
 * <p>The customer writes a policy for their data (for example a candle: open and high from the
 * store row, close from the latest realtime version). The result must keep the seq of the versions
 * it combines; the Tailer rejects a result with another seq.
 *
 * <p>A policy runs on the Tailer thread, under the lock of the {@link MergeBuffer}, when a seq is
 * taken. It must return quickly and must not call back into the stream.
 */
public final class MergePolicies {
  private MergePolicies() {}

  /** The first version that arrived. */
  public static final BinaryOperator<Version> KEEP_FIRST = (first, next) -> first;

  /** The last version that arrived. */
  public static final BinaryOperator<Version> KEEP_LAST = (first, next) -> next;

  /** The last version from the store; if the store has none, the last realtime version. */
  public static final BinaryOperator<Version> PREFER_STORE =
      (sofar, next) ->
          next.origin() == Version.Origin.STORE || sofar.origin() != Version.Origin.STORE
              ? next
              : sofar;
}
