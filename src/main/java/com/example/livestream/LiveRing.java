package com.example.livestream;

import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Ring of the most recent events: a read cache in front of the store. It is written by ONE thread,
 * the {@link Tailer}, in seq order. Readers are just cursors that live in the subscriptions, so
 * adding a subscriber costs nothing on the write path.
 *
 * <p>Slot for seq S is index S & mask. A slot holds an immutable Event (which carries its own seq),
 * so a reader can tell from ONE volatile read whether the slot is: not yet written (older seq), the
 * wanted event, or already overwritten (newer seq).
 */
public final class LiveRing {
  /** Returned by {@link #read} when the wanted seq was already overwritten (reader fell behind). */
  public static final Event OVERRUN = new Event(-1, 0, "", "");

  private final AtomicReferenceArray<Event> slots;
  private final int mask;

  public LiveRing(int capacityPowerOfTwo) {
    if (Integer.bitCount(capacityPowerOfTwo) != 1) {
      throw new IllegalArgumentException("capacity must be power of 2");
    }
    this.mask = capacityPowerOfTwo - 1;
    this.slots = new AtomicReferenceArray<>(capacityPowerOfTwo);
  }

  /**
   * Single-writer insert used by the {@link Tailer}: the ring is only a cache of events that are
   * already durable, so there is no claim CAS and no gating. Must be called by ONE thread, in seq
   * order, and BEFORE the watermark is advanced to cover the event.
   */
  public void put(Event e) {
    slots.set((int) (e.seq() & mask), e); // volatile write = publish
  }

  /**
   * @return the event; null if seq is not written yet; OVERRUN if it was overwritten. Wait-free.
   */
  public Event read(long seq) {
    Event e = slots.get((int) (seq & mask));
    if (e == null || e.seq() < seq) {
      return null;
    }
    return e.seq() == seq ? e : OVERRUN;
  }
}
