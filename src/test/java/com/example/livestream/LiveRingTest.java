package com.example.livestream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.google.common.util.concurrent.Uninterruptibles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** Contract checks and a single-writer / many-readers stress test of {@link LiveRing}. */
class LiveRingTest {

  @Test
  void rejectsCapacityThatIsNotPowerOfTwo() {
    assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> new LiveRing(100));
    assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> new LiveRing(0));
  }

  @Test
  void readReturnsNullBeforePutAndTheEventAfter() {
    LiveRing ring = new LiveRing(8);
    assertThat(ring.read(1)).isNull();
    ring.put(ev(1));
    assertThat(ring.read(1)).isEqualTo(ev(1));
    assertThat(ring.read(2)).isNull();
  }

  @Test
  void aSlotHoldingAnOlderSeqMeansNotWrittenYet() {
    LiveRing ring = new LiveRing(4);
    for (long s = 1; s <= 4; s++) {
      ring.put(ev(s));
    }
    assertThat(ring.read(5)).isNull(); // slot of 5 still holds seq 1
    assertThat(ring.read(8)).isNull();
  }

  @Test
  void aSlotHoldingANewerSeqMeansOverrun() {
    LiveRing ring = new LiveRing(4);
    for (long s = 1; s <= 6; s++) {
      ring.put(ev(s));
    }
    assertThat(ring.read(1))
        .isSameAs(LiveRing.OVERRUN); // slots of 1 and 2 were recycled by 5 and 6
    assertThat(ring.read(2)).isSameAs(LiveRing.OVERRUN);
    for (long s = 3; s <= 6; s++) {
      assertThat(ring.read(s)).isEqualTo(ev(s));
    }
  }

  @Test
  void overrunDoesNotHideLaterEvents() {
    LiveRing ring = new LiveRing(2);
    ring.put(ev(1));
    ring.put(ev(2));
    ring.put(ev(3));
    assertThat(ring.read(1)).isSameAs(LiveRing.OVERRUN);
    assertThat(ring.read(2)).isEqualTo(ev(2));
    assertThat(ring.read(3)).isEqualTo(ev(3));
  }

  /**
   * One writer (the Tailer) puts seq 1..N in order into a small ring while readers read random seqs
   * around the head. A reader must only ever see: nothing yet, an overrun, or exactly the event of
   * that seq.
   */
  @Test
  void concurrentReadersNeverSeeTheWrongEvent() throws Exception {
    int capacity = 64;
    long total = 200_000;
    LiveRing ring = new LiveRing(capacity);
    AtomicLong head = new AtomicLong();
    AtomicBoolean done = new AtomicBoolean();
    AtomicReference<String> error = new AtomicReference<>();
    AtomicLong hits = new AtomicLong();

    List<Thread> readers = new ArrayList<>();
    for (int r = 0; r < 4; r++) {
      Thread t =
          new Thread(
              () -> {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();
                while (!done.get()) {
                  long h = head.get();
                  long seq = Math.max(1, h - capacity + rnd.nextLong(capacity + 8));
                  Event e = ring.read(seq);
                  if (e == null || e == LiveRing.OVERRUN) {
                    continue;
                  }
                  hits.incrementAndGet();
                  if (e.seq() != seq || !e.payload().equals("v" + seq)) {
                    error.compareAndSet(null, "asked for " + seq + " got " + e);
                  }
                }
              });
      readers.add(t);
      t.start();
    }
    for (long s = 1; s <= total; s++) {
      ring.put(ev(s));
      head.set(s);
    }
    done.set(true);
    for (Thread t : readers) {
      Uninterruptibles.joinUninterruptibly(t);
    }
    assertThat(error.get()).isNull();
    assertThat(hits.get() > 0).as("readers never saw an event, the test proved nothing").isTrue();
  }

  @Test
  void aReaderThatIsWithinCapacityOfTheHeadAlwaysFindsTheEvent() {
    int capacity = 16;
    LiveRing ring = new LiveRing(capacity);
    for (long s = 1; s <= 1_000; s++) {
      ring.put(ev(s));
      long oldestKept = Math.max(1, s - capacity + 1);
      assertThat(ring.read(oldestKept)).isEqualTo(ev(oldestKept));
      assertThat(ring.read(s)).isEqualTo(ev(s));
    }
  }

  private static Event ev(long seq) {
    return new Event(seq, seq * 10, "k" + seq, "v" + seq);
  }
}
