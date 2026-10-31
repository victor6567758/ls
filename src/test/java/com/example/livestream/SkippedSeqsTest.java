package com.example.livestream;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.util.concurrent.Uninterruptibles;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class SkippedSeqsTest {

  @Test
  void emptyRegistryIsTheIdentity() {
    SkippedSeqs s = new SkippedSeqs();
    assertThat(s.resolve(1)).isEqualTo(1);
    assertThat(s.resolve(1_000_000)).isEqualTo(1_000_000);
  }

  @Test
  void rangeIsInclusiveOnBothEnds() {
    SkippedSeqs s = new SkippedSeqs();
    s.add(10, 14);
    assertThat(s.resolve(9)).isEqualTo(9);
    assertThat(s.resolve(10)).isEqualTo(15);
    assertThat(s.resolve(12)).isEqualTo(15);
    assertThat(s.resolve(14)).isEqualTo(15);
    assertThat(s.resolve(15)).isEqualTo(15);
    assertThat(s.resolve(16)).isEqualTo(16);
  }

  @Test
  void singleSeqRange() {
    SkippedSeqs s = new SkippedSeqs();
    s.add(3, 3);
    assertThat(s.resolve(2)).isEqualTo(2);
    assertThat(s.resolve(3)).isEqualTo(4);
    assertThat(s.resolve(4)).isEqualTo(4);
  }

  @Test
  void adjacentRangesChain() {
    SkippedSeqs s = new SkippedSeqs();
    s.add(5, 6);
    s.add(7, 9);
    s.add(10, 10);
    assertThat(s.resolve(5)).isEqualTo(11);
    assertThat(s.resolve(8)).isEqualTo(11);
    assertThat(s.resolve(10)).isEqualTo(11);
  }

  @Test
  void separateRangesDoNotInterfere() {
    SkippedSeqs s = new SkippedSeqs();
    s.add(2, 2);
    s.add(10, 20);
    assertThat(s.resolve(2)).isEqualTo(3);
    assertThat(s.resolve(3)).isEqualTo(3);
    assertThat(s.resolve(9)).isEqualTo(9);
    assertThat(s.resolve(15)).isEqualTo(21);
  }

  /**
   * A tailer thread keeps adding ranges while subscriber threads resolve cursors: never go
   * backwards, never throw.
   */
  @Test
  void concurrentAddAndResolveIsSafe() throws Exception {
    SkippedSeqs s = new SkippedSeqs();
    int ranges = 20_000;
    AtomicBoolean done = new AtomicBoolean();
    ConcurrentLinkedQueue<String> errors = new ConcurrentLinkedQueue<>();

    Thread writer =
        new Thread(
            () -> {
              for (int k = 0; k < ranges; k++) {
                s.add(10L * k + 2, 10L * k + 5); // dead: 2..5 of every block of 10
              }
              done.set(true);
            });
    Thread[] readers = new Thread[3];
    for (int r = 0; r < readers.length; r++) {
      readers[r] =
          new Thread(
              () -> {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();
                while (!done.get()) {
                  long seq = rnd.nextLong(10L * ranges);
                  long res = s.resolve(seq);
                  if (res < seq) {
                    errors.add("went backwards " + seq + " -> " + res);
                  }
                }
              });
    }
    writer.start();
    for (Thread t : readers) {
      t.start();
    }
    Uninterruptibles.joinUninterruptibly(writer);
    for (Thread t : readers) {
      Uninterruptibles.joinUninterruptibly(t);
    }
    assertThat(errors.isEmpty()).as(errors.toString()).isTrue();
    // once everything is registered the result is exact
    for (int k = 0; k < 100; k++) {
      assertThat(s.resolve(10L * k + 2)).isEqualTo(10L * k + 6);
      assertThat(s.resolve(10L * k + 5)).isEqualTo(10L * k + 6);
      assertThat(s.resolve(10L * k + 1)).isEqualTo(10L * k + 1);
    }
  }
}
