package com.example.livestream.demo;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Runs the demo as a test: it exits with a failure if any subscriber saw a gap, a duplicate or a
 * reordering.
 */
class DemoTest {

  @Test
  void demoIsConsistentWithOneRolledBackId() throws Exception {
    // 1,000 preloaded + 2 x 3,000 = ids 1..7000; id 5000 is rolled back and must be skipped after
    // the gap timeout
    assertThat(Demo.run(2, 3_000, 1_000)).isTrue();
  }

  @Test
  void demoIsConsistentWithManyWritersAndSeveralRolledBackIds() throws Exception {
    // ids 1..22000; 5000, 10000, 15000 and 20000 are rolled back
    assertThat(Demo.run(4, 5_250, 1_000)).isTrue();
  }
}
