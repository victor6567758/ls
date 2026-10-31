package com.example.livestream;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * The Tailer reports what it does about holes and store failures through SLF4J. These tests capture
 * its log output and check the messages, so that an operator watching the console (or any log
 * backend) can see when a gap timeout fired, which range was skipped, and that a hole which filled
 * in time was not an incident.
 */
class TailerLoggingTest {

  /**
   * Captures the Tailer's log events, and lets the DEBUG messages through for the duration of a
   * test.
   */
  private static final class LogCapture implements AutoCloseable {
    private final Logger logger = (Logger) LoggerFactory.getLogger(Tailer.class);
    private final Level levelBefore = logger.getLevel();
    private final List<ILoggingEvent> events = new CopyOnWriteArrayList<>();
    private final AppenderBase<ILoggingEvent> appender =
        new AppenderBase<>() {
          @Override
          protected void append(ILoggingEvent e) {
            events.add(e);
          }
        };

    LogCapture() {
      appender.setContext(logger.getLoggerContext());
      appender.start();
      logger.addAppender(appender);
      logger.setLevel(Level.DEBUG);
    }

    List<String> messages(Level level) {
      List<String> out = new ArrayList<>();
      for (ILoggingEvent e : events) {
        if (e.getLevel() == level) {
          out.add(e.getFormattedMessage());
        }
      }
      return out;
    }

    List<ILoggingEvent> events(Level level) {
      List<ILoggingEvent> out = new ArrayList<>();
      for (ILoggingEvent e : events) {
        if (e.getLevel() == level) {
          out.add(e);
        }
      }
      return out;
    }

    boolean has(Level level, String fragment) {
      return messages(level).stream().anyMatch(m -> m.contains(fragment));
    }

    @Override
    public void close() {
      logger.detachAppender(appender);
      appender.stop();
      logger.setLevel(levelBefore);
    }
  }

  @Test
  void aHoleThatNeverFillsIsLoggedAsAWarningWithTheSkippedRange() throws Exception {
    InMemoryEventStore store = new InMemoryEventStore();
    List<String> reported = new CopyOnWriteArrayList<>();
    List<Long> delivered = new CopyOnWriteArrayList<>();
    try (LogCapture log = new LogCapture();
        TailingStream stream =
            TestStreams.storeOnly(store, 16, 16, 1, 150, (a, b) -> reported.add(a + ".." + b));
        SharedSubscriber fanout = new SharedSubscriber(stream, 1, 16)) {
      fanout.subscribe(1, (e, src) -> delivered.add(e.seq()));

      store.insert(ev(1));
      store.insert(ev(2));
      store.insert(ev(4)); // seq 3 is a transaction that never commits

      Awaitility.await()
          .alias("the Tailer to give up on seq 3")
          .atMost(Duration.ofSeconds(20))
          .until(() -> stream.visibleSeq() == 4);

      assertThat(log.has(Level.DEBUG, "Hole at seq 3: seq 4 is already visible"))
          .as(log.messages(Level.DEBUG).toString())
          .isTrue();
      List<String> warnings = log.messages(Level.WARN);
      assertThat(warnings.size()).as(warnings.toString()).isEqualTo(1);
      assertThat(warnings.get(0).contains("Gap timeout: seqs 3..3 declared dead"))
          .as(warnings.get(0))
          .isTrue();
      assertThat(reported).as("the GapListener is told as well").isEqualTo(List.of("3..3"));

      Awaitility.await()
          .alias("the subscriber")
          .atMost(Duration.ofSeconds(20))
          .until(() -> delivered.size() == 3);
      assertThat(delivered).isEqualTo(List.of(1L, 2L, 4L));
    }
  }

  @Test
  void severalHolesInARowAreOneWarningForTheWholeRange() throws Exception {
    InMemoryEventStore store = new InMemoryEventStore();
    try (LogCapture log = new LogCapture();
        TailingStream stream = TestStreams.storeOnly(store, 16, 16, 1, 150, (a, b) -> {})) {
      store.insert(ev(1));
      store.insert(ev(5)); // 2, 3 and 4 are all dead

      Awaitility.await()
          .alias("the Tailer to skip the range")
          .atMost(Duration.ofSeconds(20))
          .until(() -> stream.visibleSeq() == 5);

      List<String> warnings = log.messages(Level.WARN);
      assertThat(warnings.size())
          .as("a run of dead ids costs one timeout and one warning: " + warnings)
          .isEqualTo(1);
      assertThat(warnings.get(0).contains("seqs 2..4 declared dead")).as(warnings.get(0)).isTrue();
    }
  }

  @Test
  void aHoleThatFillsBeforeTheTimeoutIsOnlyDebugLoggedAndNothingIsSkipped() throws Exception {
    InMemoryEventStore store = new InMemoryEventStore();
    List<String> reported = new CopyOnWriteArrayList<>();
    try (LogCapture log = new LogCapture();
        TailingStream stream =
            TestStreams.storeOnly(store, 16, 16, 1, 10_000, (a, b) -> reported.add(a + ".." + b))) {
      store.insert(ev(1));
      store.insert(ev(2));
      store.insert(ev(4)); // seq 3 is still in flight

      Awaitility.await()
          .alias("the Tailer to notice the hole")
          .atMost(Duration.ofSeconds(20))
          .until(() -> log.has(Level.DEBUG, "Hole at seq 3"));
      assertThat(stream.visibleSeq()).as("the Tailer waits at the hole").isEqualTo(2);

      store.insert(ev(3)); // the slow transaction commits in time

      Awaitility.await()
          .alias("the Tailer to continue")
          .atMost(Duration.ofSeconds(20))
          .until(() -> stream.visibleSeq() == 4);
      assertThat(log.has(Level.DEBUG, "Hole at seq 3 filled after"))
          .as(log.messages(Level.DEBUG).toString())
          .isTrue();
      assertThat(log.messages(Level.WARN).isEmpty())
          .as("nothing to alert on: " + log.messages(Level.WARN))
          .isTrue();
      assertThat(reported.isEmpty()).isTrue();
    }
  }

  @Test
  void aFailingStoreIsLoggedWithItsCauseAndTheTailerKeepsRetrying() throws Exception {
    InMemoryEventStore inner = new InMemoryEventStore();
    AtomicInteger failuresLeft = new AtomicInteger(2);
    EventStore flaky =
        new EventStore() {
          @Override
          public Stream<Event> read(long fromSeq, int limit) {
            if (failuresLeft.getAndDecrement() > 0) {
              throw new IllegalStateException("simulated store outage");
            }
            return inner.read(fromSeq, limit);
          }

          @Override
          public long nextVisibleSeq(long fromSeq) {
            return inner.nextVisibleSeq(fromSeq);
          }

          @Override
          public long lastSeq() {
            return inner.lastSeq();
          }

          @Override
          public long firstSeqAtOrAfter(long ts) {
            return inner.firstSeqAtOrAfter(ts);
          }
        };
    try (LogCapture log = new LogCapture();
        TailingStream stream = TestStreams.storeOnly(flaky, 16, 16, 1, 60_000, (a, b) -> {})) {
      inner.insert(ev(1));
      inner.insert(ev(2));

      Awaitility.await()
          .alias("the Tailer to recover after the outage")
          .atMost(Duration.ofSeconds(20))
          .until(() -> stream.visibleSeq() == 2);

      List<ILoggingEvent> warnings = log.events(Level.WARN);
      assertThat(warnings.size()).as(warnings.toString()).isEqualTo(2);
      for (ILoggingEvent w : warnings) {
        assertThat(w.getFormattedMessage().contains("Store read failed at seq 1, retrying in 1 ms"))
            .as(w.getFormattedMessage())
            .isTrue();
        assertThat(w.getThrowableProxy().getMessage())
            .as("the cause must be logged")
            .isEqualTo("simulated store outage");
      }
    }
  }

  @Test
  void startAndStopAreLoggedAtInfoWithTheSettingsInEffect() throws Exception {
    InMemoryEventStore store = new InMemoryEventStore();
    store.insert(ev(1));
    store.insert(ev(2));
    try (LogCapture log = new LogCapture()) {
      TailingStream stream = TestStreams.storeOnly(store, 16, 16, 1, 150, (a, b) -> {});
      Awaitility.await()
          .alias("the start message")
          .atMost(Duration.ofSeconds(20))
          .until(() -> log.has(Level.INFO, "Tailer started"));
      assertThat(
              log.has(
                  Level.INFO,
                  "Tailer started at seq 3 (read batch 16, poll 1 ms, gap timeout 150 ms)"))
          .as(log.messages(Level.INFO).toString())
          .isTrue();
      stream.close();
      assertThat(log.has(Level.INFO, "Tailer stopped"))
          .as(log.messages(Level.INFO).toString())
          .isTrue();
    }
  }

  private static Event ev(long seq) {
    return new Event(seq, seq, "k", "v");
  }
}
