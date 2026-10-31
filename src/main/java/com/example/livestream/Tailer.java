package com.example.livestream;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Inputs -> ring. Two inputs feed this thread: the store (the source of truth, read in seq order)
 * and the realtime feed (buffered by {@link TailingStream#publish}). Both may repeat a seq, and a
 * seq is delivered once, when it is sealed (see {@link MergeBuffer}): its versions are folded into
 * one result by the merge policy, that result is written to the window, and only then the watermark
 * advances.
 *
 * <p>Notifications from the writers ({@link #wakeUp()}) are only hints that cut latency: if one is
 * lost the poll timeout picks the event up anyway.
 *
 * <p>Holes: a sealed seq with no version in either input is a hole only if a higher seq is already
 * known; otherwise it is not written yet and the Tailer just waits. A hole still empty after {@code
 * gapTimeoutNanos} is declared dead (a rolled-back or never-committed transaction): it is
 * registered in {@link SkippedSeqs} and the watermark moves past it. A transaction that commits
 * later than the timeout is MISSED, so the timeout must exceed the longest writer transaction.
 */
public final class Tailer implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(Tailer.class);

  /** Where merged events go: the stream's window and subscribers, under the stream lock. */
  public interface Output {
    /** A merged event, delivered in seq order; advances the watermark to its seq. */
    void deliver(Event merged);

    /** A dead range ended at {@code seq}: the watermark moves past it. */
    void skipTo(long seq);
  }

  private final Output out;
  private final EventStore store;
  private final PersistenceWatermark wm;
  private final SkippedSeqs skipped;
  private final GapListener gapListener;
  private final MergeBuffer buffer;
  private final int batchSize;
  private final long pollNanos;
  private final long gapTimeoutNanos;
  private volatile boolean running = true;
  private volatile Thread thread;

  public Tailer(
      Output out,
      EventStore store,
      PersistenceWatermark wm,
      SkippedSeqs skipped,
      GapListener gapListener,
      MergeBuffer buffer,
      int batchSize,
      long pollNanos,
      long gapTimeoutNanos) {
    this.out = out;
    this.store = store;
    this.wm = wm;
    this.skipped = skipped;
    this.gapListener = gapListener;
    this.buffer = buffer;
    this.batchSize = batchSize;
    this.pollNanos = pollNanos;
    this.gapTimeoutNanos = gapTimeoutNanos;
  }

  public void stop() {
    running = false;
    wakeUp();
  }

  /** A writer says "something changed". Safe from any thread; unpark permits are not lost. */
  public void wakeUp() {
    Thread t = thread;
    if (t != null) {
      LockSupport.unpark(t);
    }
  }

  @Override
  public void run() {
    thread = Thread.currentThread();
    long next = wm.get() + 1; // next seq to deliver
    long readNext = next; // next seq to read from the store
    long holeAt = -1;
    long holeSince = 0;
    LOG.info(
        "Tailer started at seq {} (read batch {}, poll {} ms, gap timeout {} ms)",
        next,
        batchSize,
        TimeUnit.NANOSECONDS.toMillis(pollNanos),
        TimeUnit.NANOSECONDS.toMillis(gapTimeoutNanos));
    while (running) {
      boolean busy = false;
      try {
        long before = readNext;
        try (Stream<Event> rows = store.read(readNext, batchSize)) {
          for (Iterator<Event> it = rows.iterator();
              it.hasNext(); ) { // one row at a time, straight into the buffer
            Event e = it.next();
            if (e.seq()
                != readNext) { // the store contract: contiguous from readNext, or it is a bug
              throw new IllegalStateException(
                  "store read from seq "
                      + before
                      + " returned seq "
                      + e.seq()
                      + " where "
                      + readNext
                      + " was expected; the rows before it are kept");
            }
            if (e.seq() >= next) { // older rows are already delivered
              buffer.add(new Version(e, Version.Origin.STORE));
            }
            readNext++;
          }
        }
        if (readNext > before) {
          busy = true;
        } else {
          long visible = store.nextVisibleSeq(readNext);
          if (visible
              > readNext) { // the store has later rows, so readNext is a hole unless the feed fills
            // it
            buffer.observe(visible);
          }
        }

        while (buffer.sealed(next)) {
          Version version = buffer.take(next);
          if (version != null) {
            if (holeAt == next) {
              LOG.debug(
                  "Hole at seq {} filled after {} ms: a version arrived in time",
                  next,
                  TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - holeSince));
            }
            out.deliver(
                checked(
                    next, version.event())); // window first, then the watermark (inside the output)
            next++;
            holeAt = -1;
            busy = true;
            continue;
          }
          // sealed, but no version in either input. Nothing above it is known yet: it is not
          // written yet, not a hole
          long above = nextKnownAfter(next - 1);
          if (above == Long.MAX_VALUE) {
            break;
          }
          // a hole: a higher seq exists, so this one is missing
          long now = System.nanoTime();
          if (holeAt != next) {
            holeAt = next;
            holeSince = now;
            LOG.debug(
                "Hole at seq {}: seq {} is already visible, waiting up to {} ms for it to fill",
                next,
                above,
                TimeUnit.NANOSECONDS.toMillis(gapTimeoutNanos));
            break;
          }
          if (now - holeSince < gapTimeoutNanos) {
            break;
          }
          long end = holeEnd(next);
          LOG.warn(
              "Gap timeout: seqs {}..{} declared dead after {} ms (rolled back, or not committed in time;"
                  + " a late commit would be missed)",
              next,
              end,
              TimeUnit.NANOSECONDS.toMillis(now - holeSince));
          skipped.add(next, end); // register BEFORE moving the watermark
          gapListener.onGapSkipped(next, end);
          next = end + 1;
          readNext = Math.max(readNext, next);
          out.skipTo(end);
          buffer.discardUpTo(end);
          holeAt = -1;
          busy = true;
        }
        buffer.discardUpTo(next - 1);
      } catch (Exception ex) {
        // transient store error: retry after the poll interval
        LOG.warn(
            "Store read failed at seq {}, retrying in {} ms",
            next,
            TimeUnit.NANOSECONDS.toMillis(pollNanos),
            ex);
      }
      if (!busy) {
        LockSupport.parkNanos(pollNanos);
      }
    }
    LOG.info("Tailer stopped, next seq to read was {}", next);
  }

  /** Checks that the merged result of a sealed seq still carries that seq. */
  private Event checked(long seq, Event merged) {
    if (merged.seq() != seq) {
      throw new IllegalStateException("merge policy changed seq " + seq + " to " + merged.seq());
    }
    return merged;
  }

  /**
   * The last seq of a hole that starts at {@code from}: just before the next seq that exists in
   * either input.
   */
  private long holeEnd(long from) {
    long firstKnown = nextKnownAfter(from - 1);
    return firstKnown == Long.MAX_VALUE ? from : firstKnown - 1;
  }

  /**
   * The smallest seq above {@code seq} that either input has (a buffered version or a visible store
   * row).
   */
  private long nextKnownAfter(long seq) {
    long known = buffer.nextAbove(seq);
    long visible = store.nextVisibleSeq(seq + 1);
    if (visible > seq + 1) {
      known = Math.min(known, visible);
    }
    return known;
  }
}
