package io.rowweave.sched;

import io.rowweave.sched.stream.StreamEvent;

/**
 * The data exchange channel: an upstream sends {@link StreamEvent}s, a downstream polls them, and close
 * marks the end of the stream. In-process execution uses {@link LocalChannel}, which passes object references
 * for performance. The cross-node data plane of a cluster <b>does not go through this interface</b>,
 * since it has its own shuffle block transfer path.
 *
 * <p>Contract: send is bounded by capacity and blocks when the queue is full; after close a poll drains what
 * remains and then returns null for EOS; under a single consumer the re-armed EOS is never read a second time,
 * because <b>nothing may be polled after the first null</b>, with exactly one consumer thread per channel.
 */
public interface Channel {

    /** Pushes one event, blocking until the downstream makes room when the queue is full. */
    void send(StreamEvent event);

    /** Takes one event, blocking when there is none. Returns null when the stream has ended, meaning closed and fully drained. */
    StreamEvent poll() throws InterruptedException;

    /** Marks the end of the stream by posting the EOS sentinel, and wakes a consumer blocked on poll. */
    void close();

    /**
     * Closing out a failure: <b>poisons</b> the channel, which wakes the upstream threads parked on send with a
     * full queue so they throw {@link ChannelPoisonedException}, and drains the records already enqueued while
     * posting an error placeholder, so the next poll from the downstream certainly gets the error and never a
     * dirty row. This releases the upstream from a send-park deadlock when a middle stage fails, and keeps the
     * downstream from emitting dirty rows first. The default degrades to {@link #close()}.
     */
    default void fail(Throwable cause) {
        close();
    }

    /** Backpressure parks: how many times send has been blocked by a full queue. A channel without backpressure returns 0. */
    default long parkCount() {
        return 0;
    }

    /**
     * The progress signal for the deadlock watchdog: <b>the number of records that have successfully flowed
     * through</b>, incremented each time a consumer polls one out, and never decreasing. The watchdog sums this
     * over all channels, and reads flat throughput for a while together with every thread parked and no task
     * completed as a deadlock, which it then fails fast. A channel outside the data plane returns 0 and takes
     * no part in the judgement.
     */
    default long throughput() {
        return 0;
    }
}
