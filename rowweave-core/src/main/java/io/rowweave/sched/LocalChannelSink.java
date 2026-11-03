package io.rowweave.sched;

import io.rowweave.internal.RowSnapshot;
import io.rowweave.sched.stream.StreamEvent;

/**
 * The local producer adapter on the <b>upstream side</b> of two adjacent stages in the same worker: one
 * output of the upstream FanoutSink is sent straight into the shared {@link MultiProducerChannel} that the
 * downstream reads, passing the DataRow reference inside the process, which avoids both serialization and TCP.
 *
 * <p>It implements {@link Channel} so that FanoutSink can hold it <b>in the same list</b> as the socket-side
 * {@code SocketChannelSink} ({@code List<Channel>}, with close and fail walked uniformly). The mapping:
 * <ul>
 * <li>{@link #send} of a RECORD becomes {@code shared.send(event, 0)}, where zero wire bytes skips the byte
 * bound and leaves only the row capacity to apply backpressure, which is the no-OOM guarantee. EOS and the
 * other control events do not come through here, because EOS goes through the producerDone latch in {@link
 * #close}.</li>
 * <li>{@link #close} becomes {@code shared.producerDone()}, the multi-producer close latch that posts the
 * merged EOS only when the count reaches zero.</li>
 * <li>{@link #fail} becomes {@code shared.fail(cause)}, which poisons the merged queue so the downstream polls
 * the error, closing out the same way a socket failure does.</li>
 * </ul>
 *
 * <p><b>Reuse</b>: by default the reference is passed straight through, which holds because an operator never
 * mutates a row after emitting it. With reuse disabled, the upstream FanoutSink builds a typed row snapshot
 * before sending, which this sink does not observe, and that is the only copy point.
 */
public final class LocalChannelSink implements Channel {

    private final MultiProducerChannel shared;
    private final boolean reuse;  // true passes the reference straight through, which is the default; false takes a typed snapshot before send, the defensive mode

    public LocalChannelSink(MultiProducerChannel shared) {
        this(shared, true);
    }

    /** reuse=false builds a typed snapshot at the boundary; the socket side is unaffected, since serialization isolates it naturally. */
    public LocalChannelSink(MultiProducerChannel shared, boolean reuse) {
        if (shared == null) {
            throw new IllegalArgumentException("shared channel must not be null");
        }
        this.shared = shared;
        this.reuse = reuse;
    }

    @Override
    public void send(StreamEvent event) {
        if (event.isError()) {
            fail(event.cause());
            return;
        }
        if (!event.isRecord()) {
            return;  // EOS goes through close to producerDone; BARRIER and CANCEL never cross a shuffle, matching the socket side
        }
        StreamEvent out = reuse ? event : StreamEvent.record(RowSnapshot.copyOf(event.row()));
        shared.send(out, 0);  // Zero wire bytes: backpressure comes only from the row capacity, which is the no-OOM bound
    }

    /** Deferred batching is allowed only on a single-producer local channel with reuse enabled, so a mutable DataRow cannot be reused before the flush. */
    public boolean supportsRecordBatch() {
        return reuse && shared.singleProducer();
    }

    /** For a local fan-out only where {@link #supportsRecordBatch()} is true. */
    public void sendRecords(StreamEvent[] records, int count) {
        if (!supportsRecordBatch()) {
            throw new IllegalStateException(
                    "record batch requires reuse=true and a single-producer local channel");
        }
        shared.sendRecords(records, count);
    }

    @Override
    public StreamEvent poll() {
        throw new UnsupportedOperationException("LocalChannelSink is send-only (poll on shared channel)");
    }

    @Override
    public void close() {
        shared.producerDone();
    }

    @Override
    public void fail(Throwable cause) {
        shared.fail(cause);
    }

    @Override
    public long parkCount() {
        return shared.parkCount();
    }
}
