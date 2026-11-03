package io.rowweave.stream.spi;

import io.rowweave.stream.spi.conformance.StreamConnectorConformance;
import io.rowweave.stream.spi.conformance.StreamConnectorConformanceException;
import io.rowweave.stream.spi.conformance.StreamConformanceReport;
import io.rowweave.stream.spi.conformance.StreamSourceConformanceFixture;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A published bounded connector must prove terminal replay, not merely emit END. */
class StreamBoundedSourceConformanceKitTest {
    @Test
    void twoRecordBoundedSourcePassesReplayAndTerminalCut() throws Exception {
        Fixture fixture = new Fixture(false, true);
        StreamConformanceReport report =
                StreamConnectorConformance.verifyBoundedSource(
                        fixture);

        assertTrue(report.passed("SOURCE_POSITION_MONOTONIC"));
        assertTrue(report.passed("SOURCE_EXACT_REPLAY"));
        assertTrue(report.passed("SOURCE_END_ONCE"));
        assertTrue(report.passed("SOURCE_TERMINAL_RESTORE"));
        assertTrue(report.passed("SOURCE_WAKEUP_CANCEL_CLOSE"));
        assertTrue(report.passed("SOURCE_FRAME_LIMITS"));
        assertTrue(fixture.controlsObserved());
    }

    @Test
    void repeatedEndFailsThePublishedBoundedContract() {
        StreamConnectorConformanceException failure = assertThrows(
                StreamConnectorConformanceException.class,
                () -> StreamConnectorConformance.verifyBoundedSource(
                        new Fixture(true, false)));
        assertEquals(StreamConnectorConformanceException.Code
                .SOURCE_TERMINATION_PROTOCOL_INVALID, failure.code());
    }

    private static final class Fixture
            implements StreamSourceConformanceFixture {
        private final StreamSourceProvider provider;

        private Fixture(boolean repeatEnd, boolean temporaryNull) {
            provider = new Provider(repeatEnd, temporaryNull);
        }

        @Override public StreamSourceProvider provider() { return provider; }
        @Override public StreamSourceContext initialContext() {
            return context(null, -1L);
        }
        @Override public StreamSourceContext restoredContext(
                byte[] positionFrame, long logicalPosition) {
            return context(positionFrame, logicalPosition);
        }
        @Override public int comparePositions(byte[] left, byte[] right) {
            return Integer.compare(ByteBuffer.wrap(left).getInt(),
                    ByteBuffer.wrap(right).getInt());
        }
        @Override public long wakeupTimeoutMillis() { return 2_000L; }

        private boolean controlsObserved() {
            Provider value = (Provider) provider;
            return value.wakeups.get() > 0
                    && value.cancels.get() > 0
                    && value.closes.get() >= 3;
        }

        private static StreamSourceContext context(byte[] position,
                long logicalPosition) {
            return new StreamSourceContext("source", new byte[] {1},
                    "two-record-split", "int32-v1", new byte[] {2},
                    position, logicalPosition);
        }
    }

    private static final class Provider implements StreamSourceProvider {
        private final boolean repeatEnd;
        private final boolean temporaryNull;
        private final AtomicInteger wakeups = new AtomicInteger();
        private final AtomicInteger cancels = new AtomicInteger();
        private final AtomicInteger closes = new AtomicInteger();
        private Provider(boolean repeatEnd, boolean temporaryNull) {
            this.repeatEnd = repeatEnd;
            this.temporaryNull = temporaryNull;
        }
        @Override public String providerKey() { return "bounded-v1"; }
        @Override public String contractDigest() {
            return "bounded-contract";
        }
        @Override public StreamSourceReader open(StreamSourceContext context) {
            int restored = context.hasRestoredPosition()
                    ? ByteBuffer.wrap(context.restoredPositionFrame()).getInt()
                    : 0;
            return new StreamSourceReader() {
                private int next = restored;
                private boolean ended;
                private boolean returnedTemporaryNull;
                @Override public StreamSourceRecord next() {
                    throw new AssertionError("bounded reader uses events");
                }
                @Override public StreamSourceEvent nextEvent() {
                    if (temporaryNull && !returnedTemporaryNull) {
                        returnedTemporaryNull = true;
                        return null;
                    }
                    if (next < 2) {
                        next++;
                        return StreamSourceEvent.record(StreamSourceRecord.of(
                                new byte[] {(byte) (10 + next)}, position(next)));
                    }
                    if (!ended || repeatEnd) {
                        ended = true;
                        return StreamSourceEvent.endOfInput();
                    }
                    return null;
                }
                @Override public byte[] checkpointPositionFrame() {
                    return position(next);
                }
                @Override public void wakeUp() { wakeups.incrementAndGet(); }
                @Override public void cancel() { cancels.incrementAndGet(); }
                @Override public void close() { closes.incrementAndGet(); }
            };
        }
    }

    private static byte[] position(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }
}
