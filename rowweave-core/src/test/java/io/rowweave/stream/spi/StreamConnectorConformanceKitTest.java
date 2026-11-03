package io.rowweave.stream.spi;

import io.rowweave.stream.spi.conformance.StreamConnectorConformance;
import io.rowweave.stream.spi.conformance.StreamConformanceReport;
import io.rowweave.stream.spi.conformance.StreamEagerSinkConformanceFixture;
import io.rowweave.stream.spi.conformance.StreamSourceConformanceFixture;
import io.rowweave.stream.spi.conformance.StreamTransactionalSinkConformanceFixture;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** published connector SDK and backend-neutral conformance. */
class StreamConnectorConformanceKitTest {
    @Test
    void exactReplaySourcePassesThePublishedConformanceKit() throws Exception {
        StreamConformanceReport report = StreamConnectorConformance.verifySource(
                new SourceFixture());

        assertEquals(5, report.passedScenarioCount());
        assertTrue(report.passed("SOURCE_EXACT_REPLAY"));
        assertTrue(report.passed("SOURCE_POSITION_MONOTONIC"));
        assertTrue(report.passed("SOURCE_WATERMARK_IDLE_GENERATION"));
        assertTrue(report.passed("SOURCE_WAKEUP_CANCEL_CLOSE"));
        assertTrue(report.passed("SOURCE_FRAME_LIMITS"));
    }

    @Test
    void transactionalSinkPassesAckLossAndRestartConformance() throws Exception {
        SinkFixture fixture = new SinkFixture();
        StreamConformanceReport report =
                StreamConnectorConformance.verifyTransactionalSink(fixture);

        assertEquals(5, report.passedScenarioCount());
        assertTrue(report.passed("SINK_PREPARE_QUERY_COMMIT"));
        assertTrue(report.passed("SINK_COMMIT_ACK_LOSS"));
        assertTrue(report.passed("SINK_PROCESS_RESTART"));
        assertTrue(report.passed("SINK_ABORT_IDEMPOTENCE"));
        assertTrue(report.passed("SINK_FRAME_LIMITS"));
        assertEquals(StreamTransactionStatus.VISIBLE,
                fixture.status("tx-commit"));
        assertEquals(StreamTransactionStatus.ABORTED,
                fixture.status("tx-abort"));
    }

    @Test
    void eagerSinkPassesWriteCancelCloseAndFrameConformance() throws Exception {
        EagerSinkFixture fixture = new EagerSinkFixture();
        StreamConformanceReport report =
                StreamConnectorConformance.verifyEagerSink(fixture);

        assertEquals(3, report.passedScenarioCount());
        assertTrue(report.passed("SINK_EAGER_WRITE"));
        assertTrue(report.passed("SINK_CANCEL_CLOSE"));
        assertTrue(report.passed("SINK_FRAME_LIMITS"));
        assertEquals(1, fixture.writes);
        assertEquals(1, fixture.cancels);
        assertEquals(1, fixture.closes);
    }

    @Test
    void publicTransactionalContextIsBoundedAndDefensivelyCopied() {
        byte[] config = new byte[] {1, 2, 3};
        StreamTransactionalSinkContext context =
                new StreamTransactionalSinkContext(
                        "sink", 1, "run", "attempt", 2L, 3L, config);
        config[0] = 9;
        assertEquals(1, context.configurationFrame()[0]);
        assertEquals("sink", context.operatorUid());
        assertEquals(1, context.subtaskIndex());
        assertEquals("run", context.runId());
        assertEquals("attempt", context.attemptId());
        assertEquals(2L, context.fenceToken());
        assertEquals(3L, context.assignmentGeneration());
        assertThrows(IllegalArgumentException.class,
                () -> new StreamTransactionalSinkContext(
                        "sink", -1, "run", "attempt", 2L, 3L,
                        new byte[] {1}));
        assertThrows(IllegalArgumentException.class,
                () -> new StreamTransactionalSinkContext(
                        "sink", 0, "run", "attempt", 2L, 3L,
                        new byte[4 * 1024 * 1024 + 1]));
    }

    private static final class SourceFixture
            implements StreamSourceConformanceFixture {
        private final StreamSourceProvider provider = new SourceProvider();
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
        private static StreamSourceContext context(byte[] restored, long ordinal) {
            return new StreamSourceContext("source", new byte[] {1}, "split",
                    "int32-v1", new byte[] {2}, restored, ordinal);
        }
    }

    private static final class SourceProvider implements StreamSourceProvider {
        @Override public String providerKey() { return "source-v1"; }
        @Override public String contractDigest() { return "source-contract"; }
        @Override public StreamSourceReader open(StreamSourceContext context) {
            int restored = context.hasRestoredPosition()
                    ? ByteBuffer.wrap(context.restoredPositionFrame()).getInt() : 0;
            return new StreamSourceReader() {
                private final Semaphore wakeups = new Semaphore(0);
                private volatile boolean canceled;
                private int next = restored;
                private int event;
                @Override public StreamSourceRecord next() throws Exception {
                    StreamSourceEvent value = nextEvent();
                    return value == null ? null : value.record();
                }
                @Override public StreamSourceEvent nextEvent() throws Exception {
                    if (next < 2) {
                        int value = next++;
                        return StreamSourceEvent.record(StreamSourceRecord.of(
                                new byte[] {(byte) (10 + value)}, position(next),
                                100L + value));
                    }
                    if (event++ == 0) return StreamSourceEvent.watermark(101L);
                    if (event == 2) return StreamSourceEvent.watermarkStatus(
                            StreamSourceEvent.WatermarkStatus.IDLE, 1L);
                    if (event == 3) return StreamSourceEvent.watermarkStatus(
                            StreamSourceEvent.WatermarkStatus.ACTIVE, 2L);
                    while (!canceled && !wakeups.tryAcquire(
                            20L, TimeUnit.MILLISECONDS)) { }
                    return null;
                }
                @Override public byte[] checkpointPositionFrame() {
                    return position(next);
                }
                @Override public void wakeUp() { wakeups.release(); }
                @Override public void cancel() {
                    canceled = true;
                    wakeups.release();
                }
                @Override public void close() { cancel(); }
            };
        }
        private static byte[] position(int value) {
            return ByteBuffer.allocate(4).putInt(value).array();
        }
    }

    private static final class SinkFixture
            implements StreamTransactionalSinkConformanceFixture {
        private final Map<String, StreamTransactionStatus> states =
                new ConcurrentHashMap<>();
        private final StreamTransactionalSinkProvider provider = new SinkProvider();
        @Override public StreamTransactionalSinkProvider provider() {
            return provider;
        }
        @Override public StreamTransactionalSinkContext context() {
            return new StreamTransactionalSinkContext(
                    "sink", 0, "run", "attempt", 1L, 1L,
                    new byte[] {3});
        }
        @Override public byte[] recordFrame() { return new byte[] {7, 8}; }
        @Override public String commitTransactionIdentity() {
            return "tx-commit";
        }
        @Override public String abortTransactionIdentity() { return "tx-abort"; }
        StreamTransactionStatus status(String identity) {
            return states.get(identity);
        }

        private final class SinkProvider
                implements StreamTransactionalSinkProvider {
            @Override public String providerKey() { return "sink-v1"; }
            @Override public String contractDigest() { return "sink-contract"; }
            @Override public StreamTransactionalSinkWriter open(
                    StreamTransactionalSinkContext context) {
                return new StreamTransactionalSinkWriter() {
                    private byte[] pending;
                    @Override public void write(byte[] frame) {
                        pending = frame.clone();
                    }
                    @Override public byte[] prepare(String identity) {
                        if (pending == null) throw new IllegalStateException("no data");
                        states.putIfAbsent(identity, StreamTransactionStatus.PREPARED);
                        return Arrays.copyOf(pending, pending.length);
                    }
                    @Override public StreamTransactionStatus query(String identity) {
                        return states.getOrDefault(identity,
                                StreamTransactionStatus.UNKNOWN);
                    }
                    @Override public void commit(String identity) {
                        states.compute(identity, (key, status) -> {
                            if (status == StreamTransactionStatus.VISIBLE) return status;
                            if (status != StreamTransactionStatus.PREPARED) {
                                throw new IllegalStateException("not prepared");
                            }
                            return StreamTransactionStatus.VISIBLE;
                        });
                    }
                    @Override public void finalizeTransaction(String identity) { }
                    @Override public void abort(String identity) {
                        states.compute(identity, (key, status) -> {
                            if (status == StreamTransactionStatus.ABORTED) return status;
                            if (status == StreamTransactionStatus.VISIBLE) {
                                throw new IllegalStateException("visible");
                            }
                            return StreamTransactionStatus.ABORTED;
                        });
                    }
                    @Override public void close() { }
                };
            }
        }
    }

    private static final class EagerSinkFixture
            implements StreamEagerSinkConformanceFixture {
        private int writes;
        private int cancels;
        private int closes;
        private final StreamSinkProvider provider = new StreamSinkProvider() {
            @Override public String providerKey() { return "eager-v1"; }
            @Override public String contractDigest() { return "eager-contract"; }
            @Override public StreamSinkWriter open(StreamSinkContext context) {
                return new StreamSinkWriter() {
                    @Override public void write(byte[] frame) {
                        assertEquals(7, frame[0]);
                        writes++;
                    }
                    @Override public void cancel() { cancels++; }
                    @Override public void close() { closes++; }
                };
            }
        };
        @Override public StreamSinkProvider provider() { return provider; }
        @Override public StreamSinkContext context() {
            return new StreamSinkContext("sink", new byte[] {1});
        }
        @Override public byte[] recordFrame() { return new byte[] {7, 8}; }
        @Override public int observedWrites() { return writes; }
        @Override public int observedCancels() { return cancels; }
        @Override public int observedCloses() { return closes; }
    }
}
