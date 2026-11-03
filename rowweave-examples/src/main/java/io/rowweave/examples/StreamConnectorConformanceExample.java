package io.rowweave.examples;

import io.rowweave.stream.spi.StreamSinkContext;
import io.rowweave.stream.spi.StreamSinkProvider;
import io.rowweave.stream.spi.StreamSinkWriter;
import io.rowweave.stream.spi.StreamSourceContext;
import io.rowweave.stream.spi.StreamSourceEvent;
import io.rowweave.stream.spi.StreamSourceProvider;
import io.rowweave.stream.spi.StreamSourceReader;
import io.rowweave.stream.spi.StreamSourceRecord;
import io.rowweave.stream.spi.StreamTransactionStatus;
import io.rowweave.stream.spi.StreamTransactionalSinkContext;
import io.rowweave.stream.spi.StreamTransactionalSinkProvider;
import io.rowweave.stream.spi.StreamTransactionalSinkWriter;
import io.rowweave.stream.spi.conformance.StreamConnectorConformance;
import io.rowweave.stream.spi.conformance.StreamEagerSinkConformanceFixture;
import io.rowweave.stream.spi.conformance.StreamSourceConformanceFixture;
import io.rowweave.stream.spi.conformance.StreamTransactionalSinkConformanceFixture;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/** Runnable connector-author example for the published Stream conformance SDK. */
public final class StreamConnectorConformanceExample {
    private static final byte[] CONFIGURATION = bytes("example-config");
    private static final byte[] SPLIT = bytes("example-split");
    private static final byte[] RECORD = bytes("example-record");

    private StreamConnectorConformanceExample() { }

    public static void main(String[] arguments) throws Exception {
        for (String result : run()) {
            System.out.println(result);
        }
    }

    public static List<String> run() throws Exception {
        SourceFixture source = new SourceFixture();
        EagerSinkFixture eager = new EagerSinkFixture();
        TransactionalSinkFixture transactional =
                new TransactionalSinkFixture();
        return Collections.unmodifiableList(Arrays.asList(
                "source=" + StreamConnectorConformance
                        .verifySource(source).passedScenarioCount(),
                "eager-sink=" + StreamConnectorConformance
                        .verifyEagerSink(eager).passedScenarioCount(),
                "transactional-sink=" + StreamConnectorConformance
                        .verifyTransactionalSink(transactional)
                        .passedScenarioCount()));
    }

    private static final class SourceFixture
            implements StreamSourceConformanceFixture {
        private final StreamSourceProvider provider = new ExampleSource();

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

        private static StreamSourceContext context(
                byte[] position, long logicalPosition) {
            return new StreamSourceContext(
                    "example-source", CONFIGURATION,
                    "split-0", "int32-v1", SPLIT,
                    position, logicalPosition);
        }
    }

    private static final class ExampleSource implements StreamSourceProvider {
        @Override public String providerKey() {
            return "example-conformance-source-v1";
        }

        @Override public String contractDigest() {
            return "example-conformance-source-contract-v1";
        }

        @Override public StreamSourceReader open(StreamSourceContext context) {
            int restored = context.hasRestoredPosition()
                    ? ByteBuffer.wrap(context.restoredPositionFrame()).getInt()
                    : 0;
            return new StreamSourceReader() {
                private final Semaphore wakeups = new Semaphore(0);
                private volatile boolean canceled;
                private int nextRecord = restored;
                private int nextControl;

                @Override public StreamSourceRecord next() throws Exception {
                    StreamSourceEvent event = nextEvent();
                    return event != null
                            && event.kind() == StreamSourceEvent.Kind.RECORD
                            ? event.record() : null;
                }

                @Override public StreamSourceEvent nextEvent()
                        throws Exception {
                    if (nextRecord < 2) {
                        nextRecord++;
                        return StreamSourceEvent.record(StreamSourceRecord.of(
                                bytes("row-" + nextRecord),
                                position(nextRecord)));
                    }
                    switch (nextControl++) {
                        case 0:
                            return StreamSourceEvent.watermark(1_000L);
                        case 1:
                            return StreamSourceEvent.watermarkStatus(
                                    StreamSourceEvent.WatermarkStatus.IDLE, 1L);
                        case 2:
                            return StreamSourceEvent.watermarkStatus(
                                    StreamSourceEvent.WatermarkStatus.ACTIVE, 2L);
                        default:
                            while (!canceled) {
                                if (wakeups.tryAcquire()) return null;
                                Thread.sleep(10L);
                            }
                            return null;
                    }
                }

                @Override public byte[] checkpointPositionFrame() {
                    return position(nextRecord);
                }

                @Override public void wakeUp() { wakeups.release(); }

                @Override public void cancel() {
                    canceled = true;
                    wakeups.release();
                }

                @Override public void close() { cancel(); }
            };
        }
    }

    private static final class EagerSinkFixture
            implements StreamEagerSinkConformanceFixture {
        private final AtomicInteger writes = new AtomicInteger();
        private final AtomicInteger cancels = new AtomicInteger();
        private final AtomicInteger closes = new AtomicInteger();
        private final StreamSinkProvider provider = new StreamSinkProvider() {
            @Override public String providerKey() {
                return "example-conformance-eager-sink-v1";
            }

            @Override public String contractDigest() {
                return "example-conformance-eager-sink-contract-v1";
            }

            @Override public StreamSinkWriter open(StreamSinkContext context) {
                return new StreamSinkWriter() {
                    @Override public void write(byte[] recordFrame) {
                        writes.incrementAndGet();
                    }

                    @Override public void cancel() {
                        cancels.incrementAndGet();
                    }

                    @Override public void close() {
                        closes.incrementAndGet();
                    }
                };
            }
        };

        @Override public StreamSinkProvider provider() { return provider; }

        @Override public StreamSinkContext context() {
            return new StreamSinkContext("example-eager-sink", CONFIGURATION);
        }

        @Override public byte[] recordFrame() { return RECORD.clone(); }
        @Override public int observedWrites() { return writes.get(); }
        @Override public int observedCancels() { return cancels.get(); }
        @Override public int observedCloses() { return closes.get(); }
    }

    private static final class TransactionalSinkFixture
            implements StreamTransactionalSinkConformanceFixture {
        private final Map<String, StreamTransactionStatus> transactions =
                Collections.synchronizedMap(new LinkedHashMap<>());
        private final StreamTransactionalSinkProvider provider =
                new StreamTransactionalSinkProvider() {
                    @Override public String providerKey() {
                        return "example-conformance-transactional-sink-v1";
                    }

                    @Override public String contractDigest() {
                        return "example-conformance-transactional-sink-contract-v1";
                    }

                    @Override public StreamTransactionalSinkWriter open(
                            StreamTransactionalSinkContext context) {
                        return new ExampleTransactionalWriter(transactions);
                    }
                };

        @Override public StreamTransactionalSinkProvider provider() {
            return provider;
        }

        @Override public StreamTransactionalSinkContext context() {
            return new StreamTransactionalSinkContext(
                    "example-transactional-sink", 0,
                    "example-run", "example-attempt", 1L, 1L,
                    CONFIGURATION);
        }

        @Override public byte[] recordFrame() { return RECORD.clone(); }
        @Override public String commitTransactionIdentity() {
            return "example-commit";
        }
        @Override public String abortTransactionIdentity() {
            return "example-abort";
        }
    }

    private static final class ExampleTransactionalWriter
            implements StreamTransactionalSinkWriter {
        private final Map<String, StreamTransactionStatus> transactions;
        private boolean hasRecord;

        private ExampleTransactionalWriter(
                Map<String, StreamTransactionStatus> transactions) {
            this.transactions = transactions;
        }

        @Override public void write(byte[] recordFrame) {
            hasRecord = recordFrame != null && recordFrame.length > 0;
        }

        @Override public byte[] prepare(String identity) {
            if (!hasRecord) {
                throw new IllegalStateException("transaction has no record");
            }
            synchronized (transactions) {
                StreamTransactionStatus current = transactions.get(identity);
                if (current == null) {
                    transactions.put(identity, StreamTransactionStatus.PREPARED);
                }
            }
            return bytes("prepared-" + identity);
        }

        @Override public StreamTransactionStatus query(String identity) {
            synchronized (transactions) {
                StreamTransactionStatus status = transactions.get(identity);
                return status == null ? StreamTransactionStatus.UNKNOWN : status;
            }
        }

        @Override public void commit(String identity) {
            synchronized (transactions) {
                StreamTransactionStatus status = transactions.get(identity);
                if (status != StreamTransactionStatus.PREPARED
                        && status != StreamTransactionStatus.VISIBLE) {
                    throw new IllegalStateException(
                            "transaction is not prepared: " + identity);
                }
                transactions.put(identity, StreamTransactionStatus.VISIBLE);
            }
        }

        @Override public void finalizeTransaction(String identity) { }

        @Override public void abort(String identity) {
            synchronized (transactions) {
                StreamTransactionStatus status = transactions.get(identity);
                if (status == StreamTransactionStatus.VISIBLE) {
                    throw new IllegalStateException(
                            "visible transaction cannot abort: " + identity);
                }
                transactions.put(identity, StreamTransactionStatus.ABORTED);
            }
        }

        @Override public void close() { }
    }

    private static byte[] position(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
