package io.rowweave.examples;

import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.DataRow;
import io.rowweave.StreamJobClient;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamSession;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamCheckpointConfig;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamDeliveryGuarantee;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamRestartPolicy;
import io.rowweave.stream.api.StreamDataRowCodec;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceDiscoverySpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.spi.StreamDiscoveryCursor;
import io.rowweave.stream.spi.StreamDynamicSourceProvider;
import io.rowweave.stream.spi.StreamSourceContext;
import io.rowweave.stream.spi.StreamSourceEnumerator;
import io.rowweave.stream.spi.StreamSourceEnumeratorContext;
import io.rowweave.stream.spi.StreamSourceReader;
import io.rowweave.stream.spi.StreamSourceRecord;
import io.rowweave.stream.spi.StreamSplitChange;
import io.rowweave.stream.spi.StreamSplitDiscovery;
import io.rowweave.stream.spi.StreamTransactionStatus;
import io.rowweave.stream.spi.StreamTransactionalSinkContext;
import io.rowweave.stream.spi.StreamTransactionalSinkProvider;
import io.rowweave.stream.spi.StreamTransactionalSinkWriter;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Runnable public-SDK proof for Stream elastic primitives. */
public final class StreamElasticExample {
    private static final String SOURCE = "example-elastic-source";
    private static final String SINK = "example-elastic-sink";
    private static final String SOURCE_CONTRACT = digest("example-elastic-source");
    private static final String SINK_CONTRACT = digest("example-elastic-sink");
    private static final StreamSchema SCHEMA = new StreamSchema(
            Collections.singletonList(new StreamField(
                    "value", "value", "value", DataType.StringType, false)));

    private StreamElasticExample() { }

    public static List<String> run(Path stateRoot) throws Exception {
        DynamicSource source = new DynamicSource();
        CountingSink sink = new CountingSink();
        try (StreamJobClient client = RowWeave.createLocalStreamJobClient(
                stateRoot, Collections.singletonList(source),
                Collections.emptyList(), Collections.singletonList(sink),
                Collections.emptyList())) {
            StreamSession session = elasticSession(client);
            StreamJobHandle job = session.submit(
                    StreamSubmissionId.of("stream-elastic-elastic-example"));
            job.awaitRunning(5_000L);
            sink.awaitOne(5_000L, job, source);
            requireSucceeded(job.triggerCheckpoint(
                    StreamCommandId.of("stream-elastic-unaligned-cut"))
                    .awaitCompletion(25_000L), "unaligned checkpoint");
            requireSucceeded(job.rescale(
                    StreamCommandId.of("stream-elastic-scale-to-two"), 2)
                    .awaitCompletion(30_000L), "live rescale");
            requireSucceeded(job.cancel(
                    StreamCommandId.of("stream-elastic-cancel"))
                    .awaitCompletion(5_000L), "cancel");
        }

        PlanArtifact retract = retractArtifact();
        requireFeature(retract,
                PlanArtifact.FEATURE_STREAM_RETRACT_CHANGELOG_V2);
        return Arrays.asList("dynamic-split", "unaligned-checkpoint",
                "live-rescale", "retract");
    }

    private static StreamSession elasticSession(StreamJobClient client) {
        StreamSession session = RowWeave.createStreamSession(
                "stream-elastic-elastic", "stream-elastic-elastic", client);
        session.setParallelism(1);
        session.setMaxParallelism(8);
        session.setDeliveryGuarantee(StreamDeliveryGuarantee.EXACTLY_ONCE);
        session.setRestartPolicy(StreamRestartPolicy.fixedDelay(2, 10L));
        session.setCheckpointConfig(StreamCheckpointConfig.periodicUnaligned(
                60_000L, 20_000L, 2));
        session.setResourceProfile(resources());
        session.fromSource(StreamSourceSpec.dynamicExactOrdered(
                        SCHEMA, SOURCE, SOURCE_CONTRACT, new byte[] {1},
                        new StreamSourceDiscoverySpec(
                                25L, 1_000L, 4, 4, 2, 64, 64)))
                .uid("source")
                .sinkTo("sink", StreamSinkSpec.checkpointTransactional(
                        SINK, SINK_CONTRACT, new byte[] {2}));
        PlanArtifact artifact = session.toPlanArtifact();
        requireFeature(artifact,
                PlanArtifact.FEATURE_STREAM_DYNAMIC_SPLIT_DISCOVERY_V2);
        requireFeature(artifact,
                PlanArtifact.FEATURE_STREAM_UNALIGNED_CHECKPOINT_V2);
        return session;
    }

    private static PlanArtifact retractArtifact() {
        StreamSession session = RowWeave.createStreamSession(
                "stream-elastic-retract-contract");
        session.setChangelogMode(StreamChangelogMode.RETRACT);
        session.setResourceProfile(resources());
        session.fromSource(StreamSourceSpec.retractExactOrdered(
                        SCHEMA, Collections.singletonList("value"),
                        SOURCE, SOURCE_CONTRACT, new byte[] {3},
                        Collections.singletonList(split())))
                .uid("retract-source")
                .sinkTo("retract-sink",
                        StreamSinkSpec.retractEagerAtLeastOnce(
                                SINK, SINK_CONTRACT, new byte[] {4}));
        return session.toPlanArtifact();
    }

    private static StreamResourceProfile resources() {
        return new StreamResourceProfile(
                8, 4_096, 8_192, 0L, 0, 0,
                65_536L, 8, 64, 0L);
    }

    private static final class DynamicSource
            implements StreamDynamicSourceProvider {
        private final AtomicInteger enumerators = new AtomicInteger();
        private final AtomicInteger discoveries = new AtomicInteger();
        private final AtomicInteger readers = new AtomicInteger();
        @Override public String providerKey() { return SOURCE; }
        @Override public String contractDigest() { return SOURCE_CONTRACT; }

        @Override public StreamSourceEnumerator openEnumerator(
                StreamSourceEnumeratorContext context) {
            enumerators.incrementAndGet();
            return new StreamSourceEnumerator() {
                @Override public StreamSplitDiscovery discover(
                        StreamDiscoveryCursor cursor, int limit) {
                    discoveries.incrementAndGet();
                    if (cursor.frame().length == 0) {
                        return new StreamSplitDiscovery(1L,
                                StreamDiscoveryCursor.of(new byte[] {1}),
                                Collections.singletonList(
                                        StreamSplitChange.add(split(), 1L)));
                    }
                    return new StreamSplitDiscovery(1L, cursor,
                            Collections.emptyList());
                }

                @Override public void close() { }
            };
        }

        @Override public StreamSourceReader open(StreamSourceContext context) {
            readers.incrementAndGet();
            StreamDataRowCodec codec = new StreamDataRowCodec(SCHEMA);
            return new StreamSourceReader() {
                private final Semaphore wakeups = new Semaphore(0);
                private volatile boolean canceled;
                private int next = context.hasRestoredPosition()
                        ? ByteBuffer.wrap(
                                context.restoredPositionFrame()).getInt() : 0;

                @Override public StreamSourceRecord next() throws Exception {
                    if (next == 0) {
                        DataRow row = new DataRow(1);
                        row.set(0, "elastic");
                        next++;
                        return StreamSourceRecord.of(
                                codec.encode(row), position(next));
                    }
                    while (!canceled) {
                        if (wakeups.tryAcquire(20L,
                                TimeUnit.MILLISECONDS)) return null;
                    }
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
    }

    private static final class CountingSink
            implements StreamTransactionalSinkProvider {
        private final Semaphore writes = new Semaphore(0);
        private final Map<String, StreamTransactionStatus> transactions =
                new ConcurrentHashMap<>();
        @Override public String providerKey() { return SINK; }
        @Override public String contractDigest() { return SINK_CONTRACT; }
        @Override public StreamTransactionalSinkWriter open(
                StreamTransactionalSinkContext context) {
            return new StreamTransactionalSinkWriter() {
                @Override public void write(byte[] frame) {
                    writes.release();
                }
                @Override public byte[] prepare(String identity) {
                    transactions.putIfAbsent(identity,
                            StreamTransactionStatus.PREPARED);
                    return identity.getBytes(StandardCharsets.UTF_8);
                }
                @Override public StreamTransactionStatus query(
                        String identity) {
                    StreamTransactionStatus status = transactions.get(identity);
                    return status == null
                            ? StreamTransactionStatus.UNKNOWN : status;
                }
                @Override public void commit(String identity) {
                    transactions.put(identity, StreamTransactionStatus.VISIBLE);
                }
                @Override public void finalizeTransaction(String identity) { }
                @Override public void abort(String identity) {
                    transactions.put(identity, StreamTransactionStatus.ABORTED);
                }
                @Override public void close() { }
            };
        }
        void awaitOne(long timeoutMillis, StreamJobHandle job,
                DynamicSource source)
                throws InterruptedException {
            if (!writes.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException(
                        "Stream Sink timed out; job status=" + job.status()
                                + ", attempts=" + job.progress().attempt()
                                + ", restarts=" + job.progress().restarts()
                                + ", enumerators=" + source.enumerators.get()
                                + ", discoveries=" + source.discoveries.get()
                                + ", readers=" + source.readers.get());
            }
        }
    }

    private static StreamSplitSpec split() {
        return new StreamSplitSpec(digest("example-elastic-split"),
                "int32-v1", new byte[] {2});
    }

    private static byte[] position(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    private static void requireFeature(PlanArtifact artifact, String feature) {
        if (!artifact.requiredFeatures().contains(feature)) {
            throw new IllegalStateException(
                    "compiled plan is missing feature: " + feature);
        }
    }

    private static void requireSucceeded(
            StreamOperationStatus status, String operation) {
        if (status != StreamOperationStatus.SUCCEEDED) {
            throw new IllegalStateException(operation + " failed: " + status);
        }
    }

    private static String digest(String value) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256").digest(
                    value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(64);
            for (byte one : bytes) {
                result.append(Character.forDigit((one >>> 4) & 0x0f, 16));
                result.append(Character.forDigit(one & 0x0f, 16));
            }
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
