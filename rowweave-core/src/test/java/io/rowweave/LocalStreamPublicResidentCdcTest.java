package io.rowweave;

import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamCheckpointConfig;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamDeliveryGuarantee;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamRecoveryPointId;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.api.StreamUpsertCodec;
import io.rowweave.stream.api.StreamUpsertValue;
import io.rowweave.stream.api.StreamDataRowCodec;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.contract.StreamUpsertRecord;
import io.rowweave.stream.contract.StreamUpsertRecordCodec;
import io.rowweave.stream.spi.StreamCdcSourceContext;
import io.rowweave.stream.spi.StreamCdcCheckpointCompletion;
import io.rowweave.stream.spi.StreamCdcSourceEvent;
import io.rowweave.stream.spi.StreamCdcSourceProvider;
import io.rowweave.stream.spi.StreamCdcSourceReader;
import io.rowweave.stream.spi.StreamTransactionStatus;
import io.rowweave.stream.spi.StreamTransactionalSinkContext;
import io.rowweave.stream.spi.StreamTransactionalSinkProvider;
import io.rowweave.stream.spi.StreamTransactionalSinkWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Resident CDC executes through only public API/SPI. */
class LocalStreamPublicResidentCdcTest {
    private static final String SOURCE = "public-cdc-source-v1";
    private static final String SINK = "public-cdc-sink-v1";
    private static final String SOURCE_CONTRACT = digest("public-cdc-source");
    private static final String SINK_CONTRACT = digest("public-cdc-sink");
    private static final String SNAPSHOT = digest("snapshot-10");
    private static final StreamSchema KEY_SCHEMA = new StreamSchema(
            Collections.singletonList(new StreamField(
                    "id", "id", "id", DataType.StringType, false)));
    private static final StreamSchema ROW_SCHEMA = new StreamSchema(
            java.util.Arrays.asList(
                    new StreamField("id", "id", "id",
                            DataType.StringType, false),
                    new StreamField("amount", "amount", "amount",
                            DataType.BigDecimalType, false)));

    @Test
    void v2TransactionBoundaryAndOpaquePositionRestoreThroughPublicApi(
            @TempDir Path root) throws Exception {
        UpsertSink sink = new UpsertSink();
        V2CdcSource source = new V2CdcSource(sink);
        try (StreamJobClient client = RowWeave.createLocalStreamJobClient(
                root, Collections.singletonList(source),
                Collections.emptyList(), Collections.singletonList(sink),
                Collections.emptyList())) {
            StreamSession session = RowWeave.createStreamSession(
                    "public-resident-cdc-v2",
                    "public-resident-cdc-v2", client);
            session.setDeliveryGuarantee(StreamDeliveryGuarantee.EXACTLY_ONCE);
            session.setChangelogMode(StreamChangelogMode.UPSERT);
            session.setCheckpointConfig(StreamCheckpointConfig.periodic(
                    1_000L, 5_000L, 2));
            session.setResourceProfile(new StreamResourceProfile(
                    8, 4_096, 8_192, 0L, 0, 0,
                    32_768L, 8, 32, 0L));
            session.fromSource(StreamSourceSpec.residentCdcV2(
                            ROW_SCHEMA, Collections.singletonList("id"),
                            SOURCE, SOURCE_CONTRACT, new byte[] {1},
                            SNAPSHOT, 0L,
                            Collections.singletonList("snapshot-all")))
                    .uid("source")
                    .sinkTo("sink", StreamSinkSpec.checkpointTransactional(
                            SINK, SINK_CONTRACT, new byte[] {2}));
            io.rowweave.artifact.PlanArtifact artifact =
                    session.toPlanArtifact();

            StreamJobHandle job = session.submit(
                    StreamSubmissionId.of("public-resident-cdc-v2-submit"));
            job.awaitRunning(5_000L);
            source.awaitLog(5_000L);
            assertEquals(Collections.emptyMap(), sink.visible());
            assertEquals(0, source.completionCount());
            StreamCheckpointHandle completed = job.triggerCheckpoint(
                    StreamCommandId.of("cdc-v2-cut"));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    completed.awaitCompletion(5_000L));
            source.awaitCompletion(5_000L);
            assertEquals(1, source.completionCount());
            assertTrue(source.completionAttempts() >= 2);
            assertEquals(2L, source.completion(0).lastLogSequence());
            assertArrayEquals(bytes("0/20"),
                    source.completion(0).externalPositionFrame());
            Map<String, BigDecimal> expected = new LinkedHashMap<>();
            expected.put("A", new BigDecimal("12.50"));
            expected.put("B", new BigDecimal("7.00"));
            assertEquals(expected, sink.visible());
            job.cancel(StreamCommandId.of("cdc-v2-cancel"))
                    .awaitCompletion(5_000L);

            StreamJobHandle restored = client.restore(
                    StreamSubmissionId.of("public-resident-cdc-v2-restored"),
                    artifact, new StreamRestoreRequest(job.id(),
                            StreamRecoveryPointId.checkpoint(
                                    completed.checkpointId()), 1, 128));
            restored.awaitRunning(5_000L);
            source.awaitLog(5_000L);
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    restored.triggerCheckpoint(
                            StreamCommandId.of("cdc-v2-restored-cut"))
                            .awaitCompletion(5_000L));
            source.awaitCompletion(5_000L);
            assertEquals(2, source.completionCount());
            assertEquals(3L, source.completion(1).lastLogSequence());
            assertArrayEquals(bytes("0/30"),
                    source.completion(1).externalPositionFrame());
            expected.put("A", new BigDecimal("15.00"));
            assertEquals(expected, sink.visible());
            restored.cancel(StreamCommandId.of("cdc-v2-restored-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    @Test
    void checkpointWakeInsideTransactionWaitsForCompletedBoundary(
            @TempDir Path root) throws Exception {
        MidTransactionWakeSource source = new MidTransactionWakeSource();
        UpsertSink sink = new UpsertSink();
        try (StreamJobClient client = RowWeave.createLocalStreamJobClient(
                root, Collections.singletonList(source),
                Collections.emptyList(), Collections.singletonList(sink),
                Collections.emptyList())) {
            StreamSession session = RowWeave.createStreamSession(
                    "public-resident-cdc-mid-transaction",
                    "public-resident-cdc-mid-transaction", client);
            session.setDeliveryGuarantee(StreamDeliveryGuarantee.EXACTLY_ONCE);
            session.setChangelogMode(StreamChangelogMode.UPSERT);
            session.setCheckpointConfig(StreamCheckpointConfig.periodic(
                    60_000L, 5_000L, 2));
            session.setResourceProfile(new StreamResourceProfile(
                    8, 4_096, 8_192, 0L, 0, 0,
                    32_768L, 8, 32, 0L));
            session.fromSource(StreamSourceSpec.residentCdcV2(
                            ROW_SCHEMA, Collections.singletonList("id"),
                            SOURCE, SOURCE_CONTRACT, new byte[] {1},
                            SNAPSHOT, 0L,
                            Collections.singletonList("snapshot-all")))
                    .uid("source")
                    .sinkTo("sink", StreamSinkSpec.checkpointTransactional(
                            SINK, SINK_CONTRACT, new byte[] {2}));

            StreamJobHandle job = session.submit(StreamSubmissionId.of(
                    "public-resident-cdc-mid-transaction-submit"));
            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(5_000L));
            source.awaitMidTransaction(5_000L);

            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.triggerCheckpoint(StreamCommandId.of(
                                    "mid-transaction-cut"))
                            .awaitCompletion(5_000L));
            assertEquals(StreamJobStatus.RUNNING, job.status());
            Map<String, BigDecimal> expected = new LinkedHashMap<>();
            expected.put("A", new BigDecimal("10.00"));
            expected.put("B", new BigDecimal("12.50"));
            expected.put("C", new BigDecimal("7.00"));
            assertEquals(expected, sink.visible());
            job.cancel(StreamCommandId.of("mid-transaction-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    @Test
    void checkpointCompletedAtBoundaryConsumesItsLateWakeSignal(
            @TempDir Path root) throws Exception {
        BoundaryWakeSource source = new BoundaryWakeSource();
        UpsertSink sink = new UpsertSink();
        try (StreamJobClient client = RowWeave.createLocalStreamJobClient(
                root, Collections.singletonList(source),
                Collections.emptyList(), Collections.singletonList(sink),
                Collections.emptyList())) {
            StreamSession session = RowWeave.createStreamSession(
                    "public-resident-cdc-boundary-wake",
                    "public-resident-cdc-boundary-wake", client);
            session.setDeliveryGuarantee(StreamDeliveryGuarantee.EXACTLY_ONCE);
            session.setChangelogMode(StreamChangelogMode.UPSERT);
            session.setCheckpointConfig(StreamCheckpointConfig.periodic(
                    60_000L, 5_000L, 2));
            session.setResourceProfile(new StreamResourceProfile(
                    8, 4_096, 8_192, 0L, 0, 0,
                    32_768L, 8, 32, 0L));
            session.fromSource(StreamSourceSpec.residentCdcV2(
                            ROW_SCHEMA, Collections.singletonList("id"),
                            SOURCE, SOURCE_CONTRACT, new byte[] {1},
                            SNAPSHOT, 0L,
                            Collections.singletonList("snapshot-all")))
                    .uid("source")
                    .sinkTo("sink", StreamSinkSpec.checkpointTransactional(
                            SINK, SINK_CONTRACT, new byte[] {2}));
            StreamJobHandle job = session.submit(StreamSubmissionId.of(
                    "public-resident-cdc-boundary-wake-submit"));
            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(5_000L));
            source.awaitBoundary(5_000L);

            AtomicReference<StreamCheckpointHandle> result =
                    new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread checkpoint = new Thread(() -> {
                try {
                    result.set(job.triggerCheckpoint(StreamCommandId.of(
                            "boundary-wake-cut")));
                } catch (Throwable thrown) {
                    failure.set(thrown);
                }
            }, "boundary-wake-checkpoint");
            checkpoint.start();
            source.awaitWake(5_000L);
            source.releaseBoundary();
            checkpoint.join(5_000L);
            assertTrue(!checkpoint.isAlive());
            assertEquals(null, failure.get());
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    result.get().status());

            Thread.sleep(250L);
            assertEquals(StreamJobStatus.RUNNING, job.status(),
                    "the wake token consumed after a completed boundary "
                            + "must not terminate the resident source");
            job.cancel(StreamCommandId.of("boundary-wake-cancel"))
                    .awaitCompletion(5_000L);
        } finally {
            source.releaseBoundary();
        }
    }

    @Test
    void snapshotHandoffAndLogCommitAsOneExactCheckpoint(
            @TempDir Path root) throws Exception {
        CdcSource source = new CdcSource();
        UpsertSink sink = new UpsertSink();
        try (StreamJobClient client = RowWeave.createLocalStreamJobClient(
                root, Collections.singletonList(source),
                Collections.emptyList(), Collections.singletonList(sink),
                Collections.emptyList())) {
            StreamSession session = RowWeave.createStreamSession(
                    "public-resident-cdc", "public-resident-cdc", client);
            session.setDeliveryGuarantee(StreamDeliveryGuarantee.EXACTLY_ONCE);
            session.setChangelogMode(StreamChangelogMode.UPSERT);
            session.setCheckpointConfig(StreamCheckpointConfig.periodic(
                    1_000L, 5_000L, 2));
            session.setResourceProfile(new StreamResourceProfile(
                    8, 4_096, 8_192, 0L, 0, 0,
                    32_768L, 8, 32, 0L));
            session.fromSource(StreamSourceSpec.residentCdc(
                            ROW_SCHEMA, Collections.singletonList("id"),
                            SOURCE, SOURCE_CONTRACT, new byte[] {1},
                            SNAPSHOT, 10L,
                            Collections.singletonList("snapshot-0")))
                    .uid("source")
                    .sinkTo("sink", StreamSinkSpec.checkpointTransactional(
                            SINK, SINK_CONTRACT, new byte[] {2}));
            io.rowweave.artifact.PlanArtifact artifact =
                    session.toPlanArtifact();

            StreamJobHandle job = session.submit(
                    StreamSubmissionId.of("public-resident-cdc-submit"));
            job.awaitRunning(5_000L);
            source.awaitLog(5_000L);
            assertEquals(Collections.emptyMap(), sink.visible());

            StreamCheckpointHandle completed = job.triggerCheckpoint(
                    StreamCommandId.of("cdc-cut"));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    completed.awaitCompletion(5_000L));
            assertEquals(Collections.singletonMap(
                    "A", new BigDecimal("12.50")), sink.visible());

            job.cancel(StreamCommandId.of("cdc-cancel"))
                    .awaitCompletion(5_000L);

            StreamJobHandle restored = client.restore(
                    StreamSubmissionId.of("public-resident-cdc-restored"),
                    artifact, new StreamRestoreRequest(job.id(),
                            StreamRecoveryPointId.checkpoint(
                                    completed.checkpointId()), 1, 128));
            restored.awaitRunning(5_000L);
            source.awaitLog(5_000L);
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    restored.triggerCheckpoint(
                            StreamCommandId.of("cdc-restored-cut"))
                            .awaitCompletion(5_000L));
            assertEquals(Collections.singletonMap(
                    "A", new BigDecimal("15.00")), sink.visible());
            restored.cancel(StreamCommandId.of("cdc-restored-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    @Test
    void residentCdcRejectsWrapperKeyDifferentFromAfterImage(
            @TempDir Path root) {
        CdcSource source = new CdcSource(true);
        UpsertSink sink = new UpsertSink();
        try (StreamJobClient client = RowWeave.createLocalStreamJobClient(
                root, Collections.singletonList(source),
                Collections.emptyList(), Collections.singletonList(sink),
                Collections.emptyList())) {
            StreamSession session = RowWeave.createStreamSession(
                    "public-resident-cdc-key-drift",
                    "public-resident-cdc-key-drift", client);
            session.setDeliveryGuarantee(StreamDeliveryGuarantee.EXACTLY_ONCE);
            session.setChangelogMode(StreamChangelogMode.UPSERT);
            session.setCheckpointConfig(StreamCheckpointConfig.periodic(
                    1_000L, 5_000L, 2));
            session.setResourceProfile(new StreamResourceProfile(
                    8, 4_096, 8_192, 0L, 0, 0,
                    32_768L, 8, 32, 0L));
            session.fromSource(StreamSourceSpec.residentCdc(
                            ROW_SCHEMA, Collections.singletonList("id"),
                            SOURCE, SOURCE_CONTRACT, new byte[] {1},
                            SNAPSHOT, 10L,
                            Collections.singletonList("snapshot-0")))
                    .uid("source")
                    .sinkTo("sink", StreamSinkSpec.checkpointTransactional(
                            SINK, SINK_CONTRACT, new byte[] {2}));

            StreamJobHandle job = session.submit(
                    StreamSubmissionId.of("public-resident-cdc-key-drift"));
            assertEquals(StreamJobStatus.FAILED,
                    job.awaitTermination(5_000L));
            assertEquals(Collections.emptyMap(), sink.visible());
        }
    }

    private static final class CdcSource implements StreamCdcSourceProvider {
        private final Semaphore emittedLog = new Semaphore(0);
        private final boolean driftStableKey;

        private CdcSource() { this(false); }
        private CdcSource(boolean driftStableKey) {
            this.driftStableKey = driftStableKey;
        }
        @Override public String providerKey() { return SOURCE; }
        @Override public String contractDigest() { return SOURCE_CONTRACT; }
        @Override public StreamCdcSourceReader openCdc(
                StreamCdcSourceContext context) {
            assertEquals(SNAPSHOT, context.snapshotId());
            assertEquals(10L, context.snapshotCutSequence());
            boolean restoredLog = context.phase()
                    == StreamCdcSourceContext.Phase.LOG;
            StreamUpsertCodec codec = new StreamUpsertCodec(
                    KEY_SCHEMA, ROW_SCHEMA);
            return new StreamCdcSourceReader() {
                private final Semaphore wakeups = new Semaphore(0);
                private volatile boolean canceled;
                private boolean logSignaled;
                private int next;
                @Override public StreamCdcSourceEvent next() throws Exception {
                    if (driftStableKey && next++ == 0) {
                        byte[] keyFrame = new StreamDataRowCodec(KEY_SCHEMA)
                                .encode(key("K1"));
                        byte[] rowFrame = new StreamDataRowCodec(ROW_SCHEMA)
                                .encode(row("K2", "10.00"));
                        return StreamCdcSourceEvent.snapshotAfterImage(
                                "snapshot-0", StreamUpsertRecordCodec.encode(
                                StreamUpsertRecord.afterImage(
                                        keyFrame, rowFrame)));
                    }
                    if (restoredLog) {
                        if (next++ == 0) {
                            return StreamCdcSourceEvent.logRecord(
                                    context.lastLogSequence() + 1L,
                                    codec.encodeAfterImage(
                                            key("A"), row("A", "15.00")));
                        }
                        signalLogOnce();
                        return awaitWakeup();
                    }
                    switch (next++) {
                        case 0:
                            return StreamCdcSourceEvent.snapshotAfterImage(
                                    "snapshot-0", codec.encodeAfterImage(
                                    key("A"), row("A", "10.00")));
                        case 1:
                            return StreamCdcSourceEvent.snapshotSplitComplete(
                                    "snapshot-0");
                        case 2:
                            return StreamCdcSourceEvent.handoffComplete();
                        case 3:
                            return StreamCdcSourceEvent.logRecord(11L,
                                    codec.encodeAfterImage(
                                            key("A"), row("A", "12.50")));
                        default:
                            signalLogOnce();
                            return awaitWakeup();
                    }
                }
                private void signalLogOnce() {
                    if (!logSignaled) {
                        logSignaled = true;
                        emittedLog.release();
                    }
                }
                private StreamCdcSourceEvent awaitWakeup()
                        throws InterruptedException {
                    while (!canceled) {
                        if (wakeups.tryAcquire(
                                20L, TimeUnit.MILLISECONDS)) return null;
                    }
                    return null;
                }
                @Override public void wakeUp() { wakeups.release(); }
                @Override public void cancel() {
                    canceled = true;
                    wakeups.release();
                }
                @Override public void close() { cancel(); }
            };
        }
        void awaitLog(long timeoutMillis) throws InterruptedException {
            if (!emittedLog.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("CDC log record timed out");
            }
        }
    }

    private static final class V2CdcSource
            implements StreamCdcSourceProvider {
        private final Semaphore emittedLog = new Semaphore(0);
        private final Semaphore completedCheckpoint = new Semaphore(0);
        private final List<StreamCdcCheckpointCompletion> completions =
                new ArrayList<>();
        private final UpsertSink sink;
        private int completionAttempts;
        private V2CdcSource(UpsertSink sink) {
            this.sink = sink;
        }
        @Override public String providerKey() { return SOURCE; }
        @Override public String contractDigest() { return SOURCE_CONTRACT; }
        @Override public StreamCdcSourceReader openCdc(
                StreamCdcSourceContext context) {
            boolean restoredLog = context.phase()
                    == StreamCdcSourceContext.Phase.LOG;
            if (restoredLog) {
                assertEquals(2L, context.lastLogSequence());
                assertTrue(context.hasExternalPosition());
                assertArrayEquals(bytes("0/20"),
                        context.externalPositionFrame());
            } else {
                assertFalse(context.hasExternalPosition());
            }
            StreamUpsertCodec codec = new StreamUpsertCodec(
                    KEY_SCHEMA, ROW_SCHEMA);
            return new StreamCdcSourceReader() {
                private final Semaphore wakeups = new Semaphore(0);
                private volatile boolean canceled;
                private boolean signaled;
                private int next;
                @Override public StreamCdcSourceEvent next()
                        throws Exception {
                    if (restoredLog) {
                        if (next++ == 0) {
                            return StreamCdcSourceEvent.logRecord(3L,
                                    codec.encodeAfterImage(
                                            key("A"), row("A", "15.00")),
                                    bytes("0/30"));
                        }
                        signal();
                        return awaitWakeup();
                    }
                    switch (next++) {
                        case 0:
                            return StreamCdcSourceEvent.snapshotAfterImage(
                                    "snapshot-all", codec.encodeAfterImage(
                                            key("A"), row("A", "10.00")));
                        case 1:
                            return StreamCdcSourceEvent
                                    .snapshotHandoffComplete(
                                            "snapshot-all", bytes("0/10"));
                        case 2:
                            return StreamCdcSourceEvent.logRecord(1L,
                                    codec.encodeAfterImage(
                                            key("A"), row("A", "12.50")),
                                    null);
                        case 3:
                            return StreamCdcSourceEvent.logRecord(2L,
                                    codec.encodeAfterImage(
                                            key("B"), row("B", "7.00")),
                                    bytes("0/20"));
                        default:
                            signal();
                            return awaitWakeup();
                    }
                }
                private void signal() {
                    if (!signaled) {
                        signaled = true;
                        emittedLog.release();
                    }
                }
                private StreamCdcSourceEvent awaitWakeup()
                        throws InterruptedException {
                    while (!canceled) {
                        if (wakeups.tryAcquire(
                                20L, TimeUnit.MILLISECONDS)) return null;
                    }
                    return null;
                }
                @Override public void wakeUp() { wakeups.release(); }
                @Override public void notifyCheckpointComplete(
                        StreamCdcCheckpointCompletion completion) {
                    if (sink.visible().isEmpty()) {
                        throw new IllegalStateException(
                                "source completion preceded sink visibility");
                    }
                    synchronized (completions) {
                        completionAttempts++;
                        if (completionAttempts == 1) {
                            throw new IllegalStateException(
                                    "injected source acknowledgement loss");
                        }
                        completions.add(completion);
                    }
                    completedCheckpoint.release();
                }
                @Override public void cancel() {
                    canceled = true;
                    wakeups.release();
                }
                @Override public void close() { cancel(); }
            };
        }
        void awaitLog(long timeoutMillis) throws InterruptedException {
            if (!emittedLog.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("CDC v2 log record timed out");
            }
        }
        void awaitCompletion(long timeoutMillis) throws InterruptedException {
            if (!completedCheckpoint.tryAcquire(
                    timeoutMillis, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException(
                        "CDC v2 checkpoint completion timed out");
            }
        }
        int completionCount() {
            synchronized (completions) {
                return completions.size();
            }
        }
        int completionAttempts() {
            synchronized (completions) {
                return completionAttempts;
            }
        }
        StreamCdcCheckpointCompletion completion(int index) {
            synchronized (completions) {
                return completions.get(index);
            }
        }
    }

    private static final class MidTransactionWakeSource
            implements StreamCdcSourceProvider {
        private final Semaphore midTransaction = new Semaphore(0);

        @Override public String providerKey() { return SOURCE; }
        @Override public String contractDigest() { return SOURCE_CONTRACT; }

        @Override public StreamCdcSourceReader openCdc(
                StreamCdcSourceContext context) {
            StreamUpsertCodec codec = new StreamUpsertCodec(
                    KEY_SCHEMA, ROW_SCHEMA);
            return new StreamCdcSourceReader() {
                private final Semaphore wakeups = new Semaphore(0);
                private volatile boolean canceled;
                private int next;

                @Override public StreamCdcSourceEvent next()
                        throws Exception {
                    switch (next++) {
                        case 0:
                            return StreamCdcSourceEvent.snapshotAfterImage(
                                    "snapshot-all", codec.encodeAfterImage(
                                    key("A"), row("A", "10.00")));
                        case 1:
                            return StreamCdcSourceEvent
                                    .snapshotHandoffComplete(
                                            "snapshot-all", bytes("0/10"));
                        case 2:
                            return StreamCdcSourceEvent.logRecord(1L,
                                    codec.encodeAfterImage(
                                            key("B"), row("B", "12.50")),
                                    null);
                        case 3:
                            midTransaction.release();
                            while (!canceled) {
                                if (wakeups.tryAcquire(
                                        20L, TimeUnit.MILLISECONDS)) {
                                    return null;
                                }
                            }
                            return null;
                        case 4:
                            return StreamCdcSourceEvent.logRecord(2L,
                                    codec.encodeAfterImage(
                                            key("C"), row("C", "7.00")),
                                    bytes("0/20"));
                        default:
                            while (!canceled) {
                                if (wakeups.tryAcquire(
                                        20L, TimeUnit.MILLISECONDS)) {
                                    return null;
                                }
                            }
                            return null;
                    }
                }

                @Override public void wakeUp() { wakeups.release(); }
                @Override public void cancel() {
                    canceled = true;
                    wakeups.release();
                }
                @Override public void close() { cancel(); }
            };
        }

        void awaitMidTransaction(long timeoutMillis)
                throws InterruptedException {
            if (!midTransaction.tryAcquire(
                    timeoutMillis, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException(
                        "CDC transaction midpoint timed out");
            }
        }
    }

    private static final class BoundaryWakeSource
            implements StreamCdcSourceProvider {
        private final Semaphore boundary = new Semaphore(0);
        private final Semaphore releaseBoundary = new Semaphore(0);
        private final Semaphore wakeObserved = new Semaphore(0);

        @Override public String providerKey() { return SOURCE; }
        @Override public String contractDigest() { return SOURCE_CONTRACT; }

        @Override public StreamCdcSourceReader openCdc(
                StreamCdcSourceContext context) {
            StreamUpsertCodec codec = new StreamUpsertCodec(
                    KEY_SCHEMA, ROW_SCHEMA);
            return new StreamCdcSourceReader() {
                private final Semaphore wakeups = new Semaphore(0);
                private volatile boolean canceled;
                private int next;

                @Override public StreamCdcSourceEvent next()
                        throws Exception {
                    switch (next++) {
                        case 0:
                            return StreamCdcSourceEvent.snapshotAfterImage(
                                    "snapshot-all", codec.encodeAfterImage(
                                    key("A"), row("A", "10.00")));
                        case 1:
                            return StreamCdcSourceEvent
                                    .snapshotHandoffComplete(
                                            "snapshot-all", bytes("0/10"));
                        case 2:
                            boundary.release();
                            releaseBoundary.acquire();
                            return StreamCdcSourceEvent.logRecord(1L,
                                    codec.encodeAfterImage(
                                            key("B"), row("B", "12.50")),
                                    bytes("0/20"));
                        default:
                            while (!canceled) {
                                if (wakeups.tryAcquire(
                                        20L, TimeUnit.MILLISECONDS)) {
                                    return null;
                                }
                            }
                            return null;
                    }
                }

                @Override public void wakeUp() {
                    wakeups.release();
                    wakeObserved.release();
                }
                @Override public void cancel() {
                    canceled = true;
                    releaseBoundary.release();
                    wakeups.release();
                }
                @Override public void close() { cancel(); }
            };
        }

        void awaitBoundary(long timeoutMillis) throws InterruptedException {
            if (!boundary.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException(
                        "CDC transaction boundary timed out");
            }
        }

        void awaitWake(long timeoutMillis) throws InterruptedException {
            if (!wakeObserved.tryAcquire(
                    timeoutMillis, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("CDC wake timed out");
            }
        }

        void releaseBoundary() { releaseBoundary.release(); }
    }

    private static final class UpsertSink
            implements StreamTransactionalSinkProvider {
        private final Map<String, Transaction> transactions =
                new LinkedHashMap<>();
        private final Map<String, BigDecimal> visible = new LinkedHashMap<>();
        @Override public String providerKey() { return SINK; }
        @Override public String contractDigest() { return SINK_CONTRACT; }
        @Override public StreamTransactionalSinkWriter open(
                StreamTransactionalSinkContext context) {
            StreamUpsertCodec codec = new StreamUpsertCodec(
                    KEY_SCHEMA, ROW_SCHEMA);
            return new StreamTransactionalSinkWriter() {
                private final List<StreamUpsertValue> pending =
                        new ArrayList<>();
                @Override public void write(byte[] frame) {
                    pending.add(codec.decode(frame));
                }
                @Override public byte[] prepare(String identity) {
                    synchronized (transactions) {
                        transactions.putIfAbsent(identity,
                                new Transaction(new ArrayList<>(pending)));
                    }
                    return digest(identity).getBytes(
                            StandardCharsets.US_ASCII);
                }
                @Override public StreamTransactionStatus query(String identity) {
                    synchronized (transactions) {
                        Transaction value = transactions.get(identity);
                        return value == null ? StreamTransactionStatus.UNKNOWN
                                : value.status;
                    }
                }
                @Override public void commit(String identity) {
                    synchronized (transactions) {
                        Transaction transaction = transactions.get(identity);
                        if (transaction.status
                                == StreamTransactionStatus.VISIBLE) return;
                        for (StreamUpsertValue value : transaction.values) {
                            visible.put(value.stableKey().getString(0),
                                    value.afterImage().getBigDecimal(1));
                        }
                        transaction.status = StreamTransactionStatus.VISIBLE;
                    }
                }
                @Override public void finalizeTransaction(String identity) { }
                @Override public void abort(String identity) {
                    synchronized (transactions) {
                        transactions.get(identity).status =
                                StreamTransactionStatus.ABORTED;
                    }
                }
                @Override public void close() { }
            };
        }
        Map<String, BigDecimal> visible() {
            synchronized (transactions) {
                return new LinkedHashMap<>(visible);
            }
        }
    }

    private static final class Transaction {
        private final List<StreamUpsertValue> values;
        private StreamTransactionStatus status =
                StreamTransactionStatus.PREPARED;
        private Transaction(List<StreamUpsertValue> values) {
            this.values = values;
        }
    }

    private static DataRow key(String id) {
        DataRow row = new DataRow(1);
        row.set(0, id);
        return row;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static DataRow row(String id, String amount) {
        DataRow row = new DataRow(2);
        row.set(0, id);
        row.set(1, new BigDecimal(amount));
        return row;
    }

    private static String digest(String value) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256").digest(
                    value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(64);
            for (byte one : bytes) {
                result.append(String.format("%02x", one & 0xff));
            }
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
