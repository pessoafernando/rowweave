package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.DataStream;
import io.rowweave.DataRow;
import io.rowweave.mem.RowSerializer;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamJobClientRecoveryContract;
import io.rowweave.StreamSavepointHandle;
import io.rowweave.StreamSavepointRequest;
import io.rowweave.StreamRestoreRequest;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.StreamSession;
import io.rowweave.StreamWindowAggregateFunction;
import io.rowweave.StreamWindowContext;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamControlException;
import io.rowweave.stream.api.StreamCheckpointConfig;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamRecoveryPointId;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.api.StreamStateSpec;
import io.rowweave.stream.api.StreamTemporalJoinSpec;
import io.rowweave.stream.api.StreamTimeMode;
import io.rowweave.stream.api.StreamWatermarkPolicy;
import io.rowweave.stream.api.StreamWindowSpec;
import io.rowweave.stream.api.StreamGenericWindowSpec;
import io.rowweave.stream.api.StreamWindowAssignerSpec;
import io.rowweave.stream.api.StreamWindowTriggerSpec;
import io.rowweave.stream.api.StreamWindowResourceSpec;
import io.rowweave.stream.api.StreamEmissionLimit;
import io.rowweave.stream.api.StreamAccumulationMode;
import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.runtime.DurableStreamSourceProvider;
import io.rowweave.stream.contract.CheckpointEvidence;
import io.rowweave.stream.runtime.FileStreamSavepointStore;
import io.rowweave.stream.runtime.EagerStreamSinkProvider;
import io.rowweave.stream.runtime.EagerStreamSinkWriter;
import io.rowweave.stream.runtime.StreamSinkOpenRequest;
import io.rowweave.stream.runtime.StreamSourceOpenRequest;
import io.rowweave.stream.runtime.StreamSourceReader;
import io.rowweave.stream.runtime.StreamSourceRecord;
import io.rowweave.stream.runtime.StreamSourceEvent;
import io.rowweave.stream.runtime.TransactionalStreamSinkProvider;
import io.rowweave.stream.runtime.TransactionalStreamSinkWriter;
import io.rowweave.stream.runtime.StreamSinkTransactionOpenRequest;
import io.rowweave.stream.runtime.TransactionStatus;
import io.rowweave.stream.contract.CheckpointDecision;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Files;
import java.math.BigDecimal;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** the product Artifact executes through same-JVM roles. */
class InProcessStreamProductDispatchTest {
    private static final String SOURCE_CONTRACT = sha256("source-contract");
    private static final String SINK_CONTRACT = sha256("sink-contract");

    @Test
    void terminalEventWindowCommitsWithoutFinalWatermarkAndRestoresOnce(
            @TempDir Path root) throws Exception {
        DataRow row = new DataRow(3);
        row.set(0, "A");
        row.set(1, new java.sql.Timestamp(1L));
        row.set(2, BigDecimal.ONE);
        TerminalSource source = new TerminalSource("source-v1",
                new RowSerializer(StreamProductWindowWorkerProcessFixture
                        .inputMeta()).serialize(row), 1L);
        TerminalTransactionalSink sink = new TerminalTransactionalSink();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root, Collections.singletonMap(source.providerKey(), source),
                Collections.emptyMap(),
                Collections.singletonMap(sink.providerKey(), sink),
                ignored -> { throw new AssertionError("no function"); },
                Collections.emptyList())) {
            StreamSession definition = RowWeave.createStreamSession(
                    "terminal-event-window", "terminal-event-window",
                    cluster);
            definition.setTimeMode(StreamTimeMode.EVENT_TIME);
            definition.setParallelism(1);
            definition.setMaxParallelism(1);
            definition.setResourceProfile(new StreamResourceProfile(
                    8, 4_096, 8_192, 16_384L, 32, 32,
                    65_536L, 5, 32, 1_048_576L));
            definition.setDeliveryGuarantee(
                    io.rowweave.stream.api.StreamDeliveryGuarantee.EXACTLY_ONCE);
            definition.setCheckpointConfig(StreamCheckpointConfig.periodic(
                    60_000L, 5_000L, 2));
            definition.fromSource(StreamSourceSpec.boundedExactOrderedEventTime(
                            StreamGenericWindowProductAdmissionTest
                                    .inputSchema(), source.providerKey(),
                            SOURCE_CONTRACT, new byte[] {1},
                            Collections.singletonList(new StreamSplitSpec(
                                    sha256("terminal-window-split"),
                                    "sequence-v1", new byte[] {2})),
                            StreamWatermarkPolicy.providerEvents(1_000L)))
                    .uid("source").keyBy("adaptor").uid("key")
                    .eventTimeDecimalWindow(
                            StreamWindowSpec.tumblingDecimal("event-time",
                                    "amount", 100L, "window-start",
                                    "window-end", "total"),
                            StreamGenericWindowProductAdmissionTest
                                    .outputSchema(),
                            StreamStateSpec.decimalWindow("window-state"))
                    .uid("window").sinkTo("sink",
                            StreamSinkSpec.checkpointTransactional(
                                    "sink-v1", SINK_CONTRACT,
                                    new byte[] {3}));
            PlanArtifact artifact = definition.toPlanArtifact();
            StreamJobHandle original = definition.submit(
                    StreamSubmissionId.of("terminal-window-original"));
            assertEquals(StreamJobStatus.RUNNING,
                    original.awaitRunning(5_000L));
            assertTrue(source.firstClosed.await(5L, TimeUnit.SECONDS));
            assertEquals(0, sink.visible.get());
            io.rowweave.StreamCheckpointHandle first =
                    original.triggerCheckpoint(StreamCommandId.of(
                            "terminal-window-first"));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    first.awaitCompletion(5_000L));
            assertEquals(1, sink.visible.get());
            assertEquals(StreamJobStatus.FINISHED,
                    original.awaitTermination(5_000L));
            io.rowweave.stream.api.StreamJobEvidence evidence =
                    original.evidence();
            assertTrue(evidence.available());
            assertTrue(evidence.complete());
            assertEquals(1L, evidence.attemptProgress().recordsRead());
            assertEquals(1L, evidence.attemptProgress().recordsWritten());
            assertTrue(evidence.attemptProgress().managedStateBytes() > 0L);
            assertEquals(1,
                    evidence.attemptProgress().timerHighWaterCount());
            assertEquals(1,
                    evidence.attemptProgress().windowHighWaterCount());
            StreamJobHandle restored = cluster.restore(
                    StreamSubmissionId.of("terminal-window-restored"),
                    artifact, new StreamRestoreRequest(original.id(),
                    StreamRecoveryPointId.checkpoint(first.checkpointId()),
                    1, 1));
            assertEquals(StreamJobStatus.FINISHED,
                    restored.awaitTermination(5_000L));
            assertEquals(1, sink.visible.get());
        }
    }

    @Test
    void terminalGenericEventWindowCommitsWithoutFinalWatermarkAndRestoresOnce(
            @TempDir Path root) throws Exception {
        DataRow row = new DataRow(3);
        row.set(0, "A");
        row.set(1, new java.sql.Timestamp(1L));
        row.set(2, BigDecimal.ONE);
        TerminalSource source = new TerminalSource("source-v1",
                new RowSerializer(StreamProductWindowWorkerProcessFixture
                        .inputMeta()).serialize(row), 1L);
        TerminalTransactionalSink sink = new TerminalTransactionalSink();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root, Collections.singletonMap(source.providerKey(), source),
                Collections.emptyMap(),
                Collections.singletonMap(sink.providerKey(), sink),
                ignored -> new TerminalWindowAggregate(),
                Collections.singletonList(
                        StreamGenericWindowProductAdmissionTest.function()
                                .deploymentSetId()))) {
            StreamSession definition = RowWeave.createStreamSession(
                    "terminal-generic-window", "terminal-generic-window",
                    cluster);
            definition.setTimeMode(StreamTimeMode.EVENT_TIME);
            definition.setParallelism(1);
            definition.setMaxParallelism(1);
            definition.setResourceProfile(new StreamResourceProfile(
                    8, 4_096, 8_192, 16_384L, 32, 32,
                    65_536L, 5, 32, 1_048_576L));
            definition.setDeliveryGuarantee(
                    io.rowweave.stream.api.StreamDeliveryGuarantee.EXACTLY_ONCE);
            definition.setCheckpointConfig(StreamCheckpointConfig.periodic(
                    60_000L, 5_000L, 2));
            StreamGenericWindowSpec window =
                    StreamGenericWindowSpec.aggregate(
                    StreamWindowAssignerSpec.eventTimeTumbling(
                            "event-time", 100L),
                    StreamWindowTriggerSpec.eventTimeFinal(),
                    StreamAccumulationMode.DISCARDING, 0L,
                    StreamGenericWindowProductAdmissionTest.function(),
                    StreamGenericWindowProductAdmissionTest.outputSchema(),
                    Collections.singletonList("adaptor"),
                    new StreamStateSpec("window-state",
                            StreamStateSpec.Kind.KEYED_AGGREGATING,
                            "generic-window-state-v1", 1,
                            StreamStateSpec.TtlMode.NONE, 0L),
                    StreamWindowResourceSpec.of(8, 1_000L, 8_192L,
                            32_768L, 8,
                            StreamEmissionLimit.of(1, 4_096L)));
            definition.fromSource(StreamSourceSpec.boundedExactOrderedEventTime(
                            StreamGenericWindowProductAdmissionTest
                                    .inputSchema(), source.providerKey(),
                            SOURCE_CONTRACT, new byte[] {1},
                            Collections.singletonList(new StreamSplitSpec(
                                    sha256("terminal-generic-window-split"),
                                    "sequence-v1", new byte[] {2})),
                            StreamWatermarkPolicy.providerEvents(1_000L)))
                    .uid("source").keyBy("adaptor").uid("key")
                    .genericWindow(window).uid("window")
                    .sinkTo("sink", StreamSinkSpec.checkpointTransactional(
                            "sink-v1", SINK_CONTRACT, new byte[] {3}));
            PlanArtifact artifact = definition.toPlanArtifact();
            StreamJobHandle original = definition.submit(
                    StreamSubmissionId.of("terminal-generic-original"));
            assertEquals(StreamJobStatus.RUNNING,
                    original.awaitRunning(5_000L));
            assertTrue(source.firstClosed.await(5L, TimeUnit.SECONDS));
            assertEquals(0, sink.visible.get());
            io.rowweave.StreamCheckpointHandle first =
                    original.triggerCheckpoint(StreamCommandId.of(
                            "terminal-generic-first"));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    first.awaitCompletion(5_000L));
            assertEquals(1, sink.visible.get());
            assertEquals(StreamJobStatus.FINISHED,
                    original.awaitTermination(5_000L));
            StreamJobHandle restored = cluster.restore(
                    StreamSubmissionId.of("terminal-generic-restored"),
                    artifact, new StreamRestoreRequest(original.id(),
                    StreamRecoveryPointId.checkpoint(first.checkpointId()),
                    1, 1));
            assertEquals(StreamJobStatus.FINISHED,
                    restored.awaitTermination(5_000L));
            assertEquals(1, sink.visible.get());
        }
    }

    private static final class TerminalWindowAggregate
            implements StreamWindowAggregateFunction {
        @Override public byte[] createAccumulator() {
            return "0".getBytes(StandardCharsets.UTF_8);
        }
        @Override public byte[] add(DataRow value, byte[] current) {
            return bytes(decimal(current).add(value.getBigDecimal(2)));
        }
        @Override public byte[] merge(byte[] left, byte[] right) {
            return bytes(decimal(left).add(decimal(right)));
        }
        @Override public DataRow getResult(byte[] accumulator,
                StreamWindowContext context) {
            DataRow result = new DataRow(4);
            result.set(0, context.currentKey().get(0));
            result.set(1, context.windowStart());
            result.set(2, context.windowEnd());
            result.set(3, decimal(accumulator).setScale(10,
                    java.math.RoundingMode.HALF_UP));
            return result;
        }
        private static BigDecimal decimal(byte[] value) {
            return new BigDecimal(new String(value, StandardCharsets.UTF_8));
        }
        private static byte[] bytes(BigDecimal value) {
            return value.toPlainString().getBytes(StandardCharsets.UTF_8);
        }
    }

    @Test
    void terminalAggregateCommitsAndRestoresWithoutDuplicate(
            @TempDir Path root) throws Exception {
        DataRow row = new DataRow(2);
        row.set(0, "A");
        row.set(1, BigDecimal.ONE);
        TerminalSource source = new TerminalSource("source-v1",
                new RowSerializer(StreamProductAggregateWorkerProcessFixture
                        .dataSchema()).serialize(row), null);
        TerminalTransactionalSink sink = new TerminalTransactionalSink();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root, Collections.singletonMap(source.providerKey(), source),
                Collections.emptyMap(),
                Collections.singletonMap(sink.providerKey(), sink),
                ignored -> new StreamProductAggregateWorkerProcessFixture
                        .DecimalReadoutAggregate(), Collections.singletonList(
                StreamProductAggregateWorkerProcessFixture.function()
                        .deploymentSetId()))) {
            StreamSession definition = RowWeave.createStreamSession(
                    "terminal-aggregate", "terminal-aggregate", cluster);
            definition.setChangelogMode(StreamChangelogMode.UPSERT);
            definition.setParallelism(1);
            definition.setMaxParallelism(1);
            definition.setResourceProfile(new StreamResourceProfile(
                    8, 4_096, 8_192, 8_192L, 0, 0,
                    32_768L, 5, 32, 1_048_576L));
            definition.setDeliveryGuarantee(
                    io.rowweave.stream.api.StreamDeliveryGuarantee.EXACTLY_ONCE);
            definition.setCheckpointConfig(StreamCheckpointConfig.periodic(
                    60_000L, 5_000L, 2));
            StreamSchema schema = new StreamSchema(java.util.Arrays.asList(
                    new StreamField("adaptor", "adaptor", "adaptor",
                            DataType.StringType, false),
                    new StreamField("amount", "amount", "amount",
                            DataType.BigDecimalType, false)));
            definition.fromSource(StreamSourceSpec.boundedExactOrdered(
                            schema, source.providerKey(), SOURCE_CONTRACT,
                            new byte[] {1}, Collections.singletonList(
                            new StreamSplitSpec(sha256("aggregate-split"),
                                    "sequence-v1", new byte[] {2}))))
                    .uid("source").keyBy("adaptor").uid("key")
                    .keyedAggregate(StreamProductAggregateWorkerProcessFixture
                                    .function(), schema,
                            new StreamStateSpec("readout",
                                    StreamStateSpec.Kind.KEYED_AGGREGATING,
                                    "aggregate-decimal-state-v1", 1,
                                    StreamStateSpec.TtlMode.PROCESSING_TIME,
                                    60_000L))
                    .uid("aggregate").sinkTo("sink",
                            StreamSinkSpec.checkpointTransactional(
                                    "sink-v1", SINK_CONTRACT,
                                    new byte[] {3}));
            PlanArtifact artifact = definition.toPlanArtifact();
            StreamJobHandle original = definition.submit(
                    StreamSubmissionId.of("terminal-aggregate-original"));
            assertEquals(StreamJobStatus.RUNNING,
                    original.awaitRunning(5_000L));
            assertTrue(source.firstClosed.await(5L, TimeUnit.SECONDS));
            assertEquals(0, sink.visible.get());
            io.rowweave.StreamCheckpointHandle first =
                    original.triggerCheckpoint(StreamCommandId.of(
                            "terminal-aggregate-cut"));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    first.awaitCompletion(5_000L));
            assertEquals(1, sink.visible.get());
            assertEquals(StreamJobStatus.FINISHED,
                    original.awaitTermination(5_000L));
            StreamJobHandle restored = cluster.restore(
                    StreamSubmissionId.of("terminal-aggregate-restored"),
                    artifact, new StreamRestoreRequest(original.id(),
                            StreamRecoveryPointId.checkpoint(
                                    first.checkpointId()), 1, 1));
            assertEquals(StreamJobStatus.FINISHED,
                    restored.awaitTermination(5_000L));
            assertEquals(1, sink.visible.get());
        }
    }

    @Test
    void twoTerminalSourcesJoinAndCommitOnlyOnceAcrossRestore(
            @TempDir Path root) throws Exception {
        DataRow leftRow = new DataRow(2);
        leftRow.set(0, "A");
        leftRow.set(1, BigDecimal.ONE);
        DataRow rightRow = new DataRow(2);
        rightRow.set(0, "A");
        rightRow.set(1, new BigDecimal("2"));
        RowSerializer serializer = new RowSerializer(
                StreamProductTemporalJoinWorkerProcessFixture.dataSchema());
        TerminalSource left = new TerminalSource("left-source-v1",
                serializer.serialize(leftRow), 100L);
        TerminalSource right = new TerminalSource("right-source-v1",
                serializer.serialize(rightRow), 100L);
        TerminalTransactionalSink sink = new TerminalTransactionalSink();
        java.util.Map<String, DurableStreamSourceProvider> sources =
                new java.util.HashMap<>();
        sources.put(left.providerKey(), left);
        sources.put(right.providerKey(), right);
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root, sources, Collections.emptyMap(),
                Collections.singletonMap(sink.providerKey(), sink),
                ignored -> new StreamProductTemporalJoinWorkerProcessFixture
                        .PairSum(), Collections.singletonList(
                StreamProductTemporalJoinWorkerProcessFixture.function()
                        .deploymentSetId()))) {
            StreamSession definition = RowWeave.createStreamSession(
                    "terminal-temporal", "terminal-temporal", cluster);
            definition.setTimeMode(StreamTimeMode.EVENT_TIME);
            definition.setChangelogMode(StreamChangelogMode.UPSERT);
            definition.setParallelism(1);
            definition.setMaxParallelism(1);
            definition.setResourceProfile(new StreamResourceProfile(
                    8, 4_096, 8_192, 64 * 1_024L, 0, 0,
                    64 * 1_024L, 8, 256, 1_048_576L));
            definition.setDeliveryGuarantee(
                    io.rowweave.stream.api.StreamDeliveryGuarantee.EXACTLY_ONCE);
            definition.setCheckpointConfig(StreamCheckpointConfig.periodic(
                    60_000L, 5_000L, 2));
            StreamSchema schema = StreamProductTemporalJoinDagDispatchTest
                    .schema();
            DataStream leftStream = definition.fromSource(
                    terminalTemporalSource(schema, left.providerKey(), "left"))
                    .uid("left-source").keyBy("adaptor").uid("left-key");
            DataStream rightStream = definition.fromSource(
                    terminalTemporalSource(schema, right.providerKey(), "right"))
                    .uid("right-source").keyBy("adaptor").uid("right-key");
            leftStream.temporalJoin(rightStream,
                    new StreamTemporalJoinSpec("adaptor", "adaptor",
                            -10L, 20L, Collections.singletonList("adaptor")),
                    StreamProductTemporalJoinWorkerProcessFixture.function(),
                    schema, terminalTemporalState("left-state"),
                    terminalTemporalState("right-state"))
                    .uid("temporal-join").sinkTo("sink",
                            StreamSinkSpec.checkpointTransactional(
                                    "sink-v1", SINK_CONTRACT,
                                    new byte[] {3}));
            PlanArtifact artifact = definition.toPlanArtifact();
            StreamJobHandle original = definition.submit(
                    StreamSubmissionId.of("terminal-temporal-original"));
            assertEquals(StreamJobStatus.RUNNING,
                    original.awaitRunning(5_000L));
            assertTrue(left.firstClosed.await(5L, TimeUnit.SECONDS));
            assertTrue(right.firstClosed.await(5L, TimeUnit.SECONDS));
            assertEquals(0, sink.visible.get());
            io.rowweave.StreamCheckpointHandle first =
                    original.triggerCheckpoint(StreamCommandId.of(
                            "terminal-temporal-cut"));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    first.awaitCompletion(5_000L));
            assertEquals(1, sink.visible.get());
            assertEquals(StreamJobStatus.FINISHED,
                    original.awaitTermination(5_000L));
            StreamJobHandle restored = cluster.restore(
                    StreamSubmissionId.of("terminal-temporal-restored"),
                    artifact, new StreamRestoreRequest(original.id(),
                            StreamRecoveryPointId.checkpoint(
                                    first.checkpointId()), 1, 1));
            assertEquals(StreamJobStatus.FINISHED,
                    restored.awaitTermination(5_000L));
            assertEquals(1, sink.visible.get());
        }
    }

    private static StreamSourceSpec terminalTemporalSource(
            StreamSchema schema, String provider, String side) {
        return StreamSourceSpec.boundedExactOrderedEventTime(
                schema, provider, SOURCE_CONTRACT, new byte[] {1},
                Collections.singletonList(new StreamSplitSpec(
                        sha256(side + "-split"), "sequence-v1",
                        new byte[] {2})),
                StreamWatermarkPolicy.providerEvents(1_000L));
    }

    private static StreamStateSpec terminalTemporalState(String name) {
        return new StreamStateSpec(name, StreamStateSpec.Kind.KEYED_MAP,
                "temporal-record-v1", 1,
                StreamStateSpec.TtlMode.EVENT_TIME, 30L);
    }

    @Test
    void fixedTerminalSourceCommitsTransactionalSinkAndRestoresOnce(
            @TempDir Path root) throws Exception {
        verifyTerminalTransactionalCheckpoint(root, false);
    }

    @Test
    void fixedTerminalSourceCommitsUnalignedTransactionalCutAndRestoresOnce(
            @TempDir Path root) throws Exception {
        verifyTerminalTransactionalCheckpoint(root, true);
    }

    private static void verifyTerminalTransactionalCheckpoint(
            Path root, boolean unaligned) throws Exception {
        TerminalSource source = new TerminalSource();
        TerminalTransactionalSink sink = new TerminalTransactionalSink();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root, Collections.singletonMap(source.providerKey(), source),
                Collections.emptyMap(),
                Collections.singletonMap(sink.providerKey(), sink),
                reference -> { throw new AssertionError(reference); },
                Collections.emptyList())) {
            StreamSession definition = RowWeave.createStreamSession(
                    "terminal-transaction", "terminal-transaction", cluster);
            definition.setParallelism(1);
            definition.setMaxParallelism(1);
            definition.setResourceProfile(new StreamResourceProfile(
                    8, 1_024, 4_096, 0L, 0, 0,
                    65_536L, 4, 16, 0L));
            definition.setDeliveryGuarantee(
                    io.rowweave.stream.api.StreamDeliveryGuarantee.EXACTLY_ONCE);
            definition.setCheckpointConfig(unaligned
                    ? StreamCheckpointConfig.periodicUnaligned(
                            60_000L, 5_000L, 2)
                    : StreamCheckpointConfig.periodic(
                            60_000L, 5_000L, 2));
            definition.fromSource(boundedSourceSpec()).uid("source")
                    .sinkTo("sink", StreamSinkSpec.checkpointTransactional(
                            "sink-v1", SINK_CONTRACT, new byte[] {3}));
            PlanArtifact artifact = definition.toPlanArtifact();
            StreamJobHandle original = definition.submit(
                    StreamSubmissionId.of("terminal-transaction-original"));
            assertEquals(StreamJobStatus.RUNNING,
                    original.awaitRunning(5_000L));
            assertTrue(source.firstClosed.await(5L, TimeUnit.SECONDS));
            assertEquals(0, sink.visible.get());
            io.rowweave.StreamCheckpointHandle checkpoint =
                    original.triggerCheckpoint(StreamCommandId.of(
                            "terminal-transaction-cut"));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    checkpoint.awaitCompletion(5_000L));
            assertEquals(1, sink.visible.get());
            assertEquals(StreamJobStatus.FINISHED,
                    original.awaitTermination(5_000L));
            assertThrows(StreamControlException.class, () ->
                    original.triggerCheckpoint(StreamCommandId.of(
                            "terminal-transaction-after-finish")));

            StreamJobHandle restored = cluster.restore(
                    StreamSubmissionId.of("terminal-transaction-restored"),
                    artifact, new StreamRestoreRequest(original.id(),
                            StreamRecoveryPointId.checkpoint(
                                    checkpoint.checkpointId()), 1, 1));
            assertEquals(StreamJobStatus.FINISHED,
                    restored.awaitTermination(5_000L));
            assertEquals(1, sink.visible.get(),
                    "restoring the completed source must not publish its row twice");
        }
    }

    @Test
    void sourceSinkAndCheckpointRunThroughNoListenerProductRoles(
            @TempDir Path root) throws Exception {
        CountingSource source = new CountingSource();
        CountingSink sink = new CountingSink();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root,
                Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink))) {
            assertEquals(0, cluster.controlListenerCountForDiagnostics());
            StreamJobHandle job = session(cluster).submit(
                    StreamSubmissionId.of("in-process-data-submit"));
            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(5_000L));
            assertTrue(sink.received.await(5L, TimeUnit.SECONDS));
            assertEquals(3, sink.records.get());

            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.triggerCheckpoint(StreamCommandId.of(
                            "in-process-data-checkpoint"))
                            .awaitCompletion(5_000L));
            assertEquals(1L, job.progress().completedCheckpoints());

            job.cancel(StreamCommandId.of("in-process-data-cancel"))
                    .awaitCompletion(5_000L);
            assertEquals(StreamJobStatus.CANCELED,
                    job.awaitTermination(5_000L));
        }
    }

    @Test
    void independentSourceSinkSubgraphsComposeAndCheckpoint(
            @TempDir Path root) throws Exception {
        CountingSource source = new CountingSource();
        CountingSink sink = new CountingSink();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root, Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink))) {
            StreamSession definition = session(cluster);
            definition.fromSource(sourceSpec()).uid("second-source")
                    .sinkTo("second-sink", sinkSpec());
            StreamJobHandle job = definition.submit(
                    StreamSubmissionId.of("independent-subgraphs"));
            assertEquals(StreamJobStatus.RUNNING, job.awaitRunning(5_000L));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
            while (sink.records.get() < 6 && System.nanoTime() < deadline) {
                Thread.sleep(5L);
            }
            assertEquals(6, sink.records.get());
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.triggerCheckpoint(StreamCommandId.of("composed-cut"))
                            .awaitCompletion(5_000L));
            assertEquals(1L, job.progress().completedCheckpoints());
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.cancel(StreamCommandId.of("composed-cancel"))
                            .awaitCompletion(5_000L));
            assertEquals(StreamJobStatus.CANCELED,
                    job.awaitTermination(5_000L));
        }
    }

    @Test
    void fixedSplitsExpandSourceTasksWithoutChangingDownstreamParallelism(
            @TempDir Path root) throws Exception {
        MultiSplitSource source = new MultiSplitSource();
        CountingSink sink = new CountingSink();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root,
                Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink))) {
            StreamSession definition = multiSplitSession(cluster);
            StreamProductExecutionGraph graph =
                    StreamProductExecutionGraph.from(
                            definition.toPlanArtifact());
            assertEquals(3, graph.executionGraph.node("source")
                    .parallelism());
            assertEquals(2, graph.executionGraph.node("sink")
                    .parallelism());

            StreamJobHandle job = definition.submit(
                    StreamSubmissionId.of("multi-split-submit"));
            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(5_000L));
            assertTrue(sink.received.await(5L, TimeUnit.SECONDS));
            assertEquals(3, sink.records.get());
            assertEquals(3, source.opens.get());
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.triggerCheckpoint(StreamCommandId.of(
                            "multi-split-checkpoint"))
                            .awaitCompletion(5_000L));
            job.cancel(StreamCommandId.of("multi-split-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    @Test
    void liveScaleDownKeepsOneSourceTaskPerFixedSplit(
            @TempDir Path root) throws Exception {
        MultiSplitSource source = new MultiSplitSource();
        CountingSink sink = new CountingSink();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root,
                Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink))) {
            StreamJobHandle job = multiSplitSession(cluster).submit(
                    StreamSubmissionId.of("multi-split-live-scale-down"));
            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(5_000L));
            assertTrue(sink.received.await(5L, TimeUnit.SECONDS));

            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.rescale(StreamCommandId.of(
                            "multi-split-scale-to-one"), 1)
                            .awaitCompletion(10_000L));
            assertEquals(StreamJobStatus.RUNNING, job.status());
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.triggerCheckpoint(StreamCommandId.of(
                            "multi-split-post-scale-checkpoint"))
                            .awaitCompletion(10_000L));
            assertEquals(6, source.opens.get());
            job.cancel(StreamCommandId.of(
                    "multi-split-live-scale-down-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    @Test
    void stopWithSavepointPublishesTheJointCutBeforeStopping(
            @TempDir Path root) throws Exception {
        CountingSource source = new CountingSource();
        CountingSink sink = new CountingSink();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root,
                Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink))) {
            StreamJobHandle job = session(cluster).submit(
                    StreamSubmissionId.of("in-process-savepoint-submit"));
            assertTrue(sink.received.await(5L, TimeUnit.SECONDS));

            StreamSavepointHandle savepoint = job.stopWithSavepoint(
                    StreamCommandId.of("in-process-savepoint"),
                    new StreamSavepointRequest(
                            "file-savepoint-v1", new byte[0]));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    savepoint.awaitCompletion(5_000L));
            assertEquals(StreamJobStatus.STOPPED_WITH_SAVEPOINT,
                    job.awaitTermination(5_000L));
            try (java.util.stream.Stream<Path> files = Files.list(
                    root.resolve("checkpoints").resolve("savepoints"))) {
                assertEquals(1L, files.filter(Files::isRegularFile).count());
            }
        }
    }

    @Test
    void restoreReopensTheSourceAtTheExactSavepointPosition(
            @TempDir Path root) throws Exception {
        CountingSource source = new CountingSource();
        CountingSink sink = new CountingSink();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root,
                Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink))) {
            StreamSession definition = session(cluster);
            PlanArtifact artifact = definition.toPlanArtifact();
            StreamJobHandle original = definition.submit(
                    StreamSubmissionId.of("restore-source-submit"));
            assertTrue(sink.received.await(5L, TimeUnit.SECONDS));
            StreamSavepointHandle savepoint = original.stopWithSavepoint(
                    StreamCommandId.of("restore-source-savepoint"),
                    new StreamSavepointRequest(
                            "file-savepoint-v1", new byte[0]));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    savepoint.awaitCompletion(5_000L));

            StreamJobHandle restored = cluster.restore(
                    StreamSubmissionId.of("restored-target-submit"),
                    artifact,
                    new StreamRestoreRequest(original.id(),
                            StreamRecoveryPointId.savepoint(
                                    savepoint.savepointId()), 1, 1));
            assertEquals(StreamJobStatus.RUNNING,
                    restored.awaitRunning(5_000L));
            long deadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(5L);
            while (sink.records.get() < 6 && System.nanoTime() < deadline) {
                Thread.sleep(5L);
            }
            assertEquals(6, sink.records.get());
            assertEquals(1, source.restoredOpens.get());
            restored.cancel(StreamCommandId.of("restored-target-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    @Test
    void restoreAlsoAcceptsAnExactCompletedCheckpoint(
            @TempDir Path root) throws Exception {
        CountingSource source = new CountingSource();
        CountingSink sink = new CountingSink();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root,
                Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink))) {
            StreamJobClientRecoveryContract.verifyExactCheckpointRestore(
                    cluster, session(cluster), "in-process-exact", 1, 1,
                    () -> {
                        assertTrue(sink.received.await(5L, TimeUnit.SECONDS));
                        assertEquals(3, sink.records.get());
                    },
                    () -> { },
                    () -> {
                        awaitRecords(sink.records, 6);
                        assertEquals(1, source.restoredOpens.get());
                    });
        }
    }

    @Test
    void unalignedCheckpointRestoresPhysicalInputStateBeforeLiveData(
            @TempDir Path root) throws Exception {
        CountingSource source = new CountingSource();
        CountingSink sink = new CountingSink();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root,
                Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink))) {
            StreamSession definition = session(cluster);
            definition.setCheckpointConfig(
                    StreamCheckpointConfig.manualOnlyUnaligned(5_000L, 2));
            PlanArtifact artifact = definition.toPlanArtifact();
            StreamJobHandle original = definition.submit(
                    StreamSubmissionId.of("unaligned-restore-submit"));
            assertTrue(sink.received.await(5L, TimeUnit.SECONDS));
            io.rowweave.StreamCheckpointHandle checkpoint =
                    original.triggerCheckpoint(StreamCommandId.of(
                            "unaligned-restore-cut"));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    checkpoint.awaitCompletion(5_000L));
            assertEquals(1L, new FileStreamSavepointStore(
                    root.resolve("checkpoints"))
                    .get(checkpoint.checkpointId()).checkpointManifest()
                    .taskReceipts().stream()
                    .flatMap(receipt -> receipt.evidence().stream())
                    .filter(evidence -> evidence.kind()
                            == CheckpointEvidence.Kind.CHANNEL_STATE)
                    .count());
            original.cancel(StreamCommandId.of(
                            "unaligned-restore-original-cancel"))
                    .awaitCompletion(5_000L);

            StreamJobHandle restored = cluster.restore(
                    StreamSubmissionId.of("unaligned-restored-target"),
                    artifact, new StreamRestoreRequest(original.id(),
                            StreamRecoveryPointId.checkpoint(
                                    checkpoint.checkpointId()), 1, 1));
            assertEquals(StreamJobStatus.RUNNING,
                    restored.awaitRunning(5_000L));
            awaitRecords(sink.records, 6);
            assertEquals(1, source.restoredOpens.get());
            restored.cancel(StreamCommandId.of(
                            "unaligned-restored-target-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    private static void awaitRecords(AtomicInteger records, int expected)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        while (records.get() < expected && System.nanoTime() < deadline) {
            Thread.sleep(5L);
        }
        assertEquals(expected, records.get());
    }

    private static StreamSession session(InProcessStreamCluster client) {
        StreamSession session = RowWeave.createStreamSession(
                "in-process-data", "in-process-data", client);
        session.setParallelism(1);
        session.setMaxParallelism(1);
        session.setResourceProfile(new StreamResourceProfile(
                8, 1_024, 4_096, 0L, 0, 0,
                65_536L, 4, 16, 0L));
        session.fromSource(sourceSpec()).uid("source")
                .sinkTo("sink", sinkSpec());
        return session;
    }

    private static StreamSession multiSplitSession(
            InProcessStreamCluster client) {
        StreamSession session = RowWeave.createStreamSession(
                "multi-split", "multi-split", client);
        session.setParallelism(2);
        session.setMaxParallelism(3);
        session.setResourceProfile(new StreamResourceProfile(
                8, 1_024, 4_096, 0L, 0, 0,
                65_536L, 8, 32, 0L));
        session.fromSource(sourceSpec(3)).uid("source")
                .sinkTo("sink", sinkSpec());
        return session;
    }

    private static StreamSourceSpec sourceSpec() {
        return sourceSpec(1);
    }

    private static StreamSourceSpec boundedSourceSpec() {
        StreamSchema schema = new StreamSchema(Collections.singletonList(
                new StreamField("id", "id", "id",
                        DataType.StringType, false)));
        return StreamSourceSpec.boundedExactOrdered(schema, "source-v1",
                SOURCE_CONTRACT, new byte[] {1},
                Collections.singletonList(new StreamSplitSpec(
                        sha256("split-0"), "sequence-v1", new byte[] {0})));
    }

    private static StreamSourceSpec sourceSpec(int splitCount) {
        StreamSchema schema = new StreamSchema(Collections.singletonList(
                new StreamField("id", "id", "id",
                        DataType.StringType, false)));
        List<StreamSplitSpec> splits = new ArrayList<>();
        for (int index = 0; index < splitCount; index++) {
            splits.add(new StreamSplitSpec(
                    sha256("split-" + index), "sequence-v1",
                    new byte[] {(byte) index}));
        }
        return StreamSourceSpec.exactOrdered(schema, "source-v1",
                SOURCE_CONTRACT, new byte[] {1}, splits);
    }

    private static StreamSinkSpec sinkSpec() {
        return StreamSinkSpec.eagerAtLeastOnce(
                "sink-v1", SINK_CONTRACT, new byte[] {3});
    }

    private static final class TerminalSource
            implements DurableStreamSourceProvider {
        private final CountDownLatch firstClosed = new CountDownLatch(1);
        private final String key;
        private final byte[] recordFrame;
        private final Long eventTimestamp;

        private TerminalSource() {
            this("source-v1", new byte[] {7}, null);
        }

        private TerminalSource(String key, byte[] recordFrame,
                Long eventTimestamp) {
            this.key = key;
            this.recordFrame = recordFrame.clone();
            this.eventTimestamp = eventTimestamp;
        }

        @Override public String providerKey() { return key; }
        @Override public String contractDigest() { return SOURCE_CONTRACT; }
        @Override public StreamSourceReader open(
                StreamSourceOpenRequest request) {
            final boolean restored = request.hasRestoredPosition();
            final int initial = restored ? ByteBuffer.wrap(
                    request.restoredPositionFrame()).getInt() : 0;
            return new StreamSourceReader() {
                private int position = initial;
                @Override public StreamSourceRecord next() { return null; }
                @Override public StreamSourceEvent nextEvent() {
                    if (position == 0) {
                        position = 1;
                        return StreamSourceEvent.record(eventTimestamp == null
                                ? StreamSourceRecord.of(recordFrame,
                                        position(1))
                                : StreamSourceRecord.of(recordFrame,
                                        position(1), eventTimestamp));
                    }
                    return StreamSourceEvent.endOfInput();
                }
                @Override public byte[] checkpointPositionFrame() {
                    return position(position);
                }
                @Override public void cancel() { }
                @Override public void close() {
                    if (!restored) firstClosed.countDown();
                }
            };
        }
    }

    private static final class TerminalTransactionalSink
            implements TransactionalStreamSinkProvider {
        private final ConcurrentHashMap<String, Integer> prepared =
                new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, TransactionStatus> status =
                new ConcurrentHashMap<>();
        private final AtomicInteger visible = new AtomicInteger();

        @Override public String providerKey() { return "sink-v1"; }
        @Override public String contractDigest() { return SINK_CONTRACT; }
        @Override public TransactionalStreamSinkWriter open(
                StreamSinkTransactionOpenRequest request) {
            return new TransactionalStreamSinkWriter() {
                private int writes;
                @Override public void write(byte[] frame) { writes++; }
                @Override public byte[] prepare(String identity) {
                    prepared.putIfAbsent(identity, writes);
                    status.putIfAbsent(identity, TransactionStatus.PREPARED);
                    return identity.getBytes(StandardCharsets.UTF_8);
                }
                @Override public TransactionStatus query(String identity) {
                    return status.getOrDefault(identity,
                            TransactionStatus.UNKNOWN);
                }
                @Override public void commit(String identity,
                        CheckpointDecision decision) {
                    if (status.replace(identity, TransactionStatus.PREPARED,
                            TransactionStatus.VISIBLE)) {
                        visible.addAndGet(prepared.get(identity));
                    }
                }
                @Override public void finalizeTransaction(String identity) { }
                @Override public void abort(String identity) {
                    status.put(identity, TransactionStatus.ABORTED);
                }
                @Override public void close() { }
            };
        }
    }

    private static final class CountingSource
            implements DurableStreamSourceProvider {
        private final AtomicInteger restoredOpens = new AtomicInteger();
        @Override public String providerKey() { return "source-v1"; }
        @Override public String contractDigest() { return SOURCE_CONTRACT; }
        @Override public StreamSourceReader open(StreamSourceOpenRequest request) {
            final int startingPosition;
            final int limit;
            if (request.hasRestoredPosition()) {
                restoredOpens.incrementAndGet();
                startingPosition = ByteBuffer.wrap(
                        request.restoredPositionFrame()).getInt();
                limit = 6;
            } else {
                startingPosition = 0;
                limit = 3;
            }
            return new StreamSourceReader() {
                private final Semaphore wakeups = new Semaphore(0);
                private int next = startingPosition;
                @Override public StreamSourceRecord next() throws Exception {
                    if (next < limit) {
                        int value = next++;
                        return StreamSourceRecord.of(
                                new byte[] {(byte) value}, position(next));
                    }
                    wakeups.acquire();
                    return null;
                }
                @Override public byte[] checkpointPositionFrame() {
                    return position(next);
                }
                @Override public void wakeUp() { wakeups.release(); }
                @Override public void cancel() { wakeups.release(); }
                @Override public void close() { wakeups.release(); }
            };
        }
    }

    private static final class MultiSplitSource
            implements DurableStreamSourceProvider {
        private final AtomicInteger opens = new AtomicInteger();

        @Override public String providerKey() { return "source-v1"; }
        @Override public String contractDigest() { return SOURCE_CONTRACT; }

        @Override public StreamSourceReader open(
                StreamSourceOpenRequest request) {
            opens.incrementAndGet();
            return new StreamSourceReader() {
                private final Semaphore wakeups = new Semaphore(0);
                private boolean emitted;

                @Override public StreamSourceRecord next() throws Exception {
                    if (!emitted) {
                        emitted = true;
                        return StreamSourceRecord.of(
                                request.split().descriptorFrame(),
                                position(1));
                    }
                    wakeups.acquire();
                    return null;
                }

                @Override public byte[] checkpointPositionFrame() {
                    return position(emitted ? 1 : 0);
                }
                @Override public void wakeUp() { wakeups.release(); }
                @Override public void cancel() { wakeups.release(); }
                @Override public void close() { wakeups.release(); }
            };
        }
    }

    private static final class CountingSink
            implements EagerStreamSinkProvider {
        private final AtomicInteger records = new AtomicInteger();
        private final CountDownLatch received = new CountDownLatch(1);
        @Override public String providerKey() { return "sink-v1"; }
        @Override public String contractDigest() { return SINK_CONTRACT; }
        @Override public EagerStreamSinkWriter open(StreamSinkOpenRequest request) {
            return new EagerStreamSinkWriter() {
                @Override public void write(byte[] recordFrame) {
                    if (records.incrementAndGet() == 3) received.countDown();
                }
                @Override public void cancel() { }
                @Override public void close() { }
            };
        }
    }

    private static byte[] position(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    private static String sha256(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                    value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(digest.length * 2);
            for (byte current : digest) {
                result.append(String.format("%02x", current & 0xff));
            }
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
