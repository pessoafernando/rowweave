package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.StreamCollector;
import io.rowweave.StreamJobClientRecoveryContract;
import io.rowweave.StreamKeyedProcessFunction;
import io.rowweave.StreamMapState;
import io.rowweave.StreamProcessContext;
import io.rowweave.StreamSession;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamValueState;
import io.rowweave.StreamWindowAggregateFunction;
import io.rowweave.StreamWindowContext;
import io.rowweave.mem.RowSerializer;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamCheckpointConfig;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.api.StreamEmissionLimit;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamGenericWindowSpec;
import io.rowweave.stream.api.StreamManagedStateSpec;
import io.rowweave.stream.api.StreamProcessSpec;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamStateSpec;
import io.rowweave.stream.api.StreamAccumulationMode;
import io.rowweave.stream.api.StreamWindowAssignerSpec;
import io.rowweave.stream.api.StreamWindowResourceSpec;
import io.rowweave.stream.api.StreamWindowTriggerSpec;
import io.rowweave.stream.contract.CheckpointEvidence;
import io.rowweave.stream.contract.StreamUpsertRecord;
import io.rowweave.stream.contract.StreamUpsertRecordCodec;
import io.rowweave.stream.runtime.DurableStreamSourceProvider;
import io.rowweave.stream.runtime.EagerStreamSinkProvider;
import io.rowweave.stream.runtime.EagerStreamSinkWriter;
import io.rowweave.stream.runtime.FileStreamSavepointStore;
import io.rowweave.stream.runtime.ManagedKeyedValueState;
import io.rowweave.stream.runtime.StreamSinkOpenRequest;
import io.rowweave.stream.runtime.StreamSourceOpenRequest;
import io.rowweave.stream.runtime.StreamSourceReader;
import io.rowweave.stream.runtime.StreamSourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** offline rescale restores a verified key-group cut. */
class InProcessStreamOfflineRescaleTest {
    private static final String SOURCE_CONTRACT = sha256("rescale-source");
    private static final String SINK_CONTRACT = sha256("rescale-sink");
    private static final String PROCESS_DEPLOYMENT_SET =
            sha256("rescale-process-deployment-set");
    private static final String WINDOW_DEPLOYMENT_SET =
            sha256("rescale-window-deployment-set");

    @Test
    void unalignedCutCombinesKeyedStateWithEveryPhysicalInput(
            @TempDir Path root) throws Exception {
        String firstKey = keyForGroup(0);
        String secondKey = keyForGroup(1);
        RescaleSource source = new RescaleSource(firstKey, secondKey);
        LatestAggregateSink sink = new LatestAggregateSink();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root,
                Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink),
                reference -> {
                    if (StreamProductAggregateWorkerProcessFixture.function()
                            .equals(reference)) {
                        return new StreamProductAggregateWorkerProcessFixture
                                .DecimalReadoutAggregate();
                    }
                    throw new IllegalArgumentException(
                            "unknown aggregate deployment");
                }, Collections.singletonList(
                        StreamProductAggregateWorkerProcessFixture
                                .DEPLOYMENT_SET))) {
            StreamSession definition = session(cluster, 1);
            definition.setCheckpointConfig(
                    StreamCheckpointConfig.manualOnlyUnaligned(
                            5_000L, 2));
            StreamJobHandle job = definition.submit(
                    StreamSubmissionId.of("unaligned-keyed-submit"));
            source.awaitInitialRecords();
            io.rowweave.StreamCheckpointHandle checkpoint =
                    job.triggerCheckpoint(StreamCommandId.of(
                            "unaligned-keyed-cut"));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    checkpoint.awaitCompletion(5_000L));
            io.rowweave.stream.contract.CheckpointManifest manifest =
                    new FileStreamSavepointStore(
                            root.resolve("checkpoints"))
                            .get(checkpoint.checkpointId())
                            .checkpointManifest();
            assertEquals(1L, manifest.taskReceipts().stream()
                    .flatMap(receipt -> receipt.evidence().stream())
                    .filter(evidence -> evidence.kind()
                            == CheckpointEvidence.Kind.STATE)
                    .count());
            assertEquals(3L, manifest.taskReceipts().stream()
                    .flatMap(receipt -> receipt.evidence().stream())
                    .filter(evidence -> evidence.kind()
                            == CheckpointEvidence.Kind.CHANNEL_STATE)
                    .count());
            job.cancel(StreamCommandId.of("unaligned-keyed-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    @Test
    void liveRescaleRebindsEveryUnalignedPhysicalLane(
            @TempDir Path root) throws Exception {
        String firstKey = keyForGroup(0);
        String secondKey = keyForGroup(1);
        RescaleSource source = new RescaleSource(firstKey, secondKey);
        LatestAggregateSink sink = new LatestAggregateSink();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root,
                Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink),
                reference -> new StreamProductAggregateWorkerProcessFixture
                        .DecimalReadoutAggregate(),
                Collections.singletonList(
                        StreamProductAggregateWorkerProcessFixture
                                .DEPLOYMENT_SET))) {
            StreamSession definition = session(cluster, 1);
            definition.setCheckpointConfig(
                    StreamCheckpointConfig.manualOnlyUnaligned(
                            5_000L, 2));
            StreamJobHandle job = definition.submit(
                    StreamSubmissionId.of("unaligned-live-rescale"));
            source.awaitInitialRecords();

            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.rescale(StreamCommandId.of(
                            "unaligned-live-scale-to-two"), 2)
                            .awaitCompletion(10_000L));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.triggerCheckpoint(StreamCommandId.of(
                            "unaligned-live-post-cut"))
                            .awaitCompletion(10_000L));
            sink.awaitResults(firstKey, secondKey);
            assertEquals(new BigDecimal("4.0000000000"),
                    sink.latest.get(firstKey));
            assertEquals(new BigDecimal("6.0000000000"),
                    sink.latest.get(secondKey));
            job.cancel(StreamCommandId.of(
                    "unaligned-live-rescale-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    @Test
    void stopWithSavepointRescalesKeyGroupsAndContinuesExactSourcePosition(
            @TempDir Path root) throws Exception {
        String firstKey = keyForGroup(0);
        String secondKey = keyForGroup(1);
        RescaleSource source = new RescaleSource(firstKey, secondKey);
        LatestAggregateSink sink = new LatestAggregateSink();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root,
                Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink),
                reference -> {
                    if (StreamProductAggregateWorkerProcessFixture.function()
                            .equals(reference)) {
                        return new StreamProductAggregateWorkerProcessFixture
                                .DecimalReadoutAggregate();
                    }
                    throw new IllegalArgumentException(
                            "unknown aggregate deployment");
                }, Collections.singletonList(
                        StreamProductAggregateWorkerProcessFixture
                                .DEPLOYMENT_SET))) {
            StreamSession originalDefinition = session(cluster, 1);
            StreamSession targetDefinition = session(cluster, 2);
            StreamProductExecutionGraph.from(
                    originalDefinition.toPlanArtifact());
            String checkpointId = StreamJobClientRecoveryContract
                    .verifySavepointOfflineRescale(
                            cluster, originalDefinition, targetDefinition,
                            "in-process-rescale", 2, 2,
                            source::awaitInitialRecords,
                            () -> { },
                            () -> {
                                sink.awaitResults(firstKey, secondKey);
                                assertEquals(new BigDecimal("4.0000000000"),
                                        sink.latest.get(firstKey));
                                assertEquals(new BigDecimal("6.0000000000"),
                                        sink.latest.get(secondKey));
                                assertEquals(1,
                                        source.restoredOpens.get());
                            });
            assertEquals(2L, new FileStreamSavepointStore(
                    root.resolve("checkpoints")).get(checkpointId)
                    .checkpointManifest().taskReceipts().stream()
                    .flatMap(receipt -> receipt.evidence().stream())
                    .filter(member -> member.kind()
                            == CheckpointEvidence.Kind.STATE)
                    .count());
        }
    }

    @Test
    void liveRescaleRepartitionsKeyGroupsAndContinuesSourcePosition(
            @TempDir Path root) throws Exception {
        String firstKey = keyForGroup(0);
        String secondKey = keyForGroup(1);
        RescaleSource source = new RescaleSource(firstKey, secondKey);
        LatestAggregateSink sink = new LatestAggregateSink();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root,
                Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink),
                reference -> {
                    if (StreamProductAggregateWorkerProcessFixture.function()
                            .equals(reference)) {
                        return new StreamProductAggregateWorkerProcessFixture
                                .DecimalReadoutAggregate();
                    }
                    throw new IllegalArgumentException(
                            "unknown aggregate deployment");
                }, Collections.singletonList(
                StreamProductAggregateWorkerProcessFixture.DEPLOYMENT_SET))) {
            StreamJobHandle job = session(cluster, 1).submit(
                    StreamSubmissionId.of("live-keyed-rescale"));
            source.awaitInitialRecords();

            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.rescale(StreamCommandId.of(
                            "live-keyed-scale-to-two"), 2)
                            .awaitCompletion(5_000L));
            sink.awaitResults(firstKey, secondKey);
            assertEquals(new BigDecimal("4.0000000000"),
                    sink.latest.get(firstKey));
            assertEquals(new BigDecimal("6.0000000000"),
                    sink.latest.get(secondKey));
            assertEquals(1, source.restoredOpens.get());
            job.cancel(StreamCommandId.of("live-keyed-rescale-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    @Test
    void liveScaleDownMergesKeyGroupsIntoOneTargetTask(
            @TempDir Path root) throws Exception {
        String firstKey = keyForGroup(0);
        String secondKey = keyForGroup(1);
        RescaleSource source = new RescaleSource(firstKey, secondKey);
        LatestAggregateSink sink = new LatestAggregateSink();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root,
                Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink),
                reference -> new StreamProductAggregateWorkerProcessFixture
                        .DecimalReadoutAggregate(),
                Collections.singletonList(
                        StreamProductAggregateWorkerProcessFixture
                                .DEPLOYMENT_SET))) {
            StreamJobHandle job = session(cluster, 2).submit(
                    StreamSubmissionId.of("live-keyed-scale-down"));
            source.awaitInitialRecords();

            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.rescale(StreamCommandId.of(
                            "live-keyed-scale-to-one"), 1)
                            .awaitCompletion(5_000L));
            sink.awaitResults(firstKey, secondKey);
            assertEquals(new BigDecimal("4.0000000000"),
                    sink.latest.get(firstKey));
            assertEquals(new BigDecimal("6.0000000000"),
                    sink.latest.get(secondKey));
            job.cancel(StreamCommandId.of("live-scale-down-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    @Test
    void keyedProcessRescalesAllDeclaredStateAndContinuesSourcePosition(
            @TempDir Path root) throws Exception {
        String firstKey = keyForGroup(0);
        String secondKey = keyForGroup(1);
        RescaleSource source = new RescaleSource(firstKey, secondKey);
        LatestAggregateSink sink = new LatestAggregateSink();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root,
                Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink),
                reference -> {
                    if (processFunction().equals(reference)) {
                        return new RescaleProcessFunction();
                    }
                    throw new IllegalArgumentException(
                            "unknown process deployment");
                }, Collections.singletonList(PROCESS_DEPLOYMENT_SET))) {
            StreamSession originalDefinition = processSession(cluster, 1);
            StreamSession targetDefinition = processSession(cluster, 2);
            StreamProductExecutionGraph.from(
                    originalDefinition.toPlanArtifact());
            String checkpointId = StreamJobClientRecoveryContract
                    .verifySavepointOfflineRescale(
                            cluster, originalDefinition, targetDefinition,
                            "process-rescale", 2, 2,
                            source::awaitInitialRecords, () -> { },
                            () -> {
                                sink.awaitResults(firstKey, secondKey);
                                assertEquals(new BigDecimal("4.0000000000"),
                                        sink.latest.get(firstKey));
                                assertEquals(new BigDecimal("6.0000000000"),
                                        sink.latest.get(secondKey));
                                assertEquals(1, source.restoredOpens.get());
                            });
            assertEquals(4L, new FileStreamSavepointStore(
                    root.resolve("checkpoints")).get(checkpointId)
                    .checkpointManifest().taskReceipts().stream()
                    .flatMap(receipt -> receipt.evidence().stream())
                    .filter(member -> member.kind()
                            == CheckpointEvidence.Kind.STATE)
                    .count());
        }
    }

    @Test
    void liveRescaleMigratesEveryDeclaredKeyedProcessState(
            @TempDir Path root) throws Exception {
        String firstKey = keyForGroup(0);
        String secondKey = keyForGroup(1);
        RescaleSource source = new RescaleSource(firstKey, secondKey);
        LatestAggregateSink sink = new LatestAggregateSink();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root,
                Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink),
                reference -> {
                    if (processFunction().equals(reference)) {
                        return new RescaleProcessFunction();
                    }
                    throw new IllegalArgumentException(
                            "unknown process deployment");
                }, Collections.singletonList(PROCESS_DEPLOYMENT_SET))) {
            StreamJobHandle job = processSession(cluster, 1).submit(
                    StreamSubmissionId.of("live-process-rescale"));
            source.awaitInitialRecords();

            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.rescale(StreamCommandId.of(
                            "live-process-scale-to-two"), 2)
                            .awaitCompletion(5_000L));
            sink.awaitResults(firstKey, secondKey);
            assertEquals(new BigDecimal("4.0000000000"),
                    sink.latest.get(firstKey));
            assertEquals(new BigDecimal("6.0000000000"),
                    sink.latest.get(secondKey));
            assertEquals(1, source.restoredOpens.get());
            job.cancel(StreamCommandId.of("live-process-rescale-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    @Test
    void offlineRescaleComposesTwoKeyedStateOperators(
            @TempDir Path root) throws Exception {
        String firstKey = keyForGroup(0);
        String secondKey = keyForGroup(1);
        RescaleSource source = new RescaleSource(firstKey, secondKey);
        LatestAggregateSink sink = new LatestAggregateSink();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root,
                Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink),
                reference -> {
                    if (processFunction().equals(reference)) {
                        return new RescaleProcessFunction();
                    }
                    throw new IllegalArgumentException(
                            "unknown process deployment");
                }, Collections.singletonList(PROCESS_DEPLOYMENT_SET))) {
            StreamSession originalDefinition = chainedProcessSession(
                    cluster, 1);
            StreamSession targetDefinition = chainedProcessSession(
                    cluster, 2);
            String checkpointId = StreamJobClientRecoveryContract
                    .verifySavepointOfflineRescale(
                            cluster, originalDefinition, targetDefinition,
                            "chained-process-rescale", 2, 2,
                            source::awaitInitialRecords, () -> { },
                            () -> {
                                sink.awaitResults(firstKey,
                                        new BigDecimal("5.0000000000"),
                                        secondKey,
                                        new BigDecimal("8.0000000000"));
                                assertEquals(1,
                                        source.restoredOpens.get());
                            });
            assertEquals(8L, new FileStreamSavepointStore(
                    root.resolve("checkpoints")).get(checkpointId)
                    .checkpointManifest().taskReceipts().stream()
                    .flatMap(receipt -> receipt.evidence().stream())
                    .filter(member -> member.kind()
                            == CheckpointEvidence.Kind.STATE)
                    .count());
        }
    }

    @Test
    void offlineRescaleComposesTwoStatefulSourceBranches(
            @TempDir Path root) throws Exception {
        String firstKey = keyForGroup(0);
        String secondKey = keyForGroup(1);
        RescaleSource source = new RescaleSource(firstKey, secondKey);
        LatestAggregateSink sink = new LatestAggregateSink();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root,
                Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink),
                reference -> {
                    if (processFunction().equals(reference)) {
                        return new RescaleProcessFunction();
                    }
                    throw new IllegalArgumentException(
                            "unknown process deployment");
                }, Collections.singletonList(PROCESS_DEPLOYMENT_SET))) {
            StreamSession originalDefinition = twoSourceProcessSession(
                    cluster, 1);
            StreamSession targetDefinition = twoSourceProcessSession(
                    cluster, 2);
            String checkpointId = StreamJobClientRecoveryContract
                    .verifySavepointOfflineRescale(
                            cluster, originalDefinition, targetDefinition,
                            "two-source-process-rescale", 2, 2,
                            () -> source.awaitInitialRecords(2), () -> { },
                            () -> {
                                sink.awaitResults(firstKey, secondKey);
                                assertEquals(2,
                                        source.restoredOpens.get());
                            });
            assertEquals(10L, new FileStreamSavepointStore(
                    root.resolve("checkpoints")).get(checkpointId)
                    .checkpointManifest().taskReceipts().stream()
                    .flatMap(receipt -> receipt.evidence().stream())
                    .filter(member -> member.kind()
                            == CheckpointEvidence.Kind.STATE)
                    .count());
        }
    }

    @Test
    void offlineRescaleMigratesEveryFixedSplitWithoutAOneSplitShapeGate(
            @TempDir Path root) throws Exception {
        String firstKey = keyForGroup(0);
        String secondKey = keyForGroup(1);
        RescaleSource source = new RescaleSource(firstKey, secondKey);
        LatestAggregateSink sink = new LatestAggregateSink();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root,
                Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink),
                reference -> {
                    if (processFunction().equals(reference)) {
                        return new RescaleProcessFunction();
                    }
                    throw new IllegalArgumentException(
                            "unknown process deployment");
                }, Collections.singletonList(PROCESS_DEPLOYMENT_SET))) {
            StreamSession originalDefinition = processSession(
                    cluster, 1, false, multiSplitSourceSpec());
            StreamSession targetDefinition = processSession(
                    cluster, 2, false, multiSplitSourceSpec());
            StreamJobClientRecoveryContract.verifySavepointOfflineRescale(
                    cluster, originalDefinition, targetDefinition,
                    "multi-split-process-rescale", 2, 2,
                    () -> source.awaitInitialRecords(2), () -> { },
                    () -> {
                        sink.awaitResults(firstKey,
                                new BigDecimal("8.0000000000"),
                                secondKey,
                                new BigDecimal("12.0000000000"));
                        assertEquals(2, source.restoredOpens.get());
                    });
        }
    }

    @Test
    void genericWindowRescalesPendingNamespacesAndContinuesSourcePosition(
            @TempDir Path root) throws Exception {
        String firstKey = keyForGroup(0);
        String secondKey = keyForGroup(1);
        RescaleSource source = new RescaleSource(firstKey, secondKey);
        LatestAggregateSink sink = new LatestAggregateSink();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root,
                Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink),
                reference -> {
                    if (windowFunction().equals(reference)) {
                        return new RescaleWindowAggregate();
                    }
                    throw new IllegalArgumentException(
                            "unknown window deployment");
                }, Collections.singletonList(WINDOW_DEPLOYMENT_SET))) {
            StreamSession originalDefinition = windowSession(cluster, 1);
            StreamSession targetDefinition = windowSession(cluster, 2);
            String checkpointId = StreamJobClientRecoveryContract
                    .verifySavepointOfflineRescale(
                            cluster, originalDefinition, targetDefinition,
                            "window-rescale", 2, 2,
                            source::awaitInitialRecords, () -> { },
                            () -> {
                                sink.awaitResults(firstKey, secondKey);
                                assertEquals(1, source.restoredOpens.get());
                            });
            assertEquals(2L, new FileStreamSavepointStore(
                    root.resolve("checkpoints")).get(checkpointId)
                    .checkpointManifest().taskReceipts().stream()
                    .flatMap(receipt -> receipt.evidence().stream())
                    .filter(member -> member.kind()
                            == CheckpointEvidence.Kind.STATE)
                    .count());
        }
    }

    @Test
    void liveRescaleMigratesGenericWindowNamespaces(
            @TempDir Path root) throws Exception {
        String firstKey = keyForGroup(0);
        String secondKey = keyForGroup(1);
        RescaleSource source = new RescaleSource(firstKey, secondKey);
        LatestAggregateSink sink = new LatestAggregateSink();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root,
                Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink),
                reference -> {
                    if (windowFunction().equals(reference)) {
                        return new RescaleWindowAggregate();
                    }
                    throw new IllegalArgumentException(
                            "unknown window deployment");
                }, Collections.singletonList(WINDOW_DEPLOYMENT_SET))) {
            StreamJobHandle job = windowSession(cluster, 1).submit(
                    StreamSubmissionId.of("live-window-rescale"));
            source.awaitInitialRecords();

            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.rescale(StreamCommandId.of(
                            "live-window-scale-to-two"), 2)
                            .awaitCompletion(5_000L));
            sink.awaitResults(firstKey, secondKey);
            assertEquals(1, source.restoredOpens.get());
            job.cancel(StreamCommandId.of("live-window-rescale-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    private static StreamSession session(
            InProcessStreamCluster cluster, int parallelism) {
        StreamSession session = RowWeave.createStreamSession(
                "offline-rescale", "offline-rescale", cluster);
        session.setDefinitionId("offline-rescale-v1");
        session.setParallelism(parallelism);
        session.setMaxParallelism(2);
        session.setChangelogMode(StreamChangelogMode.UPSERT);
        session.setResourceProfile(new StreamResourceProfile(
                16, 4_096, 8_192, 1_048_576L, 128, 128,
                1_048_576L, 8, 64, 1_048_576L));
        session.fromSource(sourceSpec()).uid("source")
                .keyBy("adaptor").uid("key")
                .keyedAggregate(
                        StreamProductAggregateWorkerProcessFixture.function(),
                        inputSchema(),
                        new StreamStateSpec("readout",
                                StreamStateSpec.Kind.KEYED_AGGREGATING,
                                "aggregate-decimal-state-v1", 1,
                                StreamStateSpec.TtlMode.PROCESSING_TIME,
                                60_000L))
                .uid("aggregate")
                .sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                        "rescale-sink-v1", SINK_CONTRACT, new byte[] {3}));
        return session;
    }

    private static StreamSession processSession(
            InProcessStreamCluster cluster, int parallelism) {
        return processSession(cluster, parallelism, false);
    }

    private static StreamSession chainedProcessSession(
            InProcessStreamCluster cluster, int parallelism) {
        return processSession(cluster, parallelism, true);
    }

    private static StreamSession processSession(
            InProcessStreamCluster cluster, int parallelism,
            boolean chained) {
        return processSession(cluster, parallelism, chained, sourceSpec());
    }

    private static StreamSession processSession(
            InProcessStreamCluster cluster, int parallelism,
            boolean chained, StreamSourceSpec sourceSpec) {
        StreamSession session = RowWeave.createStreamSession(
                "process-offline-rescale", "process-offline-rescale",
                cluster);
        session.setDefinitionId("process-offline-rescale-v1");
        session.setParallelism(parallelism);
        session.setMaxParallelism(2);
        session.setChangelogMode(StreamChangelogMode.UPSERT);
        session.setResourceProfile(new StreamResourceProfile(
                16, 4_096, 8_192, 1_048_576L, 128, 0,
                1_048_576L, 8, 64, 1_048_576L));
        StreamManagedStateSpec total = StreamManagedStateSpec.bounded(
                new StreamStateSpec("total", StreamStateSpec.Kind.KEYED_VALUE,
                        "decimal-text-v1", 1,
                        StreamStateSpec.TtlMode.PROCESSING_TIME, 60_000L),
                1, 2L, 4_096L, 128L);
        StreamManagedStateSpec mirror = StreamManagedStateSpec.bounded(
                new StreamStateSpec("mirror", StreamStateSpec.Kind.KEYED_MAP,
                        "decimal-map-v1", 1,
                        StreamStateSpec.TtlMode.PROCESSING_TIME, 60_000L),
                1, 2L, 4_096L, 128L);
        StreamProcessSpec process = StreamProcessSpec.keyed(
                processFunction(), inputSchema(),
                Collections.singletonList("adaptor"),
                Arrays.asList(total, mirror), Collections.emptyList(),
                StreamEmissionLimit.of(1, 1_024L),
                StreamEmissionLimit.of(1, 1_024L));
        io.rowweave.DataStream output = session.fromSource(sourceSpec)
                .uid("source")
                .keyBy("adaptor").uid("key")
                .keyedProcess(process).uid("process");
        if (chained) {
            output = output.keyBy("adaptor").uid("second-key")
                    .keyedProcess(process).uid("second-process");
        }
        output.sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                        "rescale-sink-v1", SINK_CONTRACT, new byte[] {3}));
        return session;
    }

    private static StreamSession twoSourceProcessSession(
            InProcessStreamCluster cluster, int parallelism) {
        StreamSession session = RowWeave.createStreamSession(
                "two-source-process-offline-rescale",
                "two-source-process-offline-rescale", cluster);
        session.setDefinitionId("two-source-process-offline-rescale-v1");
        session.setParallelism(parallelism);
        session.setMaxParallelism(2);
        session.setChangelogMode(StreamChangelogMode.UPSERT);
        session.setResourceProfile(new StreamResourceProfile(
                24, 4_096, 8_192, 1_048_576L, 128, 0,
                1_048_576L, 12, 64, 1_048_576L));
        StreamManagedStateSpec total = StreamManagedStateSpec.bounded(
                new StreamStateSpec("total",
                        StreamStateSpec.Kind.KEYED_VALUE,
                        "decimal-text-v1", 1,
                        StreamStateSpec.TtlMode.PROCESSING_TIME, 60_000L),
                1, 2L, 4_096L, 128L);
        StreamManagedStateSpec mirror = StreamManagedStateSpec.bounded(
                new StreamStateSpec("mirror",
                        StreamStateSpec.Kind.KEYED_MAP,
                        "decimal-map-v1", 1,
                        StreamStateSpec.TtlMode.PROCESSING_TIME, 60_000L),
                1, 2L, 4_096L, 128L);
        StreamProcessSpec process = StreamProcessSpec.keyed(
                processFunction(), inputSchema(),
                Collections.singletonList("adaptor"),
                Arrays.asList(total, mirror), Collections.emptyList(),
                StreamEmissionLimit.of(1, 1_024L),
                StreamEmissionLimit.of(1, 1_024L));
        io.rowweave.DataStream left = session.fromSource(sourceSpec())
                .uid("left-source")
                .keyBy("adaptor").uid("left-key")
                .keyedProcess(process).uid("left-process");
        io.rowweave.DataStream right = session.fromSource(sourceSpec())
                .uid("right-source")
                .keyBy("adaptor").uid("right-key")
                .keyedProcess(process).uid("right-process");
        left.union(right).uid("union")
                .sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                        "rescale-sink-v1", SINK_CONTRACT,
                        new byte[] {3}));
        return session;
    }

    private static StreamSession windowSession(
            InProcessStreamCluster cluster, int parallelism) {
        StreamSession session = RowWeave.createStreamSession(
                "window-offline-rescale", "window-offline-rescale",
                cluster);
        session.setDefinitionId("window-offline-rescale-v1");
        session.setParallelism(parallelism);
        session.setMaxParallelism(2);
        session.setChangelogMode(StreamChangelogMode.UPSERT);
        session.setResourceProfile(new StreamResourceProfile(
                16, 4_096, 8_192, 1_048_576L, 128, 128,
                1_048_576L, 8, 64, 1_048_576L));
        session.fromSource(sourceSpec()).uid("source")
                .keyBy("adaptor").uid("key")
                .genericWindow(StreamGenericWindowSpec.aggregate(
                        StreamWindowAssignerSpec.countTumbling(2L),
                        StreamWindowTriggerSpec.count(2L),
                        StreamAccumulationMode.ACCUMULATING, 0L,
                        windowFunction(), inputSchema(),
                        Collections.singletonList("adaptor"),
                        new StreamStateSpec("window-state",
                                StreamStateSpec.Kind.KEYED_AGGREGATING,
                                "rescale-window-state-v1", 1,
                                StreamStateSpec.TtlMode.NONE, 0L),
                        StreamWindowResourceSpec.of(4, 2L,
                                4_096L, 65_536L, 2,
                                StreamEmissionLimit.of(1, 1_024L))))
                .uid("window")
                .sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                        "rescale-sink-v1", SINK_CONTRACT, new byte[] {3}));
        return session;
    }

    private static FunctionDeploymentReference processFunction() {
        byte[] configuration = "rescale-process-config".getBytes(
                StandardCharsets.UTF_8);
        return new FunctionDeploymentReference(PROCESS_DEPLOYMENT_SET,
                "rescale-process", sha256("rescale-process-jar"),
                RescaleProcessFunction.class.getName(),
                FunctionAbi.KEYED_PROCESS_V1, "config-v1",
                sha256("rescale-process-config"), configuration,
                new FunctionResourceProfile(2_048L, 0L, 1));
    }

    private static FunctionDeploymentReference windowFunction() {
        byte[] configuration = "rescale-window-config".getBytes(
                StandardCharsets.UTF_8);
        return new FunctionDeploymentReference(WINDOW_DEPLOYMENT_SET,
                "rescale-window", sha256("rescale-window-jar"),
                RescaleWindowAggregate.class.getName(),
                FunctionAbi.STREAM_WINDOW_AGGREGATE_V1, "config-v1",
                sha256("rescale-window-config"), configuration,
                new FunctionResourceProfile(2_048L, 0L, 1));
    }

    private static StreamSourceSpec sourceSpec() {
        return StreamSourceSpec.exactOrdered(
                inputSchema(), "rescale-source-v1", SOURCE_CONTRACT,
                new byte[] {1}, Collections.singletonList(
                        new StreamSplitSpec(sha256("rescale-split"),
                                "sequence-v1", new byte[] {2})));
    }

    private static StreamSourceSpec multiSplitSourceSpec() {
        return StreamSourceSpec.exactOrdered(
                inputSchema(), "rescale-source-v1", SOURCE_CONTRACT,
                new byte[] {1}, Arrays.asList(
                        new StreamSplitSpec(sha256("rescale-split-a"),
                                "sequence-v1", new byte[] {2}),
                        new StreamSplitSpec(sha256("rescale-split-b"),
                                "sequence-v1", new byte[] {3})));
    }

    private static StreamSchema inputSchema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("adaptor", "adaptor", "adaptor",
                        DataType.StringType, false),
                new StreamField("amount", "amount", "amount",
                        DataType.BigDecimalType, false)));
    }

    private static String keyForGroup(int group) {
        RowSerializer serializer = new RowSerializer(new DataSchema(
                new Field[] {new Field("adaptor", "adaptor",
                        DataType.StringType)}));
        for (int index = 0; index < 10_000; index++) {
            DataRow row = new DataRow(1);
            row.set(0, "adaptor-" + index);
            if (ManagedKeyedValueState.keyGroup(
                    serializer.serialize(row), 2) == group) {
                return row.getString(0);
            }
        }
        throw new AssertionError("no key for key group " + group);
    }

    private static final class RescaleSource
            implements DurableStreamSourceProvider {
        private final String firstKey;
        private final String secondKey;
        private final AtomicInteger restoredOpens = new AtomicInteger();
        private final Semaphore initialReady = new Semaphore(0);

        private RescaleSource(String firstKey, String secondKey) {
            this.firstKey = firstKey;
            this.secondKey = secondKey;
        }

        @Override public String providerKey() { return "rescale-source-v1"; }
        @Override public String contractDigest() { return SOURCE_CONTRACT; }

        @Override public StreamSourceReader open(StreamSourceOpenRequest request) {
            int start = request.hasRestoredPosition()
                    ? ByteBuffer.wrap(request.restoredPositionFrame()).getInt()
                    : 0;
            if (request.hasRestoredPosition()) restoredOpens.incrementAndGet();
            RowSerializer serializer = new RowSerializer(new DataSchema(
                    new Field[] {
                            new Field("adaptor", "adaptor", DataType.StringType),
                            new Field("amount", "amount", DataType.BigDecimalType)}));
            return new StreamSourceReader() {
                private final Semaphore wakeups = new Semaphore(0);
                private int next = start;

                @Override public StreamSourceRecord next() throws Exception {
                    if (start == 0 && next == 2) {
                        wakeups.acquire();
                        return null;
                    }
                    if (next < 4) {
                        int current = next++;
                        String key = current % 2 == 0 ? firstKey : secondKey;
                        BigDecimal amount = BigDecimal.valueOf(current + 1L);
                        DataRow row = new DataRow(2);
                        row.set(0, key);
                        row.set(1, amount);
                        if (next == 2) initialReady.release();
                        return StreamSourceRecord.of(
                                serializer.serialize(row), position(next));
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

        private void awaitInitialRecords() throws InterruptedException {
            awaitInitialRecords(1);
        }

        private void awaitInitialRecords(int readers)
                throws InterruptedException {
            if (!initialReady.tryAcquire(readers, 5L, TimeUnit.SECONDS)) {
                throw new AssertionError("initial records were not emitted");
            }
        }
    }

    private static final class LatestAggregateSink
            implements EagerStreamSinkProvider {
        private final Map<String, BigDecimal> latest = new ConcurrentHashMap<>();

        @Override public String providerKey() { return "rescale-sink-v1"; }
        @Override public String contractDigest() { return SINK_CONTRACT; }

        @Override public EagerStreamSinkWriter open(StreamSinkOpenRequest request) {
            RowSerializer serializer = new RowSerializer(new DataSchema(
                    new Field[] {
                            new Field("adaptor", "adaptor", DataType.StringType),
                            new Field("amount", "amount", DataType.BigDecimalType)}));
            return new EagerStreamSinkWriter() {
                @Override public void write(byte[] recordFrame) {
                    StreamUpsertRecord record = StreamUpsertRecordCodec.decode(
                            recordFrame, 8_192, 8_192);
                    DataRow row = serializer.deserialize(record.afterImageFrame());
                    latest.put(row.getString(0), row.getBigDecimal(1));
                }
                @Override public void cancel() { }
                @Override public void close() { }
            };
        }

        private void awaitResults(String firstKey, String secondKey)
                throws InterruptedException {
            awaitResults(firstKey, new BigDecimal("4.0000000000"),
                    secondKey, new BigDecimal("6.0000000000"));
        }

        private void awaitResults(String firstKey, BigDecimal firstValue,
                String secondKey, BigDecimal secondValue)
                throws InterruptedException {
            long deadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(5L);
            while (System.nanoTime() < deadline) {
                if (firstValue.equals(latest.get(firstKey))
                        && secondValue.equals(latest.get(secondKey))) return;
                Thread.sleep(5L);
            }
            throw new AssertionError(
                    "rescaled aggregate did not continue both keys: " + latest);
        }
    }

    public static final class RescaleProcessFunction
            implements StreamKeyedProcessFunction {
        private static final byte[] SUM_KEY =
                "sum".getBytes(StandardCharsets.US_ASCII);

        @Override
        public void processElement(DataRow value, StreamProcessContext context,
                StreamCollector output) {
            StreamValueState totalState = context.valueState("total");
            byte[] prior = totalState.value();
            BigDecimal total = (prior == null ? BigDecimal.ZERO
                    : new BigDecimal(new String(prior,
                    StandardCharsets.US_ASCII))).add(value.getBigDecimal(1));
            byte[] encoded = total.toPlainString().getBytes(
                    StandardCharsets.US_ASCII);
            totalState.update(encoded);
            StreamMapState mirror = context.mapState("mirror");
            mirror.put(SUM_KEY, encoded);
            DataRow result = new DataRow(2);
            result.set(0, value.getString(0));
            result.set(1, new BigDecimal(new String(
                    mirror.get(SUM_KEY), StandardCharsets.US_ASCII))
                    .setScale(10, RoundingMode.HALF_UP));
            output.collect(result);
        }
    }

    public static final class RescaleWindowAggregate
            implements StreamWindowAggregateFunction {
        @Override public byte[] createAccumulator() {
            return "0".getBytes(StandardCharsets.US_ASCII);
        }

        @Override public byte[] add(DataRow value, byte[] accumulator) {
            return new BigDecimal(new String(accumulator,
                    StandardCharsets.US_ASCII)).add(value.getBigDecimal(1))
                    .toPlainString().getBytes(StandardCharsets.US_ASCII);
        }

        @Override public byte[] merge(byte[] left, byte[] right) {
            return new BigDecimal(new String(left, StandardCharsets.US_ASCII))
                    .add(new BigDecimal(new String(
                            right, StandardCharsets.US_ASCII)))
                    .toPlainString().getBytes(StandardCharsets.US_ASCII);
        }

        @Override public DataRow getResult(byte[] accumulator,
                StreamWindowContext context) {
            DataRow result = new DataRow(2);
            result.set(0, context.currentKey().get(0));
            result.set(1, new BigDecimal(new String(
                    accumulator, StandardCharsets.US_ASCII)).setScale(
                    10, RoundingMode.HALF_UP));
            return result;
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
