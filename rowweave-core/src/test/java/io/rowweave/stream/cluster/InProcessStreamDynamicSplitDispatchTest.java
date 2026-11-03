package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamJobClient;
import io.rowweave.StreamRestoreRequest;
import io.rowweave.StreamSavepointHandle;
import io.rowweave.StreamSavepointRequest;
import io.rowweave.StreamSession;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamRecoveryPointId;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamRestartPolicy;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceDiscoverySpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.runtime.DynamicDurableStreamSourceProvider;
import io.rowweave.stream.runtime.DurableStreamSourceProvider;
import io.rowweave.stream.runtime.EagerStreamSinkProvider;
import io.rowweave.stream.runtime.EagerStreamSinkWriter;
import io.rowweave.stream.runtime.StreamSinkOpenRequest;
import io.rowweave.stream.runtime.StreamSourceEnumeratorOpenRequest;
import io.rowweave.stream.runtime.StreamSourceOpenRequest;
import io.rowweave.stream.runtime.StreamSourceReader;
import io.rowweave.stream.runtime.StreamSourceRecord;
import io.rowweave.stream.spi.StreamDiscoveryCursor;
import io.rowweave.stream.spi.StreamSourceEnumerator;
import io.rowweave.stream.spi.StreamSplitChange;
import io.rowweave.stream.spi.StreamSplitDiscovery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Dynamic and fixed sources share the generic node/subtask deployment path. */
class InProcessStreamDynamicSplitDispatchTest {

    @Test
    void discoveredSplitsRunAndCheckpointOnPlacedSourceSubtasks(
            @TempDir Path root) throws Exception {
        DynamicSource source = new DynamicSource(false, 2);
        CountingSink sink = new CountingSink(2);
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root, Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink))) {
            StreamJobHandle job = session(cluster, 2).submit(
                    StreamSubmissionId.of("dynamic-dispatch"));

            assertEquals(StreamJobStatus.RUNNING, job.awaitRunning(5_000L));
            assertTrue(sink.received.await(5L, TimeUnit.SECONDS));
            assertEquals(2, sink.records.get());
            assertEquals(1, source.enumeratorOpens.get());
            assertEquals(2, source.readerOpens.get());
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.triggerCheckpoint(StreamCommandId.of(
                            "dynamic-checkpoint"))
                            .awaitCompletion(5_000L));
            job.cancel(StreamCommandId.of("dynamic-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    @Test
    void liveRescaleCutsAndReplacesTheRunningGenericDagGeneration(
            @TempDir Path root) throws Exception {
        DynamicSource source = new DynamicSource(false, 1);
        CountingSink sink = new CountingSink(1);
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root, Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink))) {
            StreamJobHandle job = session(cluster, 1).submit(
                    StreamSubmissionId.of("dynamic-live-rescale"));

            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(5_000L));
            assertTrue(sink.received.await(5L, TimeUnit.SECONDS));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.rescale(StreamCommandId.of("dynamic-scale-to-two"), 2)
                            .awaitCompletion(5_000L));
            assertEquals(StreamJobStatus.RUNNING, job.status());
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.triggerCheckpoint(StreamCommandId.of(
                            "dynamic-after-live-rescale"))
                            .awaitCompletion(5_000L));
            assertTrue(source.enumeratorOpens.get() >= 2,
                    "the target generation must restore its source coordinator");
            job.cancel(StreamCommandId.of("dynamic-live-rescale-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    @Test
    void liveScaleDownKeepsOneSourceTaskPerActiveDynamicSplit(
            @TempDir Path root) throws Exception {
        DynamicSource source = new DynamicSource(false, 3);
        CountingSink sink = new CountingSink(3);
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root, Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink))) {
            StreamJobHandle job = session(cluster, 3).submit(
                    StreamSubmissionId.of("dynamic-live-scale-down"));

            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(5_000L));
            assertTrue(sink.received.await(5L, TimeUnit.SECONDS));
            assertEquals(3, source.readerOpens.get());
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.rescale(StreamCommandId.of(
                            "dynamic-scale-down-to-one"), 1)
                            .awaitCompletion(10_000L));
            assertEquals(StreamJobStatus.RUNNING, job.status());
            long deadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(5L);
            while (source.readerOpens.get() < 6
                    && System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }
            assertEquals(6, source.readerOpens.get(),
                    "every active split must retain a target reader owner");
            assertEquals(3, sink.records.get(),
                    "restored dynamic splits must continue exact positions");
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.triggerCheckpoint(StreamCommandId.of(
                            "dynamic-after-scale-down"))
                            .awaitCompletion(5_000L));
            job.cancel(StreamCommandId.of(
                    "dynamic-scale-down-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    @Test
    void lateDiscoveryAssignsAnAlreadyRunningIdleSourceTask(
            @TempDir Path root) throws Exception {
        DynamicSource source = new DynamicSource(true, 1);
        CountingSink sink = new CountingSink(1);
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root, Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink))) {
            StreamJobHandle job = session(cluster, 1).submit(
                    StreamSubmissionId.of("dynamic-late-dispatch"));

            assertEquals(StreamJobStatus.RUNNING, job.awaitRunning(5_000L));
            assertTrue(sink.received.await(5L, TimeUnit.SECONDS));
            assertEquals(1, sink.records.get());
            assertEquals(1, source.enumeratorOpens.get());
            assertTrue(source.discoveryCalls.get() >= 2);
            assertEquals(1, source.readerOpens.get());
            job.cancel(StreamCommandId.of("dynamic-late-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    @Test
    void eachDynamicSourceKeepsItsOwnDiscoveryInterval(
            @TempDir Path root) throws Exception {
        IntervalSource fast = new IntervalSource("fast", repeat('d'));
        IntervalSource slow = new IntervalSource("slow", repeat('e'));
        CountingSink sink = new CountingSink(1);
        Map<String, DurableStreamSourceProvider> sources =
                new LinkedHashMap<>();
        sources.put(fast.providerKey(), fast);
        sources.put(slow.providerKey(), slow);
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root, sources,
                Collections.singletonMap(sink.providerKey(), sink))) {
            StreamSession definition = RowWeave.createStreamSession(
                    "independent-discovery", "independent-discovery", cluster);
            definition.setParallelism(1);
            definition.setMaxParallelism(8);
            definition.setResourceProfile(new StreamResourceProfile(
                    8, 1_024, 4_096, 0L, 0, 0,
                    65_536L, 8, 32, 0L));
            io.rowweave.DataStream fastStream = definition.fromSource(
                    StreamSourceSpec.dynamicExactOrdered(
                            schema(), fast.providerKey(), fast.contractDigest(),
                            new byte[] {1}, new StreamSourceDiscoverySpec(
                                    10L, 1_000L, 4, 4, 2, 64, 64)))
                    .uid("fast-source");
            io.rowweave.DataStream slowStream = definition.fromSource(
                    StreamSourceSpec.dynamicExactOrdered(
                            schema(), slow.providerKey(), slow.contractDigest(),
                            new byte[] {2}, new StreamSourceDiscoverySpec(
                                    5_000L, 5_000L, 4, 4, 2, 64, 64)))
                    .uid("slow-source");
            fastStream.union(slowStream).uid("union")
                    .sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                            "sink", repeat('b'), new byte[] {2}));

            StreamJobHandle job = definition.submit(
                    StreamSubmissionId.of("independent-discovery-submit"));
            assertEquals(StreamJobStatus.RUNNING, job.awaitRunning(5_000L));
            assertTrue(fast.secondDiscovery.await(2L, TimeUnit.SECONDS));
            assertEquals(1, slow.discoveryCalls.get(),
                    "the fast source must not pull the slow source early");
            assertTrue(sink.received.await(2L, TimeUnit.SECONDS));
            job.cancel(StreamCommandId.of("independent-discovery-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    @Test
    void completedDynamicSplitReleasesTheSameTaskForTheNextSplit(
            @TempDir Path root) throws Exception {
        DynamicSource source = new DynamicSource(false, 2, true, true);
        CountingSink sink = new CountingSink(2);
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root, Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink))) {
            StreamJobHandle job = session(cluster, 1).submit(
                    StreamSubmissionId.of("dynamic-sequential-splits"));

            assertEquals(StreamJobStatus.RUNNING, job.awaitRunning(5_000L));
            assertTrue(sink.received.await(5L, TimeUnit.SECONDS));
            assertEquals(2, sink.records.get());
            assertEquals(1, source.enumeratorOpens.get());
            assertEquals(2, source.readerOpens.get());
            assertTrue(source.discoveryCalls.get() >= 2);
            assertEquals(StreamJobStatus.RUNNING, job.status());
            job.cancel(StreamCommandId.of("dynamic-sequential-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    @Test
    void completedSplitCanBeRemovedAndRediscoveredAtANewGeneration(
            @TempDir Path root) throws Exception {
        DynamicSource source = new DynamicSource(false, 1, false, true,
                false, true, false);
        source.holdRemoval.set(true);
        CountingSink sink = new CountingSink(2);
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root, Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink))) {
            StreamJobHandle job = session(cluster, 1).submit(
                    StreamSubmissionId.of("dynamic-remove-readd"));
            assertEquals(StreamJobStatus.RUNNING, job.awaitRunning(5_000L));
            long firstRecordDeadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(5L);
            while (sink.records.get() < 1
                    && System.nanoTime() < firstRecordDeadline) {
                Thread.sleep(10L);
            }
            assertEquals(1, sink.records.get());
            assertTrue(source.firstReaderClosed.await(5L, TimeUnit.SECONDS),
                    "the first generation must finish before REMOVE");
            source.allowRemoval.countDown();
            assertTrue(sink.received.await(20L, TimeUnit.SECONDS),
                    "rediscovered split records did not arrive; status="
                            + job.status() + ", readers="
                            + source.readerOpens.get() + ", discoveries="
                            + source.discoveryCalls.get() + ", records="
                            + sink.records.get() + ", emitted="
                            + source.recordEvents.get() + ", restored="
                            + source.restoredPositionOpens.get()
                            + ", restarts=" + job.progress().restarts());
            assertEquals(2, source.readerOpens.get());
            assertEquals(2, sink.records.get());
            long deadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(5L);
            while (source.discoveryCalls.get() < 4
                    && System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }
            assertTrue(source.discoveryCalls.get() >= 4,
                    "the old completion must survive a later progress poll");
            assertEquals(StreamJobStatus.RUNNING, job.status());
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.triggerCheckpoint(StreamCommandId.of(
                            "dynamic-remove-readd-checkpoint"))
                            .awaitCompletion(5_000L));
            job.cancel(StreamCommandId.of("dynamic-remove-readd-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    @Test
    void finishedSplitRemovalLeavesAnIdleCheckpointWithoutOldPosition(
            @TempDir Path root) throws Exception {
        DynamicSource source = new DynamicSource(false, 1, false, true,
                false, true, false);
        source.holdRemoval.set(true);
        source.holdReadd.set(true);
        CountingSink sink = new CountingSink(2);
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root, Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink))) {
            StreamSession definition = session(cluster, 1);
            PlanArtifact artifact = definition.toPlanArtifact();
            StreamJobHandle job = definition.submit(
                    StreamSubmissionId.of("dynamic-finished-remove-cut"));
            StreamJobHandle restored = null;
            try {
                assertEquals(StreamJobStatus.RUNNING,
                        job.awaitRunning(5_000L));
                long deadline = System.nanoTime()
                        + TimeUnit.SECONDS.toNanos(5L);
                while (sink.records.get() < 1
                        && System.nanoTime() < deadline) {
                    Thread.sleep(10L);
                }
                assertEquals(1, sink.records.get(),
                        () -> "status=" + job.status()
                                + " readers=" + source.readerOpens.get()
                                + " discoveries=" + source.discoveryCalls.get()
                                + " restarts=" + job.progress().restarts());
                assertTrue(source.firstReaderClosed.await(
                        5L, TimeUnit.SECONDS));
                source.allowRemoval.countDown();
                assertTrue(source.readdBlocked.await(5L, TimeUnit.SECONDS),
                        "REMOVE must complete before the checkpoint cut");
                io.rowweave.StreamCheckpointHandle checkpoint =
                        job.triggerCheckpoint(StreamCommandId.of(
                                "dynamic-finished-remove-checkpoint"));
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        checkpoint.awaitCompletion(5_000L));
                assertEquals(StreamJobStatus.RUNNING, job.status());
                job.cancel(StreamCommandId.of(
                        "dynamic-finished-remove-cancel"))
                        .awaitCompletion(5_000L);
                restored = cluster.restore(StreamSubmissionId.of(
                        "dynamic-finished-remove-restored"), artifact,
                        new StreamRestoreRequest(job.id(),
                                StreamRecoveryPointId.checkpoint(
                                        checkpoint.checkpointId()), 1, 8));
                assertEquals(StreamJobStatus.RUNNING,
                        restored.awaitRunning(5_000L));
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        restored.triggerCheckpoint(StreamCommandId.of(
                                "dynamic-finished-remove-restored-cut"))
                                .awaitCompletion(5_000L));
                source.allowReadd.countDown();
                assertTrue(sink.received.await(5L, TimeUnit.SECONDS));
                assertEquals(0, source.unexpectedRestoredPositions.get(),
                        "a new split generation must not inherit the removed cut");
            } finally {
                source.allowRemoval.countDown();
                source.allowReadd.countDown();
                if (job.status() == StreamJobStatus.RUNNING) {
                    job.cancel(StreamCommandId.of(
                            "dynamic-finished-remove-cancel"))
                            .awaitCompletion(5_000L);
                }
                if (restored != null
                        && restored.status() == StreamJobStatus.RUNNING) {
                    restored.cancel(StreamCommandId.of(
                            "dynamic-finished-remove-restored-cancel"))
                            .awaitCompletion(5_000L);
                }
            }
        }
    }

    @Test
    void removingAnActiveSplitStopsItsReaderBeforeReusingItsTask(
            @TempDir Path root) throws Exception {
        DynamicSource source = new DynamicSource(false, 2, false, true,
                false, false, true);
        CountingSink sink = new CountingSink(2);
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root, Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink))) {
            StreamJobHandle job = session(cluster, 1).submit(
                    StreamSubmissionId.of("dynamic-active-remove"));
            long deadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(5L);
            while (sink.records.get() < 1
                    && System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }
            assertEquals(1, sink.records.get(),
                    "the first split must become active before removal");
            source.allowActiveReplacement.countDown();
            assertEquals(StreamJobStatus.RUNNING, job.awaitRunning(5_000L));
            assertTrue(sink.received.await(5L, TimeUnit.SECONDS));
            assertEquals(2, source.readerOpens.get(),
                    "replacement split must start on the released task");
            assertEquals(1, source.readerCancels.get(),
                    "removed reader must be physically stopped");
            assertEquals(StreamJobStatus.RUNNING, job.status());
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.triggerCheckpoint(StreamCommandId.of(
                            "dynamic-active-remove-checkpoint"))
                            .awaitCompletion(5_000L));
            job.cancel(StreamCommandId.of("dynamic-active-remove-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    @Test
    void fixedSplitsUseCoordinatorAssignmentsWithoutChangingBoundedEnd(
            @TempDir Path root) throws Exception {
        BoundedFixedSource source = new BoundedFixedSource();
        CountingSink sink = new CountingSink(3);
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root, Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink))) {
            StreamSession definition = RowWeave.createStreamSession(
                    "fixed-coordinator", "fixed-coordinator", cluster);
            definition.setParallelism(1);
            definition.setMaxParallelism(8);
            definition.setResourceProfile(new StreamResourceProfile(
                    8, 1_024, 4_096, 0L, 0, 0,
                    65_536L, 8, 32, 0L));
            definition.fromSource(StreamSourceSpec.boundedExactOrdered(
                    schema(), "fixed", repeat('c'), new byte[] {3},
                    Arrays.asList(DynamicSource.split(0),
                            DynamicSource.split(1),
                            DynamicSource.split(2))))
                    .uid("source")
                    .sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                            "sink", repeat('b'), new byte[] {2}));

            StreamJobHandle job = definition.submit(
                    StreamSubmissionId.of("fixed-coordinator-dispatch"));
            StreamJobStatus started = job.awaitRunning(5_000L);
            assertTrue(started == StreamJobStatus.RUNNING
                    || started == StreamJobStatus.FINISHED);
            assertTrue(sink.received.await(5L, TimeUnit.SECONDS));
            assertEquals(3, sink.records.get());
            assertEquals(3, source.readerOpens.get());
            assertEquals(StreamJobStatus.FINISHED,
                    job.awaitTermination(5_000L));
        }
    }

    @Test
    void fixedFinishedSplitDoesNotStealSurvivorsPositionOnRestore(
            @TempDir Path root) throws Exception {
        BoundedFixedSource source = new BoundedFixedSource(true);
        CountingSink sink = new CountingSink(2);
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root, Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink))) {
            StreamSession definition = RowWeave.createStreamSession(
                    "fixed-reassignment", "fixed-reassignment", cluster);
            definition.setParallelism(2);
            definition.setMaxParallelism(8);
            definition.setResourceProfile(new StreamResourceProfile(
                    8, 1_024, 4_096, 0L, 0, 0,
                    65_536L, 8, 32, 0L));
            definition.fromSource(StreamSourceSpec.boundedExactOrdered(
                    schema(), "fixed", repeat('c'), new byte[] {3},
                    Arrays.asList(DynamicSource.split(0),
                            DynamicSource.split(1))))
                    .uid("source")
                    .sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                            "sink", repeat('b'), new byte[] {2}));
            PlanArtifact artifact = definition.toPlanArtifact();
            StreamJobHandle original = definition.submit(
                    StreamSubmissionId.of("fixed-reassignment-original"));
            assertEquals(StreamJobStatus.RUNNING,
                    original.awaitRunning(5_000L));
            assertTrue(sink.received.await(5L, TimeUnit.SECONDS));
            assertTrue(source.firstEnded.await(5L, TimeUnit.SECONDS));
            assertEquals(StreamJobStatus.RUNNING, original.status());
            io.rowweave.StreamCheckpointHandle checkpoint =
                    original.triggerCheckpoint(StreamCommandId.of(
                            "fixed-reassignment-checkpoint"));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    checkpoint.awaitCompletion(5_000L));
            original.cancel(StreamCommandId.of(
                    "fixed-reassignment-original-cancel"))
                    .awaitCompletion(5_000L);
            StreamJobHandle restored = cluster.restore(
                    StreamSubmissionId.of("fixed-reassignment-restored"),
                    artifact, new StreamRestoreRequest(original.id(),
                            StreamRecoveryPointId.checkpoint(
                                    checkpoint.checkpointId()), 2, 8));
            assertEquals(StreamJobStatus.RUNNING,
                    restored.awaitRunning(5_000L));
            assertEquals(2, sink.records.get(),
                    "surviving split must resume from its own position");
            restored.cancel(StreamCommandId.of(
                    "fixed-reassignment-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    @Test
    void savepointRestoreContinuesFromTheCheckpointedDiscoveryCursor(
            @TempDir Path root) throws Exception {
        DynamicSource source = new DynamicSource(false, 1);
        CountingSink sink = new CountingSink(2);
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root, Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink))) {
            StreamSession definition = session(cluster, 1);
            PlanArtifact artifact = definition.toPlanArtifact();
            StreamJobHandle original = definition.submit(
                    StreamSubmissionId.of("dynamic-savepoint"));
            assertEquals(StreamJobStatus.RUNNING,
                    original.awaitRunning(5_000L));
            while (sink.records.get() < 1) Thread.sleep(5L);

            StreamSavepointHandle savepoint = original.stopWithSavepoint(
                    StreamCommandId.of("dynamic-savepoint-stop"),
                    new StreamSavepointRequest(
                            "file-savepoint-v1", new byte[0]));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    savepoint.awaitCompletion(5_000L));

            StreamJobHandle restored = cluster.restore(
                    StreamSubmissionId.of("dynamic-savepoint-restored"),
                    artifact, new StreamRestoreRequest(original.id(),
                            StreamRecoveryPointId.savepoint(
                                    savepoint.savepointId()), 1, 8));
            assertEquals(StreamJobStatus.RUNNING,
                    restored.awaitRunning(5_000L));
            assertTrue(source.enumeratorOpens.get() >= 2);
            assertArrayEquals(new byte[] {1},
                    source.enumeratorInitialCursors.get(1));
            restored.cancel(StreamCommandId.of("dynamic-restored-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    @Test
    void workerIncarnationReplacementRestoresDynamicCoordinatorAndPosition(
            @TempDir Path root) throws Exception {
        DynamicSource source = new DynamicSource(false, 1);
        CountingSink sink = new CountingSink(1);
        Map<String, DurableStreamSourceProvider> sources =
                Collections.singletonMap(source.providerKey(), source);
        Map<String, EagerStreamSinkProvider> sinks =
                Collections.singletonMap(sink.providerKey(), sink);
        Path checkpoints = root.resolve("checkpoints");
        StreamSession definition = dynamicSession(null, 1);
        PlanArtifact artifact = definition.toPlanArtifact();
        StreamWorkerCapabilityProfile firstProfile = replacementProfile(
                artifact, "worker", "incarnation-a");
        AtomicReference<java.util.List<RemoteStreamWorkerEndpoint>> directory =
                new AtomicReference<>();
        StreamRemoteWorkerServer first = StreamRemoteWorkerServer.inProcess(
                firstProfile, sources, sinks,
                reference -> { throw new AssertionError(); }, checkpoints);
        StreamRemoteWorkerServer replacement = null;
        InProcessStreamJobClient client = null;
        try {
            directory.set(Collections.singletonList(
                    RemoteStreamWorkerEndpoint.inProcess(first)));
            RemoteStreamSourceSinkRunFactory runs =
                    new RemoteStreamSourceSinkRunFactory(
                            root.resolve("runs"), 1L,
                            "replacement-master", directory::get,
                            checkpoints);
            client = new InProcessStreamJobClient(root.resolve("jobs"), runs);
            StreamSession submitted = dynamicSession(client, 1);
            StreamJobHandle job = submitted.submit(
                    StreamSubmissionId.of("dynamic-worker-replacement"));
            assertEquals(StreamJobStatus.RUNNING, job.awaitRunning(5_000L));
            assertTrue(sink.received.await(5L, TimeUnit.SECONDS));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.triggerCheckpoint(StreamCommandId.of(
                            "dynamic-before-worker-loss"))
                            .awaitCompletion(5_000L));

            source.blockNextDiscovery();
            assertTrue(source.blockedDiscoveryEntered.await(
                    5L, TimeUnit.SECONDS));

            StreamWorkerCapabilityProfile replacementProfile =
                    replacementProfile(artifact, "worker", "incarnation-b");
            replacement = StreamRemoteWorkerServer.inProcess(
                    replacementProfile, sources, sinks,
                    reference -> { throw new AssertionError(); }, checkpoints);
            directory.set(Collections.singletonList(
                    RemoteStreamWorkerEndpoint.inProcess(replacement)));
            first.close();

            long deadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(10L);
            while (job.progress().restarts() < 1L
                    && System.nanoTime() < deadline) {
                Thread.sleep(20L);
            }
            assertTrue(job.progress().restarts() >= 1L,
                    () -> "status=" + job.status()
                            + " progress=" + job.progress().restarts());
            assertEquals(StreamJobStatus.RUNNING, job.awaitRunning(5_000L));
            assertEquals(2, source.enumeratorOpens.get());
            deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
            while (source.readerOpens.get() < 2
                    && System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }
            assertEquals(2, source.readerOpens.get());
            assertEquals(1, sink.records.get(),
                    "restored source position must suppress replay");
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.triggerCheckpoint(StreamCommandId.of(
                            "dynamic-after-worker-loss"))
                            .awaitCompletion(5_000L));
            job.cancel(StreamCommandId.of(
                    "dynamic-worker-replacement-cancel"))
                    .awaitCompletion(5_000L);
        } finally {
            if (client != null) client.close();
            if (replacement != null) replacement.close();
            first.close();
        }
    }

    @Test
    void replacementMasterRestoresDynamicCoordinatorCursorAndPosition(
            @TempDir Path root) throws Exception {
        DynamicSource source = new DynamicSource(false, 1);
        CountingSink sink = new CountingSink(1);
        Map<String, DurableStreamSourceProvider> sources =
                Collections.singletonMap(source.providerKey(), source);
        Map<String, EagerStreamSinkProvider> sinks =
                Collections.singletonMap(sink.providerKey(), sink);
        Path checkpoints = root.resolve("checkpoints");
        PlanArtifact artifact = dynamicSession(null, 1).toPlanArtifact();
        StreamWorkerCapabilityProfile profile = replacementProfile(
                artifact, "worker", "stable-incarnation");
        StreamRemoteWorkerServer worker = StreamRemoteWorkerServer.inProcess(
                profile, sources, sinks,
                reference -> { throw new AssertionError(); }, checkpoints);
        InProcessStreamJobClient firstClient = null;
        InProcessStreamJobClient replacementClient = null;
        try {
            java.util.List<RemoteStreamWorkerEndpoint> endpoints =
                    Collections.singletonList(
                            RemoteStreamWorkerEndpoint.inProcess(worker));
            firstClient = new InProcessStreamJobClient(root.resolve("jobs"),
                    new RemoteStreamSourceSinkRunFactory(
                            root.resolve("runs"), 1L, "master-a",
                            endpoints, checkpoints));
            StreamJobHandle submitted = dynamicSession(firstClient, 1).submit(
                    StreamSubmissionId.of("dynamic-master-replacement"));
            assertEquals(StreamJobStatus.RUNNING,
                    submitted.awaitRunning(5_000L));
            assertTrue(sink.received.await(5L, TimeUnit.SECONDS));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    submitted.triggerCheckpoint(StreamCommandId.of(
                            "dynamic-before-master-loss"))
                            .awaitCompletion(5_000L));
            io.rowweave.stream.api.StreamJobId jobId = submitted.id();

            firstClient.close();
            firstClient = null;
            replacementClient = new InProcessStreamJobClient(
                    root.resolve("jobs"),
                    new RemoteStreamSourceSinkRunFactory(
                            root.resolve("runs"), 2L, "master-b",
                            endpoints, checkpoints));
            StreamJobHandle attached = replacementClient.attach(jobId);
            assertEquals(StreamJobStatus.RUNNING,
                    attached.awaitRunning(5_000L));
            long deadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(5L);
            while (source.readerOpens.get() < 2
                    && System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }
            assertEquals(2, source.enumeratorOpens.get());
            assertEquals(2, source.readerOpens.get());
            assertEquals(1, sink.records.get(),
                    "replacement Master must restore the durable source cut");
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    attached.triggerCheckpoint(StreamCommandId.of(
                            "dynamic-after-master-loss"))
                            .awaitCompletion(5_000L));
            attached.cancel(StreamCommandId.of(
                    "dynamic-master-replacement-cancel"))
                    .awaitCompletion(5_000L);
        } finally {
            if (firstClient != null) firstClient.close();
            if (replacementClient != null) replacementClient.close();
            worker.close();
        }
    }

    @Test
    void dynamicFinishedSplitReassignsSurvivorsPositionBySplitId(
            @TempDir Path root) throws Exception {
        DynamicSource source = new DynamicSource(false, 2,
                false, false, true);
        CountingSink sink = new CountingSink(2);
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root, Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink))) {
            StreamSession definition = session(cluster, 2);
            PlanArtifact artifact = definition.toPlanArtifact();
            StreamJobHandle original = definition.submit(
                    StreamSubmissionId.of("dynamic-reassignment-original"));
            assertEquals(StreamJobStatus.RUNNING,
                    original.awaitRunning(5_000L));
            assertTrue(sink.received.await(5L, TimeUnit.SECONDS));
            assertTrue(source.firstEnded.await(5L, TimeUnit.SECONDS));
            io.rowweave.StreamCheckpointHandle checkpoint =
                    original.triggerCheckpoint(StreamCommandId.of(
                            "dynamic-reassignment-checkpoint"));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    checkpoint.awaitCompletion(5_000L));
            original.cancel(StreamCommandId.of(
                    "dynamic-reassignment-original-cancel"))
                    .awaitCompletion(5_000L);
            StreamJobHandle restored = cluster.restore(
                    StreamSubmissionId.of("dynamic-reassignment-restored"),
                    artifact, new StreamRestoreRequest(original.id(),
                            StreamRecoveryPointId.checkpoint(
                                    checkpoint.checkpointId()), 2, 8));
            assertEquals(StreamJobStatus.RUNNING,
                    restored.awaitRunning(5_000L));
            long readyDeadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(5L);
            while (source.restoredSecondOpens.get() < 1
                    && System.nanoTime() < readyDeadline) {
                Thread.sleep(10L);
            }
            assertEquals(1, source.restoredSecondOpens.get(),
                    "the surviving split must reopen from its own cut");
            assertEquals(2, sink.records.get());
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    restored.triggerCheckpoint(StreamCommandId.of(
                            "dynamic-reassignment-after-restore"))
                            .awaitCompletion(5_000L));
            restored.cancel(StreamCommandId.of(
                    "dynamic-reassignment-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    private static StreamSession session(InProcessStreamCluster client,
            int parallelism) {
        return dynamicSession(client, parallelism);
    }

    private static StreamSession dynamicSession(StreamJobClient client,
            int parallelism) {
        StreamSession session = client == null
                ? RowWeave.createStreamSession(
                        "dynamic-dispatch", "dynamic-dispatch")
                : RowWeave.createStreamSession(
                        "dynamic-dispatch", "dynamic-dispatch", client);
        session.setParallelism(parallelism);
        session.setMaxParallelism(8);
        session.setRestartPolicy(StreamRestartPolicy.fixedDelay(2, 10L));
        session.setResourceProfile(new StreamResourceProfile(
                8, 1_024, 4_096, 0L, 0, 0,
                65_536L, 8, 32, 0L));
        session.fromSource(StreamSourceSpec.dynamicExactOrdered(
                schema(), "dynamic", repeat('a'), new byte[] {1},
                new StreamSourceDiscoverySpec(
                        10L, 1_000L, 8, 8, 8, 64, 64)))
                .uid("source")
                .sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                        "sink", repeat('b'), new byte[] {2}));
        return session;
    }

    private static StreamWorkerCapabilityProfile replacementProfile(
            PlanArtifact artifact, String workerId, String incarnation) {
        return new StreamWorkerCapabilityProfile(
                workerId, incarnation, 2, 1,
                StreamTestGraph.from(artifact).product
                        .executionCapabilities(),
                32, 64, 4 * 1024 * 1024,
                1024L * 1024L * 1024L, 128);
    }

    private static StreamSchema schema() {
        return new StreamSchema(Collections.singletonList(
                new StreamField("id", "id", "id",
                        DataType.StringType, false)));
    }

    private static final class DynamicSource
            implements DynamicDurableStreamSourceProvider {
        private final boolean delayFirstDiscovery;
        private final int splitCount;
        private final boolean oneSplitPerDiscovery;
        private final boolean boundedReaders;
        private final boolean finishFirstOnly;
        private final boolean reuseCompleted;
        private final boolean replaceActive;
        private final CountDownLatch firstEnded = new CountDownLatch(1);
        private final AtomicInteger enumeratorOpens = new AtomicInteger();
        private final AtomicInteger discoveryCalls = new AtomicInteger();
        private final AtomicInteger readerOpens = new AtomicInteger();
        private final AtomicInteger recordEvents = new AtomicInteger();
        private final AtomicInteger restoredPositionOpens = new AtomicInteger();
        private final AtomicInteger readerCancels = new AtomicInteger();
        private final AtomicInteger restoredSecondOpens =
                new AtomicInteger();
        private final AtomicInteger unexpectedRestoredPositions =
                new AtomicInteger();
        private final AtomicBoolean blockNextDiscovery = new AtomicBoolean();
        private final CountDownLatch blockedDiscoveryEntered =
                new CountDownLatch(1);
        private final CountDownLatch releaseBlockedDiscovery =
                new CountDownLatch(1);
        private final CountDownLatch allowActiveReplacement =
                new CountDownLatch(1);
        private final AtomicBoolean holdReadd = new AtomicBoolean();
        private final AtomicBoolean holdRemoval = new AtomicBoolean();
        private final CountDownLatch allowRemoval = new CountDownLatch(1);
        private final CountDownLatch firstReaderClosed =
                new CountDownLatch(1);
        private final CountDownLatch readdBlocked = new CountDownLatch(1);
        private final CountDownLatch allowReadd = new CountDownLatch(1);
        private final java.util.List<byte[]> enumeratorInitialCursors =
                Collections.synchronizedList(new java.util.ArrayList<>());

        private DynamicSource(boolean delayFirstDiscovery, int splitCount) {
            this(delayFirstDiscovery, splitCount, false, false);
        }

        private DynamicSource(boolean delayFirstDiscovery, int splitCount,
                boolean oneSplitPerDiscovery, boolean boundedReaders) {
            this(delayFirstDiscovery, splitCount,
                    oneSplitPerDiscovery, boundedReaders, false);
        }

        private DynamicSource(boolean delayFirstDiscovery, int splitCount,
                boolean oneSplitPerDiscovery, boolean boundedReaders,
                boolean finishFirstOnly) {
            this(delayFirstDiscovery, splitCount, oneSplitPerDiscovery,
                    boundedReaders, finishFirstOnly, false, false);
        }

        private DynamicSource(boolean delayFirstDiscovery, int splitCount,
                boolean oneSplitPerDiscovery, boolean boundedReaders,
                boolean finishFirstOnly, boolean reuseCompleted,
                boolean replaceActive) {
            this.delayFirstDiscovery = delayFirstDiscovery;
            this.splitCount = splitCount;
            this.oneSplitPerDiscovery = oneSplitPerDiscovery;
            this.boundedReaders = boundedReaders;
            this.finishFirstOnly = finishFirstOnly;
            this.reuseCompleted = reuseCompleted;
            this.replaceActive = replaceActive;
        }
        @Override public String providerKey() { return "dynamic"; }
        @Override public String contractDigest() { return repeat('a'); }

        @Override public StreamSourceEnumerator openEnumerator(
                StreamSourceEnumeratorOpenRequest request) {
            enumeratorOpens.incrementAndGet();
            return new StreamSourceEnumerator() {
                private boolean first = true;
                @Override public StreamSplitDiscovery discover(
                        StreamDiscoveryCursor cursor, int limit) {
                    if (first) {
                        enumeratorInitialCursors.add(cursor.frame());
                        first = false;
                    }
                    int call = discoveryCalls.incrementAndGet();
                    if (blockNextDiscovery.compareAndSet(true, false)) {
                        blockedDiscoveryEntered.countDown();
                        try {
                            releaseBlockedDiscovery.await();
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException(
                                    "dynamic discovery interrupted",
                                    interrupted);
                        }
                    }
                    if (delayFirstDiscovery && call == 1) {
                        return new StreamSplitDiscovery(0L, cursor,
                                Collections.emptyList());
                    }
                    if (reuseCompleted || replaceActive) {
                        int phase = cursor.frame().length == 0
                                ? 0 : cursor.frame()[0];
                        if (phase == 1 && holdRemoval.get()
                                && allowRemoval.getCount() != 0L) {
                            return new StreamSplitDiscovery(1L, cursor,
                                    Collections.emptyList());
                        }
                        if (phase == 2 && holdReadd.get()) {
                            readdBlocked.countDown();
                            if (allowReadd.getCount() != 0L) {
                                return new StreamSplitDiscovery(2L, cursor,
                                        Collections.emptyList());
                            }
                        }
                        if (replaceActive && phase == 1) {
                            try {
                                if (!allowActiveReplacement.await(
                                        900L, TimeUnit.MILLISECONDS)) {
                                    throw new IllegalStateException(
                                            "active replacement was not released");
                                }
                            } catch (InterruptedException interrupted) {
                                Thread.currentThread().interrupt();
                                throw new IllegalStateException(
                                        "active replacement interrupted",
                                        interrupted);
                            }
                            return new StreamSplitDiscovery(3L,
                                    StreamDiscoveryCursor.of(
                                            new byte[] {3}),
                                    Arrays.asList(StreamSplitChange.remove(
                                            split(0).splitId(), 1L),
                                            StreamSplitChange.add(
                                                    split(1), 1L)));
                        }
                        if (phase >= 3) {
                            return new StreamSplitDiscovery(phase, cursor,
                                    Collections.emptyList());
                        }
                        StreamSplitChange change = phase == 0
                                ? StreamSplitChange.add(split(0), 1L)
                                : phase == 1
                                ? StreamSplitChange.remove(
                                        split(0).splitId(), 1L)
                                : StreamSplitChange.add(
                                        split(replaceActive ? 1 : 0),
                                        replaceActive ? 1L : 2L);
                        return new StreamSplitDiscovery(phase + 1L,
                                StreamDiscoveryCursor.of(new byte[] {
                                        (byte) (phase + 1)}),
                                Collections.singletonList(change));
                    }
                    if (oneSplitPerDiscovery) {
                        int index = cursor.frame().length == 0
                                ? 0 : cursor.frame()[0];
                        if (index >= splitCount) {
                            return new StreamSplitDiscovery(index, cursor,
                                    Collections.emptyList());
                        }
                        return new StreamSplitDiscovery(index + 1L,
                                StreamDiscoveryCursor.of(new byte[] {
                                        (byte) (index + 1)}),
                                Collections.singletonList(
                                        StreamSplitChange.add(
                                                split(index), 1L)));
                    }
                    if (cursor.frame().length != 0) {
                        return new StreamSplitDiscovery(1L, cursor,
                                Collections.emptyList());
                    }
                    java.util.List<StreamSplitChange> changes =
                            new java.util.ArrayList<>();
                    for (int index = 0; index < splitCount; index++) {
                        changes.add(StreamSplitChange.add(
                                split(index), 1L));
                    }
                    return new StreamSplitDiscovery(1L,
                            StreamDiscoveryCursor.of(new byte[] {1}),
                            changes);
                }
                @Override public void close() { }
            };
        }

        private void blockNextDiscovery() {
            blockNextDiscovery.set(true);
        }

        @Override public StreamSourceReader open(
                StreamSourceOpenRequest request) {
            readerOpens.incrementAndGet();
            if (request.hasRestoredPosition()) {
                restoredPositionOpens.incrementAndGet();
            }
            if (holdReadd.get() && request.hasRestoredPosition()) {
                unexpectedRestoredPositions.incrementAndGet();
            }
            if (request.hasRestoredPosition()
                    && request.split().descriptorFrame()[0] == 1) {
                restoredSecondOpens.incrementAndGet();
            }
            boolean finishThis = finishFirstOnly
                    && request.split().descriptorFrame()[0] == 0;
            return new StreamSourceReader() {
                private final Semaphore wakeups = new Semaphore(0);
                private boolean emitted = request.hasRestoredPosition()
                        && ByteBuffer.wrap(request.restoredPositionFrame())
                        .getInt() >= 1;
                private boolean ended;
                @Override public StreamSourceRecord next() throws Exception {
                    if (boundedReaders || finishThis) {
                        throw new AssertionError(
                                "bounded dynamic reader must use nextEvent");
                    }
                    if (!emitted) {
                        emitted = true;
                        recordEvents.incrementAndGet();
                        return StreamSourceRecord.of(
                                request.split().descriptorFrame(),
                                ByteBuffer.allocate(4).putInt(1).array());
                    }
                    wakeups.acquire();
                    return null;
                }
                @Override public io.rowweave.stream.runtime.StreamSourceEvent
                nextEvent() throws Exception {
                    if (!boundedReaders && !finishThis) {
                        return StreamSourceReader.super.nextEvent();
                    }
                    if (!emitted) {
                        emitted = true;
                        recordEvents.incrementAndGet();
                        return io.rowweave.stream.runtime.StreamSourceEvent
                                .record(StreamSourceRecord.of(
                                        request.split().descriptorFrame(),
                                        ByteBuffer.allocate(4)
                                                .putInt(1).array()));
                    }
                    if (!ended) {
                        if (replaceActive
                                && request.split().descriptorFrame()[0] == 0) {
                            wakeups.acquire();
                            return null;
                        }
                        ended = true;
                        if (finishThis) firstEnded.countDown();
                        return io.rowweave.stream.runtime.StreamSourceEvent
                                .endOfInput();
                    }
                    wakeups.acquire();
                    return null;
                }
                @Override public byte[] checkpointPositionFrame() {
                    return ByteBuffer.allocate(4)
                            .putInt(emitted ? 1 : 0).array();
                }
                @Override public void wakeUp() { wakeups.release(); }
                @Override public void cancel() {
                    readerCancels.incrementAndGet();
                    wakeups.release();
                }
                @Override public void close() {
                    if (holdRemoval.get()
                            && request.split().descriptorFrame()[0] == 0) {
                        firstReaderClosed.countDown();
                    }
                    wakeups.release();
                }
            };
        }

        private static StreamSplitSpec split(int index) {
            return new StreamSplitSpec(sha256("split-" + index),
                    "position-v1", new byte[] {(byte) index});
        }
    }

    private static final class CountingSink
            implements EagerStreamSinkProvider {
        private final AtomicInteger records = new AtomicInteger();
        private final CountDownLatch received;

        private CountingSink(int expectedRecords) {
            this.received = new CountDownLatch(expectedRecords);
        }
        @Override public String providerKey() { return "sink"; }
        @Override public String contractDigest() { return repeat('b'); }
        @Override public EagerStreamSinkWriter open(
                StreamSinkOpenRequest request) {
            return new EagerStreamSinkWriter() {
                @Override public void write(byte[] recordFrame) {
                    records.incrementAndGet();
                    received.countDown();
                }
                @Override public void cancel() { }
                @Override public void close() { }
            };
        }
    }

    private static final class BoundedFixedSource
            implements DurableStreamSourceProvider {
        private final AtomicInteger readerOpens = new AtomicInteger();
        private final boolean holdSecond;
        private final CountDownLatch firstEnded = new CountDownLatch(1);

        private BoundedFixedSource() { this(false); }
        private BoundedFixedSource(boolean holdSecond) {
            this.holdSecond = holdSecond;
        }
        @Override public String providerKey() { return "fixed"; }
        @Override public String contractDigest() { return repeat('c'); }
        @Override public StreamSourceReader open(
                StreamSourceOpenRequest request) {
            readerOpens.incrementAndGet();
            boolean second = request.split().descriptorFrame()[0] == 1;
            return new StreamSourceReader() {
                private final Semaphore wakeups = new Semaphore(0);
                private boolean emitted = request.hasRestoredPosition()
                        && ByteBuffer.wrap(request.restoredPositionFrame())
                        .getInt() >= 1;
                private boolean ended;
                @Override public StreamSourceRecord next() {
                    throw new AssertionError(
                            "bounded fixed reader must use nextEvent");
                }
                @Override public io.rowweave.stream.runtime.StreamSourceEvent
                nextEvent() throws Exception {
                    if (!emitted) {
                        emitted = true;
                        return io.rowweave.stream.runtime.StreamSourceEvent
                                .record(StreamSourceRecord.of(
                                        request.split().descriptorFrame(),
                                        ByteBuffer.allocate(4)
                                                .putInt(1).array()));
                    }
                    if (!ended) {
                        if (holdSecond && second) {
                            wakeups.acquire();
                            return io.rowweave.stream.runtime
                                    .StreamSourceEvent.watermark(0L);
                        }
                        ended = true;
                        if (!second) firstEnded.countDown();
                        return io.rowweave.stream.runtime.StreamSourceEvent
                                .endOfInput();
                    }
                    return null;
                }
                @Override public byte[] checkpointPositionFrame() {
                    return ByteBuffer.allocate(4)
                            .putInt(emitted ? 1 : 0).array();
                }
                @Override public void wakeUp() { wakeups.release(); }
                @Override public void cancel() { wakeups.release(); }
                @Override public void close() { wakeups.release(); }
            };
        }
    }

    private static final class IntervalSource
            implements DynamicDurableStreamSourceProvider {
        private final String providerKey;
        private final String contractDigest;
        private final AtomicInteger discoveryCalls = new AtomicInteger();
        private final CountDownLatch secondDiscovery = new CountDownLatch(1);

        private IntervalSource(String providerKey, String contractDigest) {
            this.providerKey = providerKey;
            this.contractDigest = contractDigest;
        }

        @Override public String providerKey() { return providerKey; }
        @Override public String contractDigest() { return contractDigest; }

        @Override public StreamSourceEnumerator openEnumerator(
                StreamSourceEnumeratorOpenRequest request) {
            return new StreamSourceEnumerator() {
                @Override public StreamSplitDiscovery discover(
                        StreamDiscoveryCursor cursor, int limit) {
                    int call = discoveryCalls.incrementAndGet();
                    if (call == 1) {
                        return new StreamSplitDiscovery(1L,
                                StreamDiscoveryCursor.of(new byte[] {1}),
                                Collections.emptyList());
                    }
                    secondDiscovery.countDown();
                    if (cursor.frame().length == 1
                            && cursor.frame()[0] == 1) {
                        return new StreamSplitDiscovery(2L,
                                StreamDiscoveryCursor.of(new byte[] {2}),
                                Collections.singletonList(
                                        StreamSplitChange.add(
                                                DynamicSource.split(0), 1L)));
                    }
                    return new StreamSplitDiscovery(2L, cursor,
                            Collections.emptyList());
                }
                @Override public void close() { }
            };
        }

        @Override public StreamSourceReader open(
                StreamSourceOpenRequest request) {
            return new StreamSourceReader() {
                private final Semaphore wakeups = new Semaphore(0);
                private boolean emitted;
                @Override public StreamSourceRecord next() throws Exception {
                    if (!emitted) {
                        emitted = true;
                        return StreamSourceRecord.of(
                                request.split().descriptorFrame(),
                                ByteBuffer.allocate(4).putInt(1).array());
                    }
                    wakeups.acquire();
                    return null;
                }
                @Override public byte[] checkpointPositionFrame() {
                    return ByteBuffer.allocate(4)
                            .putInt(emitted ? 1 : 0).array();
                }
                @Override public void wakeUp() { wakeups.release(); }
                @Override public void cancel() { wakeups.release(); }
                @Override public void close() { wakeups.release(); }
            };
        }
    }

    private static String sha256(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                    value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(64);
            for (byte current : digest) {
                result.append(Character.forDigit((current >>> 4) & 0xf, 16));
                result.append(Character.forDigit(current & 0xf, 16));
            }
            return result.toString();
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static String repeat(char value) {
        char[] result = new char[64];
        Arrays.fill(result, value);
        return new String(result);
    }
}
