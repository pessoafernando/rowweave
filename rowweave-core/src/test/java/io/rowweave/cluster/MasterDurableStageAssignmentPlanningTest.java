package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.Source;
import io.rowweave.CollectionSource;
import io.rowweave.DataSchema;
import io.rowweave.artifact.InMemoryArtifactRepository;
import io.rowweave.artifact.ArtifactReferenceAuthority;
import io.rowweave.artifact.ArtifactReferenceAuthorityProvider;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.registry.ServiceRegistry;
import io.rowweave.cluster.rpc.InProcessRpcRegistry;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.AttemptOutputProfile;
import io.rowweave.run.DurableStageAssignmentAuthority;
import io.rowweave.run.DurableStageAssignmentAuthorityProvider;
import io.rowweave.run.InMemoryRunStateStore;
import io.rowweave.run.RunStateStore;
import io.rowweave.run.RunStateStoreProvider;
import io.rowweave.run.RunSubmission;
import io.rowweave.run.RunSubmissionCodec;
import io.rowweave.run.StageAcceptanceDecision;
import io.rowweave.run.StageAssignmentAcceptance;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentManifestCodec;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageAssignmentSnapshot;
import io.rowweave.run.StageManifestPublication;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.security.MessageDigest;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** durable physical intent must exist before RUNNING and every ASSIGN. */
class MasterDurableStageAssignmentPlanningTest {
    private static final String HOST = "durable-plan-worker";
    private static final int PORT = 61_277;
    private static final int DATA_PORT = 62_277;
    private static final String INCARNATION = "inc-durable-plan";
    private static final DataSchema META = new DataSchema(new Field("id", DataType.IntegerType));
    private static final DataSchema LEFT = new DataSchema(
            new Field("lk", DataType.IntegerType), new Field("lv", DataType.StringType));
    private static final DataSchema RIGHT = new DataSchema(
            new Field("rk", DataType.IntegerType), new Field("rv", DataType.StringType));
    private static final DataSchema JOIN = new DataSchema(
            new Field("lk", DataType.IntegerType), new Field("lv", DataType.StringType),
            new Field("rk", DataType.IntegerType), new Field("rv", DataType.StringType));

    @Test
    void wholeGraphPublishesIntentWhileDispatchingBeforeTheOnlyAssignment() {
        StageAssignmentManifest manifest = assertManifestPrecedesAssignments(
                "whole", wholeGraph(), 8);
        assertEquals(AttemptOutputProfile.DISCARD, manifest.outputProfile());
    }

    @Test
    void shuffleManifestCoversEveryPhysicalInstanceBeforeTheFirstAssignment() {
        StageAssignmentManifest manifest = assertManifestPrecedesAssignments(
                "shuffle", shuffleGraph(), 16);
        List<StageAssignmentRecord> sources = new ArrayList<>();
        for (StageAssignmentRecord record : manifest.assignments()) {
            if (record.role() == StageAssignmentRole.SOURCE_STAGE) sources.add(record);
        }
        assertEquals(2, sources.size());
        assertEquals(2, sources.get(0).sourceSplitCount());
        assertEquals(1, sources.get(1).sourceSplitCount());
        assertFalse(MessageDigest.isEqual(sources.get(0).sourceOwnershipDigest(),
                sources.get(1).sourceOwnershipDigest()));
    }

    @Test
    void binaryManifestIdentifiesEveryJoinConsumerInstance() {
        StageAssignmentManifest manifest = assertManifestPrecedesAssignments(
                "binary", binaryGraph(), 32);
        assertEquals(2L, manifest.assignments().stream()
                .filter(record -> record.role() == StageAssignmentRole.JOIN_CONSUMER)
                .count());
    }

    @Test
    void multiOutputManifestContainsBothPhysicalSinksWithoutDuplicatingTheSource() {
        StageAssignmentManifest manifest = assertManifestPrecedesAssignments(
                "multi-output", multiOutputGraph(), 32);
        assertEquals(2L, manifest.assignments().stream()
                .filter(record -> record.role() == StageAssignmentRole.SINK).count());
        assertEquals(1L, manifest.assignments().stream()
                .filter(record -> record.role() == StageAssignmentRole.SOURCE_STAGE).count());
    }

    @Test
    void moreThan4096PhysicalAssignmentsFailBeforeArtifactRunOrWorkerSideEffects() {
        InProcessRegistry registry = new InProcessRegistry();
        InMemoryArtifactRepository artifacts = new InMemoryArtifactRepository(
                4, 8L * 1024L * 1024L);
        try (MasterServer master = new MasterServer(
                60_000L, 0, registry,
                io.rowweave.monitor.JobMonitorRegistry.NOOP, artifacts)) {
            RunSubmission submission = new RunSubmission(
                    "run-too-wide", "attempt-too-wide", "publication-too-wide",
                    1, 0L, 10L);
            PlanGraph tooWide = tooWideGraph();
            byte[] envelope = RunSubmissionCodec.encode(submission,
                    PlanArtifactCodec.encode(LegacyPlanArtifactAdapter.wrap(
                            "durable-planning-too-wide", tooWide)));

            RpcMessage result = master.handler().apply(RpcMessage.submitRun(envelope));

            assertFalse(result.success());
            assertTrue(result.errorMessage().contains("exceeds limit"),
                    result.errorMessage());
            assertEquals(0, artifacts.size());
            assertNull(registry.runStateStore().get("run-too-wide"));
            assertTrue(registry.liveWorkers().isEmpty());
        }
    }

    @Test
    void missingAssignmentCapabilityFailsBeforeArtifactRunOrWorkerLookup() {
        RunOnlyRegistry registry = new RunOnlyRegistry();
        InMemoryArtifactRepository artifacts = new InMemoryArtifactRepository(
                4, 8L * 1024L * 1024L);
        try (MasterServer master = new MasterServer(
                60_000L, 0, registry,
                io.rowweave.monitor.JobMonitorRegistry.NOOP, artifacts)) {
            RunSubmission submission = new RunSubmission(
                    "run-no-assignment-authority", "attempt-1", "publication-1",
                    1, 0L, 10L);
            byte[] envelope = RunSubmissionCodec.encode(submission,
                    PlanArtifactCodec.encode(LegacyPlanArtifactAdapter.wrap(
                            "durable-no-assignment-authority", wholeGraph())));

            RpcMessage result = master.handler().apply(RpcMessage.submitRun(envelope));

            assertFalse(result.success());
            assertTrue(result.errorMessage().contains(
                    "stage assignment authority capability unavailable"),
                    result.errorMessage());
            assertEquals(0, artifacts.size());
            assertNull(registry.store.get("run-no-assignment-authority"));
            assertEquals(0, registry.liveWorkerReads);
        }
    }

    private static StageAssignmentManifest assertManifestPrecedesAssignments(
            String suffix, PlanGraph graph, int slots) {
        RecordingRegistry registry = new RecordingRegistry();
        List<RpcMessage> assignments = new CopyOnWriteArrayList<>();
        AtomicBoolean rejectionMatrixChecked = new AtomicBoolean();
        try (MasterServer master = new MasterServer(
                60_000L, 0, registry, io.rowweave.monitor.JobMonitorRegistry.NOOP,
                new InMemoryArtifactRepository(16, 8L * 1024L * 1024L))) {
            InProcessRpcRegistry.Registration endpoint = InProcessRpcRegistry.register(
                    HOST, PORT, request -> {
                        if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                            assertNotNull(registry.published,
                                    "full manifest must be durable before the first ASSIGN");
                            assertNotNull(registry.store.assignmentSnapshot(
                                    request.runAttemptFence()));
                            assignments.add(request);
                            StageCommand assigned = StageCommandCodecV2
                                    .preflight(request.stageCommandBlob(), ignored -> { })
                                    .materialize(io.rowweave.plan.ObjectTrustContext.localTrusted(),
                                            ignored -> { });
                            if (assigned.input() != null) {
                                assertTrue(assigned.input()
                                                instanceof io.rowweave.AssignedSourceBatch,
                                        "every source assignment, including one split, must carry an assigned batch");
                            }
                            assertNotNull(request.stageAssignmentTokenBytes(),
                                    "durable ASSIGN must carry its authority token");
                            io.rowweave.run.StageAssignmentToken token =
                                    StageAssignmentManifestCodec.decodeToken(
                                            request.stageAssignmentTokenBytes());
                            assertEquals(request.stageId(), token.runtimeStageId());
                            if (token.role() == StageAssignmentRole.SOURCE_STAGE
                                    || token.role() == StageAssignmentRole.WHOLE_GRAPH) {
                                assertTrue(token.sourceSplitCount() > 0);
                                assertNotNull(token.sourceOwnershipDigest());
                                assertNotNull(assigned.sourceSplitAssignment());
                                assertEquals(token.sourceSplitCount(),
                                        assigned.sourceSplitAssignment().sourceSplitCount());
                                assertTrue(MessageDigest.isEqual(
                                        token.sourceOwnershipDigest(),
                                        assigned.sourceSplitAssignment().ownershipDigest()));
                            }
                            if (rejectionMatrixChecked.compareAndSet(false, true)) {
                                byte[] wrongDigest = sha256(request.stageCommandBlob());
                                wrongDigest[0] ^= 1;
                                RpcMessage wrongCommand = master.handler().apply(
                                        RpcMessage.assignmentAccepted(
                                                request.workerId(), request.stageId(),
                                                request.stageAssignmentTokenBytes(), wrongDigest,
                                                INCARNATION, request.runAttemptFence()));
                                assertEquals(RpcMessage.Type.JOB_RESULT, wrongCommand.type());
                                RpcMessage wrongWorker = master.handler().apply(
                                        RpcMessage.assignmentAccepted(
                                                "wrong-worker", request.stageId(),
                                                request.stageAssignmentTokenBytes(),
                                                sha256(request.stageCommandBlob()),
                                                INCARNATION, request.runAttemptFence()));
                                assertEquals(RpcMessage.Type.JOB_RESULT, wrongWorker.type());
                                RpcMessage wrongFence = master.handler().apply(
                                        RpcMessage.assignmentAccepted(
                                                request.workerId(), request.stageId(),
                                                request.stageAssignmentTokenBytes(),
                                                sha256(request.stageCommandBlob()),
                                                INCARNATION,
                                                new io.rowweave.run.RunAttemptFence(
                                                        request.runAttemptFence().runId(),
                                                        request.runAttemptFence().attemptId(), 99L)));
                                assertEquals(RpcMessage.Type.JOB_RESULT, wrongFence.type());
                                assertNull(registry.store.assignmentSnapshot(
                                        request.runAttemptFence()).acceptance(
                                        token.physicalOrdinal()));
                            }
                            RpcMessage receipt = master.handler().apply(
                                    RpcMessage.assignmentAccepted(
                                            request.workerId(), request.stageId(),
                                            request.stageAssignmentTokenBytes(),
                                            sha256(request.stageCommandBlob()),
                                            INCARNATION, request.runAttemptFence()));
                            assertEquals(RpcMessage.Type.ACK, receipt.type(),
                                    receipt.errorMessage());
                            assertNotNull(registry.store.assignmentSnapshot(
                                    request.runAttemptFence()).acceptance(
                                    token.physicalOrdinal()));
                            byte[] commandDigest = sha256(request.stageCommandBlob());
                            RpcMessage terminal = master.handler().apply(
                                    RpcMessage.durableStageReport(
                                            request.workerId(),
                                            io.rowweave.run.StageTerminalReceipt.succeeded(
                                                    token, commandDigest, 0L, 0L,
                                                    System.currentTimeMillis())));
                            assertEquals(RpcMessage.Type.ACK, terminal.type(),
                                    terminal.errorMessage());
                        }
                        return RpcMessage.ack();
                    });
            try {
                master.handler().apply(RpcMessage.registerWorker(
                        "w", HOST, PORT, slots, DATA_PORT, INCARNATION));
                RunSubmission submission = new RunSubmission(
                        "run-" + suffix, "attempt-" + suffix,
                        "publication-" + suffix, 1, 0L, 10L);
                byte[] envelope = RunSubmissionCodec.encode(submission,
                        PlanArtifactCodec.encode(LegacyPlanArtifactAdapter.wrap(
                                "durable-planning-" + suffix, graph)));

                RpcMessage result = master.handler().apply(RpcMessage.submitRun(envelope));

                assertTrue(result.success(), result.errorMessage());
                StageAssignmentManifest manifest = registry.published;
                assertNotNull(manifest);
                assertEquals(AttemptStatus.DISPATCHING, registry.statusAtPublish,
                        "manifest publication must precede markRunning");
                assertEquals(master.lastPhysicalPlan().instances().size(),
                        manifest.assignmentCount());
                assertEquals(manifest.assignmentCount(), assignments.size());
                assertExactPhysicalMapping(master.lastPhysicalPlan(), manifest, assignments);
                return manifest;
            } finally {
                endpoint.close();
            }
        }
    }

    private static void assertExactPhysicalMapping(
            PhysicalPlan physical, StageAssignmentManifest manifest,
            List<RpcMessage> assignments) {
        Map<Long, RpcMessage> byRuntimeId = new HashMap<>();
        for (RpcMessage assignment : assignments) {
            byRuntimeId.put(assignment.stageId(), assignment);
        }
        List<PhysicalPlan.Instance> instances = physical.instances();
        for (int ordinal = 0; ordinal < instances.size(); ordinal++) {
            PhysicalPlan.Instance instance = instances.get(ordinal);
            StageAssignmentRecord record = manifest.assignment(ordinal);
            assertEquals(ordinal, record.physicalOrdinal());
            assertEquals(instance.logicalStage(), record.logicalStage());
            assertEquals(instance.instanceIndex(), record.instanceIndex());
            assertEquals(instance.workerId(), record.workerId());
            assertEquals(HOST, record.host());
            assertEquals(PORT, record.rpcPort());
            assertEquals(DATA_PORT, record.dataPort());
            assertEquals(INCARNATION, record.workerIncarnationId());
            if (record.role() == StageAssignmentRole.SOURCE_STAGE
                    || record.role() == StageAssignmentRole.WHOLE_GRAPH) {
                assertTrue(record.sourceSplitCount() > 0);
                assertNotNull(record.sourceOwnershipDigest());
                assertFalse(record.legacySourceOwnership());
            } else {
                assertEquals(0, record.sourceSplitCount());
                assertNull(record.sourceOwnershipDigest());
            }
            RpcMessage assignment = byRuntimeId.get(record.runtimeStageId());
            assertNotNull(assignment, "manifest runtimeStageId must be the routed ASSIGN id");
            assertEquals(record.workerId(), assignment.workerId());
        }
    }

    private static PlanGraph wholeGraph() {
        return new PlanGraph(Arrays.asList(
                DurableTestSources.source(META, Collections.emptyList(), "whole-source")
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(META, 0, new DiscardSink(META)).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output")));
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static PlanGraph shuffleGraph() {
        return new PlanGraph(Arrays.asList(
                PlanNode.sourceFromInputs(META, Arrays.<Source>asList(
                                new CollectionSource(META, "shuffle-source-0", Collections.emptyList()),
                                new CollectionSource(META, "shuffle-source-1", Collections.emptyList()),
                                new CollectionSource(META, "shuffle-source-2", Collections.emptyList())))
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.unary("distinct", META, 0)
                        .listParam("keys", Collections.singletonList("id")).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "distinct"),
                PlanNode.sink(META, 1, new DiscardSink(META)).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output")), 2);
    }

    private static PlanGraph binaryGraph() {
        return new PlanGraph(Arrays.asList(
                DurableTestSources.source(LEFT, Collections.emptyList(), "left-source-split")
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "left-source"),
                DurableTestSources.source(RIGHT, Collections.emptyList(), "right-source-split")
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "right-source"),
                PlanNode.binary("join", JOIN, 0, 1)
                        .param("joinType", "INNER")
                        .listParam("leftKeys", Collections.singletonList("lk"))
                        .listParam("rightKeys", Collections.singletonList("rk"))
                        .build().withParam(PlanNode.OPERATOR_UID_PARAM, "join"),
                PlanNode.sink(JOIN, 2, new DiscardSink(JOIN)).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output")), 2);
    }

    private static PlanGraph multiOutputGraph() {
        return new PlanGraph(Arrays.asList(
                DurableTestSources.source(META, Collections.emptyList(), "multi-output-source")
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.unary("distinct", META, 0)
                        .listParam("keys", Collections.singletonList("id")).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "distinct"),
                PlanNode.sink(META, 1, new DiscardSink(META)).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output-a"),
                PlanNode.sink(META, 1, new DiscardSink(META)).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output-b")), 2);
    }

    private static PlanGraph tooWideGraph() {
        List<Source> inputs = new ArrayList<>();
        for (int i = 0; i < 4096; i++) {
            inputs.add(new CollectionSource(
                    META, "wide-source-" + i, Collections.emptyList()));
        }
        return new PlanGraph(Arrays.asList(
                PlanNode.sourceFromInputs(META, inputs)
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.unary("distinct", META, 0)
                        .listParam("keys", Collections.singletonList("id")).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "distinct"),
                PlanNode.sink(META, 1, new DiscardSink(META)).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output")), 4096);
    }

    private static final class RecordingRegistry implements ServiceRegistry,
            RunStateStoreProvider, DurableStageAssignmentAuthorityProvider,
            ArtifactReferenceAuthorityProvider {
        private final InProcessRegistry workers = new InProcessRegistry();
        private final InMemoryRunStateStore store = new InMemoryRunStateStore();
        private volatile StageAssignmentManifest published;
        private volatile AttemptStatus statusAtPublish;

        private final DurableStageAssignmentAuthority assignments =
                new DurableStageAssignmentAuthority() {
                    @Override
                    public StageManifestPublication publish(StageAssignmentManifest manifest) {
                        statusAtPublish = store.get(manifest.fence().runId())
                                .currentAttempt().status();
                        StageManifestPublication result = store.publish(manifest);
                        if (result.code() == StageManifestPublication.Code.PUBLISHED
                                || result.code() == StageManifestPublication.Code.ALREADY_PUBLISHED) {
                            published = result.snapshot().manifest();
                        }
                        return result;
                    }

                    @Override
                    public StageAssignmentSnapshot assignmentSnapshot(
                            io.rowweave.run.RunAttemptFence fence) {
                        return store.assignmentSnapshot(fence);
                    }

                    @Override
                    public StageAcceptanceDecision accept(StageAssignmentAcceptance acceptance) {
                        return store.accept(acceptance);
                    }

                    @Override
                    public io.rowweave.run.StageTerminalDecision recordTerminal(
                            io.rowweave.run.StageTerminalReceipt receipt) {
                        return store.recordTerminal(receipt);
                    }
                };

        @Override
        public RunStateStore runStateStore() { return store; }

        @Override
        public DurableStageAssignmentAuthority durableStageAssignmentAuthority() {
            return assignments;
        }

        @Override
        public ArtifactReferenceAuthority artifactReferenceAuthority() { return store; }

        @Override
        public void register(RegistryWorker worker) { workers.register(worker); }

        @Override
        public boolean tryClaimExclusive(RegistryWorker worker) {
            return workers.tryClaimExclusive(worker);
        }

        @Override
        public void report(RegistryWorker worker) { workers.report(worker); }

        @Override
        public void renew(String workerId) { workers.renew(workerId); }

        @Override
        public void deregister(String workerId) { workers.deregister(workerId); }

        @Override
        public List<RegistryWorker> liveWorkers() { return workers.liveWorkers(); }

        @Override
        public RegistryWorker worker(String workerId) { return workers.worker(workerId); }

        @Override
        public void watch(WatchListener listener) { workers.watch(listener); }
    }

    private static final class RunOnlyRegistry implements ServiceRegistry,
            RunStateStoreProvider {
        private final InProcessRegistry workers = new InProcessRegistry();
        private final InMemoryRunStateStore store = new InMemoryRunStateStore();
        private final RunStateStore runAuthority = new RunStateStore() {
            @Override
            public io.rowweave.run.RunDecision apply(
                    io.rowweave.run.RunCommand command) {
                return store.apply(command);
            }

            @Override
            public io.rowweave.run.RunSnapshot get(String runId) {
                return store.get(runId);
            }

            @Override
            public List<io.rowweave.run.ExecutionEvent> events(
                    String runId, long afterSequence, int limit) {
                return store.events(runId, afterSequence, limit);
            }
        };
        private int liveWorkerReads;

        @Override
        public RunStateStore runStateStore() { return runAuthority; }

        @Override
        public void register(RegistryWorker worker) { workers.register(worker); }

        @Override
        public boolean tryClaimExclusive(RegistryWorker worker) {
            return workers.tryClaimExclusive(worker);
        }

        @Override
        public void report(RegistryWorker worker) { workers.report(worker); }

        @Override
        public void renew(String workerId) { workers.renew(workerId); }

        @Override
        public void deregister(String workerId) { workers.deregister(workerId); }

        @Override
        public List<RegistryWorker> liveWorkers() {
            liveWorkerReads++;
            return workers.liveWorkers();
        }

        @Override
        public RegistryWorker worker(String workerId) { return workers.worker(workerId); }

        @Override
        public void watch(WatchListener listener) { workers.watch(listener); }
    }
}

