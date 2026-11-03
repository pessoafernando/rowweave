package io.rowweave.stream.cluster;

import io.rowweave.portable.PortableDataType;
import io.rowweave.portable.PortableField;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.stream.api.StreamCheckpointConfig;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamRestartPolicy;
import io.rowweave.stream.contract.PortableStreamJob;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.StreamEdge;
import io.rowweave.stream.contract.StreamContractException;
import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.contract.StreamNodeExecutionDescriptor;
import io.rowweave.stream.contract.StreamSinkBinding;
import io.rowweave.stream.contract.StreamSourceBinding;
import io.rowweave.stream.contract.StreamSourceSplit;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** freeze the node/edge foundation before runtime migration. */
class StreamTopologyNeutralFoundationTest {
    @Test
    void everyPortableOpcodeHasOneNodeLocalExecutionDefinition() {
        assertEquals(EnumSet.allOf(StreamOperatorCode.class),
                StreamOperatorDefinitionRegistry.registeredOpcodes());

        Set<StreamOperatorCode> debt =
                StreamOperatorDefinitionRegistry.legacyShapeAdapterOpcodes();
        assertTrue(debt.isEmpty(),
                "every opcode must use the unified task lifecycle");
        for (StreamOperatorCode opcode : StreamOperatorCode.values()) {
            assertTrue(StreamOperatorDefinitionRegistry.definition(opcode)
                    .statePartitioning() != null,
                    () -> "missing rescale state contract for " + opcode);
        }
    }

    @Test
    void taskFactoryCoversEveryPortableOpcode() {
        EnumSet<StreamOperatorCode> covered = EnumSet.noneOf(
                StreamOperatorCode.class);
        for (StreamOperatorCode opcode : StreamOperatorCode.values()) {
            if (StreamOperatorTaskFactory.supportsInline(opcode)
                    || StreamOperatorTaskFactory.supportsMailbox(opcode)
                    || StreamOperatorTaskFactory.supportsRouting(opcode)
                    || supportsEndpoint(opcode)) {
                covered.add(opcode);
            }
        }
        assertEquals(EnumSet.allOf(StreamOperatorCode.class), covered);
    }

    @Test
    void wholeJobNormalizesIntoImmutableNodeAndEdgeLocalContracts() {
        PortableStreamJob job = job();
        StreamExecutionGraph graph = StreamExecutionGraph.from(job);

        assertEquals(Arrays.asList("source", "sink"), graph.nodeUids());
        StreamExecutionGraph.Node source = graph.node("source");
        StreamExecutionGraph.Node sink = graph.node("sink");
        assertSame(job.plan().nodes().get(0), source.portableNode());
        assertEquals(PortableStreamPlan.ChangelogMode.APPEND,
                source.changelogMode());
        assertEquals(PortableStreamPlan.TimeMode.PROCESSING_TIME,
                source.timeMode());
        assertEquals(2, source.parallelism());
        assertEquals(128, source.maximumParallelism());
        assertTrue(source.inputEdges().isEmpty());
        assertEquals(1, source.outputEdges().size());
        assertEquals(1, sink.inputEdges().size());
        assertTrue(sink.outputEdges().isEmpty());
        assertSame(source.outputEdges().get(0), sink.inputEdges().get(0));
        assertEquals(StreamEdge.Partitioning.FORWARD,
                sink.inputEdges().get(0).partitioning());
        assertTrue(source.resourceRequest().maximumResidentBytes() > 0L);
        assertTrue(source.resourceRequest().maximumFrameBytes() > 0);
    }

    @Test
    void liveRescaleOverridesEveryNodeLocallyWithoutChangingThePortablePlan() {
        PortableStreamJob job = job();

        StreamExecutionGraph target =
                StreamExecutionGraph.forParallelism(job, 3);

        assertSame(job, target.portableJob());
        assertEquals(2, job.plan().parallelism());
        assertEquals(3, target.node("source").parallelism());
        assertEquals(3, target.node("sink").parallelism());
        assertEquals(128, target.node("source").maximumParallelism());
        assertEquals(128, target.node("sink").maximumParallelism());
        assertThrows(IllegalArgumentException.class,
                () -> StreamExecutionGraph.forParallelism(job, 129));
    }

    @Test
    void legacySourceParallelismIsDerivedFromItsFrozenSplits() {
        PortableStreamJob base = job(false);
        PortableStreamNode sourceBase = base.plan().nodes().get(0);
        StreamSourceBinding binding = (StreamSourceBinding)
                sourceBase.binding();
        StreamSourceBinding oneSplit = new StreamSourceBinding(
                binding.providerKey(), binding.providerContractDigest(),
                binding.configurationDigest(), binding.configurationFrame(),
                Collections.singletonList(binding.fixedSplits().get(0)),
                binding.replayGuarantee());
        PortableStreamNode source = new PortableStreamNode(
                sourceBase.operatorUid(), sourceBase.opcode(),
                sourceBase.inputOperatorUids(),
                sourceBase.inputSchemaFingerprints(),
                sourceBase.outputSchema(), sourceBase.stableKeyFields(),
                sourceBase.stateDescriptors(), sourceBase.scalarParameters(),
                oneSplit);
        PortableStreamPlan plan = new PortableStreamPlan(
                base.plan().planVersion(), base.plan().deliveryGuarantee(),
                2, base.plan().maxParallelism(),
                base.plan().changelogMode(), base.plan().timeMode(),
                base.plan().rootSinkOperatorUids(),
                Arrays.asList(source, base.plan().nodes().get(1)),
                base.plan().edges(), base.plan().requiredCapabilities());
        StreamExecutionGraph graph = StreamExecutionGraph.from(
                new PortableStreamJob(plan, base.checkpointConfig(),
                        base.restartPolicy(), base.resourceProfile()));

        assertEquals(1, graph.node("source").parallelism());
        assertEquals(2, graph.node("sink").parallelism());
    }

    @Test
    void placementExpansionUsesEveryNodeSubtaskAndNodeLocalResources() {
        StreamExecutionGraph graph = StreamExecutionGraph.from(job(true));
        java.util.List<StreamTaskPlacementRequest> tasks =
                StreamNodeTaskPlacementPlanner.requests(graph, 1, 1,
                        java.util.Arrays.asList(
                                PortableStreamPlan.STREAM_CAPABILITY,
                                StreamWorkerCapabilityProfile.CONTROL_CAPABILITY,
                                StreamWorkerCapabilityProfile.DATA_CAPABILITY));

        assertEquals(3, tasks.size());
        assertEquals("source", tasks.get(0).identity().operatorUid());
        assertEquals(0, tasks.get(0).identity().subtaskIndex());
        assertEquals("source", tasks.get(1).identity().operatorUid());
        assertEquals(1, tasks.get(1).identity().subtaskIndex());
        assertEquals("sink", tasks.get(2).identity().operatorUid());
        assertEquals(1, tasks.get(0).channels());
        assertEquals(2, tasks.get(2).channels());
        assertEquals(1, tasks.get(0).fileDescriptorCount());
        assertEquals(1, tasks.get(1).fileDescriptorCount());
        assertEquals(4, tasks.get(2).fileDescriptorCount());
        assertEquals(24_576L, tasks.get(0).residentBytes());
        assertEquals(28_672L, tasks.get(2).residentBytes());
        assertEquals(4, tasks.get(0).threads());
        assertEquals(2, tasks.get(2).threads());
    }

    @Test
    void placementRejectsAJobWhosePhysicalSocketsExceedItsFdCeiling() {
        PortableStreamJob base = job(true);
        StreamResourceProfile resources = base.resourceProfile();
        PortableStreamJob oneDescriptor = new PortableStreamJob(
                base.plan(), base.checkpointConfig(), base.restartPolicy(),
                new StreamResourceProfile(resources.mailboxEvents(),
                        resources.maximumFrameBytes(),
                        resources.channelCreditBytes(),
                        resources.managedStateBytes(), resources.timerCount(),
                        resources.windowCount(), resources.checkpointBytes(),
                        resources.threadCount(), 1,
                        resources.spillDiskBytes()));
        StreamExecutionGraph graph = StreamExecutionGraph.from(oneDescriptor);

        assertThrows(IllegalArgumentException.class, () ->
                StreamNodeTaskPlacementPlanner.requests(graph, 1, 1,
                        Arrays.asList(PortableStreamPlan.STREAM_CAPABILITY,
                                StreamWorkerCapabilityProfile
                                        .CONTROL_CAPABILITY,
                                StreamWorkerCapabilityProfile
                                        .DATA_CAPABILITY)));
    }

    @Test
    void placementPreflightRejectsTheWholeFdDemandBeforeTaskExpansion() {
        PortableStreamJob base = job(true);
        PortableStreamNode oldSource = base.plan().nodes().get(0);
        PortableStreamNode oldSink = base.plan().nodes().get(1);
        StreamSourceBinding binding = (StreamSourceBinding)
                oldSource.binding();
        PortableStreamNode source = new PortableStreamNode(
                oldSource.operatorUid(), oldSource.opcode(),
                oldSource.inputOperatorUids(),
                oldSource.inputSchemaFingerprints(),
                oldSource.outputSchema(), oldSource.stableKeyFields(),
                oldSource.stateDescriptors(), oldSource.scalarParameters(),
                new StreamSourceBinding(binding.providerKey(),
                        binding.providerContractDigest(),
                        binding.configurationDigest(),
                        binding.configurationFrame(),
                        Collections.singletonList(binding.fixedSplits().get(0)),
                        binding.replayGuarantee()),
                new StreamNodeExecutionDescriptor(
                        PortableStreamPlan.ChangelogMode.APPEND,
                        PortableStreamPlan.TimeMode.PROCESSING_TIME,
                        1, 4_096, 8_192L, 4_096L, 0L, 2));
        PortableStreamNode sink = new PortableStreamNode(
                oldSink.operatorUid(), oldSink.opcode(),
                oldSink.inputOperatorUids(),
                oldSink.inputSchemaFingerprints(), oldSink.outputSchema(),
                oldSink.stableKeyFields(), oldSink.stateDescriptors(),
                oldSink.scalarParameters(), oldSink.binding(),
                new StreamNodeExecutionDescriptor(
                        PortableStreamPlan.ChangelogMode.APPEND,
                        PortableStreamPlan.TimeMode.PROCESSING_TIME,
                        4_096, 4_096, 4_096L, 4_096L, 0L, 1));
        PortableStreamPlan plan = new PortableStreamPlan(
                base.plan().planVersion(), base.plan().deliveryGuarantee(),
                1, 4_096, base.plan().changelogMode(),
                base.plan().timeMode(), base.plan().rootSinkOperatorUids(),
                Arrays.asList(source, sink), base.plan().edges(),
                base.plan().requiredCapabilities());
        StreamExecutionGraph graph = StreamExecutionGraph.from(
                new PortableStreamJob(plan, base.checkpointConfig(),
                        base.restartPolicy(), base.resourceProfile()));

        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> StreamNodeTaskPlacementPlanner.preflight(graph));
        assertTrue(failure.getMessage().contains("job ceiling"));
    }

    @Test
    void placementPreflightRejectsInsufficientWorkerSlotsBeforeTaskExpansion() {
        StreamExecutionGraph graph = StreamExecutionGraph.from(job(true));
        StreamWorkerCapabilityProfile worker =
                new StreamWorkerCapabilityProfile("worker", "incarnation",
                        1, 1, Arrays.asList(
                        PortableStreamPlan.STREAM_CAPABILITY,
                        PortableStreamPlan.NODE_EXECUTION_CONTRACT_CAPABILITY,
                        StreamWorkerCapabilityProfile.CONTROL_CAPABILITY,
                        StreamWorkerCapabilityProfile.DATA_CAPABILITY),
                        2, 4_096, 4_096, 1_000_000L, 64);

        StreamClusterProtocolException failure = assertThrows(
                StreamClusterProtocolException.class,
                () -> StreamNodeTaskPlacementPlanner.preflightCapacity(
                        graph, Collections.singletonList(worker), 1, 1,
                        worker.capabilities(), (request, profile) -> true));
        assertEquals(StreamClusterProtocolException.Code.INSUFFICIENT_CAPACITY,
                failure.code());
    }

    @Test
    void placementPreflightChargesCoordinatorMetadataBeforeTaskExpansion() {
        StreamExecutionGraph graph = StreamExecutionGraph.from(job(true));

        StreamClusterProtocolException failure = assertThrows(
                StreamClusterProtocolException.class,
                () -> StreamNodeTaskPlacementPlanner.preflight(graph, 1L));
        assertEquals(StreamClusterProtocolException.Code.LIMIT_EXCEEDED,
                failure.code());
    }

    @Test
    void placementPreflightCountsNodeLocalWorkerChannels() {
        StreamExecutionGraph graph = StreamExecutionGraph.from(job(true));
        StreamWorkerCapabilityProfile worker =
                new StreamWorkerCapabilityProfile("worker", "incarnation",
                        1, 1, Arrays.asList(
                        PortableStreamPlan.STREAM_CAPABILITY,
                        PortableStreamPlan.NODE_EXECUTION_CONTRACT_CAPABILITY,
                        StreamWorkerCapabilityProfile.CONTROL_CAPABILITY,
                        StreamWorkerCapabilityProfile.DATA_CAPABILITY),
                        4, 1, 4_096, 1_000_000L, 64);

        StreamClusterProtocolException failure = assertThrows(
                StreamClusterProtocolException.class,
                () -> StreamNodeTaskPlacementPlanner.preflightCapacity(
                        graph, Collections.singletonList(worker), 1, 1,
                        worker.capabilities(), (request, profile) -> true));
        assertEquals(StreamClusterProtocolException.Code.INSUFFICIENT_CAPACITY,
                failure.code());
    }

    @Test
    void placementCountsInboundListenerAndAcceptedSocketSeparately() {
        PortableStreamJob base = job(true);
        StreamResourceProfile resources = base.resourceProfile();
        PortableStreamJob fiveDescriptors = new PortableStreamJob(
                base.plan(), base.checkpointConfig(), base.restartPolicy(),
                new StreamResourceProfile(resources.mailboxEvents(),
                        resources.maximumFrameBytes(),
                        resources.channelCreditBytes(),
                        resources.managedStateBytes(), resources.timerCount(),
                        resources.windowCount(), resources.checkpointBytes(),
                        resources.threadCount(), 5,
                        resources.spillDiskBytes()));

        assertThrows(IllegalArgumentException.class, () ->
                StreamNodeTaskPlacementPlanner.requests(
                        StreamExecutionGraph.from(fiveDescriptors), 1, 1,
                        Arrays.asList(PortableStreamPlan.STREAM_CAPABILITY,
                                StreamWorkerCapabilityProfile
                                        .CONTROL_CAPABILITY,
                                StreamWorkerCapabilityProfile
                                        .DATA_CAPABILITY)));
    }

    @Test
    void jobCheckpointCeilingMustCoverEveryNodePayloadLease() {
        PortableStreamJob base = job(true);
        StreamResourceProfile resources = base.resourceProfile();
        StreamContractException failure = assertThrows(
                StreamContractException.class, () -> new PortableStreamJob(
                        base.plan(), base.checkpointConfig(),
                        base.restartPolicy(), new StreamResourceProfile(
                        resources.mailboxEvents(),
                        resources.maximumFrameBytes(),
                        resources.channelCreditBytes(),
                        resources.managedStateBytes(), resources.timerCount(),
                        resources.windowCount(), 1L,
                        resources.threadCount(),
                        resources.fileDescriptorCount(),
                        resources.spillDiskBytes())));

        assertEquals("INVALID_JOB_RESOURCE_CEILING", failure.code());
    }

    @Test
    void workerAndTaskPlacementEnvelopesCarryFdLeases() throws Exception {
        assertEquals(int.class, StreamTaskPlacementRequest.class
                .getMethod("fileDescriptorCount").getReturnType());
        assertEquals(int.class, StreamWorkerCapabilityProfile.class
                .getMethod("maximumFileDescriptorCount").getReturnType());
        String pool = new String(Files.readAllBytes(sourceRoot().resolve(
                "StreamWorkerCapabilityPool.java")), StandardCharsets.UTF_8);
        String worker = new String(Files.readAllBytes(sourceRoot().resolve(
                "StreamRemoteWorkerServer.java")), StandardCharsets.UTF_8);
        assertTrue(pool.contains("fileDescriptorCount"));
        assertTrue(worker.contains("fileDescriptorCount"));
    }

    @Test
    void physicalPlanExpandsLogicalEdgesBetweenPlacedSubtasks() {
        StreamExecutionGraph graph = StreamExecutionGraph.from(job(true));
        StreamWorkerPlacement placement = new StreamWorkerPlacement(
                Arrays.asList(
                        new StreamWorkerPlacement.Assignment(
                                0, "source", 0, "worker-a", "inc-a"),
                        new StreamWorkerPlacement.Assignment(
                                1, "source", 1, "worker-b", "inc-b"),
                        new StreamWorkerPlacement.Assignment(
                                2, "sink", 0, "worker-c", "inc-c")));

        StreamPhysicalExecutionPlan physical =
                StreamPhysicalExecutionPlan.from(graph, placement);

        assertEquals(3, physical.tasks().size());
        assertEquals(2, physical.lanes().size());
        assertEquals("source", physical.lanes().get(0)
                .source().operatorUid());
        assertEquals(0, physical.lanes().get(0)
                .source().subtaskIndex());
        assertEquals("source", physical.lanes().get(1)
                .source().operatorUid());
        assertEquals(1, physical.lanes().get(1)
                .source().subtaskIndex());
        assertEquals("sink", physical.lanes().get(0)
                .target().operatorUid());
        assertEquals(StreamEdge.Partitioning.FORWARD,
                physical.lanes().get(0).partitioning());
        assertEquals("worker-a", physical.lanes().get(0)
                .sourceAssignment().workerId());
        assertEquals("worker-c", physical.lanes().get(0)
                .targetAssignment().workerId());
        assertTrue(physical.lanes().get(0).physicalChannelId()
                .contains("source#0->sink#0"));
    }

    @Test
    void equalParallelismForwardIsPointwiseWithoutInspectingNodeShape() {
        StreamExecutionGraph graph = StreamExecutionGraph.from(job());
        StreamWorkerPlacement placement = new StreamWorkerPlacement(
                Arrays.asList(
                        new StreamWorkerPlacement.Assignment(
                                0, "source", 0, "worker-a", "inc-a"),
                        new StreamWorkerPlacement.Assignment(
                                1, "source", 1, "worker-b", "inc-b"),
                        new StreamWorkerPlacement.Assignment(
                                2, "sink", 0, "worker-c", "inc-c"),
                        new StreamWorkerPlacement.Assignment(
                                3, "sink", 1, "worker-d", "inc-d")));

        StreamPhysicalExecutionPlan physical =
                StreamPhysicalExecutionPlan.from(graph, placement);

        assertEquals(2, physical.lanes().size());
        assertEquals(0, physical.lanes().get(0).source().subtaskIndex());
        assertEquals(0, physical.lanes().get(0).target().subtaskIndex());
        assertEquals(1, physical.lanes().get(1).source().subtaskIndex());
        assertEquals(1, physical.lanes().get(1).target().subtaskIndex());
    }

    @Test
    void progressAdmissionUsesPhysicalLaneCreditRatherThanTaskCountProxy() {
        PortableStreamJob base = job(true);
        PortableStreamPlan original = base.plan();
        PortableStreamPlan plan = new PortableStreamPlan(
                original.planVersion(), original.deliveryGuarantee(),
                original.parallelism(), original.maxParallelism(),
                original.changelogMode(), original.timeMode(),
                original.rootSinkOperatorUids(), original.nodes(),
                Collections.singletonList(new StreamEdge(
                        "source", "sink", 0, "source-sink",
                        StreamEdge.Partitioning.FORWARD, 1_024,
                        64 * 1024 * 1024)),
                original.requiredCapabilities());
        StreamExecutionGraph graph = StreamExecutionGraph.from(
                new PortableStreamJob(plan, base.checkpointConfig(),
                        base.restartPolicy(), base.resourceProfile()));
        StreamWorkerPlacement placement = new StreamWorkerPlacement(
                Arrays.asList(
                        new StreamWorkerPlacement.Assignment(
                                0, "source", 0, "worker-a", "inc-a"),
                        new StreamWorkerPlacement.Assignment(
                                1, "source", 1, "worker-b", "inc-b"),
                        new StreamWorkerPlacement.Assignment(
                                2, "sink", 0, "worker-c", "inc-c")));
        assertEquals(2, StreamPhysicalExecutionPlan.from(
                graph, placement).lanes().size());
        StreamClusterRunState state = StreamClusterRunState.initial(
                3L, "master", "run", "attempt", 9L, 4L, 5L, placement);
        StreamTaskProgressAggregator aggregator =
                new StreamTaskProgressAggregator(state, graph);
        long observedBytes = 60L * 1024L * 1024L;
        StreamTaskProgress withinOneLane = new StreamTaskProgress(
                "run", "attempt", 9L, 3L, 4L,
                new StreamTaskIdentity("source", 0), 1L, true,
                10L, 0L, observedBytes, 0L, Long.MIN_VALUE, true,
                10L, observedBytes, 0, observedBytes, 0L, 0, 0, 0L);
        assertTrue(aggregator.offer(withinOneLane),
                "one lane below its 64 MiB credit must remain observable");
        StreamTaskProgress aboveOneLane = new StreamTaskProgress(
                "run", "attempt", 9L, 3L, 4L,
                new StreamTaskIdentity("source", 0), 2L, true,
                11L, 0L, 65L * 1024L * 1024L, 0L,
                Long.MIN_VALUE, true, 11L, 65L * 1024L * 1024L,
                0, 65L * 1024L * 1024L, 0L, 0, 0, 0L);
        assertFalse(aggregator.offer(aboveOneLane),
                "one lane above its 64 MiB credit must fail closed");
    }

    @Test
    void explicitNodeSemanticsSurviveWireAndOverrideLegacyPlanDefaults() {
        PortableStreamJob original = job(true);
        PortableStreamJob decoded = io.rowweave.stream.contract
                .PortableStreamJobCodec.decode(io.rowweave.stream.contract
                .PortableStreamJobCodec.encode(original));
        StreamExecutionGraph graph = StreamExecutionGraph.from(decoded);

        assertEquals(2, graph.node("source").parallelism());
        assertEquals(1, graph.node("sink").parallelism());
        assertEquals(64, graph.node("sink").maximumParallelism());
        assertEquals(8_192L,
                graph.node("source").resourceRequest().maximumResidentBytes());
        assertEquals(4_096L,
                graph.node("sink").resourceRequest().maximumResidentBytes());
        assertEquals(2, graph.node("source").resourceRequest().threadCount());
    }

    @Test
    void nodeTaskRuntimeConsumesTheSameResourceAuthorityAsPlacement() {
        PortableStreamJob portable = job(true);
        StreamExecutionGraph graph = StreamExecutionGraph.from(portable);
        StreamExecutionGraph.ResourceRequest lease = graph.node("source")
                .resourceRequest();
        StreamNodeTaskContext context = StreamNodeTaskContext.from(
                "artifact", graph, "source");

        assertEquals(lease.maximumFrameBytes(),
                context.resources().maximumFrameBytes());
        assertEquals(lease.maximumResidentBytes(),
                context.resources().managedStateBytes());
        assertEquals(lease.timerCount(),
                context.resources().timerCount());
        assertEquals(lease.windowCount(),
                context.resources().windowCount());
        assertEquals(lease.checkpointBytes(),
                context.resources().checkpointBytes());
        assertEquals(lease.spillDiskBytes(),
                context.resources().spillDiskBytes());
        assertEquals(lease.threadCount(),
                context.resources().threadCount());
    }

    @Test
    void nodeExecutionExtensionIsCapabilityGatedAndAllNodeComplete() {
        PortableStreamPlan explicit = job(true).plan();
        StreamContractException missingCapability = assertThrows(
                StreamContractException.class, () -> new PortableStreamPlan(
                explicit.planVersion(), explicit.deliveryGuarantee(),
                explicit.parallelism(), explicit.maxParallelism(),
                explicit.changelogMode(), explicit.timeMode(),
                explicit.rootSinkOperatorUids(), explicit.nodes(),
                explicit.edges(), Collections.singletonList(
                PortableStreamPlan.STREAM_CAPABILITY)));
        assertEquals("MISSING_CAPABILITY", missingCapability.code());

        PortableStreamPlan legacy = job(false).plan();
        StreamContractException incomplete = assertThrows(
                StreamContractException.class, () -> new PortableStreamPlan(
                legacy.planVersion(), legacy.deliveryGuarantee(),
                legacy.parallelism(), legacy.maxParallelism(),
                legacy.changelogMode(), legacy.timeMode(),
                legacy.rootSinkOperatorUids(), legacy.nodes(), legacy.edges(),
                Arrays.asList(PortableStreamPlan.STREAM_CAPABILITY,
                        PortableStreamPlan.NODE_EXECUTION_CONTRACT_CAPABILITY)));
        assertEquals("MISSING_NODE_EXECUTION_CONTRACT", incomplete.code());
    }

    @Test
    void legacyShapeDispatcherDebtCannotGrowSilently() throws Exception {
        assertFalse(Files.exists(sourceRoot().resolve(
                "StreamSourceSinkProductPlan.java")),
                "the whole-job shape projection must not exist");
        assertFalse(Files.exists(sourceRoot().resolve(
                "StreamStatelessDagOutput.java")),
                "legacy whole-graph evaluators must not ship in production");
        assertDebt("StreamRemoteWorkerServer.java", 0, 0, 0, 0);
        assertDebt("RemoteStreamSourceSinkRunFactory.java", 0, 0, 0, 0);
        assertFalse(Files.exists(sourceRoot().resolve(
                "StreamClusterExecutor.java")));
        Path runtime = sourceRoot().getParent().resolve("runtime");
        assertFalse(Files.exists(runtime.resolve(
                "ExactlyOnceStreamDagExecutor.java")));
        assertFalse(Files.exists(runtime.resolve("StreamDagExecutor.java")));
    }

    @Test
    void workerStoresExactlyOneOperatorLifecyclePerPhysicalTask()
            throws Exception {
        String source = new String(Files.readAllBytes(sourceRoot().resolve(
                "StreamRemoteWorkerServer.java")), StandardCharsets.UTF_8);
        assertTrue(source.contains(
                "class OperatorTaskSlot implements AutoCloseable"));
        assertTrue(source.contains(
                "private volatile StreamOperatorTask task;"));
        assertFalse(source.contains("class TaskSlot"),
                "a whole-graph task bucket would reintroduce a shape path");
        assertFalse(source.contains("StreamStatelessDagOutput"),
                "the Worker must not execute a graph inside an endpoint task");
        assertFalse(source.contains("startTopologyNeutralSources("));
        assertFalse(source.contains(
                "multiple transactional Stream Sinks share a completion scope"));
        assertFalse(source.contains(
                "final List<StreamWindowOperatorTask> windowTasks;"));
        assertFalse(source.contains(
                "final List<StreamGenericWindowOperatorTask> genericWindowTasks;"));
        assertFalse(source.contains(
                "final List<StreamKeyedAggregateOperatorTask> aggregateTasks;"));
        assertFalse(source.contains(
                "final List<StreamKeyedProcessOperatorTask> processTasks;"));
        assertFalse(source.contains(
                "final StreamTemporalJoinOperatorTask temporalTask;"));
    }

    @Test
    void workerCreatesMailboxOperatorsOnlyThroughNodeFactory()
            throws Exception {
        String worker = new String(Files.readAllBytes(sourceRoot().resolve(
                "StreamRemoteWorkerServer.java")), StandardCharsets.UTF_8);
        String runner = new String(Files.readAllBytes(sourceRoot().resolve(
                "StreamRemoteOperatorTaskRunner.java")), StandardCharsets.UTF_8);
        assertTrue(runner.contains("StreamOperatorTaskFactory.createMailbox("));
        assertFalse(worker.contains("new StreamWindowOperatorTask("));
        assertFalse(worker.contains("new StreamKeyedAggregateOperatorTask("));
        assertFalse(worker.contains("new StreamKeyedProcessOperatorTask("));
        assertFalse(worker.contains("new StreamTemporalJoinOperatorTask("));
        assertFalse(worker.contains(
                "StreamGenericWindowOperatorTask.composed("));
        assertFalse(worker.contains(
                "StreamKeyedProcessOperatorTask.composed("));
        assertFalse(worker.contains("current.snapshot(checkpoint"),
                "control RPC must not bypass the task barrier lifecycle");
        assertTrue(worker.contains(
                "operator.checkpoint(checkpoint)"));
        assertFalse(worker.contains("admitted.maximumFrameBytes()"),
                "endpoint tasks must consume their node-local frame lease");
        assertTrue(worker.contains(
                "node.resourceRequest().maximumFrameBytes()"));
        String factory = new String(Files.readAllBytes(sourceRoot().resolve(
                "StreamOperatorTaskFactory.java")), StandardCharsets.UTF_8);
        assertFalse(factory.contains("instanceof LocalStreamChannel"),
                "mailbox tasks must accept local and remote input gates");
        assertFalse(factory.contains("requires two local mailbox lanes"),
                "temporal join must not be a local-only shape gate");
        String gate = new String(Files.readAllBytes(sourceRoot().resolve(
                "StreamMultiInputGate.java")), StandardCharsets.UTF_8);
        assertFalse(gate.contains("List<LocalStreamChannel>"),
                "multi-input gate must accept remote input lanes");
        String genericWindow = new String(Files.readAllBytes(
                sourceRoot().resolve("StreamGenericWindowOperatorTask.java")),
                StandardCharsets.UTF_8);
        assertFalse(genericWindow.contains("LocalStreamChannel"),
                "processing-time windows must poll the transport-neutral gate");
    }

    @Test
    void endpointsAndRoutingNodesUseTheSameTaskFactoryAndLifecycle()
            throws Exception {
        String worker = new String(Files.readAllBytes(sourceRoot().resolve(
                "StreamRemoteWorkerServer.java")), StandardCharsets.UTF_8);
        String runner = new String(Files.readAllBytes(sourceRoot().resolve(
                "StreamRemoteOperatorTaskRunner.java")), StandardCharsets.UTF_8);
        assertTrue(worker.contains(
                "StreamOperatorTaskFactory.createEndpoint("));
        assertTrue(runner.contains(
                "StreamOperatorTaskFactory.createRouting("));
        assertTrue(runner.contains(
                "StreamOperatorTaskFactory.createMailbox("));
        assertTrue(worker.contains("private volatile StreamOperatorTask task"));
        assertFalse(worker.contains("volatile LocalStreamSourceTask"));
        assertFalse(worker.contains("volatile LocalStreamSinkTask"));
        assertFalse(worker.contains(
                "volatile LocalTransactionalStreamSinkTask"));
    }

    @Test
    void workerAdmitsThePortableExecutionGraphWithoutAProductShapeProjection()
            throws Exception {
        String source = new String(Files.readAllBytes(sourceRoot().resolve(
                "StreamRemoteWorkerServer.java")), StandardCharsets.UTF_8);
        assertFalse(source.contains("StreamSourceSinkProductPlan"));
        assertTrue(source.contains("StreamProductExecutionGraph.decode("));

        String graph = new String(Files.readAllBytes(sourceRoot().resolve(
                "StreamProductExecutionGraph.java")), StandardCharsets.UTF_8);
        for (String token : StreamShapeGateDebtManifest
                .shapePredicateTokens()) {
            assertFalse(graph.contains(token), token);
        }
        assertFalse(graph.contains("unaryStatefulNode"));
        assertFalse(graph.contains("requiresComposed"));
        assertFalse(graph.contains("workerGraphRequest("),
                "resource admission must be node/subtask-local");
        assertFalse(graph.contains("sourceCheckpointKinds("),
                "checkpoint closure must use actual task participants");
        assertFalse(graph.contains("boolean isResidentCdc()"),
                "source implementation selection must remain node-local");
    }

    @Test
    void statefulTasksNeverClassifyTheirImmediatePredecessorByOpcode()
            throws Exception {
        Pattern predecessorShapeGate = Pattern.compile(
                "producer\\(\\)\\s*\\.opcode\\(\\)\\s*[!=]=?\\s*"
                        + "StreamOperatorCode\\.KEY_BY");
        try (Stream<Path> files = Files.walk(sourceRoot())) {
            files.filter(path -> path.toString().endsWith(".java"))
                    .forEach(path -> {
                        try {
                            String source = new String(
                                    Files.readAllBytes(path),
                                    StandardCharsets.UTF_8);
                            assertFalse(predecessorShapeGate.matcher(source)
                                            .find(), path.toString());
                        } catch (java.io.IOException failure) {
                            throw new IllegalStateException(failure);
                        }
                    });
        }
    }

    @Test
    void workerTaskAndFenceOwnershipAreRunScoped() throws Exception {
        String server = new String(Files.readAllBytes(sourceRoot().resolve(
                "StreamRemoteWorkerServer.java")), StandardCharsets.UTF_8);
        assertTrue(server.contains(
                "Map<OperatorTaskKey, OperatorTaskSlot>"));
        assertFalse(server.contains(
                "Map<StreamTaskIdentity, OperatorTaskSlot>"));
        assertTrue(server.contains("closeOperatorTasks(command)"));
        assertTrue(server.contains("WorkerResourceLeases"));

        String worker = new String(Files.readAllBytes(sourceRoot().resolve(
                "StreamClusterWorker.java")), StandardCharsets.UTF_8);
        assertTrue(worker.contains("Map<String, ControlFence>"));
        assertFalse(worker.contains("private ControlFence controlFence;"));
    }

    @Test
    void masterRunFactoryDoesNotClassifyAWholeJobShape() throws Exception {
        String source = new String(Files.readAllBytes(sourceRoot().resolve(
                "RemoteStreamSourceSinkRunFactory.java")),
                StandardCharsets.UTF_8);
        assertFalse(source.contains("StreamSourceSinkProductPlan"));
        assertTrue(source.contains("StreamProductExecutionGraph.from("));
        assertFalse(source.contains("fixedSplits().size() != 1"),
                "offline rescale must migrate split-local positions");
        assertTrue(source.contains("StreamSourceSplitAssigner.assign("));

        String worker = new String(Files.readAllBytes(sourceRoot().resolve(
                "StreamRemoteWorkerServer.java")), StandardCharsets.UTF_8);
        assertTrue(worker.contains("sourceRestoreMember(restoreMembers,"));
        assertTrue(worker.contains(
                "split.splitId().equals(member.subjectId())"));
    }

    private static void assertDebt(String file, int temporal, int coProcess,
            int serialStateful, int validation) throws Exception {
        String source = new String(Files.readAllBytes(sourceRoot().resolve(file)),
                StandardCharsets.UTF_8);
        assertEquals(temporal, occurrences(source, "isTemporalJoin("), file);
        assertEquals(coProcess, occurrences(source, "isKeyedCoProcess("), file);
        assertEquals(serialStateful,
                occurrences(source, "hasSerialUnaryStatefulGraph("), file);
        assertEquals(validation,
                occurrences(source, "validateStatefulShape("), file);
    }

    private static int occurrences(String text, String token) {
        int count = 0;
        int offset = 0;
        while ((offset = text.indexOf(token, offset)) >= 0) {
            count++;
            offset += token.length();
        }
        return count;
    }

    private static boolean supportsEndpoint(StreamOperatorCode opcode) {
        try {
            java.lang.reflect.Method method = StreamOperatorTaskFactory.class
                    .getDeclaredMethod("supportsEndpoint",
                            StreamOperatorCode.class);
            method.setAccessible(true);
            return (Boolean) method.invoke(null, opcode);
        } catch (ReflectiveOperationException missing) {
            return false;
        }
    }

    private static Path sourceRoot() {
        Path cwd = Paths.get("").toAbsolutePath();
        Path local = cwd.resolve("src/main/java/io/rowweave/stream/cluster");
        return Files.isDirectory(local) ? local : cwd.resolve(
                "rowweave-core/src/main/java/io/rowweave/stream/cluster");
    }

    private static PortableStreamJob job() {
        return job(false);
    }

    private static PortableStreamJob job(boolean explicitNodeContracts) {
        PortableRowSchema schema = new PortableRowSchema(Collections.singletonList(
                new PortableField("id", "id", "id",
                        PortableDataType.LONG, false)));
        byte[] configuration = new byte[0];
        PortableStreamNode source = new PortableStreamNode(
                "source", StreamOperatorCode.SOURCE,
                Collections.emptyList(), Collections.emptyList(), schema,
                Collections.emptyList(), Collections.emptyList(),
                Collections.emptyMap(), new StreamSourceBinding(
                "source-provider", repeat('a'), sha256(configuration),
                configuration, Arrays.asList(
                new StreamSourceSplit(repeat('b'), "position-v1",
                        new byte[] {1}),
                new StreamSourceSplit(repeat('d'), "position-v1",
                        new byte[] {2})),
                StreamSourceBinding.ReplayGuarantee.EXACT_ORDERED),
                explicitNodeContracts ? new StreamNodeExecutionDescriptor(
                        PortableStreamPlan.ChangelogMode.APPEND,
                        PortableStreamPlan.TimeMode.PROCESSING_TIME,
                        2, 128, 8_192L, 4_096L, 0L, 2) : null);
        PortableStreamNode sink = new PortableStreamNode(
                "sink", StreamOperatorCode.SINK,
                Collections.singletonList("source"),
                Collections.singletonList(schema.fingerprint()), schema,
                Collections.emptyList(), Collections.emptyList(),
                Collections.emptyMap(), new StreamSinkBinding(
                "sink-provider", repeat('c'), sha256(configuration),
                configuration,
                StreamSinkBinding.VisibilityProfile.EAGER_AT_LEAST_ONCE),
                explicitNodeContracts ? new StreamNodeExecutionDescriptor(
                        PortableStreamPlan.ChangelogMode.APPEND,
                        PortableStreamPlan.TimeMode.PROCESSING_TIME,
                        1, 64, 4_096L, 4_096L, 0L, 1) : null);
        PortableStreamPlan plan = new PortableStreamPlan(
                PortableStreamPlan.CURRENT_VERSION,
                PortableStreamPlan.DeliveryGuarantee.AT_LEAST_ONCE,
                2, 128, PortableStreamPlan.ChangelogMode.APPEND,
                PortableStreamPlan.TimeMode.PROCESSING_TIME,
                Collections.singletonList("sink"), Arrays.asList(source, sink),
                Collections.singletonList(new StreamEdge(
                "source", "sink", 0, "source-sink",
                StreamEdge.Partitioning.FORWARD, 1_024, 4_096)),
                explicitNodeContracts ? Arrays.asList(
                        PortableStreamPlan.STREAM_CAPABILITY,
                        PortableStreamPlan.NODE_EXECUTION_CONTRACT_CAPABILITY)
                        : Collections.singletonList(
                        PortableStreamPlan.STREAM_CAPABILITY));
        return new PortableStreamJob(plan,
                StreamCheckpointConfig.manualOnly(10_000L, 1),
                StreamRestartPolicy.fixedDelay(1, 0L),
                StreamResourceProfile.defaults());
    }

    private static String repeat(char value) {
        char[] chars = new char[64];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static String sha256(byte[] value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value);
            StringBuilder result = new StringBuilder(64);
            for (byte current : digest) {
                result.append(String.format("%02x", current & 0xff));
            }
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
