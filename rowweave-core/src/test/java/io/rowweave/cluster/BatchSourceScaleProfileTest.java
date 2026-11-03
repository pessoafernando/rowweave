package io.rowweave.cluster;

import io.rowweave.AssignedSourceBatch;
import io.rowweave.BatchSourceSplitBinding;
import io.rowweave.CollectionSource;
import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.Source;
import io.rowweave.DataSchema;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.plan.BatchSourcePlanValidator;
import io.rowweave.plan.InputSharding;
import io.rowweave.plan.ObjectTrustContext;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanGraphEnvelopeScanner;
import io.rowweave.plan.PlanNode;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.AttemptOutputProfile;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentManifestCodec;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageAssignmentSnapshot;
import io.rowweave.run.StageAssignmentToken;
import io.rowweave.run.SourceSplitAssignment;
import io.rowweave.sched.net.ShuffleEndpoint;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** bounded source control-plane profile at the declared split scales. */
@Tag("profile")
class BatchSourceScaleProfileTest {
    private static final int[] SPLIT_COUNTS = {1, 64, 512, 4096};
    private static final int MAX_PARALLELISM = 8;
    private static final long CONTROL_RESIDENT_LIMIT = 256L * 1024L * 1024L;
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType));

    @Test
    void artifactAssignmentTokenAndCommandRemainBoundedThrough4096Splits() {
        for (int splitCount : SPLIT_COUNTS) {
            ProfileResult result = profile(splitCount);
            System.out.printf("[SOURCE-V2-PROFILE] splits=%d artifact=%dB "
                            + "root=%dB maxShard=%dB maxToken=%dB maxCommand=%dB "
                            + "resident=%dB%n",
                    splitCount, result.artifactBytes, result.rootBytes,
                    result.maxShardBytes, result.maxTokenBytes,
                    result.maxCommandBytes, result.maxResidentBytes);
        }
    }

    private static ProfileResult profile(int splitCount) {
        List<Source> inputs = new ArrayList<>(splitCount);
        for (int i = 0; i < splitCount; i++) {
            inputs.add(new CollectionSource(
                    META, String.format("split-%04d", i), Collections.emptyList()));
        }
        int parallelism = Math.min(MAX_PARALLELISM, splitCount);
        PlanGraph graph = new PlanGraph(Arrays.asList(
                PlanNode.sourceFromInputs(META, inputs)
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(META, 0, new DiscardSink(META)).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output")), parallelism);
        BatchSourcePlanValidator.validateDurable(graph);

        PlanArtifact artifact = LegacyPlanArtifactAdapter.wrap(
                "source-scale-" + splitCount, graph);
        byte[] artifactFrame = PlanArtifactCodec.encode(artifact);
        assertTrue(artifactFrame.length <= PlanGraphEnvelopeScanner.MAX_PLAN_BYTES);
        AtomicLong artifactResident = new AtomicLong();
        PlanArtifact decoded = PlanArtifactCodec.decode(
                artifactFrame, artifactResident::addAndGet);
        PlanGraph restored = LegacyPlanArtifactAdapter.unwrapValidated(
                decoded, ObjectTrustContext.localTrusted(
                        BatchSourceScaleProfileTest.class.getClassLoader()),
                artifactResident::addAndGet);
        assertEquals(splitCount,
                restored.node(0).sourceSplitBindings().size());
        assertTrue(artifactResident.get() <= CONTROL_RESIDENT_LIMIT,
                "artifact resident bound at splits=" + splitCount);

        ExecutionGraph execution = ExecutionGraphBuilder.buildWithSinkBoundary(restored);
        List<RegistryWorker> workers = workers();
        PhysicalPlan physical = StageAssignment.assign(
                execution, parallelism, workerIds(workers));
        RunAttemptFence fence = new RunAttemptFence(
                "run-" + splitCount, "attempt-1", 1L);
        AtomicLong runtimeId = new AtomicLong(1000L);
        DurableStageAssignmentPlan planned = DurableStageAssignmentPlan.create(
                fence, decoded.artifactId(), restored, execution, physical,
                workers, runtimeId::incrementAndGet);

        StageAssignmentManifest manifest = planned.manifest();
        assertEquals(AttemptOutputProfile.DISCARD, manifest.outputProfile());
        byte[] root = StageAssignmentManifestCodec.encodeRoot(manifest);
        assertTrue(root.length <= StageAssignmentManifestCodec.MAX_ROOT_FRAME_BYTES);
        List<byte[]> shards = new ArrayList<>();
        int maxShard = 0;
        for (int i = 0; i < manifest.shardCount(); i++) {
            byte[] shard = StageAssignmentManifestCodec.encodeShard(manifest, i);
            shards.add(shard);
            maxShard = Math.max(maxShard, shard.length);
            assertTrue(shard.length <= StageAssignmentManifestCodec.MAX_SHARD_FRAME_BYTES);
        }
        StageAssignmentManifest roundTrip =
                StageAssignmentManifestCodec.decode(root, shards);
        assertArrayEquals(root, StageAssignmentManifestCodec.encodeRoot(roundTrip));
        for (int i = 0; i < shards.size(); i++) {
            assertArrayEquals(shards.get(i),
                    StageAssignmentManifestCodec.encodeShard(roundTrip, i));
        }

        List<List<BatchSourceSplitBinding>> buckets = InputSharding.shardSourceBindings(
                restored.node(0).sourceSplitBindings(), parallelism);
        int ownedSplits = 0;
        int maxToken = 0;
        int maxCommand = 0;
        long maxCommandResident = 0L;
        StageAssignmentSnapshot snapshot = StageAssignmentSnapshot.initial(manifest);
        for (StageAssignmentRecord record : manifest.assignments()) {
            StageAssignmentToken token = snapshot.token(record.physicalOrdinal());
            byte[] tokenFrame = StageAssignmentManifestCodec.encodeToken(token);
            maxToken = Math.max(maxToken, tokenFrame.length);
            assertTrue(tokenFrame.length <= StageAssignmentManifestCodec.MAX_TOKEN_FRAME_BYTES);
            assertEquals(token, StageAssignmentManifestCodec.decodeToken(tokenFrame));
            if (record.role() != StageAssignmentRole.SOURCE_STAGE) continue;

            List<BatchSourceSplitBinding> bucket = buckets.get(record.instanceIndex());
            SourceSplitAssignment ownership = planned.sourceAssignment(
                    record.runtimeStageId());
            assertEquals(bucket.size(), ownership.sourceSplitCount());
            ownedSplits += bucket.size();
            StageCommand command = StageCommand.sourceStage(
                            META, META, 8, new AssignedSourceBatch(bucket),
                            placeholderFragment(), new ShuffleEndpoint("127.0.0.1", 7301))
                    .withRunAttemptFence(fence)
                    .withSourceSplitAssignment(ownership);
            AtomicLong commandResident = new AtomicLong();
            byte[] commandFrame = StageCommandCodecV2.encode(
                    command, ObjectTrustContext.localTrusted(
                            BatchSourceScaleProfileTest.class.getClassLoader()),
                    commandResident::addAndGet);
            maxCommand = Math.max(maxCommand, commandFrame.length);
            PreparedStageCommand prepared = StageCommandCodecV2.preflight(
                    commandFrame, commandResident::addAndGet);
            assertEquals(bucket.size(),
                    prepared.sourceSplitAssignment().sourceSplitCount());
            StageCommand materialized = prepared.materialize(
                    ObjectTrustContext.localTrusted(
                            BatchSourceScaleProfileTest.class.getClassLoader()),
                    commandResident::addAndGet);
            assertEquals(bucket.size(),
                    ((AssignedSourceBatch) materialized.input()).bindings().size());
            maxCommandResident = Math.max(maxCommandResident, commandResident.get());
            assertTrue(commandResident.get() <= CONTROL_RESIDENT_LIMIT,
                    "command resident bound at splits=" + splitCount);
        }
        assertEquals(splitCount, ownedSplits);
        return new ProfileResult(artifactFrame.length, root.length, maxShard,
                maxToken, maxCommand,
                Math.max(artifactResident.get(), maxCommandResident));
    }

    private static PlanGraph placeholderFragment() {
        return new PlanGraph(Collections.singletonList(PlanNode.placeholderSource(META)));
    }

    private static List<RegistryWorker> workers() {
        List<RegistryWorker> workers = new ArrayList<>();
        for (int i = 0; i < MAX_PARALLELISM; i++) {
            workers.add(new RegistryWorker("worker-" + i, "127.0.0.1",
                    7100 + i, 32, 7200 + i, "inc-" + i));
        }
        return workers;
    }

    private static List<String> workerIds(List<RegistryWorker> workers) {
        List<String> ids = new ArrayList<>(workers.size());
        for (RegistryWorker worker : workers) ids.add(worker.workerId());
        return ids;
    }

    private static final class ProfileResult {
        final int artifactBytes;
        final int rootBytes;
        final int maxShardBytes;
        final int maxTokenBytes;
        final int maxCommandBytes;
        final long maxResidentBytes;

        ProfileResult(int artifact, int root, int shard, int token,
                      int command, long resident) {
            artifactBytes = artifact;
            rootBytes = root;
            maxShardBytes = shard;
            maxTokenBytes = token;
            maxCommandBytes = command;
            maxResidentBytes = resident;
        }
    }
}
