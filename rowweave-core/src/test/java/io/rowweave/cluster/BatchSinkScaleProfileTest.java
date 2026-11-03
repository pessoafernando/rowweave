package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.ManifestFileSink;
import io.rowweave.DataSchema;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.plan.ObjectTrustContext;
import io.rowweave.plan.OutputCommitValidator;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanGraphEnvelopeScanner;
import io.rowweave.plan.PlanNode;
import io.rowweave.run.AttemptOutputProfile;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentManifestCodec;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** executable Sink plan profile up to the 4096-assignment hard limit. */
@Tag("profile")
class BatchSinkScaleProfileTest {
    private static final int[] SINK_COUNTS = {1, 64, 512, 4095};
    private static final long CONTROL_RESIDENT_LIMIT = 256L * 1024L * 1024L;
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType));

    @Test
    void artifactAndAssignmentFramesRemainBoundedAtMaximumExecutableSinkCount() {
        for (int sinkCount : SINK_COUNTS) {
            ProfileResult result = profile(sinkCount);
            System.out.printf("[SINK-V2-PLAN-PROFILE] sinks=%d assignments=%d "
                            + "artifact=%dB root=%dB maxShard=%dB maxToken=%dB "
                            + "resident=%dB%n",
                    sinkCount, result.assignments, result.artifactBytes,
                    result.rootBytes, result.maxShardBytes,
                    result.maxTokenBytes, result.residentBytes);
        }
    }

    private static ProfileResult profile(int sinkCount) {
        String domain = Paths.get(System.getProperty("java.io.tmpdir"),
                "rowweave-sink-v2-profile").toString();
        List<PlanNode> nodes = new ArrayList<>(sinkCount + 1);
        nodes.add(DurableTestSources.source(
                        META, Collections.emptyList(), "sink-profile-source")
                .withParam(PlanNode.OPERATOR_UID_PARAM, "source"));
        for (int sink = 0; sink < sinkCount; sink++) {
            nodes.add(PlanNode.sink(META, 0, new ManifestFileSink(
                            META, domain, String.format("member-%04d", sink)))
                    .build().withParam(
                            PlanNode.OPERATOR_UID_PARAM,
                            String.format("output-%04d", sink)));
        }
        PlanGraph graph = new PlanGraph(nodes, 1);
        OutputCommitValidator.validateDurable(graph);

        PlanArtifact artifact = LegacyPlanArtifactAdapter.wrap(
                "sink-scale-" + sinkCount, graph);
        byte[] artifactFrame = PlanArtifactCodec.encode(artifact);
        assertTrue(artifactFrame.length <= PlanGraphEnvelopeScanner.MAX_PLAN_BYTES);
        AtomicLong resident = new AtomicLong();
        PlanArtifact decoded = PlanArtifactCodec.decode(
                artifactFrame, resident::addAndGet);
        PlanGraph restored = LegacyPlanArtifactAdapter.unwrapValidated(
                decoded, ObjectTrustContext.localTrusted(
                        BatchSinkScaleProfileTest.class.getClassLoader()),
                resident::addAndGet);
        assertEquals(sinkCount, restored.sinkCount());
        assertTrue(resident.get() <= CONTROL_RESIDENT_LIMIT);

        ExecutionGraph execution =
                ExecutionGraphBuilder.buildWithSinkBoundary(restored);
        List<RegistryWorker> workers = Collections.singletonList(
                new RegistryWorker("worker", "127.0.0.1",
                        7100, 32, 7200, "incarnation"));
        PhysicalPlan physical = StageAssignment.assign(
                execution, 1, Collections.singletonList("worker"));
        DurableStageAssignmentPlan assignment = DurableStageAssignmentPlan.create(
                new RunAttemptFence(
                        "run-" + sinkCount, "attempt-1", 1L),
                decoded.artifactId(), restored, execution, physical, workers,
                new AtomicLong(1000L)::incrementAndGet);
        StageAssignmentManifest manifest = assignment.manifest();
        assertEquals(AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1,
                manifest.outputProfile());
        assertEquals(sinkCount + 1, manifest.assignmentCount());
        assertTrue(manifest.assignmentCount()
                <= StageAssignmentManifest.MAX_ASSIGNMENTS);

        byte[] root = StageAssignmentManifestCodec.encodeRoot(manifest);
        int maxShard = 0;
        int maxToken = 0;
        for (int shard = 0; shard < manifest.shardCount(); shard++) {
            byte[] encoded = StageAssignmentManifestCodec.encodeShard(manifest, shard);
            maxShard = Math.max(maxShard, encoded.length);
            assertTrue(encoded.length
                    <= StageAssignmentManifestCodec.MAX_SHARD_FRAME_BYTES);
        }
        io.rowweave.run.StageAssignmentSnapshot snapshot =
                io.rowweave.run.StageAssignmentSnapshot.initial(manifest);
        for (int ordinal = 0; ordinal < manifest.assignmentCount(); ordinal++) {
            byte[] token = StageAssignmentManifestCodec.encodeToken(
                    snapshot.token(ordinal));
            maxToken = Math.max(maxToken, token.length);
            assertTrue(token.length
                    <= StageAssignmentManifestCodec.MAX_TOKEN_FRAME_BYTES);
        }
        return new ProfileResult(manifest.assignmentCount(), artifactFrame.length,
                root.length, maxShard, maxToken, resident.get());
    }

    private static final class ProfileResult {
        final int assignments;
        final int artifactBytes;
        final int rootBytes;
        final int maxShardBytes;
        final int maxTokenBytes;
        final long residentBytes;

        ProfileResult(int assignments, int artifactBytes, int rootBytes,
                      int maxShardBytes, int maxTokenBytes, long residentBytes) {
            this.assignments = assignments;
            this.artifactBytes = artifactBytes;
            this.rootBytes = rootBytes;
            this.maxShardBytes = maxShardBytes;
            this.maxTokenBytes = maxTokenBytes;
            this.residentBytes = residentBytes;
        }
    }
}
