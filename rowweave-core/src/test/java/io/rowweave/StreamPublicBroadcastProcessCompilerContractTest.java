package io.rowweave;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.stream.api.StreamBroadcastStateSpec;
import io.rowweave.stream.api.StreamCoProcessSpec;
import io.rowweave.stream.api.StreamEmissionLimit;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.contract.PortableStreamJob;
import io.rowweave.stream.contract.PortableStreamJobArtifact;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.StreamEdge;
import io.rowweave.stream.contract.StreamProcessBinding;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** broadcast input is version-frozen and resource bounded. */
class StreamPublicBroadcastProcessCompilerContractTest {

    @Test
    void immutableSnapshotCompilesToBroadcastEdgeAndStateProfile() {
        StreamBroadcastStateSpec broadcast =
                StreamBroadcastStateSpec.immutableSnapshot(
                        "rules", "snapshot-42", sha256("rules-snapshot"),
                        "rule-map-v1", 1, 10_000L, 16L * 1024L * 1024L,
                        64L * 1024L, 2);
        StreamCoProcessSpec process = baseSpec().withBroadcastRight(broadcast);
        StreamSession session = RowWeave.createStreamSession("broadcast-process");
        DataStream main = session.fromSource(source("main", mainSchema()))
                .uid("main").keyBy("id").uid("main-key");
        DataStream rules = session.fromSource(source("rules", ruleSchema()))
                .uid("rules");
        main.keyedCoProcess(rules, process).uid("process")
                .sinkTo("sink", sink());

        PlanArtifact artifact = session.toPlanArtifact();
        PortableStreamJob job = PortableStreamJobArtifact.decodeValidated(artifact);
        PortableStreamNode node = job.plan().nodes().get(3);
        StreamProcessBinding binding = (StreamProcessBinding) node.binding();
        assertEquals("snapshot-42",
                binding.broadcastState().snapshotOrVersionId());
        assertEquals(10_000L, binding.broadcastState().maximumEntries());
        assertEquals(StreamEdge.Partitioning.BROADCAST,
                edge(job, "process", 1).partitioning());
        assertEquals(StreamEdge.Partitioning.FORWARD,
                edge(job, "process", 0).partitioning());
        assertTrue(job.plan().requiredCapabilities().contains(
                PortableStreamPlan.BROADCAST_STATE_CAPABILITY));
        assertTrue(artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_BROADCAST_STATE_V1));
    }

    @Test
    void broadcastAuthorityAndResourceBoundsFailClosed() {
        assertThrows(IllegalArgumentException.class, () ->
                StreamBroadcastStateSpec.immutableSnapshot(
                        "rules", "snapshot", "not-a-digest", "rule-v1", 1,
                        10L, 1024L, 128L, 1));
        assertThrows(IllegalArgumentException.class, () ->
                StreamBroadcastStateSpec.immutableSnapshot(
                        "rules", "snapshot", sha256("snapshot"), "rule-v1", 1,
                        10L, 1024L, 2048L, 1));
        assertThrows(IllegalArgumentException.class, () ->
                StreamBroadcastStateSpec.replayableChangelog(
                        "rules", "", sha256("version"), "rule-v1", 1,
                        10L, 1024L, 128L, 1));
    }

    private static StreamCoProcessSpec baseSpec() {
        return StreamCoProcessSpec.keyed(reference(), outputSchema(),
                Collections.singletonList("id"),
                Collections.emptyList(), Collections.emptyList(),
                StreamEmissionLimit.of(1, 4096L),
                StreamEmissionLimit.of(1, 4096L),
                StreamEmissionLimit.of(1, 4096L));
    }

    private static StreamEdge edge(PortableStreamJob job, String target,
            int index) {
        for (StreamEdge edge : job.plan().edges()) {
            if (edge.targetOperatorUid().equals(target)
                    && edge.targetInputIndex() == index) return edge;
        }
        throw new AssertionError("edge not found");
    }

    private static StreamSchema mainSchema() {
        return new StreamSchema(Collections.singletonList(
                new StreamField("id", "id", "id", DataType.LongType, false)));
    }

    private static StreamSchema ruleSchema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("rule-id", "ruleId", "ruleId",
                        DataType.StringType, false),
                new StreamField("value", "value", "value",
                        DataType.StringType, true)));
    }

    private static StreamSchema outputSchema() { return mainSchema(); }

    private static StreamSourceSpec source(String id, StreamSchema schema) {
        return StreamSourceSpec.exactOrdered(schema, "source-v1",
                sha256("source-contract"), new byte[] {(byte) id.length()},
                Collections.singletonList(new StreamSplitSpec(
                        sha256("split-" + id), "sequence-v1", new byte[] {2})));
    }

    private static StreamSinkSpec sink() {
        return StreamSinkSpec.eagerAtLeastOnce(
                "sink-v1", sha256("sink-contract"), new byte[] {3});
    }

    private static FunctionDeploymentReference reference() {
        byte[] config = new byte[] {4};
        return new FunctionDeploymentReference(sha256("deployment-set"),
                "functions", sha256("jar"), "example.BroadcastProcess",
                FunctionAbi.KEYED_CO_PROCESS_V1, "config-v1", sha256(config),
                config, new FunctionResourceProfile(1024L, 0L, 4));
    }

    private static String sha256(String value) {
        return sha256(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String sha256(byte[] value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value);
            StringBuilder result = new StringBuilder(64);
            for (byte current : digest) result.append(String.format(
                    java.util.Locale.ROOT, "%02x", current & 0xff));
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
