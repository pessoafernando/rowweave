package io.rowweave.stream.runtime;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.portable.PortableDataType;
import io.rowweave.portable.PortableField;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.stream.contract.CheckpointEvidence;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.CheckpointManifest;
import io.rowweave.stream.contract.CheckpointTaskReceipt;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.PortableStreamPlanArtifact;
import io.rowweave.stream.contract.PortableStreamPlanCodec;
import io.rowweave.stream.contract.StreamEdge;
import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.contract.StreamSavepoint;
import io.rowweave.stream.contract.StreamSinkBinding;
import io.rowweave.stream.contract.StreamSourceBinding;
import io.rowweave.stream.contract.StreamSourceSplit;
import io.rowweave.stream.contract.StreamStateDescriptor;
import org.junit.jupiter.api.Test;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** exact savepoint restore is artifact/state-contract exact. */
class StreamSavepointRestoreValidatorTest {
    private static final String A = digest('a');
    private static final String B = digest('b');
    private static final String C = digest('c');

    @Test
    void exactArtifactAndCanonicalPlanAreAccepted() {
        PortableStreamPlan plan = plan("serializer-v1", 1, 128);
        PlanArtifact artifact = PortableStreamPlanArtifact.wrap(
                "definition-1", plan);
        StreamSavepoint savepoint = savepoint(artifact,
                planDigest(plan));

        assertEquals(plan, StreamSavepointRestoreValidator.validateExact(
                savepoint, artifact));
    }

    @Test
    void stateOrMaxParallelismDriftFailsClosed() {
        PortableStreamPlan source = plan("serializer-v1", 1, 128);
        PlanArtifact sourceArtifact = PortableStreamPlanArtifact.wrap(
                "definition-1", source);
        StreamSavepoint savepoint = savepoint(sourceArtifact,
                planDigest(source));

        assertIncompatible(savepoint, PortableStreamPlanArtifact.wrap(
                "definition-1", plan("serializer-v2", 1, 128)));
        assertIncompatible(savepoint, PortableStreamPlanArtifact.wrap(
                "definition-1", plan("serializer-v1", 2, 128)));
        assertIncompatible(savepoint, PortableStreamPlanArtifact.wrap(
                "definition-1", plan("serializer-v1", 1, 256)));
    }

    @Test
    void forgedPlanDigestFailsBeforeRestore() {
        PortableStreamPlan plan = plan("serializer-v1", 1, 128);
        PlanArtifact artifact = PortableStreamPlanArtifact.wrap(
                "definition-1", plan);
        StreamSavepoint forged = savepoint(artifact, C);

        assertIncompatible(forged, artifact);
    }

    private static void assertIncompatible(
            StreamSavepoint savepoint, PlanArtifact artifact) {
        StreamCheckpointException failure = assertThrows(
                StreamCheckpointException.class,
                () -> StreamSavepointRestoreValidator.validateExact(
                        savepoint, artifact));
        assertEquals(StreamCheckpointException.Code.SAVEPOINT_INCOMPATIBLE,
                failure.code());
    }

    private static StreamSavepoint savepoint(
            PlanArtifact artifact, String encodedPlanDigest) {
        CheckpointIdentity identity = new CheckpointIdentity(
                "definition-1", artifact.artifactId(), "run-1", "attempt-1",
                1L, 2L, "coordinator-1", 3L, encodedPlanDigest,
                B, 1, 1);
        CheckpointTaskReceipt receipt = new CheckpointTaskReceipt(
                identity, "source#0", 1L, "worker-1",
                Collections.singletonList(new CheckpointEvidence(
                        CheckpointEvidence.Kind.STATE, "state-member", A, 32L)));
        return new StreamSavepoint("savepoint-1",
                new CheckpointManifest(identity,
                        Collections.singletonList(receipt)));
    }

    private static PortableStreamPlan plan(
            String serializer, int schemaVersion, int maxParallelism) {
        PortableRowSchema schema = new PortableRowSchema(
                Collections.singletonList(new PortableField(
                        "value", "value", "value",
                        PortableDataType.STRING, false)));
        byte[] sourceConfig = new byte[] {1};
        byte[] sinkConfig = new byte[] {2};
        StreamStateDescriptor state = new StreamStateDescriptor(
                "readout", StreamStateDescriptor.Kind.KEYED_VALUE,
                serializer, schemaVersion,
                StreamStateDescriptor.TtlMode.NONE, 0L);
        PortableStreamNode source = new PortableStreamNode(
                "source", StreamOperatorCode.SOURCE,
                Collections.emptyList(), Collections.emptyList(), schema,
                Collections.emptyList(), Collections.singletonList(state),
                Collections.emptyMap(), new StreamSourceBinding(
                "source-provider", A, sha256(sourceConfig), sourceConfig,
                Collections.singletonList(new StreamSourceSplit(
                        B, "position-v1", new byte[] {3})),
                StreamSourceBinding.ReplayGuarantee.EXACT_ORDERED));
        PortableStreamNode sink = new PortableStreamNode(
                "sink", StreamOperatorCode.SINK,
                Collections.singletonList("source"),
                Collections.singletonList(schema.fingerprint()), schema,
                Collections.emptyList(), Collections.emptyList(),
                Collections.emptyMap(), new StreamSinkBinding(
                "sink-provider", C, sha256(sinkConfig), sinkConfig,
                StreamSinkBinding.VisibilityProfile.CHECKPOINT_TRANSACTIONAL));
        return new PortableStreamPlan(
                PortableStreamPlan.CURRENT_VERSION,
                PortableStreamPlan.DeliveryGuarantee.EXACTLY_ONCE,
                1, maxParallelism, PortableStreamPlan.ChangelogMode.APPEND,
                PortableStreamPlan.TimeMode.PROCESSING_TIME,
                Collections.singletonList("sink"), Arrays.asList(source, sink),
                Collections.singletonList(new StreamEdge(
                "source", "sink", 0, "source-sink-0",
                StreamEdge.Partitioning.FORWARD, 4096, 16 * 1024)),
                Collections.singletonList(PortableStreamPlan.STREAM_CAPABILITY));
    }

    private static String planDigest(PortableStreamPlan plan) {
        return sha256(PortableStreamPlanCodec.encode(plan));
    }

    private static String sha256(byte[] value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value);
            StringBuilder text = new StringBuilder(64);
            for (byte item : digest) text.append(String.format("%02x", item & 0xff));
            return text.toString();
        } catch (Exception impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static String digest(char value) {
        char[] chars = new char[64];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
