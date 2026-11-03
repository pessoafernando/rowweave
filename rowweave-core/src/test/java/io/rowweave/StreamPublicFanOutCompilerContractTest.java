package io.rowweave;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.stream.api.StreamApiException;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.contract.PortableStreamJob;
import io.rowweave.stream.contract.PortableStreamJobArtifact;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** fan-out is one canonical graph, independent of sink registration order. */
class StreamPublicFanOutCompilerContractTest {

    @Test
    void sharedSourceCompilesOnceAndSinkRegistrationOrderIsCanonical() {
        PlanArtifact forward = graph(false).toPlanArtifact();
        PlanArtifact reverse = graph(true).toPlanArtifact();

        assertEquals(forward.artifactId(), reverse.artifactId());
        assertArrayEquals(forward.payload(), reverse.payload());
        PortableStreamJob job = PortableStreamJobArtifact.decodeValidated(forward);
        assertEquals(5, job.plan().nodes().size());
        assertEquals(4, job.plan().edges().size());
        assertEquals(Arrays.asList("sink-a", "sink-b"),
                job.plan().rootSinkOperatorUids());
        assertEquals(1L, job.plan().nodes().stream()
                .filter(node -> node.operatorUid().equals("source")).count());
    }

    @Test
    void duplicateSinkIdentityFailsBeforeArtifact() {
        StreamSession session = RowWeave.createStreamSession("duplicate-sink");
        DataStream source = session.fromSource(source()).uid("source");
        source.sinkTo("sink", sink("a"));
        source.sinkTo("sink", sink("b"));
        StreamApiException failure = assertThrows(
                StreamApiException.class, session::toPlanArtifact);
        assertEquals(StreamApiException.Code.DUPLICATE_OPERATOR_UID,
                failure.code());
    }

    private static StreamSession graph(boolean reverseRegistration) {
        StreamSession session = RowWeave.createStreamSession("canonical-fan-out");
        session.setDefinitionId("canonical-fan-out-v1");
        DataStream source = session.fromSource(source()).uid("source");
        DataStream branchA = source.filter(filter("A")).uid("filter-a");
        DataStream branchB = source.filter(filter("B")).uid("filter-b");
        if (reverseRegistration) {
            branchB.sinkTo("sink-b", sink("b"));
            branchA.sinkTo("sink-a", sink("a"));
        } else {
            branchA.sinkTo("sink-a", sink("a"));
            branchB.sinkTo("sink-b", sink("b"));
        }
        return session;
    }

    private static StreamSourceSpec source() {
        StreamSchema schema = new StreamSchema(Collections.singletonList(
                new StreamField("id", "id", "id", DataType.StringType, false)));
        return StreamSourceSpec.exactOrdered(schema, "source-v1",
                sha256("source-contract"), new byte[] {1},
                Collections.singletonList(new StreamSplitSpec(
                        sha256("split"), "sequence-v1", new byte[] {2})));
    }

    private static StreamSinkSpec sink(String suffix) {
        return StreamSinkSpec.eagerAtLeastOnce("sink-" + suffix,
                sha256("sink-contract-" + suffix), new byte[] {(byte) suffix.charAt(0)});
    }

    private static FunctionDeploymentReference filter(String suffix) {
        byte[] config = new byte[] {(byte) suffix.charAt(0)};
        return new FunctionDeploymentReference(sha256("deployment-set"),
                "stream-functions", sha256("jar"), "example.Filter" + suffix,
                FunctionAbi.FILTER_V1, "stream-function-config-v1",
                sha256(config), config, new FunctionResourceProfile(1_024L, 0L, 1));
    }

    private static String sha256(String value) {
        return sha256(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String sha256(byte[] value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value);
            StringBuilder result = new StringBuilder(digest.length * 2);
            for (byte current : digest) result.append(String.format("%02x", current & 0xff));
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
