package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataStream;
import io.rowweave.DataType;
import io.rowweave.StreamSession;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamStateSpec;
import io.rowweave.stream.api.StreamTemporalJoinSpec;
import io.rowweave.stream.api.StreamTimeMode;
import io.rowweave.stream.api.StreamWatermarkPolicy;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.StreamOperatorCode;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** canonical multi-source temporal fan-in reaches product admission. */
class StreamProductTemporalJoinPlanAdmissionTest {

    @Test
    void publicFiniteTemporalJoinIsAdmittedAsANodeLocalBinaryOperator() {
        StreamProductExecutionGraph admitted = assertDoesNotThrow(
                () -> StreamProductExecutionGraph.from(artifact()));

        int sources = 0;
        PortableStreamNode join = null;
        for (PortableStreamNode node : admitted.plan.nodes()) {
            if (node.opcode() == StreamOperatorCode.SOURCE) sources++;
            if (node.opcode() == StreamOperatorCode.TEMPORAL_JOIN) join = node;
        }
        assertEquals(2, sources);
        assertNotNull(join);
        assertEquals(2, join.inputOperatorUids().size());
        assertEquals(PortableStreamPlan.ChangelogMode.UPSERT,
                admitted.plan.changelogMode());
    }

    @Test
    void independentMultiSourceBranchesArePartOfTheGeneralDagClosure() {
        StreamSession session = RowWeave.createStreamSession(
                "non-temporal-multi-source");
        session.setTimeMode(StreamTimeMode.EVENT_TIME);
        session.fromSource(source("left")).uid("left-source")
                .sinkTo("left-sink", StreamSinkSpec.eagerAtLeastOnce(
                        "sink-v1", digest("sink-contract"), new byte[] {3}));
        session.fromSource(source("right")).uid("right-source")
                .sinkTo("right-sink", StreamSinkSpec.eagerAtLeastOnce(
                        "sink-v1", digest("sink-contract"), new byte[] {3}));
        PlanArtifact nonTemporal = session.toPlanArtifact();

        StreamProductExecutionGraph admitted = assertDoesNotThrow(
                () -> StreamProductExecutionGraph.from(nonTemporal));
        assertEquals(2, admitted.sources.size());
        assertEquals(2, admitted.sinks.size());
    }

    private static PlanArtifact artifact() {
        StreamSession session = RowWeave.createStreamSession("product-temporal-join");
        session.setTimeMode(StreamTimeMode.EVENT_TIME);
        session.setChangelogMode(StreamChangelogMode.UPSERT);
        DataStream left = session.fromSource(source("left"))
                .uid("left-source").keyBy("adaptor").uid("left-key");
        DataStream right = session.fromSource(source("right"))
                .uid("right-source").keyBy("adaptor").uid("right-key");
        left.temporalJoin(right,
                        new StreamTemporalJoinSpec("adaptor", "adaptor",
                                -1_000L, 2_000L,
                                Collections.singletonList("adaptor")),
                        function(), schema(), state("left-state"),
                        state("right-state"))
                .uid("temporal-join")
                .sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                        "sink-v1", digest("sink-contract"), new byte[] {3}));
        return session.toPlanArtifact();
    }

    private static StreamSourceSpec source(String side) {
        return StreamSourceSpec.exactOrderedEventTime(
                schema(), side + "-source-v1", digest(side + "-contract"),
                side.getBytes(StandardCharsets.UTF_8),
                Collections.singletonList(new StreamSplitSpec(
                        digest(side + "-split"), "sequence-v1",
                        new byte[] {2})),
                StreamWatermarkPolicy.providerEvents(30_000L));
    }

    private static StreamStateSpec state(String name) {
        return new StreamStateSpec(name, StreamStateSpec.Kind.KEYED_MAP,
                "temporal-record-v1", 1,
                StreamStateSpec.TtlMode.EVENT_TIME, 3_000L);
    }

    private static FunctionDeploymentReference function() {
        byte[] config = new byte[] {4};
        return new FunctionDeploymentReference(digest("deployment-set"),
                "stream-functions", digest("join-jar"), "example.Join",
                FunctionAbi.CO_GROUP_V1, "stream-function-config-v1",
                digest(config), config,
                new FunctionResourceProfile(1_024L, 0L, 1));
    }

    private static StreamSchema schema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("adaptor", "adaptor", "adaptor",
                        DataType.StringType, false),
                new StreamField("amount", "amount", "amount",
                        DataType.BigDecimalType, false)));
    }

    private static String digest(String value) {
        return digest(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String digest(byte[] value) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(value);
            StringBuilder result = new StringBuilder(hash.length * 2);
            for (byte current : hash) {
                result.append(String.format("%02x", current & 0xff));
            }
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
