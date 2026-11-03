package io.rowweave;

import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.stream.api.StreamApiException;
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
import io.rowweave.stream.contract.PortableStreamJob;
import io.rowweave.stream.contract.PortableStreamJobArtifact;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.StreamOperatorCode;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** temporal join is an explicit finite event-time binary contract. */
class StreamPublicTemporalJoinCompilerContractTest {

    @Test
    void temporalJoinCompilesTwoInputsFiniteStateAndCapabilities() {
        StreamSession session = eventTimeUpsertSession("temporal-join");
        DataStream left = session.fromSource(source("left")).uid("left-source")
                .keyBy("adaptor-id").uid("left-key");
        DataStream right = session.fromSource(source("right")).uid("right-source")
                .keyBy("adaptor-id").uid("right-key");
        left.temporalJoin(right, joinSpec(),
                        reference(FunctionAbi.CO_GROUP_V1, "Join"), schema(),
                        joinState("left-state"), joinState("right-state"))
                .uid("join").sinkTo("sink", sink());

        PortableStreamJob job = PortableStreamJobArtifact.decodeValidated(
                session.toPlanArtifact());
        PortableStreamNode join = job.plan().nodes().get(4);
        assertEquals(StreamOperatorCode.TEMPORAL_JOIN, join.opcode());
        assertEquals(Arrays.asList("left-key", "right-key"),
                join.inputOperatorUids());
        assertEquals(2, join.stateDescriptors().size());
        assertEquals("-1000", join.scalarParameters().get("join.lowerBoundMillis"));
        assertEquals("2000", join.scalarParameters().get("join.upperBoundMillis"));
        assertTrue(job.plan().requiredCapabilities().contains(
                PortableStreamPlan.TEMPORAL_JOIN_CAPABILITY));
        assertTrue(job.plan().requiredCapabilities().contains(
                PortableStreamPlan.UPSERT_CAPABILITY));
    }

    @Test
    void temporalJoinRejectsProcessingTimeAndAppendPlans() {
        StreamSession processing = RowWeave.createStreamSession("processing-join");
        addJoin(processing);
        StreamApiException wrongTime = assertThrows(
                StreamApiException.class, processing::toPlanArtifact);
        assertEquals(StreamApiException.Code.TIME_MODE_MISMATCH, wrongTime.code());

        StreamSession append = RowWeave.createStreamSession("append-join");
        append.setTimeMode(StreamTimeMode.EVENT_TIME);
        addJoin(append);
        StreamApiException wrongChangelog = assertThrows(
                StreamApiException.class, append::toPlanArtifact);
        assertEquals(StreamApiException.Code.CHANGELOG_MODE_MISMATCH,
                wrongChangelog.code());
    }

    @Test
    void temporalJoinRejectsWrongAbiAndCrossSessionInput() {
        StreamSession session = eventTimeUpsertSession("wrong-join-abi");
        DataStream left = session.fromSource(source("left")).uid("left-source")
                .keyBy("adaptor-id").uid("left-key");
        DataStream right = session.fromSource(source("right")).uid("right-source")
                .keyBy("adaptor-id").uid("right-key");
        left.temporalJoin(right, joinSpec(), reference(FunctionAbi.MAP_V1, "Map"),
                        schema(), joinState("left-state"), joinState("right-state"))
                .uid("join").sinkTo("sink", sink());
        StreamApiException wrongAbi = assertThrows(
                StreamApiException.class, session::toPlanArtifact);
        assertEquals(StreamApiException.Code.FUNCTION_ABI_MISMATCH, wrongAbi.code());

        StreamSession other = eventTimeUpsertSession("other-session");
        DataStream foreign = other.fromSource(source("foreign")).uid("foreign-source")
                .keyBy("adaptor-id").uid("foreign-key");
        assertThrows(IllegalArgumentException.class,
                () -> left.temporalJoin(foreign, joinSpec(),
                        reference(FunctionAbi.CO_GROUP_V1, "Join"), schema(),
                        joinState("left-state"), joinState("right-state")));
    }

    private static void addJoin(StreamSession session) {
        DataStream left = session.fromSource(source("left")).uid("left-source")
                .keyBy("adaptor-id").uid("left-key");
        DataStream right = session.fromSource(source("right")).uid("right-source")
                .keyBy("adaptor-id").uid("right-key");
        left.temporalJoin(right, joinSpec(),
                        reference(FunctionAbi.CO_GROUP_V1, "Join"), schema(),
                        joinState("left-state"), joinState("right-state"))
                .uid("join").sinkTo("sink", sink());
    }

    private static StreamSession eventTimeUpsertSession(String name) {
        StreamSession session = RowWeave.createStreamSession(name);
        session.setTimeMode(StreamTimeMode.EVENT_TIME);
        session.setChangelogMode(StreamChangelogMode.UPSERT);
        return session;
    }

    private static StreamTemporalJoinSpec joinSpec() {
        return new StreamTemporalJoinSpec("adaptor-id", "adaptor-id",
                -1_000L, 2_000L, Collections.singletonList("adaptor-id"));
    }

    private static StreamStateSpec joinState(String name) {
        return new StreamStateSpec(name, StreamStateSpec.Kind.KEYED_MAP,
                "temporal-record-v1", 1, StreamStateSpec.TtlMode.EVENT_TIME,
                3_000L);
    }

    private static StreamSchema schema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("adaptor-id", "adaptorId", "adaptorId",
                        DataType.StringType, false),
                new StreamField("amount", "amount", "amount",
                        DataType.BigDecimalType, true)));
    }

    private static StreamSourceSpec source(String id) {
        return StreamSourceSpec.exactOrderedEventTime(schema(), "source-v1",
                sha256("source-contract"), id.getBytes(StandardCharsets.UTF_8),
                Collections.singletonList(new StreamSplitSpec(
                        sha256(id + "-split"), "sequence-v1", new byte[] {2})),
                StreamWatermarkPolicy.providerEvents(30_000L));
    }

    private static StreamSinkSpec sink() {
        return StreamSinkSpec.eagerAtLeastOnce(
                "sink-v1", sha256("sink-contract"), new byte[] {3});
    }

    private static FunctionDeploymentReference reference(
            FunctionAbi abi, String name) {
        byte[] config = new byte[] {4};
        return new FunctionDeploymentReference(sha256("deployment-set"),
                "stream-functions", sha256("jar"), "example." + name, abi,
                "stream-function-config-v1", sha256(config), config,
                new FunctionResourceProfile(1_024L, 0L, 1));
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
