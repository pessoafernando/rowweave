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
import io.rowweave.stream.contract.PortableStreamJob;
import io.rowweave.stream.contract.PortableStreamJobArtifact;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.contract.StreamStateDescriptor;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** bounded keyed aggregate state is explicit in the public plan. */
class StreamPublicKeyedAggregateCompilerContractTest {

    @Test
    void keyedAggregateCompilesStableStateAndOutputSchema() {
        StreamStateSpec state = new StreamStateSpec("readout-state",
                StreamStateSpec.Kind.KEYED_AGGREGATING, "decimal-aggregate-v1",
                2, StreamStateSpec.TtlMode.PROCESSING_TIME, 60_000L);
        StreamSession session = aggregateSession("keyed-aggregate");
        session.fromSource(source(schema(false))).uid("source")
                .keyBy("adaptor-id").uid("key")
                .keyedAggregate(reference(FunctionAbi.STREAM_AGGREGATE_V1,
                                "Aggregate"),
                        schema(false), state).uid("aggregate")
                .sinkTo("sink", sink());

        PortableStreamJob job = PortableStreamJobArtifact.decodeValidated(
                session.toPlanArtifact());
        PortableStreamNode aggregate = job.plan().nodes().get(2);
        assertEquals(StreamOperatorCode.KEYED_AGGREGATE, aggregate.opcode());
        assertEquals(Collections.singletonList("adaptor-id"),
                aggregate.stableKeyFields());
        assertEquals(1, aggregate.stateDescriptors().size());
        StreamStateDescriptor descriptor = aggregate.stateDescriptors().get(0);
        assertEquals("readout-state", descriptor.stateName());
        assertEquals(StreamStateDescriptor.Kind.KEYED_AGGREGATING,
                descriptor.kind());
        assertEquals(StreamStateDescriptor.TtlMode.PROCESSING_TIME,
                descriptor.ttlMode());
        assertEquals(60_000L, descriptor.ttlMillis());
    }

    @Test
    void aggregateRequiresPriorKeyAndPreservedNonNullOutputKey() {
        StreamSession unkeyed = aggregateSession("unkeyed-aggregate");
        unkeyed.fromSource(source(schema(false))).uid("source")
                .keyedAggregate(reference(FunctionAbi.STREAM_AGGREGATE_V1,
                                "Aggregate"),
                        schema(false), state()).uid("aggregate")
                .sinkTo("sink", sink());
        StreamApiException missing = assertThrows(
                StreamApiException.class, unkeyed::toPlanArtifact);
        assertEquals(StreamApiException.Code.MISSING_STABLE_KEY, missing.code());

        StreamSession nullable = aggregateSession("nullable-output-key");
        nullable.fromSource(source(schema(false))).uid("source")
                .keyBy("adaptor-id").uid("key")
                .keyedAggregate(reference(FunctionAbi.STREAM_AGGREGATE_V1,
                                "Aggregate"),
                        schema(true), state()).uid("aggregate")
                .sinkTo("sink", sink());
        StreamApiException invalid = assertThrows(
                StreamApiException.class, nullable::toPlanArtifact);
        assertEquals(StreamApiException.Code.INVALID_STABLE_KEY, invalid.code());
    }

    @Test
    void aggregateRejectsWrongFunctionAbi() {
        StreamSession session = aggregateSession("wrong-aggregate-abi");
        session.fromSource(source(schema(false))).uid("source")
                .keyBy("adaptor-id").uid("key")
                .keyedAggregate(reference(FunctionAbi.MAP_V1, "Map"),
                        schema(false), state()).uid("aggregate")
                .sinkTo("sink", sink());
        StreamApiException failure = assertThrows(
                StreamApiException.class, session::toPlanArtifact);
        assertEquals(StreamApiException.Code.FUNCTION_ABI_MISMATCH, failure.code());
    }

    @Test
    void aggregateRejectsBatchReduceAbiAndAppendChangelog() {
        StreamSession batchAbi = aggregateSession("batch-reduce-abi");
        batchAbi.fromSource(source(schema(false))).uid("source")
                .keyBy("adaptor-id").uid("key")
                .keyedAggregate(reference(FunctionAbi.REDUCE_GROUP_V1,
                                "BatchAggregate"),
                        schema(false), state()).uid("aggregate")
                .sinkTo("sink", sink());
        StreamApiException wrongAbi = assertThrows(
                StreamApiException.class, batchAbi::toPlanArtifact);
        assertEquals(StreamApiException.Code.FUNCTION_ABI_MISMATCH,
                wrongAbi.code());

        StreamSession append = RowWeave.createStreamSession("append-aggregate");
        append.fromSource(source(schema(false))).uid("source")
                .keyBy("adaptor-id").uid("key")
                .keyedAggregate(reference(FunctionAbi.STREAM_AGGREGATE_V1,
                                "Aggregate"),
                        schema(false), state()).uid("aggregate")
                .sinkTo("sink", sink());
        StreamApiException wrongMode = assertThrows(
                StreamApiException.class, append::toPlanArtifact);
        assertEquals(StreamApiException.Code.CHANGELOG_MODE_MISMATCH,
                wrongMode.code());
    }

    @Test
    void aggregateRequiresAggregatingStateWithFiniteTtl() {
        assertStateFailure(new StreamStateSpec("state",
                StreamStateSpec.Kind.KEYED_MAP, "aggregate-v1", 1,
                StreamStateSpec.TtlMode.PROCESSING_TIME, 60_000L));
        assertStateFailure(new StreamStateSpec("state",
                StreamStateSpec.Kind.KEYED_AGGREGATING, "aggregate-v1", 1,
                StreamStateSpec.TtlMode.NONE, 0L));
    }

    private static void assertStateFailure(StreamStateSpec state) {
        StreamSession session = aggregateSession("invalid-state-"
                + state.kind() + "-" + state.ttlMode());
        session.fromSource(source(schema(false))).uid("source")
                .keyBy("adaptor-id").uid("key")
                .keyedAggregate(reference(FunctionAbi.STREAM_AGGREGATE_V1,
                                "Aggregate"),
                        schema(false), state).uid("aggregate")
                .sinkTo("sink", sink());
        StreamApiException failure = assertThrows(
                StreamApiException.class, session::toPlanArtifact);
        assertEquals(StreamApiException.Code.INVALID_STATE_DESCRIPTOR,
                failure.code());
    }

    private static StreamStateSpec state() {
        return new StreamStateSpec("readout-state",
                StreamStateSpec.Kind.KEYED_AGGREGATING, "decimal-aggregate-v1",
                1, StreamStateSpec.TtlMode.PROCESSING_TIME, 60_000L);
    }

    private static StreamSession aggregateSession(String name) {
        StreamSession session = RowWeave.createStreamSession(name);
        session.setChangelogMode(StreamChangelogMode.UPSERT);
        return session;
    }

    private static StreamSchema schema(boolean nullableKey) {
        return new StreamSchema(Arrays.asList(
                new StreamField("adaptor-id", "adaptorId", "adaptorId",
                        DataType.StringType, nullableKey),
                new StreamField("amount", "amount", "amount",
                        DataType.BigDecimalType, true)));
    }

    private static StreamSourceSpec source(StreamSchema schema) {
        return StreamSourceSpec.exactOrdered(schema, "source-v1",
                sha256("source-contract"), new byte[] {1},
                Collections.singletonList(new StreamSplitSpec(
                        sha256("split"), "sequence-v1", new byte[] {2})));
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
