package io.rowweave;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamCheckpointConfig;
import io.rowweave.stream.api.StreamDeliveryGuarantee;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamApiException;
import io.rowweave.stream.api.StreamAsyncOrdering;
import io.rowweave.stream.api.StreamAsyncReplaySpec;
import io.rowweave.stream.api.StreamAsyncResourceSpec;
import io.rowweave.stream.api.StreamAsyncWaitSpec;
import io.rowweave.stream.api.StreamEmissionLimit;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamTimeMode;
import io.rowweave.stream.contract.PortableStreamJobArtifact;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.PortableStreamPlan;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** public resident CDC description is capability-gated. */
class StreamPublicResidentCdcCompilerContractTest {

    @Test
    void residentCdcCompilesStableUpsertSnapshotLogContract() {
        StreamSession session = session("resident-cdc");

        PlanArtifact artifact = session.toPlanArtifact();
        PortableStreamPlan plan = PortableStreamJobArtifact.decodeValidated(
                artifact).plan();
        PortableStreamNode source = plan.nodes().get(0);

        assertEquals(PortableStreamPlan.ChangelogMode.UPSERT,
                plan.changelogMode());
        assertEquals(Collections.singletonList("id"),
                source.stableKeyFields());
        assertEquals("RESIDENT_CDC_SNAPSHOT_LOG",
                source.scalarParameters().get("source.mode"));
        assertEquals("STREAM_CDC_ENUMERATOR_V1",
                source.scalarParameters().get(
                        "source.enumeratorStateFormat"));
        assertTrue(plan.requiredCapabilities().contains(
                PortableStreamPlan.RESIDENT_CDC_CAPABILITY));
        assertTrue(artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_RESIDENT_CDC_V1));
    }

    @Test
    void residentCdcV2RequiresIndependentWorkerCapability() {
        StreamSession session = configured("resident-cdc-v2");
        session.fromSource(StreamSourceSpec.residentCdcV2(
                        schema(), Collections.singletonList("id"),
                        "cdc-source-v2", digest("cdc-source-contract-v2"),
                        new byte[] {1}, digest("snapshot-v2"), 0L,
                        Collections.singletonList("snapshot-all")))
                .uid("source")
                .sinkTo("sink", sink());

        PlanArtifact artifact = session.toPlanArtifact();
        PortableStreamPlan plan = PortableStreamJobArtifact.decodeValidated(
                artifact).plan();
        PortableStreamNode source = plan.nodes().get(0);

        assertEquals("STREAM_CDC_ENUMERATOR_V2",
                source.scalarParameters().get(
                        "source.enumeratorStateFormat"));
        assertTrue(plan.requiredCapabilities().contains(
                PortableStreamPlan.RESIDENT_CDC_V2_CAPABILITY));
        assertTrue(artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_RESIDENT_CDC_V2));
    }

    @Test
    void residentCdcAllowsEagerAtLeastOnceAndRejectsWrongModes() {
        StreamSession weaker = configured("cdc-weaker");
        weaker.setDeliveryGuarantee(StreamDeliveryGuarantee.AT_LEAST_ONCE);
        source(weaker).sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                "cdc-eager-v1", digest("cdc-eager-contract"),
                new byte[] {3}));
        PortableStreamPlan weakerPlan = PortableStreamJobArtifact
                .decodeValidated(weaker.toPlanArtifact()).plan();
        assertEquals(PortableStreamPlan.DeliveryGuarantee.AT_LEAST_ONCE,
                weakerPlan.deliveryGuarantee());

        StreamSession append = session("cdc-append");
        append.setChangelogMode(StreamChangelogMode.APPEND);
        assertEquals(StreamApiException.Code.CHANGELOG_MODE_MISMATCH,
                assertThrows(StreamApiException.class,
                        append::toPlanArtifact).code());

        StreamSession eventTime = session("cdc-event-time");
        eventTime.setTimeMode(StreamTimeMode.EVENT_TIME);
        assertEquals(StreamApiException.Code.TIME_MODE_MISMATCH,
                assertThrows(StreamApiException.class,
                        eventTime::toPlanArtifact).code());
    }

    @Test
    void residentCdcRequiresNonNullStableSchemaKey() {
        StreamSchema nullable = new StreamSchema(Collections.singletonList(
                new StreamField("id", "id", "id",
                        DataType.StringType, true)));
        assertThrows(IllegalArgumentException.class,
                () -> StreamSourceSpec.residentCdc(
                        nullable, Collections.singletonList("id"),
                        "cdc-source-v1", digest("cdc-source-contract"),
                        new byte[] {1}, digest("snapshot"), 10L,
                        Collections.singletonList("snapshot-0")));
    }

    @Test
    void residentCdcRejectsKeyChangingAndKeyLosingRedistributionAtCompile() {
        StreamSession changed = configured("cdc-key-change");
        source(changed).keyBy("adaptor").uid("adaptor-key")
                .sinkTo("sink", sink());
        assertEquals(StreamApiException.Code.CHANGELOG_MODE_MISMATCH,
                assertThrows(StreamApiException.class,
                        changed::toPlanArtifact).code());

        StreamSession lost = configured("cdc-key-loss");
        source(lost).map(function(FunctionAbi.MAP_V1), schema())
                .uid("map").keyBy("id").uid("id-key")
                .sinkTo("sink", sink());
        assertEquals(StreamApiException.Code.CHANGELOG_MODE_MISMATCH,
                assertThrows(StreamApiException.class,
                        lost::toPlanArtifact).code());

        StreamSession asyncChanged = configured("cdc-async-key-change");
        source(asyncChanged).asyncWait(StreamAsyncWaitSpec.of(
                        function(FunctionAbi.STREAM_ASYNC_V1), schema(),
                        Collections.singletonList("adaptor"),
                        StreamAsyncOrdering.ORDERED,
                        StreamAsyncReplaySpec.identityKeyed(
                                "request-id-v1", digest("request-key")),
                        StreamAsyncResourceSpec.of(4, 64 * 1024L, 1_000L,
                                2, 10L, 100L,
                                StreamEmissionLimit.of(1, 4096L))))
                .uid("async-adaptor").sinkTo("sink", sink());
        assertEquals(StreamApiException.Code.CHANGELOG_MODE_MISMATCH,
                assertThrows(StreamApiException.class,
                        asyncChanged::toPlanArtifact).code());

        StreamSchema changedCodec = new StreamSchema(Arrays.asList(
                new StreamField("id", "id", "id",
                        DataType.LongType, false),
                new StreamField("adaptor", "adaptor", "adaptor",
                        DataType.StringType, false),
                new StreamField("amount", "amount", "amount",
                        DataType.BigDecimalType, true)));
        StreamSession asyncCodecChanged = configured(
                "cdc-async-key-codec-change");
        source(asyncCodecChanged).asyncWait(StreamAsyncWaitSpec.of(
                        function(FunctionAbi.STREAM_ASYNC_V1), changedCodec,
                        Collections.singletonList("id"),
                        StreamAsyncOrdering.ORDERED,
                        StreamAsyncReplaySpec.identityKeyed(
                                "request-id-v1", digest("request-key")),
                        StreamAsyncResourceSpec.of(4, 64 * 1024L, 1_000L,
                                2, 10L, 100L,
                                StreamEmissionLimit.of(1, 4096L))))
                .uid("async-id-codec-change").sinkTo("sink", sink());
        assertEquals(StreamApiException.Code.CHANGELOG_MODE_MISMATCH,
                assertThrows(StreamApiException.class,
                        asyncCodecChanged::toPlanArtifact).code());
    }

    private static StreamSession session(String name) {
        StreamSession session = configured(name);
        source(session).sinkTo("sink", sink());
        return session;
    }

    private static StreamSession configured(String name) {
        StreamSession session = RowWeave.createStreamSession(name);
        session.setDeliveryGuarantee(StreamDeliveryGuarantee.EXACTLY_ONCE);
        session.setChangelogMode(StreamChangelogMode.UPSERT);
        session.setCheckpointConfig(StreamCheckpointConfig.periodic(
                1_000L, 5_000L, 2));
        return session;
    }

    private static DataStream source(StreamSession session) {
        return session.fromSource(StreamSourceSpec.residentCdc(
                        schema(), Collections.singletonList("id"),
                        "cdc-source-v1", digest("cdc-source-contract"),
                        new byte[] {1}, digest("snapshot"), 10L,
                        Collections.singletonList("snapshot-0")))
                .uid("source");
    }

    private static StreamSinkSpec sink() {
        return StreamSinkSpec.checkpointTransactional(
                "cdc-sink-v1", digest("cdc-sink-contract"), new byte[] {3});
    }

    private static FunctionDeploymentReference function(FunctionAbi abi) {
        byte[] configuration = new byte[] {4};
        return new FunctionDeploymentReference(digest("deployment"),
                "functions", digest("jar"), "example.Map", abi,
                "config-v1", digest(configuration), configuration,
                new FunctionResourceProfile(1_024L, 0L, 1));
    }

    private static StreamSchema schema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("id", "id", "id",
                        DataType.StringType, false),
                new StreamField("adaptor", "adaptor", "adaptor",
                        DataType.StringType, false),
                new StreamField("amount", "amount", "amount",
                        DataType.BigDecimalType, true)));
    }

    private static String digest(String value) {
        return digest(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String digest(byte[] value) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256").digest(
                    value);
            StringBuilder result = new StringBuilder(64);
            for (byte one : bytes) {
                result.append(String.format("%02x", one & 0xff));
            }
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
