package io.rowweave;

import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamDeliveryGuarantee;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceDiscoverySpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamTimeMode;
import io.rowweave.stream.contract.PortableStreamJob;
import io.rowweave.stream.contract.PortableStreamJobArtifact;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.PortableStreamPlanCodec;
import io.rowweave.stream.contract.StreamContractException;
import io.rowweave.stream.contract.StreamSourceBinding;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** dynamic discovery is an explicit v2 portable contract. */
class StreamPublicDynamicSourceCompilerContractTest {

    @Test
    void dynamicSourceCompilesAsV2WithoutChangingFixedV1Bytes() {
        StreamSourceDiscoverySpec discovery = new StreamSourceDiscoverySpec(
                100L, 1_000L, 64, 32, 8, 1_024, 4_096);
        StreamSession dynamic = session("dynamic", 4);
        dynamic.fromSource(StreamSourceSpec.dynamicExactOrdered(
                schema(), "dynamic-source-v2", sha256("dynamic-contract"),
                new byte[] {1, 2}, discovery)).uid("source")
                .sinkTo("sink", sink());

        PortableStreamJob dynamicJob = PortableStreamJobArtifact
                .decodeValidated(dynamic.toPlanArtifact());
        PortableStreamPlan plan = dynamicJob.plan();
        byte[] encoded = PortableStreamPlanCodec.encode(plan);
        StreamSourceBinding source = (StreamSourceBinding)
                plan.nodes().get(0).binding();

        assertEquals(PortableStreamPlan.DYNAMIC_SOURCE_VERSION,
                plan.planVersion());
        assertArrayEquals("RWXXPSP2".getBytes(StandardCharsets.US_ASCII),
                Arrays.copyOf(encoded, 8));
        assertTrue(plan.requiredCapabilities().contains(
                PortableStreamPlan.DYNAMIC_SPLIT_DISCOVERY_CAPABILITY));
        assertTrue(source.dynamicDiscovery());
        assertEquals(discovery.maximumKnownSplits(),
                source.discoveryDescriptor().maximumKnownSplits());
        assertEquals(0, source.fixedSplits().size());
        assertEquals(4, plan.nodes().get(0).executionDescriptor()
                .parallelism());
        assertEquals(plan, PortableStreamPlanCodec.decode(encoded));
        StreamContractException rejected = assertThrows(
                StreamContractException.class,
                () -> PortableStreamPlanCodec.decodeV1(encoded));
        assertEquals("UNSUPPORTED_ENVELOPE_VERSION", rejected.code());

        StreamSession fixed = session("fixed", 4);
        fixed.fromSource(StreamSourceSpec.exactOrdered(
                schema(), "fixed-source-v1", sha256("fixed-contract"),
                new byte[] {3}, Collections.singletonList(
                        new StreamSplitSpec(sha256("split"), "position-v1",
                                new byte[] {4})))).uid("source")
                .sinkTo("sink", sink());
        PortableStreamPlan fixedPlan = PortableStreamJobArtifact
                .decodeValidated(fixed.toPlanArtifact()).plan();
        byte[] fixedBytes = PortableStreamPlanCodec.encode(fixedPlan);
        assertEquals(PortableStreamPlan.FIXED_SOURCE_VERSION,
                fixedPlan.planVersion());
        assertArrayEquals("RWXXPSP1".getBytes(StandardCharsets.US_ASCII),
                Arrays.copyOf(fixedBytes, 8));
        assertArrayEquals(fixedBytes, PortableStreamPlanCodec.encode(
                PortableStreamPlanCodec.decode(fixedBytes)));
    }

    private static StreamSession session(String suffix, int parallelism) {
        StreamSession session = RowWeave.createStreamSession("split-" + suffix);
        session.setDefinitionId("split-" + suffix);
        session.setParallelism(parallelism);
        session.setMaxParallelism(128);
        session.setDeliveryGuarantee(StreamDeliveryGuarantee.AT_LEAST_ONCE);
        session.setChangelogMode(StreamChangelogMode.APPEND);
        session.setTimeMode(StreamTimeMode.PROCESSING_TIME);
        return session;
    }

    private static StreamSchema schema() {
        return new StreamSchema(Collections.singletonList(
                new StreamField("id", "id", "id", DataType.LongType,
                        false)));
    }

    private static StreamSinkSpec sink() {
        return StreamSinkSpec.eagerAtLeastOnce(
                "sink-v1", sha256("sink-contract"), new byte[] {9});
    }

    private static String sha256(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                    value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(64);
            for (byte current : digest) {
                result.append(Character.forDigit((current >>> 4) & 0xf, 16));
                result.append(Character.forDigit(current & 0xf, 16));
            }
            return result.toString();
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}
