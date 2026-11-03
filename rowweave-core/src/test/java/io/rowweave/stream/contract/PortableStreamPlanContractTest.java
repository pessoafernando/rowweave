package io.rowweave.stream.contract;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.portable.PortableDataType;
import io.rowweave.portable.PortableField;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.stream.api.StreamCheckpointConfig;
import io.rowweave.stream.api.StreamCheckpointMode;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamRestartPolicy;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** canonical Stream plan and outer Artifact projection. */
class PortableStreamPlanContractTest {

    private static final String DIGEST_A = repeat('a', 64);
    private static final String DIGEST_B = repeat('b', 64);

    @Test
    void canonicalPlanRoundTripsAndProjectsExactlyIntoArtifact() {
        PortableStreamPlan plan = appendPlan(
                PortableStreamPlan.DeliveryGuarantee.EXACTLY_ONCE,
                StreamSinkBinding.VisibilityProfile.CHECKPOINT_TRANSACTIONAL);

        byte[] encoded = PortableStreamPlanCodec.encode(plan);
        assertArrayEquals(encoded, PortableStreamPlanCodec.encode(
                PortableStreamPlanCodec.decode(encoded)));
        assertEquals(plan, PortableStreamPlanCodec.decode(encoded));
        assertArrayEquals("RWXXPSP1".getBytes(java.nio.charset.StandardCharsets.US_ASCII),
                PortableStreamPlanCodec.magic());

        PlanArtifact artifact = PortableStreamPlanArtifact.wrap("stream-ledger", plan);
        PlanArtifact decodedArtifact = PlanArtifactCodec.decode(
                PlanArtifactCodec.encode(artifact));
        assertEquals(PlanArtifact.PayloadFormat.PORTABLE_STREAM_PLAN_V1,
                decodedArtifact.payloadFormat());
        assertEquals(PlanArtifact.RuntimeTarget.STREAM, decodedArtifact.runtimeTarget());
        assertEquals(PlanArtifact.Boundedness.UNBOUNDED, decodedArtifact.boundedness());
        assertEquals(PlanArtifact.ChangelogMode.APPEND, decodedArtifact.changelogMode());
        assertEquals(PlanArtifact.TimeMode.EVENT_TIME, decodedArtifact.timeMode());
        assertEquals(plan, PortableStreamPlanArtifact.decodeValidated(decodedArtifact));
    }

    @Test
    void guaranteeIsDerivedFromSourceReplayAndSinkVisibilityCapabilities() {
        assertFailure("DELIVERY_GUARANTEE_MISMATCH", () -> appendPlan(
                PortableStreamPlan.DeliveryGuarantee.EXACTLY_ONCE,
                StreamSinkBinding.VisibilityProfile.EAGER_AT_LEAST_ONCE));

        PortableStreamPlan atLeastOnce = appendPlan(
                PortableStreamPlan.DeliveryGuarantee.AT_LEAST_ONCE,
                StreamSinkBinding.VisibilityProfile.EAGER_AT_LEAST_ONCE);
        assertEquals(PortableStreamPlan.DeliveryGuarantee.AT_LEAST_ONCE,
                atLeastOnce.deliveryGuarantee());

        assertFailure("SOURCE_NOT_REPLAYABLE", () -> new StreamSourceBinding(
                "log-v1", DIGEST_A, sha256Hex(new byte[]{1}), new byte[]{1},
                Collections.singletonList(new StreamSourceSplit(
                        DIGEST_B, "stream-position-v1", new byte[]{2})),
                StreamSourceBinding.ReplayGuarantee.NONE));
    }

    @Test
    void topologyKeyAndParallelismLegalityFailBeforeArtifactAdmission() {
        PortableStreamPlan valid = appendPlan(
                PortableStreamPlan.DeliveryGuarantee.EXACTLY_ONCE,
                StreamSinkBinding.VisibilityProfile.CHECKPOINT_TRANSACTIONAL);
        assertThrows(UnsupportedOperationException.class,
                () -> valid.nodes().add(valid.nodes().get(0)));

        assertFailure("INVALID_PARALLELISM", () -> copyPlan(
                valid, 129, 128, valid.nodes()));

        PortableStreamNode sink = valid.nodes().get(1);
        PortableStreamNode drift = new PortableStreamNode(
                sink.operatorUid(), sink.opcode(), sink.inputOperatorUids(),
                Collections.singletonList(repeat('f', 64)), sink.outputSchema(),
                sink.stableKeyFields(), sink.stateDescriptors(),
                sink.scalarParameters(), sink.binding());
        assertFailure("SCHEMA_MISMATCH", () -> copyPlan(
                valid, 2, 128, Arrays.asList(valid.nodes().get(0), drift)));

        PlanArtifact artifact = PortableStreamPlanArtifact.wrap("stream-ledger", valid);
        java.util.List<io.rowweave.artifact.OperatorDescriptor> descriptors =
                new java.util.ArrayList<>(artifact.operators());
        io.rowweave.artifact.OperatorDescriptor first = descriptors.get(0);
        descriptors.set(0, new io.rowweave.artifact.OperatorDescriptor(
                first.operatorId(), first.kind(), "rowweave.stream.opcode.999",
                first.schemaFingerprint(), first.inputOperatorIds()));
        PlanArtifact tampered = new PlanArtifact(
                artifact.definitionId(), artifact.planSchemaVersion(),
                artifact.minimumReaderVersion(), artifact.runtimeTarget(),
                artifact.boundedness(), artifact.changelogMode(), artifact.timeMode(),
                artifact.requiredFeatures(), artifact.optionalFeatures(), descriptors,
                artifact.payloadFormat(), artifact.payload());
        StreamContractException failure = assertThrows(
                StreamContractException.class,
                () -> PortableStreamPlanArtifact.decodeValidated(tampered));
        assertEquals("STREAM_DESCRIPTOR_MISMATCH", failure.code());
        assertNotEquals(artifact.artifactId(), tampered.artifactId());
    }

    @Test
    void malformedUtf8AndMissingUpsertKeyFailClosed() throws Exception {
        PortableStreamPlan valid = appendPlan(
                PortableStreamPlan.DeliveryGuarantee.EXACTLY_ONCE,
                StreamSinkBinding.VisibilityProfile.CHECKPOINT_TRANSACTIONAL);
        byte[] malformed = PortableStreamPlanCodec.encode(valid);
        byte[] needle = "log-v1".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        int offset = indexOf(malformed, needle);
        malformed[offset] = (byte) 0xc3;
        malformed[offset + 1] = 0x28;
        rewriteChecksum(malformed);
        assertFailure("MALFORMED_UTF8", () -> PortableStreamPlanCodec.decode(malformed));

        java.util.List<String> upsertCapabilities = Arrays.asList(
                PortableStreamPlan.STREAM_CAPABILITY,
                PortableStreamPlan.UPSERT_CAPABILITY,
                PortableStreamPlan.RESIDENT_CDC_CAPABILITY);
        PortableStreamPlan upsert = residentCdcPlan(
                PortableStreamPlan.ChangelogMode.UPSERT, upsertCapabilities,
                "STREAM_CDC_ENUMERATOR_V1");
        PortableStreamNode sink = upsert.nodes().get(1);
        PortableStreamNode keylessSink = new PortableStreamNode(
                sink.operatorUid(), sink.opcode(), sink.inputOperatorUids(),
                sink.inputSchemaFingerprints(), sink.outputSchema(),
                Collections.emptyList(), sink.stateDescriptors(),
                sink.scalarParameters(), sink.binding());
        assertFailure("MISSING_UPSERT_KEY", () -> new PortableStreamPlan(
                upsert.planVersion(), upsert.deliveryGuarantee(),
                upsert.parallelism(), upsert.maxParallelism(),
                upsert.changelogMode(), upsert.timeMode(),
                upsert.rootSinkOperatorUids(),
                Arrays.asList(upsert.nodes().get(0), keylessSink),
                upsert.edges(), upsertCapabilities));
    }

    @Test
    void functionOperatorsRoundTripWithExactStreamAbiAndDeploymentIdentity() {
        PortableStreamPlan plan = mapPlan(StreamFunctionBinding.FunctionKind.MAP);

        byte[] encoded = PortableStreamPlanCodec.encode(plan);
        assertEquals(plan, PortableStreamPlanCodec.decode(encoded));
        PlanArtifact artifact = PortableStreamPlanArtifact.wrap("stream-map", plan);
        assertTrue(artifact.requiredFeatures().contains(
                PlanArtifact.DEPLOYMENT_FEATURE_PREFIX + DIGEST_A));

        PortableStreamNode map = plan.nodes().get(1);
        StreamFunctionBinding binding = (StreamFunctionBinding) map.binding();
        assertEquals(StreamFunctionBinding.FunctionKind.MAP, binding.functionKind());
        assertEquals(DIGEST_A, binding.deploymentReference().deploymentSetId());

        assertFailure("FUNCTION_KIND_MISMATCH", () ->
                mapPlan(StreamFunctionBinding.FunctionKind.FILTER));
    }

    @Test
    void stateTimeDomainAndDisconnectedOperatorsFailPlanLegality() {
        PortableStreamPlan valid = appendPlan(
                PortableStreamPlan.DeliveryGuarantee.EXACTLY_ONCE,
                StreamSinkBinding.VisibilityProfile.CHECKPOINT_TRANSACTIONAL);
        PortableStreamNode source = valid.nodes().get(0);
        PortableStreamNode statefulSource = new PortableStreamNode(
                source.operatorUid(), source.opcode(), source.inputOperatorUids(),
                source.inputSchemaFingerprints(), source.outputSchema(),
                source.stableKeyFields(), Collections.singletonList(
                new StreamStateDescriptor("cursor", StreamStateDescriptor.Kind.OPERATOR_LIST,
                        "stream.cursor.v1", 1,
                        StreamStateDescriptor.TtlMode.EVENT_TIME, 1000L)),
                source.scalarParameters(), source.binding());
        assertFailure("STATE_TIME_MODE_MISMATCH", () -> new PortableStreamPlan(
                valid.planVersion(), valid.deliveryGuarantee(), valid.parallelism(),
                valid.maxParallelism(), valid.changelogMode(),
                PortableStreamPlan.TimeMode.PROCESSING_TIME,
                valid.rootSinkOperatorUids(),
                Arrays.asList(statefulSource, valid.nodes().get(1)),
                valid.edges(),
                valid.requiredCapabilities()));

        PortableStreamNode disconnected = new PortableStreamNode(
                "unused-source", StreamOperatorCode.SOURCE,
                Collections.emptyList(), Collections.emptyList(), schema(),
                Collections.emptyList(), Collections.emptyList(),
                Collections.emptyMap(), new StreamSourceBinding(
                "log-v1", DIGEST_A, sha256Hex(new byte[]{4}), new byte[]{4},
                Collections.singletonList(new StreamSourceSplit(
                        repeat('c', 64), "stream-position-v1", new byte[]{5})),
                StreamSourceBinding.ReplayGuarantee.EXACT_ORDERED));
        assertFailure("UNREACHABLE_OPERATOR", () -> copyPlan(
                valid, valid.parallelism(), valid.maxParallelism(),
                Arrays.asList(valid.nodes().get(0), valid.nodes().get(1), disconnected)));
    }

    @Test
    void keyAndFinalEventTimeWindowContractsAreClosedBeforeAdmission() {
        java.util.Map<String, String> tumbling = new java.util.LinkedHashMap<>();
        tumbling.put("window.type", "TUMBLING");
        tumbling.put("window.sizeMillis", "60000");
        tumbling.put("window.slideMillis", "60000");
        tumbling.put("window.trigger", "FINAL");
        PortableStreamPlan valid = windowPlan(
                PortableStreamPlan.TimeMode.EVENT_TIME, tumbling);
        assertEquals(valid, PortableStreamPlanCodec.decode(
                PortableStreamPlanCodec.encode(valid)));

        assertFailure("TIME_MODE_MISMATCH", () -> windowPlan(
                PortableStreamPlan.TimeMode.PROCESSING_TIME, tumbling));
        java.util.Map<String, String> unbounded = new java.util.LinkedHashMap<>(tumbling);
        unbounded.put("window.sizeMillis", "0");
        assertFailure("INVALID_WINDOW", () -> windowPlan(
                PortableStreamPlan.TimeMode.EVENT_TIME, unbounded));

        PortableStreamPlan base = appendPlan(
                PortableStreamPlan.DeliveryGuarantee.EXACTLY_ONCE,
                StreamSinkBinding.VisibilityProfile.CHECKPOINT_TRANSACTIONAL);
        PortableStreamNode source = base.nodes().get(0);
        PortableStreamNode keyBy = new PortableStreamNode(
                "key-by", StreamOperatorCode.KEY_BY,
                Collections.singletonList("source"),
                Collections.singletonList(source.outputSchema().fingerprint()),
                source.outputSchema(), Collections.emptyList(),
                Collections.emptyList(), Collections.emptyMap(), null);
        PortableStreamNode sink = new PortableStreamNode(
                "sink", StreamOperatorCode.SINK,
                Collections.singletonList("key-by"),
                Collections.singletonList(source.outputSchema().fingerprint()),
                source.outputSchema(), Collections.emptyList(),
                Collections.emptyList(), Collections.emptyMap(),
                base.nodes().get(1).binding());
        assertFailure("MISSING_STABLE_KEY", () -> new PortableStreamPlan(
                base.planVersion(), base.deliveryGuarantee(), base.parallelism(),
                base.maxParallelism(), base.changelogMode(), base.timeMode(),
                base.rootSinkOperatorUids(), Arrays.asList(source, keyBy, sink),
                base.edges(),
                base.requiredCapabilities()));
    }

    @Test
    void temporalJoinRequiresFiniteEventTimeStateAndCapabilityBeforeAdmission() {
        java.util.Map<String, String> interval = new java.util.LinkedHashMap<>();
        interval.put("join.leftKeyField", "id");
        interval.put("join.lowerBoundMillis", "-1000");
        interval.put("join.rightKeyField", "id");
        interval.put("join.upperBoundMillis", "2000");
        PortableStreamPlan valid = temporalJoinPlan(
                PortableStreamPlan.TimeMode.EVENT_TIME, interval,
                Arrays.asList(PortableStreamPlan.STREAM_CAPABILITY,
                        "rowweave.runtime.stream.temporal-join.v1",
                        "rowweave.runtime.stream.upsert.v1"),
                temporalJoinStates());
        assertEquals(valid, PortableStreamPlanCodec.decode(
                PortableStreamPlanCodec.encode(valid)));

        assertFailure("TIME_MODE_MISMATCH", () -> temporalJoinPlan(
                PortableStreamPlan.TimeMode.PROCESSING_TIME, interval,
                valid.requiredCapabilities(), temporalJoinStates()));

        java.util.Map<String, String> missingUpper =
                new java.util.LinkedHashMap<>(interval);
        missingUpper.remove("join.upperBoundMillis");
        assertFailure("INVALID_TEMPORAL_JOIN", () -> temporalJoinPlan(
                PortableStreamPlan.TimeMode.EVENT_TIME, missingUpper,
                valid.requiredCapabilities(), temporalJoinStates()));

        java.util.Map<String, String> reversed =
                new java.util.LinkedHashMap<>(interval);
        reversed.put("join.lowerBoundMillis", "2001");
        assertFailure("INVALID_TEMPORAL_JOIN", () -> temporalJoinPlan(
                PortableStreamPlan.TimeMode.EVENT_TIME, reversed,
                valid.requiredCapabilities(), temporalJoinStates()));

        assertFailure("MISSING_CAPABILITY", () -> temporalJoinPlan(
                PortableStreamPlan.TimeMode.EVENT_TIME, interval,
                Collections.singletonList(PortableStreamPlan.STREAM_CAPABILITY),
                temporalJoinStates()));

        assertFailure("INVALID_TEMPORAL_JOIN_STATE", () -> temporalJoinPlan(
                PortableStreamPlan.TimeMode.EVENT_TIME, interval,
                valid.requiredCapabilities(), Collections.singletonList(
                        temporalJoinStates().get(0))));
    }

    @Test
    void residentCdcSourceRequiresExactEnumeratorUpsertAndCapability() {
        PortableStreamPlan valid = residentCdcPlan(
                PortableStreamPlan.ChangelogMode.UPSERT,
                Arrays.asList(PortableStreamPlan.STREAM_CAPABILITY,
                        PortableStreamPlan.UPSERT_CAPABILITY,
                        PortableStreamPlan.RESIDENT_CDC_CAPABILITY),
                "STREAM_CDC_ENUMERATOR_V1");
        assertEquals(valid, PortableStreamPlanCodec.decode(
                PortableStreamPlanCodec.encode(valid)));

        assertFailure("MISSING_CAPABILITY", () -> residentCdcPlan(
                PortableStreamPlan.ChangelogMode.UPSERT,
                Arrays.asList(PortableStreamPlan.STREAM_CAPABILITY,
                        PortableStreamPlan.UPSERT_CAPABILITY),
                "STREAM_CDC_ENUMERATOR_V1"));
        assertFailure("CHANGELOG_MODE_MISMATCH", () -> residentCdcPlan(
                PortableStreamPlan.ChangelogMode.APPEND,
                Arrays.asList(PortableStreamPlan.STREAM_CAPABILITY,
                        PortableStreamPlan.RESIDENT_CDC_CAPABILITY),
                "STREAM_CDC_ENUMERATOR_V1"));
        assertFailure("INVALID_RESIDENT_CDC_SOURCE", () -> residentCdcPlan(
                PortableStreamPlan.ChangelogMode.UPSERT,
                Arrays.asList(PortableStreamPlan.STREAM_CAPABILITY,
                        PortableStreamPlan.UPSERT_CAPABILITY,
                        PortableStreamPlan.RESIDENT_CDC_CAPABILITY),
                "BOUNDED_CDC_FRONTIER_V1"));
    }

    @Test
    void residentCdcRejectsMultipleEnumeratorSplitsBeforeExecution() {
        PortableStreamPlan base = residentCdcPlan(
                PortableStreamPlan.ChangelogMode.UPSERT,
                Arrays.asList(PortableStreamPlan.STREAM_CAPABILITY,
                        PortableStreamPlan.UPSERT_CAPABILITY,
                        PortableStreamPlan.RESIDENT_CDC_CAPABILITY),
                "STREAM_CDC_ENUMERATOR_V1");
        PortableStreamNode sourceBase = base.nodes().get(0);
        StreamSourceBinding binding = (StreamSourceBinding)
                sourceBase.binding();
        StreamSourceBinding multiple = new StreamSourceBinding(
                binding.providerKey(), binding.providerContractDigest(),
                binding.configurationDigest(), binding.configurationFrame(),
                Arrays.asList(binding.fixedSplits().get(0),
                        new StreamSourceSplit(repeat('e', 64),
                                "stream-position-v1", new byte[] {42})),
                binding.replayGuarantee());
        PortableStreamNode source = new PortableStreamNode(
                sourceBase.operatorUid(), sourceBase.opcode(),
                sourceBase.inputOperatorUids(),
                sourceBase.inputSchemaFingerprints(),
                sourceBase.outputSchema(), sourceBase.stableKeyFields(),
                sourceBase.stateDescriptors(), sourceBase.scalarParameters(),
                multiple);

        assertFailure("INVALID_RESIDENT_CDC_SOURCE", () ->
                new PortableStreamPlan(base.planVersion(),
                        base.deliveryGuarantee(), base.parallelism(),
                        base.maxParallelism(), base.changelogMode(),
                        base.timeMode(), base.rootSinkOperatorUids(),
                        Arrays.asList(source, base.nodes().get(1)),
                        base.edges(), base.requiredCapabilities()));
    }

    @Test
    void portablePlanRejectsResidentCdcKeyChangeBeforeExecution() {
        PortableStreamPlan base = residentCdcPlan(
                PortableStreamPlan.ChangelogMode.UPSERT,
                Arrays.asList(PortableStreamPlan.STREAM_CAPABILITY,
                        PortableStreamPlan.UPSERT_CAPABILITY,
                        PortableStreamPlan.RESIDENT_CDC_CAPABILITY),
                "STREAM_CDC_ENUMERATOR_V1");
        PortableStreamNode source = base.nodes().get(0);
        PortableStreamNode key = new PortableStreamNode(
                "adaptor-key", StreamOperatorCode.KEY_BY,
                Collections.singletonList(source.operatorUid()),
                Collections.singletonList(source.outputSchema().fingerprint()),
                source.outputSchema(), Collections.singletonList("adaptor"),
                Collections.emptyList(), Collections.emptyMap(), null);
        PortableStreamNode baseSink = base.nodes().get(1);
        PortableStreamNode sink = new PortableStreamNode(
                baseSink.operatorUid(), baseSink.opcode(),
                Collections.singletonList(key.operatorUid()),
                Collections.singletonList(key.outputSchema().fingerprint()),
                key.outputSchema(), key.stableKeyFields(),
                baseSink.stateDescriptors(), baseSink.scalarParameters(),
                baseSink.binding());

        assertFailure("CHANGELOG_MODE_MISMATCH", () ->
                new PortableStreamPlan(base.planVersion(),
                        base.deliveryGuarantee(), base.parallelism(),
                        base.maxParallelism(), base.changelogMode(),
                        base.timeMode(), base.rootSinkOperatorUids(),
                        Arrays.asList(source, key, sink), Arrays.asList(
                        new StreamEdge("source", "adaptor-key", 0,
                                "source-key", StreamEdge.Partitioning.HASH,
                                1024, 4096),
                        new StreamEdge("adaptor-key", "sink", 0,
                                "key-sink", StreamEdge.Partitioning.FORWARD,
                        1024, 4096)), base.requiredCapabilities()));
    }

    @Test
    void portablePlanRejectsNonTombstoneUpsertKeyChangeBeforeExecution() {
        java.util.Map<String, String> interval =
                new java.util.LinkedHashMap<>();
        interval.put("join.leftKeyField", "id");
        interval.put("join.lowerBoundMillis", "-1000");
        interval.put("join.rightKeyField", "id");
        interval.put("join.upperBoundMillis", "2000");
        PortableStreamPlan base = temporalJoinPlan(
                PortableStreamPlan.TimeMode.EVENT_TIME, interval,
                Arrays.asList(PortableStreamPlan.STREAM_CAPABILITY,
                        PortableStreamPlan.UPSERT_CAPABILITY,
                        PortableStreamPlan.TEMPORAL_JOIN_CAPABILITY),
                temporalJoinStates());
        PortableStreamNode join = base.nodes().get(2);
        PortableStreamNode key = new PortableStreamNode(
                "adaptor-key", StreamOperatorCode.KEY_BY,
                Collections.singletonList(join.operatorUid()),
                Collections.singletonList(join.outputSchema().fingerprint()),
                join.outputSchema(), Collections.singletonList("adaptor"),
                Collections.emptyList(), Collections.emptyMap(), null);
        PortableStreamNode sinkBase = base.nodes().get(3);
        PortableStreamNode sink = new PortableStreamNode(
                sinkBase.operatorUid(), sinkBase.opcode(),
                Collections.singletonList(key.operatorUid()),
                Collections.singletonList(key.outputSchema().fingerprint()),
                key.outputSchema(), key.stableKeyFields(),
                sinkBase.stateDescriptors(), sinkBase.scalarParameters(),
                sinkBase.binding());

        assertFailure("CHANGELOG_MODE_MISMATCH", () ->
                new PortableStreamPlan(base.planVersion(),
                        base.deliveryGuarantee(), base.parallelism(),
                        base.maxParallelism(), base.changelogMode(),
                        base.timeMode(), base.rootSinkOperatorUids(),
                        Arrays.asList(base.nodes().get(0),
                                base.nodes().get(1), join, key, sink),
                        Arrays.asList(
                                new StreamEdge("left-source",
                                        join.operatorUid(), 0,
                                        "left-join-0",
                                        StreamEdge.Partitioning.HASH,
                                        1024, 4096),
                                new StreamEdge("right-source",
                                        join.operatorUid(), 1,
                                        "right-join-1",
                                        StreamEdge.Partitioning.HASH,
                                        1024, 4096),
                                new StreamEdge(join.operatorUid(),
                                        key.operatorUid(), 0, "join-key",
                                        StreamEdge.Partitioning.HASH,
                                        1024, 4096),
                                new StreamEdge(key.operatorUid(),
                                        sink.operatorUid(), 0, "key-sink",
                                        StreamEdge.Partitioning.FORWARD,
                                        1024, 4096)),
                        base.requiredCapabilities()));
    }

    @Test
    void portableUnionRejectsMixedInferredInputChangelogModes() {
        PortableStreamPlan base = residentCdcPlan(
                PortableStreamPlan.ChangelogMode.UPSERT,
                Arrays.asList(PortableStreamPlan.STREAM_CAPABILITY,
                        PortableStreamPlan.UPSERT_CAPABILITY,
                        PortableStreamPlan.RESIDENT_CDC_CAPABILITY),
                "STREAM_CDC_ENUMERATOR_V1");
        PortableStreamNode resident = base.nodes().get(0);
        PortableStreamNode append = new PortableStreamNode(
                "append-source", StreamOperatorCode.SOURCE,
                Collections.emptyList(), Collections.emptyList(),
                resident.outputSchema(), resident.stableKeyFields(),
                Collections.emptyList(), Collections.emptyMap(),
                new StreamSourceBinding("append-log-v1", DIGEST_B,
                        sha256Hex(new byte[] {8}), new byte[] {8},
                        Collections.singletonList(new StreamSourceSplit(
                                repeat('c', 64), "stream-position-v1",
                                new byte[] {9})),
                        StreamSourceBinding.ReplayGuarantee.EXACT_ORDERED));
        PortableStreamNode union = new PortableStreamNode(
                "union", StreamOperatorCode.UNION,
                Arrays.asList(resident.operatorUid(), append.operatorUid()),
                Arrays.asList(resident.outputSchema().fingerprint(),
                        append.outputSchema().fingerprint()),
                resident.outputSchema(), resident.stableKeyFields(),
                Collections.emptyList(), Collections.emptyMap(), null);
        PortableStreamNode baseSink = base.nodes().get(1);
        PortableStreamNode sink = new PortableStreamNode(
                baseSink.operatorUid(), baseSink.opcode(),
                Collections.singletonList(union.operatorUid()),
                Collections.singletonList(union.outputSchema().fingerprint()),
                union.outputSchema(), union.stableKeyFields(),
                baseSink.stateDescriptors(), baseSink.scalarParameters(),
                baseSink.binding());

        assertFailure("CHANGELOG_MODE_MISMATCH", () ->
                new PortableStreamPlan(base.planVersion(),
                        base.deliveryGuarantee(), base.parallelism(),
                        base.maxParallelism(), base.changelogMode(),
                        base.timeMode(), base.rootSinkOperatorUids(),
                        Arrays.asList(resident, append, union, sink),
                        Arrays.asList(
                                new StreamEdge("source", "union", 0,
                                        "resident-union",
                                        StreamEdge.Partitioning.FORWARD,
                                        1024, 4096),
                                new StreamEdge("append-source", "union", 1,
                                        "append-union",
                                        StreamEdge.Partitioning.FORWARD,
                                        1024, 4096),
                                new StreamEdge("union", "sink", 0,
                                        "union-sink",
                                        StreamEdge.Partitioning.FORWARD,
                                        1024, 4096)),
                        Arrays.asList(PortableStreamPlan.STREAM_CAPABILITY,
                                PortableStreamPlan.UPSERT_CAPABILITY,
                                PortableStreamPlan.RESIDENT_CDC_CAPABILITY,
                                PortableStreamPlan.UNION_CAPABILITY)));
    }

    @Test
    void nodeExecutionChangelogMustEqualItsDerivedOutputContract() {
        java.util.List<String> capabilities = Arrays.asList(
                PortableStreamPlan.STREAM_CAPABILITY,
                PortableStreamPlan.UPSERT_CAPABILITY,
                PortableStreamPlan.RESIDENT_CDC_CAPABILITY,
                PortableStreamPlan.NODE_EXECUTION_CONTRACT_CAPABILITY);
        PortableStreamPlan base = residentCdcPlan(
                PortableStreamPlan.ChangelogMode.UPSERT,
                Arrays.asList(PortableStreamPlan.STREAM_CAPABILITY,
                        PortableStreamPlan.UPSERT_CAPABILITY,
                        PortableStreamPlan.RESIDENT_CDC_CAPABILITY),
                "STREAM_CDC_ENUMERATOR_V1");
        PortableStreamNode source = withExecution(base.nodes().get(0),
                PortableStreamPlan.ChangelogMode.APPEND, 1, 4);
        PortableStreamNode sink = withExecution(base.nodes().get(1),
                PortableStreamPlan.ChangelogMode.UPSERT);

        assertFailure("INVALID_NODE_EXECUTION_CONTRACT", () ->
                new PortableStreamPlan(base.planVersion(),
                        base.deliveryGuarantee(), base.parallelism(),
                        base.maxParallelism(), base.changelogMode(),
                        base.timeMode(), base.rootSinkOperatorUids(),
                        Arrays.asList(source, sink), base.edges(),
                        capabilities));
    }

    @Test
    void nodeExecutionTimeMustEqualThePlanTimeAuthority() {
        PortableStreamPlan base = appendPlan(
                PortableStreamPlan.DeliveryGuarantee.EXACTLY_ONCE,
                StreamSinkBinding.VisibilityProfile.CHECKPOINT_TRANSACTIONAL);
        PortableStreamNode sourceBase = base.nodes().get(0);
        PortableStreamNode source = new PortableStreamNode(
                sourceBase.operatorUid(), sourceBase.opcode(),
                sourceBase.inputOperatorUids(),
                sourceBase.inputSchemaFingerprints(), sourceBase.outputSchema(),
                sourceBase.stableKeyFields(), sourceBase.stateDescriptors(),
                sourceBase.scalarParameters(), sourceBase.binding(),
                new StreamNodeExecutionDescriptor(
                        PortableStreamPlan.ChangelogMode.APPEND,
                        PortableStreamPlan.TimeMode.PROCESSING_TIME,
                        1, 128, 1_048_576L, 1_048_576L, 0L, 1));
        PortableStreamNode sink = withExecution(base.nodes().get(1),
                PortableStreamPlan.ChangelogMode.APPEND, 2, 4);

        assertFailure("INVALID_NODE_EXECUTION_CONTRACT", () ->
                new PortableStreamPlan(base.planVersion(),
                        base.deliveryGuarantee(), base.parallelism(),
                        base.maxParallelism(), base.changelogMode(),
                        base.timeMode(), base.rootSinkOperatorUids(),
                        Arrays.asList(source, sink), base.edges(), Arrays.asList(
                        PortableStreamPlan.STREAM_CAPABILITY,
                        PortableStreamPlan.NODE_EXECUTION_CONTRACT_CAPABILITY)));
    }

    @Test
    void checkpointPayloadParticipantRequiresPositiveNodeLease() {
        PortableStreamPlan base = appendPlan(
                PortableStreamPlan.DeliveryGuarantee.EXACTLY_ONCE,
                StreamSinkBinding.VisibilityProfile.CHECKPOINT_TRANSACTIONAL);
        PortableStreamNode sourceBase = base.nodes().get(0);
        PortableStreamNode source = new PortableStreamNode(
                sourceBase.operatorUid(), sourceBase.opcode(),
                sourceBase.inputOperatorUids(),
                sourceBase.inputSchemaFingerprints(),
                sourceBase.outputSchema(), sourceBase.stableKeyFields(),
                sourceBase.stateDescriptors(), sourceBase.scalarParameters(),
                sourceBase.binding(), new StreamNodeExecutionDescriptor(
                PortableStreamPlan.ChangelogMode.APPEND,
                PortableStreamPlan.TimeMode.EVENT_TIME,
                1, 128, 1_048_576L, 0L, 0L, 2));
        PortableStreamNode sink = withExecution(base.nodes().get(1),
                PortableStreamPlan.ChangelogMode.APPEND, 2, 4);

        PortableStreamPlan plan = new PortableStreamPlan(base.planVersion(),
                base.deliveryGuarantee(), base.parallelism(),
                base.maxParallelism(), base.changelogMode(),
                base.timeMode(), base.rootSinkOperatorUids(),
                Arrays.asList(source, sink), base.edges(), Arrays.asList(
                PortableStreamPlan.STREAM_CAPABILITY,
                PortableStreamPlan.EVENT_TIME_ASSIGNMENT_CAPABILITY,
                PortableStreamPlan.NODE_EXECUTION_CONTRACT_CAPABILITY));

        assertFailure("INVALID_NODE_EXECUTION_CONTRACT", () ->
                new PortableStreamJob(plan,
                        StreamCheckpointConfig.periodic(1_000L, 10_000L, 1),
                        StreamRestartPolicy.fixedDelay(1, 0L),
                        StreamResourceProfile.defaults()));
    }

    @Test
    void alignedJobKeepsTheFrozenV1EnvelopeAndLegacyReader() {
        PortableStreamPlan base = appendPlan(
                PortableStreamPlan.DeliveryGuarantee.EXACTLY_ONCE,
                StreamSinkBinding.VisibilityProfile.CHECKPOINT_TRANSACTIONAL);
        PortableStreamPlan plan = withCapabilities(base, Arrays.asList(
                PortableStreamPlan.STREAM_CAPABILITY,
                PortableStreamPlan.EVENT_TIME_ASSIGNMENT_CAPABILITY));
        PortableStreamJob job = new PortableStreamJob(plan,
                StreamCheckpointConfig.periodic(1_000L, 10_000L, 2),
                StreamRestartPolicy.fixedDelay(1, 0L),
                StreamResourceProfile.defaults());

        byte[] encoded = PortableStreamJobCodec.encode(job);

        assertArrayEquals("RWXXPSJ1".getBytes(
                java.nio.charset.StandardCharsets.US_ASCII),
                Arrays.copyOf(encoded, 8));
        assertEquals(StreamCheckpointMode.ALIGNED,
                PortableStreamJobCodec.decodeV1(encoded)
                        .checkpointConfig().mode());
        assertArrayEquals(encoded, PortableStreamJobCodec.encode(
                PortableStreamJobCodec.decode(encoded)));
    }

    @Test
    void unalignedJobUsesV2AndLegacyReaderFailsClosed() {
        PortableStreamPlan base = appendPlan(
                PortableStreamPlan.DeliveryGuarantee.EXACTLY_ONCE,
                StreamSinkBinding.VisibilityProfile.CHECKPOINT_TRANSACTIONAL);
        PortableStreamPlan v2 = withCapabilities(base, Arrays.asList(
                PortableStreamPlan.STREAM_CAPABILITY,
                PortableStreamPlan.EVENT_TIME_ASSIGNMENT_CAPABILITY,
                PortableStreamPlan.UNALIGNED_CHECKPOINT_CAPABILITY));
        PortableStreamJob job = new PortableStreamJob(v2,
                StreamCheckpointConfig.periodicUnaligned(
                        1_000L, 10_000L, 2),
                StreamRestartPolicy.fixedDelay(1, 0L),
                StreamResourceProfile.defaults());

        byte[] encoded = PortableStreamJobCodec.encode(job);

        assertArrayEquals("RWXXPSJ2".getBytes(
                java.nio.charset.StandardCharsets.US_ASCII),
                Arrays.copyOf(encoded, 8));
        assertEquals(job, PortableStreamJobCodec.decode(encoded));
        assertEquals(StreamCheckpointMode.UNALIGNED,
                PortableStreamJobCodec.decode(encoded)
                        .checkpointConfig().mode());
        assertFailure("UNSUPPORTED_ENVELOPE_VERSION",
                () -> PortableStreamJobCodec.decodeV1(encoded));
    }

    @Test
    void unalignedModeRequiresItsPlanCapability() {
        PortableStreamPlan base = appendPlan(
                PortableStreamPlan.DeliveryGuarantee.EXACTLY_ONCE,
                StreamSinkBinding.VisibilityProfile.CHECKPOINT_TRANSACTIONAL);
        PortableStreamPlan plan = withCapabilities(base, Arrays.asList(
                PortableStreamPlan.STREAM_CAPABILITY,
                PortableStreamPlan.EVENT_TIME_ASSIGNMENT_CAPABILITY));

        assertFailure("MISSING_CAPABILITY", () -> new PortableStreamJob(plan,
                StreamCheckpointConfig.periodicUnaligned(
                        1_000L, 10_000L, 2),
                StreamRestartPolicy.fixedDelay(1, 0L),
                StreamResourceProfile.defaults()));
    }

    @Test
    void unalignedModeRequiresPositiveDurableChannelStateLease() {
        PortableStreamPlan base = appendPlan(
                PortableStreamPlan.DeliveryGuarantee.AT_LEAST_ONCE,
                StreamSinkBinding.VisibilityProfile.EAGER_AT_LEAST_ONCE);
        PortableStreamPlan plan = withCapabilities(base, Arrays.asList(
                PortableStreamPlan.STREAM_CAPABILITY,
                PortableStreamPlan.EVENT_TIME_ASSIGNMENT_CAPABILITY,
                PortableStreamPlan.UNALIGNED_CHECKPOINT_CAPABILITY));
        StreamResourceProfile noCheckpointStorage =
                new StreamResourceProfile(8, 1_024, 4_096,
                        0L, 0, 0, 0L, 4, 16, 0L);

        assertFailure("INVALID_JOB_RESOURCE_CEILING",
                () -> new PortableStreamJob(plan,
                        StreamCheckpointConfig.manualOnlyUnaligned(
                                10_000L, 1),
                        StreamRestartPolicy.fixedDelay(1, 0L),
                        noCheckpointStorage));
    }

    @Test
    void independentAppendSinkRemainsValidInMixedUpsertPlan() {
        PortableStreamPlan resident = residentCdcPlan(
                PortableStreamPlan.ChangelogMode.UPSERT,
                Arrays.asList(PortableStreamPlan.STREAM_CAPABILITY,
                        PortableStreamPlan.UPSERT_CAPABILITY,
                        PortableStreamPlan.RESIDENT_CDC_CAPABILITY),
                "STREAM_CDC_ENUMERATOR_V1");
        PortableStreamPlan append = appendPlan(
                PortableStreamPlan.DeliveryGuarantee.EXACTLY_ONCE,
                StreamSinkBinding.VisibilityProfile.CHECKPOINT_TRANSACTIONAL);
        PortableStreamNode appendSourceBase = append.nodes().get(0);
        PortableStreamNode appendSource = new PortableStreamNode(
                "append-source", appendSourceBase.opcode(),
                appendSourceBase.inputOperatorUids(),
                appendSourceBase.inputSchemaFingerprints(),
                appendSourceBase.outputSchema(), Collections.emptyList(),
                appendSourceBase.stateDescriptors(),
                appendSourceBase.scalarParameters(), appendSourceBase.binding());
        PortableStreamNode appendSinkBase = append.nodes().get(1);
        PortableStreamNode appendSink = new PortableStreamNode(
                "append-sink", appendSinkBase.opcode(),
                Collections.singletonList(appendSource.operatorUid()),
                appendSinkBase.inputSchemaFingerprints(),
                appendSinkBase.outputSchema(), Collections.emptyList(),
                appendSinkBase.stateDescriptors(),
                appendSinkBase.scalarParameters(), appendSinkBase.binding());

        assertDoesNotThrow(() -> new PortableStreamPlan(
                resident.planVersion(), resident.deliveryGuarantee(),
                resident.parallelism(), resident.maxParallelism(),
                resident.changelogMode(), resident.timeMode(),
                Arrays.asList("sink", "append-sink"),
                Arrays.asList(resident.nodes().get(0), appendSource,
                        resident.nodes().get(1), appendSink),
                Arrays.asList(resident.edges().get(0), new StreamEdge(
                        "append-source", "append-sink", 0,
                        "append-source-sink", StreamEdge.Partitioning.FORWARD,
                        1024, 4096)), resident.requiredCapabilities()));
    }

    @Test
    void sourceExecutionParallelismMustEqualItsFixedSplitCount() {
        PortableStreamPlan base = appendPlan(
                PortableStreamPlan.DeliveryGuarantee.EXACTLY_ONCE,
                StreamSinkBinding.VisibilityProfile.CHECKPOINT_TRANSACTIONAL);
        java.util.List<String> capabilities = Arrays.asList(
                PortableStreamPlan.STREAM_CAPABILITY,
                PortableStreamPlan.NODE_EXECUTION_CONTRACT_CAPABILITY);
        PortableStreamNode sourceWithOneSplit = withExecution(
                base.nodes().get(0), PortableStreamPlan.ChangelogMode.APPEND,
                2, 4);
        PortableStreamNode sink = withExecution(base.nodes().get(1),
                PortableStreamPlan.ChangelogMode.APPEND, 2, 4);
        assertFailure("INVALID_NODE_EXECUTION_CONTRACT", () ->
                new PortableStreamPlan(base.planVersion(),
                        base.deliveryGuarantee(), base.parallelism(),
                        base.maxParallelism(), base.changelogMode(),
                        base.timeMode(), base.rootSinkOperatorUids(),
                        Arrays.asList(sourceWithOneSplit, sink), base.edges(),
                        capabilities));

        StreamSourceBinding oneSplit = (StreamSourceBinding)
                base.nodes().get(0).binding();
        StreamSourceBinding twoSplits = new StreamSourceBinding(
                oneSplit.providerKey(), oneSplit.providerContractDigest(),
                oneSplit.configurationDigest(), oneSplit.configurationFrame(),
                Arrays.asList(oneSplit.fixedSplits().get(0),
                        new StreamSourceSplit(repeat('c', 64),
                                "stream-position-v1", new byte[] {7})),
                oneSplit.replayGuarantee());
        PortableStreamNode twoSplitSource = new PortableStreamNode(
                "source", StreamOperatorCode.SOURCE,
                Collections.emptyList(), Collections.emptyList(),
                base.nodes().get(0).outputSchema(), Collections.emptyList(),
                Collections.emptyList(), Collections.emptyMap(), twoSplits,
                new StreamNodeExecutionDescriptor(
                        PortableStreamPlan.ChangelogMode.APPEND,
                        PortableStreamPlan.TimeMode.EVENT_TIME, 1, 128,
                        1_048_576L, 1_048_576L, 0L, 4));
        assertFailure("INVALID_NODE_EXECUTION_CONTRACT", () ->
                new PortableStreamPlan(base.planVersion(),
                        base.deliveryGuarantee(), base.parallelism(),
                        base.maxParallelism(), base.changelogMode(),
                        base.timeMode(), base.rootSinkOperatorUids(),
                        Arrays.asList(twoSplitSource, sink), base.edges(),
                        capabilities));
    }

    @Test
    void eventTimeSourceExecutionMustLeaseItsWatchdogThread() {
        PortableStreamPlan base = appendPlan(
                PortableStreamPlan.DeliveryGuarantee.EXACTLY_ONCE,
                StreamSinkBinding.VisibilityProfile.CHECKPOINT_TRANSACTIONAL);
        java.util.Map<String, String> policy =
                new java.util.LinkedHashMap<>();
        policy.put("source.time.timestamp", "PROVIDER_RECORD");
        policy.put("source.time.watermark", "PROVIDER_EVENT");
        policy.put("source.time.status", "GENERATION_FENCED_ACTIVE_IDLE");
        policy.put("source.time.idleTimeoutMillis", "1000");
        PortableStreamNode source = new PortableStreamNode(
                "source", StreamOperatorCode.SOURCE,
                Collections.emptyList(), Collections.emptyList(),
                base.nodes().get(0).outputSchema(), Collections.emptyList(),
                Collections.emptyList(), policy, base.nodes().get(0).binding(),
                new StreamNodeExecutionDescriptor(
                        PortableStreamPlan.ChangelogMode.APPEND,
                        PortableStreamPlan.TimeMode.EVENT_TIME, 1, 128,
                        1_048_576L, 1_048_576L, 0L, 1));
        PortableStreamNode sink = withExecution(base.nodes().get(1),
                PortableStreamPlan.ChangelogMode.APPEND, 2, 4);

        assertFailure("INVALID_NODE_EXECUTION_CONTRACT", () ->
                new PortableStreamPlan(base.planVersion(),
                        base.deliveryGuarantee(), base.parallelism(),
                        base.maxParallelism(), base.changelogMode(),
                        base.timeMode(), base.rootSinkOperatorUids(),
                        Arrays.asList(source, sink), base.edges(), Arrays.asList(
                        PortableStreamPlan.STREAM_CAPABILITY,
                        PortableStreamPlan.SOURCE_TIME_POLICY_CAPABILITY,
                        PortableStreamPlan.NODE_EXECUTION_CONTRACT_CAPABILITY)));
    }

    @Test
    void legacySourceSplitCountMustNotExceedThePlanParallelismCeiling() {
        PortableStreamPlan base = appendPlan(
                PortableStreamPlan.DeliveryGuarantee.EXACTLY_ONCE,
                StreamSinkBinding.VisibilityProfile.CHECKPOINT_TRANSACTIONAL);
        StreamSourceBinding original = (StreamSourceBinding)
                base.nodes().get(0).binding();
        List<StreamSourceSplit> splits = new ArrayList<>();
        for (int index = 0; index <= base.maxParallelism(); index++) {
            byte[] identity = ByteBuffer.allocate(4).putInt(index).array();
            splits.add(new StreamSourceSplit(sha256Hex(identity),
                    "stream-position-v1", identity));
        }
        StreamSourceBinding oversized = new StreamSourceBinding(
                original.providerKey(), original.providerContractDigest(),
                original.configurationDigest(),
                original.configurationFrame(), splits,
                original.replayGuarantee());
        PortableStreamNode source = new PortableStreamNode(
                "source", StreamOperatorCode.SOURCE,
                Collections.emptyList(), Collections.emptyList(),
                base.nodes().get(0).outputSchema(), Collections.emptyList(),
                Collections.emptyList(), Collections.emptyMap(), oversized);

        assertFailure("INVALID_NODE_EXECUTION_CONTRACT", () ->
                new PortableStreamPlan(base.planVersion(),
                        base.deliveryGuarantee(), base.parallelism(),
                        base.maxParallelism(), base.changelogMode(),
                        base.timeMode(), base.rootSinkOperatorUids(),
                        Arrays.asList(source, base.nodes().get(1)),
                        base.edges(), base.requiredCapabilities()));
    }

    @Test
    void nodeResidentLeaseMustCoverEveryIncidentEdgeFrame() {
        PortableStreamPlan base = appendPlan(
                PortableStreamPlan.DeliveryGuarantee.EXACTLY_ONCE,
                StreamSinkBinding.VisibilityProfile.CHECKPOINT_TRANSACTIONAL);
        PortableStreamNode sourceBase = base.nodes().get(0);
        PortableStreamNode source = new PortableStreamNode(
                sourceBase.operatorUid(), sourceBase.opcode(),
                sourceBase.inputOperatorUids(),
                sourceBase.inputSchemaFingerprints(), sourceBase.outputSchema(),
                sourceBase.stableKeyFields(), sourceBase.stateDescriptors(),
                sourceBase.scalarParameters(), sourceBase.binding(),
                new StreamNodeExecutionDescriptor(
                        PortableStreamPlan.ChangelogMode.APPEND,
                        PortableStreamPlan.TimeMode.EVENT_TIME,
                        1, 128, 1L, 1_048_576L, 0L, 1));
        PortableStreamNode sink = withExecution(base.nodes().get(1),
                PortableStreamPlan.ChangelogMode.APPEND, 2, 4);

        assertFailure("INVALID_NODE_EXECUTION_CONTRACT", () ->
                new PortableStreamPlan(base.planVersion(),
                        base.deliveryGuarantee(), base.parallelism(),
                        base.maxParallelism(), base.changelogMode(),
                        base.timeMode(), base.rootSinkOperatorUids(),
                        Arrays.asList(source, sink), base.edges(), Arrays.asList(
                        PortableStreamPlan.STREAM_CAPABILITY,
                        PortableStreamPlan.NODE_EXECUTION_CONTRACT_CAPABILITY)));
    }

    @Test
    void passThroughOperatorsMustUseTheActualInputSchemaAndKeyContract() {
        PortableRowSchema forgedSchema = new PortableRowSchema(Arrays.asList(
                new PortableField("id", "id", "id",
                        PortableDataType.LONG, false),
                new PortableField("adaptor", "adaptor", "adaptor",
                        PortableDataType.STRING, false),
                new PortableField("amount", "amount", "amount",
                        PortableDataType.STRING, true)));

        assertFailure("SCHEMA_MISMATCH", () -> passThroughPlan(
                StreamOperatorCode.FILTER, forgedSchema,
                Collections.emptyList()));
        assertFailure("INVALID_STABLE_KEY", () -> passThroughPlan(
                StreamOperatorCode.FILTER, schema(),
                Collections.singletonList("id")));
        assertFailure("SCHEMA_MISMATCH", () -> passThroughPlan(
                StreamOperatorCode.KEY_BY, forgedSchema,
                Collections.singletonList("id")));
    }

    @Test
    void sinkMetadataMustEqualItsActualEdgeBoundInputContract() {
        PortableStreamPlan base = residentCdcPlan(
                PortableStreamPlan.ChangelogMode.UPSERT,
                Arrays.asList(PortableStreamPlan.STREAM_CAPABILITY,
                        PortableStreamPlan.UPSERT_CAPABILITY,
                        PortableStreamPlan.RESIDENT_CDC_CAPABILITY),
                "STREAM_CDC_ENUMERATOR_V1");
        PortableStreamNode sinkBase = base.nodes().get(1);
        PortableStreamNode wrongKey = new PortableStreamNode(
                sinkBase.operatorUid(), sinkBase.opcode(),
                sinkBase.inputOperatorUids(),
                sinkBase.inputSchemaFingerprints(), sinkBase.outputSchema(),
                Collections.singletonList("adaptor"),
                sinkBase.stateDescriptors(), sinkBase.scalarParameters(),
                sinkBase.binding());
        assertFailure("INVALID_STABLE_KEY", () ->
                new PortableStreamPlan(base.planVersion(),
                        base.deliveryGuarantee(), base.parallelism(),
                        base.maxParallelism(), base.changelogMode(),
                        base.timeMode(), base.rootSinkOperatorUids(),
                        Arrays.asList(base.nodes().get(0), wrongKey),
                        base.edges(), base.requiredCapabilities()));

        PortableRowSchema wrongSchema = new PortableRowSchema(Arrays.asList(
                new PortableField("id", "id", "id",
                        PortableDataType.LONG, false),
                new PortableField("adaptor", "adaptor", "adaptor",
                        PortableDataType.STRING, false),
                new PortableField("amount", "amount", "amount",
                        PortableDataType.STRING, true)));
        PortableStreamNode schemaDrift = new PortableStreamNode(
                sinkBase.operatorUid(), sinkBase.opcode(),
                sinkBase.inputOperatorUids(),
                sinkBase.inputSchemaFingerprints(), wrongSchema,
                Collections.singletonList("id"),
                sinkBase.stateDescriptors(), sinkBase.scalarParameters(),
                sinkBase.binding());
        assertFailure("SCHEMA_MISMATCH", () ->
                new PortableStreamPlan(base.planVersion(),
                        base.deliveryGuarantee(), base.parallelism(),
                        base.maxParallelism(), base.changelogMode(),
                        base.timeMode(), base.rootSinkOperatorUids(),
                        Arrays.asList(base.nodes().get(0), schemaDrift),
                        base.edges(), base.requiredCapabilities()));

        PortableStreamPlan append = appendPlan(
                PortableStreamPlan.DeliveryGuarantee.EXACTLY_ONCE,
                StreamSinkBinding.VisibilityProfile.CHECKPOINT_TRANSACTIONAL);
        PortableStreamNode appendSourceBase = append.nodes().get(0);
        PortableStreamNode appendSource = new PortableStreamNode(
                appendSourceBase.operatorUid(), appendSourceBase.opcode(),
                appendSourceBase.inputOperatorUids(),
                appendSourceBase.inputSchemaFingerprints(),
                appendSourceBase.outputSchema(),
                Collections.singletonList("id"),
                appendSourceBase.stateDescriptors(),
                appendSourceBase.scalarParameters(),
                appendSourceBase.binding());
        PortableStreamNode appendSinkBase = append.nodes().get(1);
        PortableStreamNode appendWrongKey = new PortableStreamNode(
                appendSinkBase.operatorUid(), appendSinkBase.opcode(),
                appendSinkBase.inputOperatorUids(),
                appendSinkBase.inputSchemaFingerprints(),
                appendSinkBase.outputSchema(),
                Collections.singletonList("adaptor"),
                appendSinkBase.stateDescriptors(),
                appendSinkBase.scalarParameters(), appendSinkBase.binding());
        assertFailure("INVALID_STABLE_KEY", () ->
                new PortableStreamPlan(append.planVersion(),
                        append.deliveryGuarantee(), append.parallelism(),
                        append.maxParallelism(), append.changelogMode(),
                        append.timeMode(), append.rootSinkOperatorUids(),
                        Arrays.asList(appendSource, appendWrongKey),
                        append.edges(), append.requiredCapabilities()));
    }

    @Test
    void providerConfigurationSplitsAndEdgesHaveStableBoundedIdentity() {
        PortableStreamPlan valid = appendPlan(
                PortableStreamPlan.DeliveryGuarantee.EXACTLY_ONCE,
                StreamSinkBinding.VisibilityProfile.CHECKPOINT_TRANSACTIONAL);
        StreamSourceBinding source = (StreamSourceBinding) valid.nodes().get(0).binding();
        StreamSinkBinding sink = (StreamSinkBinding) valid.nodes().get(1).binding();

        assertEquals(sha256Hex(new byte[]{1}), source.configurationDigest());
        assertEquals(DIGEST_B, source.fixedSplits().get(0).splitId());
        assertEquals("stream-position-v1", source.fixedSplits().get(0).positionCodecId());
        assertEquals(sha256Hex(new byte[]{3}), sink.configurationDigest());
        assertEquals(Collections.singletonList(new StreamEdge(
                "source", "sink", 0, "source-sink-0",
                StreamEdge.Partitioning.FORWARD, 1024, 4096)), valid.edges());
        assertEquals(valid, PortableStreamPlanCodec.decode(
                PortableStreamPlanCodec.encode(valid)));

        assertFailure("CONFIGURATION_DIGEST_MISMATCH", () -> new StreamSourceBinding(
                "log-v1", DIGEST_A, DIGEST_A, new byte[]{1},
                Collections.singletonList(new StreamSourceSplit(
                        DIGEST_B, "stream-position-v1", new byte[]{2})),
                StreamSourceBinding.ReplayGuarantee.EXACT_ORDERED));
        assertFailure("DUPLICATE_SPLIT_ID", () -> new StreamSourceBinding(
                "log-v1", DIGEST_A, sha256Hex(new byte[]{1}), new byte[]{1},
                Arrays.asList(
                        new StreamSourceSplit(DIGEST_B, "stream-position-v1", new byte[]{2}),
                        new StreamSourceSplit(DIGEST_B, "stream-position-v1", new byte[]{4})),
                StreamSourceBinding.ReplayGuarantee.EXACT_ORDERED));
        assertFailure("INVALID_EDGE_CREDIT", () -> new StreamEdge(
                "source", "sink", 0, "source-sink-0",
                StreamEdge.Partitioning.FORWARD, 4096, 1024));

        StreamEdge wrongTargetInput = new StreamEdge(
                "source", "sink", 1, "source-sink-1",
                StreamEdge.Partitioning.FORWARD, 1024, 4096);
        assertFailure("INVALID_EDGE_SET", () -> new PortableStreamPlan(
                valid.planVersion(), valid.deliveryGuarantee(), valid.parallelism(),
                valid.maxParallelism(), valid.changelogMode(), valid.timeMode(),
                valid.rootSinkOperatorUids(), valid.nodes(),
                Collections.singletonList(wrongTargetInput),
                valid.requiredCapabilities()));

        StreamEdge rebalance = new StreamEdge(
                "source", "sink", 0, "source-sink-rebalance",
                StreamEdge.Partitioning.REBALANCE, 1024, 4096);
        assertFailure("UNSUPPORTED_PARTITIONING", () ->
                new PortableStreamPlan(valid.planVersion(),
                        valid.deliveryGuarantee(), valid.parallelism(),
                        valid.maxParallelism(), valid.changelogMode(),
                        valid.timeMode(), valid.rootSinkOperatorUids(),
                        valid.nodes(), Collections.singletonList(rebalance),
                        valid.requiredCapabilities()));
    }

    private static PortableStreamPlan appendPlan(
            PortableStreamPlan.DeliveryGuarantee guarantee,
            StreamSinkBinding.VisibilityProfile visibility) {
        PortableRowSchema schema = schema();
        PortableStreamNode source = new PortableStreamNode(
                "source", StreamOperatorCode.SOURCE,
                Collections.emptyList(), Collections.emptyList(), schema,
                Collections.emptyList(), Collections.emptyList(),
                Collections.emptyMap(), new StreamSourceBinding(
                        "log-v1", DIGEST_A, sha256Hex(new byte[]{1}), new byte[]{1},
                        Collections.singletonList(new StreamSourceSplit(
                                DIGEST_B, "stream-position-v1", new byte[]{2})),
                        StreamSourceBinding.ReplayGuarantee.EXACT_ORDERED));
        PortableStreamNode sink = new PortableStreamNode(
                "sink", StreamOperatorCode.SINK,
                Collections.singletonList("source"),
                Collections.singletonList(schema.fingerprint()), schema,
                Collections.emptyList(), Collections.emptyList(),
                Collections.emptyMap(), new StreamSinkBinding(
                        "manifest-stream-v1", DIGEST_B,
                        sha256Hex(new byte[]{3}), new byte[]{3},
                        visibility));
        return new PortableStreamPlan(
                PortableStreamPlan.CURRENT_VERSION, guarantee, 2, 128,
                PortableStreamPlan.ChangelogMode.APPEND,
                PortableStreamPlan.TimeMode.EVENT_TIME,
                Collections.singletonList("sink"), Arrays.asList(source, sink),
                Collections.singletonList(new StreamEdge(
                        "source", "sink", 0, "source-sink-0",
                        StreamEdge.Partitioning.FORWARD, 1024, 4096)),
                Collections.singletonList(PortableStreamPlan.STREAM_CAPABILITY));
    }

    private static PortableStreamPlan mapPlan(
            StreamFunctionBinding.FunctionKind functionKind) {
        PortableStreamPlan base = appendPlan(
                PortableStreamPlan.DeliveryGuarantee.EXACTLY_ONCE,
                StreamSinkBinding.VisibilityProfile.CHECKPOINT_TRANSACTIONAL);
        PortableStreamNode source = base.nodes().get(0);
        PortableRowSchema schema = source.outputSchema();
        FunctionAbi abi = functionKind == StreamFunctionBinding.FunctionKind.FILTER
                ? FunctionAbi.FILTER_V1 : FunctionAbi.MAP_V1;
        FunctionDeploymentReference deployment = new FunctionDeploymentReference(
                DIGEST_A, "stream-functions", DIGEST_B,
                "example.StreamMap", abi, "stream-config-v1",
                sha256Hex(new byte[]{9}), new byte[]{9},
                new FunctionResourceProfile(1024L, 0L, 1));
        PortableStreamNode map = new PortableStreamNode(
                "map", StreamOperatorCode.MAP,
                Collections.singletonList("source"),
                Collections.singletonList(schema.fingerprint()), schema,
                Collections.emptyList(), Collections.emptyList(),
                Collections.emptyMap(),
                new StreamFunctionBinding(functionKind, deployment));
        PortableStreamNode sink = new PortableStreamNode(
                "sink", StreamOperatorCode.SINK,
                Collections.singletonList("map"),
                Collections.singletonList(schema.fingerprint()), schema,
                Collections.emptyList(), Collections.emptyList(),
                Collections.emptyMap(), base.nodes().get(1).binding());
        return new PortableStreamPlan(
                base.planVersion(), base.deliveryGuarantee(), base.parallelism(),
                base.maxParallelism(), base.changelogMode(), base.timeMode(),
                base.rootSinkOperatorUids(), Arrays.asList(source, map, sink),
                Arrays.asList(
                        new StreamEdge("source", "map", 0, "source-map-0",
                                StreamEdge.Partitioning.FORWARD, 1024, 4096),
                        new StreamEdge("map", "sink", 0, "map-sink-0",
                                StreamEdge.Partitioning.FORWARD, 1024, 4096)),
                base.requiredCapabilities());
    }

    private static PortableStreamPlan passThroughPlan(
            StreamOperatorCode opcode, PortableRowSchema outputSchema,
            List<String> stableKeys) {
        PortableStreamPlan base = appendPlan(
                PortableStreamPlan.DeliveryGuarantee.EXACTLY_ONCE,
                StreamSinkBinding.VisibilityProfile.CHECKPOINT_TRANSACTIONAL);
        PortableStreamNode source = base.nodes().get(0);
        StreamNodeBinding binding = null;
        if (opcode == StreamOperatorCode.FILTER) {
            FunctionDeploymentReference deployment =
                    new FunctionDeploymentReference(
                            DIGEST_A, "stream-functions", DIGEST_B,
                            "example.StreamFilter", FunctionAbi.FILTER_V1,
                            "stream-config-v1", sha256Hex(new byte[]{9}),
                            new byte[]{9},
                            new FunctionResourceProfile(1024L, 0L, 1));
            binding = new StreamFunctionBinding(
                    StreamFunctionBinding.FunctionKind.FILTER, deployment);
        }
        PortableStreamNode passThrough = new PortableStreamNode(
                "pass-through", opcode, Collections.singletonList("source"),
                Collections.singletonList(
                        source.outputSchema().fingerprint()),
                outputSchema, stableKeys, Collections.emptyList(),
                Collections.emptyMap(), binding);
        PortableStreamNode sink = new PortableStreamNode(
                "sink", StreamOperatorCode.SINK,
                Collections.singletonList("pass-through"),
                Collections.singletonList(outputSchema.fingerprint()),
                outputSchema, stableKeys, Collections.emptyList(),
                Collections.emptyMap(), base.nodes().get(1).binding());
        return new PortableStreamPlan(
                base.planVersion(), base.deliveryGuarantee(),
                base.parallelism(), base.maxParallelism(),
                base.changelogMode(), base.timeMode(),
                base.rootSinkOperatorUids(),
                Arrays.asList(source, passThrough, sink),
                Arrays.asList(
                        new StreamEdge("source", "pass-through", 0,
                                "source-pass-through-0",
                                StreamEdge.Partitioning.FORWARD, 1024, 4096),
                        new StreamEdge("pass-through", "sink", 0,
                                "pass-through-sink-0",
                                StreamEdge.Partitioning.FORWARD, 1024, 4096)),
                base.requiredCapabilities());
    }

    private static PortableStreamPlan windowPlan(
            PortableStreamPlan.TimeMode timeMode,
            java.util.Map<String, String> parameters) {
        PortableStreamPlan base = appendPlan(
                PortableStreamPlan.DeliveryGuarantee.EXACTLY_ONCE,
                StreamSinkBinding.VisibilityProfile.CHECKPOINT_TRANSACTIONAL);
        PortableStreamNode source = base.nodes().get(0);
        PortableRowSchema schema = source.outputSchema();
        PortableStreamNode window = new PortableStreamNode(
                "window", StreamOperatorCode.EVENT_TIME_WINDOW,
                Collections.singletonList("source"),
                Collections.singletonList(schema.fingerprint()), schema,
                Collections.singletonList("id"),
                Collections.singletonList(new StreamStateDescriptor(
                        "window-state", StreamStateDescriptor.Kind.KEYED_AGGREGATING,
                        "stream.decimal-aggregate.v1", 1,
                        StreamStateDescriptor.TtlMode.NONE, 0L)),
                parameters, null);
        PortableStreamNode sink = new PortableStreamNode(
                "sink", StreamOperatorCode.SINK,
                Collections.singletonList("window"),
                Collections.singletonList(schema.fingerprint()), schema,
                Collections.singletonList("id"), Collections.emptyList(),
                Collections.emptyMap(), base.nodes().get(1).binding());
        return new PortableStreamPlan(
                base.planVersion(), base.deliveryGuarantee(), base.parallelism(),
                base.maxParallelism(), base.changelogMode(), timeMode,
                base.rootSinkOperatorUids(), Arrays.asList(source, window, sink),
                Arrays.asList(
                        new StreamEdge("source", "window", 0, "source-window-0",
                                StreamEdge.Partitioning.HASH, 1024, 4096),
                        new StreamEdge("window", "sink", 0, "window-sink-0",
                                StreamEdge.Partitioning.FORWARD, 1024, 4096)),
                base.requiredCapabilities());
    }

    private static PortableStreamPlan temporalJoinPlan(
            PortableStreamPlan.TimeMode timeMode,
            java.util.Map<String, String> parameters,
            java.util.List<String> capabilities,
            java.util.List<StreamStateDescriptor> states) {
        PortableStreamPlan base = appendPlan(
                PortableStreamPlan.DeliveryGuarantee.EXACTLY_ONCE,
                StreamSinkBinding.VisibilityProfile.CHECKPOINT_TRANSACTIONAL);
        PortableRowSchema schema = schema();
        PortableStreamNode left = new PortableStreamNode(
                "left-source", StreamOperatorCode.SOURCE,
                Collections.emptyList(), Collections.emptyList(), schema,
                Collections.singletonList("id"), Collections.emptyList(),
                Collections.emptyMap(), new StreamSourceBinding(
                        "left-log-v1", DIGEST_A,
                        sha256Hex(new byte[]{11}), new byte[]{11},
                        Collections.singletonList(new StreamSourceSplit(
                                repeat('c', 64), "stream-position-v1",
                                new byte[]{12})),
                        StreamSourceBinding.ReplayGuarantee.EXACT_ORDERED));
        PortableStreamNode right = new PortableStreamNode(
                "right-source", StreamOperatorCode.SOURCE,
                Collections.emptyList(), Collections.emptyList(), schema,
                Collections.singletonList("id"), Collections.emptyList(),
                Collections.emptyMap(), new StreamSourceBinding(
                        "right-log-v1", DIGEST_A,
                        sha256Hex(new byte[]{13}), new byte[]{13},
                        Collections.singletonList(new StreamSourceSplit(
                                repeat('d', 64), "stream-position-v1",
                                new byte[]{14})),
                        StreamSourceBinding.ReplayGuarantee.EXACT_ORDERED));
        FunctionDeploymentReference deployment = new FunctionDeploymentReference(
                DIGEST_A, "stream-functions", DIGEST_B,
                "example.StreamTemporalJoin", FunctionAbi.CO_GROUP_V1,
                "stream-config-v1", sha256Hex(new byte[]{15}),
                new byte[]{15}, new FunctionResourceProfile(1024L, 0L, 1));
        PortableStreamNode join = new PortableStreamNode(
                "temporal-join", StreamOperatorCode.TEMPORAL_JOIN,
                Arrays.asList("left-source", "right-source"),
                Arrays.asList(schema.fingerprint(), schema.fingerprint()), schema,
                Collections.singletonList("id"), states, parameters,
                new StreamFunctionBinding(
                        StreamFunctionBinding.FunctionKind.TEMPORAL_JOIN,
                        deployment));
        PortableStreamNode sink = new PortableStreamNode(
                "sink", StreamOperatorCode.SINK,
                Collections.singletonList("temporal-join"),
                Collections.singletonList(schema.fingerprint()), schema,
                Collections.singletonList("id"), Collections.emptyList(),
                Collections.emptyMap(), base.nodes().get(1).binding());
        return new PortableStreamPlan(
                base.planVersion(), base.deliveryGuarantee(), 1, 128,
                PortableStreamPlan.ChangelogMode.UPSERT, timeMode,
                Collections.singletonList("sink"),
                Arrays.asList(left, right, join, sink),
                Arrays.asList(
                        new StreamEdge("left-source", "temporal-join", 0,
                                "left-join-0", StreamEdge.Partitioning.HASH,
                                1024, 4096),
                        new StreamEdge("right-source", "temporal-join", 1,
                                "right-join-1", StreamEdge.Partitioning.HASH,
                                1024, 4096),
                        new StreamEdge("temporal-join", "sink", 0,
                                "join-sink-0", StreamEdge.Partitioning.FORWARD,
                                1024, 4096)), capabilities);
    }

    private static java.util.List<StreamStateDescriptor> temporalJoinStates() {
        return Arrays.asList(
                new StreamStateDescriptor(
                        "left-state", StreamStateDescriptor.Kind.KEYED_MAP,
                        "stream.temporal-side.v1", 1,
                        StreamStateDescriptor.TtlMode.EVENT_TIME, 60_000L),
                new StreamStateDescriptor(
                        "right-state", StreamStateDescriptor.Kind.KEYED_MAP,
                        "stream.temporal-side.v1", 1,
                StreamStateDescriptor.TtlMode.EVENT_TIME, 60_000L));
    }

    private static PortableStreamPlan residentCdcPlan(
            PortableStreamPlan.ChangelogMode changelogMode,
            java.util.List<String> capabilities,
            String enumeratorFormat) {
        PortableStreamPlan base = appendPlan(
                PortableStreamPlan.DeliveryGuarantee.EXACTLY_ONCE,
                StreamSinkBinding.VisibilityProfile.CHECKPOINT_TRANSACTIONAL);
        PortableStreamNode baseSource = base.nodes().get(0);
        java.util.Map<String, String> parameters =
                new java.util.LinkedHashMap<>();
        parameters.put("source.enumeratorStateFormat", enumeratorFormat);
        parameters.put("source.mode", "RESIDENT_CDC_SNAPSHOT_LOG");
        PortableStreamNode source = new PortableStreamNode(
                baseSource.operatorUid(), baseSource.opcode(),
                baseSource.inputOperatorUids(),
                baseSource.inputSchemaFingerprints(), baseSource.outputSchema(),
                Collections.singletonList("id"),
                baseSource.stateDescriptors(), parameters,
                baseSource.binding());
        PortableStreamNode baseSink = base.nodes().get(1);
        PortableStreamNode sink = new PortableStreamNode(
                baseSink.operatorUid(), baseSink.opcode(),
                baseSink.inputOperatorUids(),
                baseSink.inputSchemaFingerprints(), baseSink.outputSchema(),
                changelogMode == PortableStreamPlan.ChangelogMode.UPSERT
                        ? Collections.singletonList("id")
                        : Collections.emptyList(),
                baseSink.stateDescriptors(), baseSink.scalarParameters(),
                baseSink.binding());
        return new PortableStreamPlan(
                base.planVersion(), base.deliveryGuarantee(),
                base.parallelism(), base.maxParallelism(), changelogMode,
                base.timeMode(), base.rootSinkOperatorUids(),
                Arrays.asList(source, sink), base.edges(), capabilities);
    }

    private static PortableStreamPlan copyPlan(
            PortableStreamPlan source, int parallelism, int maxParallelism,
            java.util.List<PortableStreamNode> nodes) {
        return new PortableStreamPlan(
                source.planVersion(), source.deliveryGuarantee(), parallelism,
                maxParallelism, source.changelogMode(), source.timeMode(),
                source.rootSinkOperatorUids(), nodes, source.edges(),
                source.requiredCapabilities());
    }

    private static PortableStreamPlan withCapabilities(
            PortableStreamPlan source, List<String> capabilities) {
        return new PortableStreamPlan(source.planVersion(),
                source.deliveryGuarantee(), source.parallelism(),
                source.maxParallelism(), source.changelogMode(),
                source.timeMode(), source.rootSinkOperatorUids(),
                source.nodes(), source.edges(), capabilities);
    }

    private static PortableStreamNode withExecution(
            PortableStreamNode node,
            PortableStreamPlan.ChangelogMode changelogMode) {
        return withExecution(node, changelogMode, 2, 4);
    }

    private static PortableStreamNode withExecution(
            PortableStreamNode node,
            PortableStreamPlan.ChangelogMode changelogMode,
            int parallelism, int maximumThreads) {
        return new PortableStreamNode(node.operatorUid(), node.opcode(),
                node.inputOperatorUids(), node.inputSchemaFingerprints(),
                node.outputSchema(), node.stableKeyFields(),
                node.stateDescriptors(), node.scalarParameters(),
                node.binding(), new StreamNodeExecutionDescriptor(
                changelogMode, PortableStreamPlan.TimeMode.EVENT_TIME,
                parallelism, 128, 1_048_576L, 1_048_576L, 0L,
                maximumThreads));
    }

    private static PortableRowSchema schema() {
        return new PortableRowSchema(Arrays.asList(
                new PortableField("id", "id", "id", PortableDataType.LONG, false),
                new PortableField("adaptor", "adaptor", "adaptor",
                        PortableDataType.STRING, false),
                new PortableField("amount", "amount", "amount",
                        PortableDataType.BIG_DECIMAL, true)));
    }

    private static void assertFailure(String code, org.junit.jupiter.api.function.Executable action) {
        StreamContractException failure = assertThrows(StreamContractException.class, action);
        assertEquals(code, failure.code(), failure.getMessage());
        assertTrue(failure.path() != null && !failure.path().isEmpty());
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static String sha256Hex(byte[] value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value);
            StringBuilder result = new StringBuilder(digest.length * 2);
            for (byte current : digest) {
                result.append(String.format("%02x", current & 0xff));
            }
            return result.toString();
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static int indexOf(byte[] haystack, byte[] needle) {
        outer: for (int index = 0; index <= haystack.length - needle.length; index++) {
            for (int offset = 0; offset < needle.length; offset++) {
                if (haystack[index + offset] != needle[offset]) continue outer;
            }
            return index;
        }
        throw new AssertionError("needle not found");
    }

    private static void rewriteChecksum(byte[] frame) throws Exception {
        int bodyLength = ByteBuffer.wrap(frame).getInt(12);
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                Arrays.copyOfRange(frame, 16, 16 + bodyLength));
        System.arraycopy(digest, 0, frame, 16 + bodyLength, digest.length);
    }
}
