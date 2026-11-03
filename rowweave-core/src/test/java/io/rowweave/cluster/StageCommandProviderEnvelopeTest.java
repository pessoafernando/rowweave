package io.rowweave.cluster;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.DataType;
import io.rowweave.DurableSinkCodecDescriptor;
import io.rowweave.Field;
import io.rowweave.ProviderBatchSink;
import io.rowweave.DataSchema;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.deployment.DeploymentMemoryBudget;
import io.rowweave.deployment.DeploymentValidationException;
import io.rowweave.deployment.DurableBatchSinkProviderRegistry;
import io.rowweave.deployment.VerifiedDeploymentSet;
import io.rowweave.plan.ObjectTrustContext;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.SinkAttemptIdentity;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** provider config must cross StageCommand without OIS. */
class StageCommandProviderEnvelopeTest {
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType));

    @Test
    void providerOutputUsesNativeStageEnvelopeAndRoundTrips() {
        ProviderBatchSink output = output();
        PlanGraph fragment = new PlanGraph(Collections.singletonList(
                PlanNode.placeholderSource(META)));
        StageCommand command = StageCommand.sink(META, 8, 1, fragment, output);

        byte[] wire = StageCommandCodecV2.encode(
                command, ObjectTrustContext.localTrusted(), ignored -> { });
        assertFalse(containsJavaSerializationHeader(wire),
                "provider config must not enter a Java serialization stream");
        PreparedStageCommand prepared = StageCommandCodecV2.preflight(
                wire, ignored -> { });
        StageCommand restored = prepared.materialize(
                ObjectTrustContext.localTrusted(), ignored -> { });
        assertTrue(restored.output() instanceof ProviderBatchSink);
        ProviderBatchSink rebuilt =
                (ProviderBatchSink) restored.output();
        assertEquals(output.batchSinkDescriptor(), rebuilt.batchSinkDescriptor());
        assertEquals(output.configurationCodec(), rebuilt.configurationCodec());
        assertArrayEquals(output.configurationBytes(), rebuilt.configurationBytes());
    }

    @Test
    void workerStaticAdmissionRejectsUnknownProviderBeforeMaterialization()
            throws Exception {
        StageCommand command = StageCommand.sink(
                META, 8, 1,
                new PlanGraph(Collections.singletonList(
                        PlanNode.placeholderSource(META))), output());
        byte[] wire = StageCommandCodecV2.encode(
                command, ObjectTrustContext.localTrusted(), ignored -> { });
        PreparedStageCommand prepared = StageCommandCodecV2.preflight(
                wire, ignored -> { });
        try (VerifiedDeploymentSet deployments = VerifiedDeploymentSet.load(
                Collections.emptyList(), Collections.emptyMap(),
                DeploymentMemoryBudget.forTesting(16L * 1024L * 1024L))) {
            DurableBatchSinkProviderRegistry registry =
                    DurableBatchSinkProviderRegistry.scan(
                            deployments,
                            DurableBatchSinkProviderRegistry.Role.WORKER);
            DeploymentValidationException rejected = assertThrows(
                    DeploymentValidationException.class,
                    () -> prepared.validateDurableSinkProvider(
                            registry, ignored -> { }));
            assertEquals("PROVIDER_UNKNOWN", rejected.code());
        }
    }

    @Test
    void workerStaticAdmissionAcceptsSchemaChangingFragmentByItsRootSchema()
            throws Exception {
        DataSchema drifted = new DataSchema(new Field("other", DataType.StringType));
        BatchSinkDescriptor descriptor = new BatchSinkDescriptor(
                BatchSinkDescriptor.CURRENT_VERSION, "rowweave.manifest-file", 1,
                SchemaFingerprint.of(META), "test:destination", "test:domain",
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);
        ProviderBatchSink output = new ProviderBatchSink(
                META, descriptor,
                new DurableSinkCodecDescriptor(
                        "rowweave.manifest-config", 1, 1024 * 1024),
                new byte[]{1});
        StageCommand command = StageCommand.sink(
                drifted, 8, 1,
                new PlanGraph(java.util.Arrays.asList(
                        PlanNode.placeholderSource(drifted),
                        PlanNode.unary("transform", META, 0).build())), output);
        PreparedStageCommand prepared = StageCommandCodecV2.preflight(
                StageCommandCodecV2.encode(command,
                        ObjectTrustContext.localTrusted(), ignored -> { }),
                ignored -> { });

        try (VerifiedDeploymentSet deployments = VerifiedDeploymentSet.load(
                Collections.emptyList(), Collections.emptyMap(),
                DeploymentMemoryBudget.forTesting(16L * 1024L * 1024L))) {
            DurableBatchSinkProviderRegistry registry =
                    DurableBatchSinkProviderRegistry.scan(
                            deployments,
                            DurableBatchSinkProviderRegistry.Role.WORKER);
            ProviderBatchSink admitted =
                    prepared.providerOutputForAdmission(
                            registry, ignored -> { });
            assertEquals(output.batchSinkDescriptor(),
                    admitted.batchSinkDescriptor());
            assertEquals(SchemaFingerprint.of(META),
                    SchemaFingerprint.of(admitted.getSchema()));
            StageCommand materialized = prepared.materialize(
                    ObjectTrustContext.localTrusted(), ignored -> { });
            assertEquals(SchemaFingerprint.of(META), SchemaFingerprint.of(
                    materialized.output().getSchema()));
        }
    }

    @Test
    void workerStaticAdmissionRejectsFragmentRootSchemaBeforeMaterialization()
            throws Exception {
        DataSchema drifted = new DataSchema(new Field("other", DataType.StringType));
        BatchSinkDescriptor descriptor = new BatchSinkDescriptor(
                BatchSinkDescriptor.CURRENT_VERSION, "rowweave.manifest-file", 1,
                SchemaFingerprint.of(META), "test:destination", "test:domain",
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);
        ProviderBatchSink output = new ProviderBatchSink(
                META, descriptor,
                new DurableSinkCodecDescriptor(
                        "rowweave.manifest-config", 1, 1024 * 1024),
                new byte[]{1});
        StageCommand command = StageCommand.sink(
                META, 8, 1,
                new PlanGraph(java.util.Arrays.asList(
                        PlanNode.placeholderSource(META),
                        PlanNode.unary("transform", drifted, 0).build())), output);
        PreparedStageCommand prepared = StageCommandCodecV2.preflight(
                StageCommandCodecV2.encode(command,
                        ObjectTrustContext.localTrusted(), ignored -> { }),
                ignored -> { });

        try (VerifiedDeploymentSet deployments = VerifiedDeploymentSet.load(
                Collections.emptyList(), Collections.emptyMap(),
                DeploymentMemoryBudget.forTesting(16L * 1024L * 1024L))) {
            DurableBatchSinkProviderRegistry registry =
                    DurableBatchSinkProviderRegistry.scan(
                            deployments,
                            DurableBatchSinkProviderRegistry.Role.WORKER);
            IllegalArgumentException rejected = assertThrows(
                    IllegalArgumentException.class,
                    () -> prepared.validateDurableSinkProvider(
                            registry, ignored -> { }));
            assertTrue(rejected.getMessage().contains("schema mismatch"));
        }
    }

    @Test
    void providerStageCarriesExactSinkIdentityAndPublicationFence()
            throws Exception {
        ProviderBatchSink output = output();
        RunAttemptFence fence = new RunAttemptFence("run", "attempt", 7L);
        StageAssignmentRecord record = StageAssignmentRecord.create(
                fence, 11L, 0, 101L, 2, 0, StageAssignmentRole.SINK,
                "worker", "127.0.0.1", 9001, 9002, "incarnation");
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fence, "artifact", 11L, Collections.singletonList(record));
        SinkAttemptIdentity identity = SinkAttemptIdentity.forAssignment(
                manifest, record, "sink-op", output.batchSinkDescriptor());
        StageCommand command = StageCommand.sink(
                META, 8, 1,
                new PlanGraph(Collections.singletonList(
                        PlanNode.placeholderSource(META))), output);
        StageCommand.class.getMethod(
                "withDurableSinkContext", SinkAttemptIdentity.class,
                String.class, long.class).invoke(
                command, identity, "publication", 13L);

        StageCommand restored = StageCommandCodecV2.preflight(
                StageCommandCodecV2.encode(command,
                        ObjectTrustContext.localTrusted(), ignored -> { }),
                ignored -> { }).materialize(
                ObjectTrustContext.localTrusted(), ignored -> { });
        assertEquals(identity, StageCommand.class.getMethod(
                "sinkAttemptIdentity").invoke(restored));
        assertEquals("publication", StageCommand.class.getMethod(
                "publicationId").invoke(restored));
        assertEquals(13L, StageCommand.class.getMethod(
                "publicationGeneration").invoke(restored));
    }

    @Test
    void wholeGraphProviderProxyIsReadmittedFromFragmentWithoutOis()
            throws Exception {
        BatchSinkDescriptor descriptor = new BatchSinkDescriptor(
                BatchSinkDescriptor.CURRENT_VERSION, "rowweave.manifest-file", 1,
                SchemaFingerprint.of(META), "test:destination", "test:domain",
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);
        ProviderBatchSink output = new ProviderBatchSink(
                META, descriptor,
                new DurableSinkCodecDescriptor(
                        "rowweave.manifest-config", 1, 1024 * 1024),
                new byte[]{1});
        PlanGraph graph = new PlanGraph(java.util.Arrays.asList(
                PlanNode.placeholderSource(META),
                PlanNode.sink(META, 0, output).build()));
        RunAttemptFence fence = new RunAttemptFence(
                "whole-run", "attempt", 7L);
        StageAssignmentRecord record = StageAssignmentRecord.create(
                fence, 11L, 0, 101L, 2, 0,
                StageAssignmentRole.WHOLE_GRAPH,
                "worker", "127.0.0.1", 9001, 9002, "incarnation");
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fence, "artifact", 11L, Collections.singletonList(record));
        SinkAttemptIdentity identity = SinkAttemptIdentity.forAssignment(
                manifest, record, "sink-op", descriptor);
        StageCommand command = StageCommand.wholeGraph(graph)
                .withRunAttemptFence(fence)
                .withDurableSinkContext(identity, "publication", 13L);
        PreparedStageCommand prepared = StageCommandCodecV2.preflight(
                StageCommandCodecV2.encode(command,
                        ObjectTrustContext.localTrusted(), ignored -> { }),
                ignored -> { });

        try (VerifiedDeploymentSet deployments = VerifiedDeploymentSet.load(
                Collections.emptyList(), Collections.emptyMap(),
                DeploymentMemoryBudget.forTesting(16L * 1024L * 1024L))) {
            DurableBatchSinkProviderRegistry registry =
                    DurableBatchSinkProviderRegistry.scan(
                            deployments,
                            DurableBatchSinkProviderRegistry.Role.WORKER);
            ProviderBatchSink admitted =
                    prepared.providerOutputForAdmission(
                            registry, ignored -> { });
            assertEquals(descriptor, admitted.batchSinkDescriptor());
            StageCommand materialized = prepared.materialize(
                    ObjectTrustContext.localTrusted(), ignored -> { });
            assertEquals(descriptor,
                    materialized.providerOutputProxy().batchSinkDescriptor());
        }
    }

    private static ProviderBatchSink output() {
        BatchSinkDescriptor descriptor = new BatchSinkDescriptor(
                BatchSinkDescriptor.CURRENT_VERSION, "test.adversarial", 1,
                SchemaFingerprint.of(META), "test:destination", "test:domain",
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);
        return new ProviderBatchSink(META, descriptor,
                new DurableSinkCodecDescriptor("test.config", 1, 1024),
                new byte[]{1, 3, 3, 7});
    }

    private static boolean containsJavaSerializationHeader(byte[] bytes) {
        for (int i = 0; i + 3 < bytes.length; i++) {
            if ((bytes[i] & 0xff) == 0xac && (bytes[i + 1] & 0xff) == 0xed
                    && bytes[i + 2] == 0 && bytes[i + 3] == 5) {
                return true;
            }
        }
        return false;
    }
}
