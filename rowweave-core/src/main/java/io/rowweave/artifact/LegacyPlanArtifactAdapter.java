package io.rowweave.artifact;

import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanGraphEnvelopeScanner;
import io.rowweave.plan.ObjectTrustContext;
import io.rowweave.plan.PlanNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/** Explicit trusted adapter between the current Batch PlanGraph and the Artifact envelope. */
public final class LegacyPlanArtifactAdapter {

    private LegacyPlanArtifactAdapter() {
    }

    public static PlanArtifact wrap(String definitionId, PlanGraph graph) {
        return wrap(definitionId, graph, ObjectTrustContext.localTrusted(), ignored -> { });
    }

    /** When a payload must survive an mTLS RPC, the writer uses the same trust context as the in-process deployment set. */
    public static PlanArtifact wrap(
            String definitionId, PlanGraph graph, ObjectTrustContext trustContext) {
        return wrap(definitionId, graph, trustContext, ignored -> { });
    }

    /** Trust-aware construction entry point carrying an explicit resident reservation. */
    public static PlanArtifact wrap(
            String definitionId, PlanGraph graph, ObjectTrustContext trustContext,
            java.util.function.LongConsumer reservation) {
        if (graph == null) {
            throw ArtifactText.failure("INVALID_SEMANTICS", "payload.planGraph", "PlanGraph is required");
        }
        if (trustContext == null || reservation == null) {
            throw new IllegalArgumentException("object trust context and reservation are required");
        }
        List<String> requiredFeatures = requiredFeatures(
                graph, PlanArtifact.FEATURE_TRUSTED_LEGACY_PLAN_GRAPH);
        PlanArtifact artifact = new PlanArtifact(
                definitionId,
                PlanArtifact.CURRENT_PLAN_SCHEMA_VERSION,
                PlanArtifact.CURRENT_READER_VERSION,
                PlanArtifact.RuntimeTarget.BATCH,
                PlanArtifact.Boundedness.BOUNDED,
                changelogMode(graph),
                PlanArtifact.TimeMode.NONE,
                requiredFeatures,
                Collections.emptyList(),
                descriptors(graph),
                PlanArtifact.PayloadFormat.TRUSTED_LEGACY_PLAN_GRAPH,
                graph.toBytes(trustContext, reservation));
        return PlanArtifact.withLocalPayloadReference(artifact, graph);
    }

    /**
     * durable writer. The graph rejects every non-core Sink before
     * any Source/function/Sink Java serialization callback can run.
     */
    public static PlanArtifact wrapDurable(
            String definitionId, PlanGraph graph, ObjectTrustContext trustContext,
            java.util.function.LongConsumer reservation) {
        if (graph == null) {
            throw ArtifactText.failure(
                    "INVALID_SEMANTICS", "payload.planGraph", "PlanGraph is required");
        }
        if (trustContext == null || reservation == null) {
            throw new IllegalArgumentException(
                    "object trust context and reservation are required");
        }
        PlanArtifact artifact = new PlanArtifact(
                definitionId,
                PlanArtifact.CURRENT_PLAN_SCHEMA_VERSION,
                PlanArtifact.CURRENT_READER_VERSION,
                PlanArtifact.RuntimeTarget.BATCH,
                PlanArtifact.Boundedness.BOUNDED,
                changelogMode(graph),
                PlanArtifact.TimeMode.NONE,
                requiredFeatures(graph,
                        PlanArtifact.FEATURE_TRUSTED_LEGACY_PLAN_GRAPH),
                Collections.emptyList(),
                descriptors(graph),
                PlanArtifact.PayloadFormat.TRUSTED_LEGACY_PLAN_GRAPH,
                graph.toDurableBytes(trustContext, reservation));
        return PlanArtifact.withLocalPayloadReference(artifact, graph);
    }

    /** Same-JVM closure compatibility: it cannot be encoded or cross an RPC, and executes only through the transient original graph. */
    public static PlanArtifact wrapLocal(String definitionId, PlanGraph graph) {
        if (graph == null) {
            throw ArtifactText.failure("INVALID_SEMANTICS", "payload.planGraph", "PlanGraph is required");
        }
        PlanArtifact artifact = new PlanArtifact(
                definitionId,
                PlanArtifact.CURRENT_PLAN_SCHEMA_VERSION,
                PlanArtifact.CURRENT_READER_VERSION,
                PlanArtifact.RuntimeTarget.BATCH,
                PlanArtifact.Boundedness.BOUNDED,
                changelogMode(graph),
                PlanArtifact.TimeMode.NONE,
                requiredFeatures(graph, PlanArtifact.FEATURE_LOCAL_ATTACHED_PLAN_GRAPH),
                Collections.emptyList(),
                descriptors(graph),
                PlanArtifact.PayloadFormat.LOCAL_ATTACHED_PLAN_GRAPH,
                new byte[0]);
        return PlanArtifact.withLocalPayloadReference(artifact, graph);
    }

    private static List<String> requiredFeatures(PlanGraph graph, String payloadFeature) {
        List<String> requiredFeatures = new ArrayList<>();
        requiredFeatures.add(payloadFeature);
        boolean hasExplicit = false;
        boolean hasPositional = false;
        for (int i = 0; i < graph.nodeCount(); i++) {
            if (graph.node(i).param(PlanNode.OPERATOR_UID_PARAM) == null) {
                hasPositional = true;
            } else {
                hasExplicit = true;
            }
        }
        if (hasExplicit) {
            requiredFeatures.add(PlanArtifact.FEATURE_EXPLICIT_OPERATOR_IDS);
        }
        if (hasPositional) {
            requiredFeatures.add(PlanArtifact.FEATURE_LEGACY_POSITIONAL_OPERATOR_IDS);
        }
        for (int i = 0; i < graph.nodeCount(); i++) {
            if (graph.node(i).kind() == PlanNode.Kind.SINK
                    && graph.node(i).output() != null
                    && (graph.node(i).output().getClass()
                    == io.rowweave.ProviderBatchSink.class
                    || graph.node(i).output().getClass()
                    == io.rowweave.ManifestFileSink.class)) {
                requiredFeatures.add(
                        PlanArtifact.FEATURE_SINK_PROVIDER_CONFIG_ENVELOPE);
                break;
            }
        }
        for (int i = 0; i < graph.nodeCount(); i++) {
            if (graph.node(i).hasIncrementalPlanDescriptor()) {
                requiredFeatures.add(PlanArtifact.FEATURE_BOUNDED_INCREMENTAL_FRONTIER);
                break;
            }
        }
        return requiredFeatures;
    }

    private static PlanArtifact.ChangelogMode changelogMode(PlanGraph graph) {
        for (int i = 0; i < graph.nodeCount(); i++) {
            if (graph.node(i).hasIncrementalPlanDescriptor()) {
                return PlanArtifact.ChangelogMode.UPSERT;
            }
        }
        return PlanArtifact.ChangelogMode.APPEND;
    }

    public static PlanGraph unwrapValidated(PlanArtifact artifact) {
        // LocalJobClient does not go through the control RPC, so the zero-serialization path for a large in-JVM CollectionSource is kept,
        // while artifact metadata is still checked against the original graph. Master and Cluster both call the explicit
        // trust-context overload and never take this local-reference shortcut.
        if (artifact != null && artifact.localPayloadReference() instanceof PlanGraph) {
            if (artifact.payloadFormat() == PlanArtifact.PayloadFormat.LOCAL_ATTACHED_PLAN_GRAPH) {
                PlanArtifactValidator.validateForLocalRuntime(artifact);
            } else {
                PlanArtifactValidator.validateForCurrentRuntime(artifact);
            }
            PlanGraph localGraph = (PlanGraph) artifact.localPayloadReference();
            assertDescriptorsMatch(artifact, localGraph);
            IncrementalPlanArtifactValidator.validate(artifact, localGraph);
            IncrementalRuntimeGate.rejectGenericExecution(localGraph);
            return localGraph;
        }
        return unwrapValidated(artifact, value -> { });
    }

    public static PlanGraph unwrapValidated(PlanArtifact artifact,
                                            java.util.function.LongConsumer reservation) {
        return unwrapValidated(artifact, ObjectTrustContext.localTrusted(), reservation);
    }

    public static PlanGraph unwrapValidated(PlanArtifact artifact,
                                            ObjectTrustContext trustContext,
                                            java.util.function.LongConsumer reservation) {
        return unwrapValidated(
                artifact, trustContext, reservation, false);
    }

    /**
     * Frontier-aware runtime decode. The returned graph still carries its
     * incremental descriptor, so every generic executor continues to reject it.
     */
    public static PlanGraph unwrapIncrementalValidated(
            PlanArtifact artifact, ObjectTrustContext trustContext,
            java.util.function.LongConsumer reservation) {
        return unwrapValidated(
                artifact, trustContext, reservation, true);
    }

    private static PlanGraph unwrapValidated(
            PlanArtifact artifact, ObjectTrustContext trustContext,
            java.util.function.LongConsumer reservation,
            boolean incrementalRuntime) {
        if (reservation == null) throw new IllegalArgumentException("reservation is required");
        if (trustContext == null) throw new IllegalArgumentException("object trust context is required");
        if (artifact != null && artifact.payloadFormat()
                == PlanArtifact.PayloadFormat.PORTABLE_BATCH_PLAN_V1) {
            throw ArtifactText.failure("PORTABLE_ADAPTER_REQUIRED", "payload.format",
                    "portable payload requires the closed portable compiler");
        }
        if (incrementalRuntime && (artifact == null
                || !artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_BOUNDED_INCREMENTAL_FRONTIER))) {
            throw ArtifactText.failure(
                    "INCREMENTAL_RUNTIME_REQUIRED", "payload.incremental",
                    "frontier-aware decode requires an incremental Artifact");
        }
        if (artifact != null
                && artifact.payloadFormat() == PlanArtifact.PayloadFormat.LOCAL_ATTACHED_PLAN_GRAPH) {
            PlanArtifactValidator.validateForLocalRuntime(artifact);
            Object localReference = artifact.localPayloadReference();
            if (!(localReference instanceof PlanGraph)) {
                throw ArtifactText.failure("LOCAL_PAYLOAD_REFERENCE_MISSING", "payload.localReference",
                        "local-attached artifact lost its in-process PlanGraph reference");
            }
            PlanGraph localGraph = (PlanGraph) localReference;
            assertDescriptorsMatch(artifact, localGraph);
            IncrementalPlanArtifactValidator.validate(artifact, localGraph);
            if (!incrementalRuntime) {
                IncrementalRuntimeGate.rejectGenericExecution(localGraph);
            }
            return localGraph;
        }
        PlanArtifactValidator.validateForCurrentRuntime(artifact);
        final PlanGraph graph;
        final byte[] payload;
        try {
            reservation.accept(artifact.payloadSize());
            payload = artifact.payload();
            PlanGraphEnvelopeScanner.Scan scan =
                    PlanGraphEnvelopeScanner.scan(payload, reservation);
            assertDescriptorsMatch(artifact, scan.operatorDescriptors());
            graph = PlanGraph.fromBytes(payload, trustContext, reservation);
        } catch (PlanArtifactValidationException descriptorMismatch) {
            throw descriptorMismatch;
        } catch (RuntimeException payloadFailure) {
            throw new PlanArtifactValidationException("LEGACY_DESCRIPTOR_MISMATCH", "payload.bytes",
                    "legacy PlanGraph payload cannot be decoded", payloadFailure);
        }
        assertDescriptorsMatch(artifact, graph);
        IncrementalPlanArtifactValidator.validate(artifact, graph);
        if (!incrementalRuntime) {
            IncrementalRuntimeGate.rejectGenericExecution(graph);
        }
        Object localReference = artifact.localPayloadReference();
        if (localReference != null) {
            if (!(localReference instanceof PlanGraph)) {
                throw ArtifactText.failure("LEGACY_DESCRIPTOR_MISMATCH", "payload.localReference",
                        "local payload reference is not a PlanGraph");
            }
            PlanGraph localGraph = (PlanGraph) localReference;
            assertDescriptorsMatch(artifact, localGraph);
            IncrementalPlanArtifactValidator.validate(artifact, localGraph);
            if (!incrementalRuntime) {
                IncrementalRuntimeGate.rejectGenericExecution(localGraph);
            }
            return localGraph;
        }
        return graph;
    }

    private static void assertDescriptorsMatch(PlanArtifact artifact, PlanGraph graph) {
        assertDescriptorsMatch(artifact, descriptors(graph));
    }

    private static void assertDescriptorsMatch(
            PlanArtifact artifact, List<OperatorDescriptor> actual) {
        List<OperatorDescriptor> expected = artifact.operators();
        int common = Math.min(actual.size(), expected.size());
        for (int i = 0; i < common; i++) {
            if (!expected.get(i).equals(actual.get(i))) {
                throw ArtifactText.failure("LEGACY_DESCRIPTOR_MISMATCH", "operators[" + i + "]",
                        "legacy PlanGraph descriptor differs from artifact metadata");
            }
        }
        if (actual.size() != expected.size()) {
            throw ArtifactText.failure("LEGACY_DESCRIPTOR_MISMATCH", "operators",
                    "legacy PlanGraph operator count differs from artifact metadata");
        }
    }

    private static List<OperatorDescriptor> descriptors(PlanGraph graph) {
        List<OperatorDescriptor> descriptors = new ArrayList<>(graph.nodeCount());
        for (int i = 0; i < graph.nodeCount(); i++) {
            PlanNode node = graph.node(i);
            List<String> inputs = new ArrayList<>(node.childIndices().length);
            for (int child : node.childIndices()) {
                if (child < 0 || child >= i) {
                    throw ArtifactText.failure("NON_TOPOLOGICAL_OPERATOR_INPUT",
                            "payload.nodes[" + i + "].children",
                            "legacy PlanGraph child must precede consumer: " + child);
                }
                inputs.add(operatorId(graph.node(child), child));
            }
            descriptors.add(new OperatorDescriptor(
                    operatorId(node, i), kind(node.kind()), node.typeId(),
                    SchemaFingerprint.of(node.outMeta()), inputs));
        }
        return descriptors;
    }

    private static String operatorId(PlanNode node, int index) {
        String explicit = node.param(PlanNode.OPERATOR_UID_PARAM);
        return explicit == null ? String.format(Locale.ROOT, "legacy-op-%06d", index) : explicit;
    }

    private static OperatorDescriptor.Kind kind(PlanNode.Kind kind) {
        switch (kind) {
            case SOURCE: return OperatorDescriptor.Kind.SOURCE;
            case UNARY: return OperatorDescriptor.Kind.UNARY;
            case BINARY: return OperatorDescriptor.Kind.BINARY;
            case SINK: return OperatorDescriptor.Kind.SINK;
            default: throw new AssertionError("unknown PlanNode kind: " + kind);
        }
    }
}
