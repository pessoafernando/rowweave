package io.rowweave.plan;

import io.rowweave.DiscardSink;
import io.rowweave.BatchSinkDescriptor;
import io.rowweave.BatchSink;
import io.rowweave.Sink;
import io.rowweave.ManifestFileSink;
import io.rowweave.ProviderBatchSink;
import io.rowweave.DurableSinkProviderKey;

import java.util.HashSet;
import java.util.Set;

/** A graph with several SINK nodes may contain only Sinks whose coordinated commit is explicitly supported. */
public final class OutputCommitValidator {

    private OutputCommitValidator() {
    }

    public static void validate(PlanGraph graph) {
        validate(graph, false);
    }

    private static void validate(PlanGraph graph, boolean admittedProviderProxy) {
        ObjectStorageQualityPlanValidator.validateFullGraph(graph);
        TypedQualityPublicationPlanValidator.validateFullGraph(graph);
        TypedRowClassificationPlanValidator.validateFullGraph(graph);
        int sinkCount = 0;
        int manifestSinkCount = 0;
        String manifestDomain = null;
        Set<String> manifestMembers = new HashSet<>();
        for (int i = 0; i < graph.nodeCount(); i++) {
            if (graph.node(i).kind() == PlanNode.Kind.SINK) {
                sinkCount++;
                Sink output = graph.node(i).output();
                if (output instanceof ManifestFileSink) {
                    manifestSinkCount++;
                    ManifestFileSink manifest = (ManifestFileSink) output;
                    if (manifestDomain == null) {
                        manifestDomain = manifest.domainId();
                    } else if (!manifestDomain.equals(manifest.domainId())) {
                        throw new IllegalArgumentException(
                                "all ManifestFileSink sinks must use the same manifest commit domain");
                    }
                    if (!manifestMembers.add(manifest.memberName())) {
                        throw new IllegalArgumentException(
                                "duplicate manifest member in one job: " + manifest.memberName());
                    }
                }
            }
        }
        if (manifestSinkCount > 0 && manifestSinkCount != sinkCount) {
            throw new IllegalArgumentException(
                    "manifest commit job cannot mix ManifestFileSink with legacy or external outputs");
        }
        if (sinkCount <= 1) {
            return;
        }
        for (int i = 0; i < graph.nodeCount(); i++) {
            PlanNode node = graph.node(i);
            if (node.kind() != PlanNode.Kind.SINK) {
                continue;
            }
            Sink output = node.output();
            if (output == null) {
                throw new IllegalStateException("SINK node " + i + " has no Sink instance");
            }
            if (!output.supportsCoordinatedCommit()
                    && !(admittedProviderProxy
                    && output.getClass() == ProviderBatchSink.class)) {
                throw new IllegalArgumentException(
                        "multi-output job requires coordinated commit support: SINK node=" + i
                                + " output=" + output.getClass().getName());
            }
        }
    }

    /** A durable Run accepts only the built-in Sinks whose publication outcome can be rebuilt after the process dies. */
    public static void validateDurable(PlanGraph graph) {
        int discardCount = 0;
        int manifestCount = 0;
        int providerCount = 0;
        DurableSinkProviderKey providerKey = null;
        String providerDomain = null;
        Set<String> providerDestinations = new HashSet<>();
        for (int i = 0; i < graph.nodeCount(); i++) {
            PlanNode node = graph.node(i);
            if (node.kind() != PlanNode.Kind.SINK) {
                continue;
            }
            Sink output = node.output();
            if (output == null) {
                throw new IllegalStateException("SINK node " + i + " has no Sink instance");
            }
            node.validateCurrentSinkBinding();
            BatchSinkDescriptor descriptor = node.batchSinkDescriptor();
            if (output.getClass() == DiscardSink.class) {
                requireBuiltInDescriptor(i, descriptor, "rowweave.discard",
                        BatchSinkDescriptor.CapabilityProfile.SIDE_EFFECT_FREE_V1,
                        "none:discard", "none:side-effect-free");
                discardCount++;
            } else if (output.getClass() == ManifestFileSink.class) {
                ManifestFileSink manifest = (ManifestFileSink) output;
                requireBuiltInDescriptor(i, descriptor, "rowweave.manifest-file",
                        BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                        manifest.exclusiveDestinationId(), manifest.domainId());
                manifestCount++;
            } else if (output.getClass() == ProviderBatchSink.class) {
                DurableSinkProviderKey currentKey = new DurableSinkProviderKey(
                        descriptor.connectorType(), descriptor.connectorVersion(),
                        descriptor.capabilityProfile());
                if (providerKey == null) {
                    providerKey = currentKey;
                    providerDomain = descriptor.commitDomainId();
                } else if (!providerKey.equals(currentKey)
                        || !providerDomain.equals(descriptor.commitDomainId())) {
                    throw new IllegalArgumentException(
                            "PROVIDER_COMPOSITION_UNSUPPORTED");
                }
                if (!providerDestinations.add(descriptor.destinationId())) {
                    throw new IllegalArgumentException(
                            "PROVIDER_COMPOSITION_UNSUPPORTED");
                }
                providerCount++;
            } else if (output instanceof BatchSink) {
                throw new IllegalArgumentException(
                        "UNSUPPORTED_DURABLE_SINK_PROVIDER: SINK node=" + i
                                + " connector=" + descriptor.connectorType()
                                + " profile=" + descriptor.capabilityProfile());
            } else {
                throw new IllegalArgumentException(
                        "durable run requires crash-reconcilable output: SINK node=" + i
                                + " output=" + output.getClass().getName());
            }
        }
        if (discardCount > 0 && manifestCount > 0) {
            throw new IllegalArgumentException(
                    "durable sink capability profiles cannot mix Manifest and Discard outputs");
        }
        if (providerCount > 0 && (discardCount > 0 || manifestCount > 0)) {
            throw new IllegalArgumentException(
                    "PROVIDER_COMPOSITION_UNSUPPORTED");
        }
        // ProviderBatchSink deliberately remains an inert legacy Sink
        // (all lifecycle methods fail). Only this durable exact-class branch
        // may recognize coordination supplied by the Master/Worker SPI.
        validate(graph, providerCount > 0);
    }

    private static void requireBuiltInDescriptor(
            int nodeIndex, BatchSinkDescriptor descriptor,
            String connectorType,
            BatchSinkDescriptor.CapabilityProfile profile,
            String destinationId, String commitDomainId) {
        if (!connectorType.equals(descriptor.connectorType())
                || descriptor.connectorVersion() != 1
                || descriptor.capabilityProfile() != profile
                || !destinationId.equals(descriptor.destinationId())
                || !commitDomainId.equals(descriptor.commitDomainId())) {
            throw new IllegalArgumentException(
                    "INVALID_DURABLE_SINK_CAPABILITY: SINK node=" + nodeIndex);
        }
    }
}
