package io.rowweave.plan;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.DurableBatchSinkProviderContract;
import io.rowweave.DurableSinkProviderKey;
import io.rowweave.DiscardSink;
import io.rowweave.ManifestFileSink;
import io.rowweave.Sink;
import io.rowweave.ProviderBatchSink;
import io.rowweave.deployment.DurableBatchSinkProviderRegistry;

import java.util.HashSet;
import java.util.Set;

/** Static provider/codec/composition admission over the no-classloading Plan view. */
public final class DurableSinkProviderPlanPreflight {

    private DurableSinkProviderPlanPreflight() {}

    public static void validate(
            PlanGraphEnvelopeScanner.Scan scan,
            DurableBatchSinkProviderRegistry registry) {
        if (scan == null || registry == null) {
            throw new IllegalArgumentException(
                    "provider Plan scan and registry are required");
        }
        if (!scan.durableSinkEnvelopes().isEmpty()) {
            validateDurableEnvelopes(scan.durableSinkEnvelopes(), registry);
            return;
        }
        DurableSinkProviderKey groupKey = null;
        String groupDomain = null;
        for (PlanGraphEnvelopeScanner.ProviderSinkEnvelope envelope
                : scan.providerSinkEnvelopes()) {
            BatchSinkDescriptor descriptor = envelope.descriptor();
            DurableSinkProviderKey key = new DurableSinkProviderKey(
                    descriptor.connectorType(), descriptor.connectorVersion(),
                    descriptor.capabilityProfile());
            DurableBatchSinkProviderContract contract =
                    registry.requireContract(key);
            if (!key.matches(descriptor)
                    || !contract.configurationCodec().equals(
                    envelope.configurationCodec())) {
                throw new IllegalArgumentException(
                        "PROVIDER_CODEC_UNSUPPORTED");
            }
            if (envelope.configurationBytes().length
                    > contract.configurationCodec().maxPayloadBytes()) {
                throw new IllegalArgumentException("PROVIDER_BUDGET_INVALID");
            }
            if (groupKey == null) {
                groupKey = key;
                groupDomain = descriptor.commitDomainId();
            } else if (!groupKey.equals(key)
                    || !groupDomain.equals(descriptor.commitDomainId())) {
                throw new IllegalArgumentException(
                        "PROVIDER_COMPOSITION_UNSUPPORTED");
            }
        }
    }

    private static void validateDurableEnvelopes(
            java.util.List<PlanGraphEnvelopeScanner.DurableSinkEnvelope> sinks,
            DurableBatchSinkProviderRegistry registry) {
        DurableSinkProviderKey groupKey = null;
        String groupDomain = null;
        Set<String> destinations = new HashSet<>();
        for (PlanGraphEnvelopeScanner.DurableSinkEnvelope sink : sinks) {
            BatchSinkDescriptor descriptor = sink.descriptor();
            DurableSinkProviderKey key = new DurableSinkProviderKey(
                    descriptor.connectorType(), descriptor.connectorVersion(),
                    descriptor.capabilityProfile());
            if (sink.kind()
                    == PlanGraphEnvelopeScanner.DurableSinkEnvelope.Kind.PROVIDER) {
                requireProviderBinding(descriptor, sink.configurationCodec(),
                        sink.configurationBytes().length, registry);
            } else {
                requireExactBuiltin(sink.kind(), descriptor);
                registry.requireContract(key);
            }
            if (descriptor.capabilityProfile()
                    != BatchSinkDescriptor.CapabilityProfile.SIDE_EFFECT_FREE_V1
                    && !destinations.add(descriptor.destinationId())) {
                throw new IllegalArgumentException(
                        "PROVIDER_COMPOSITION_UNSUPPORTED");
            }
            if (groupKey == null) {
                groupKey = key;
                groupDomain = descriptor.commitDomainId();
            } else if (!groupKey.equals(key)
                    || !groupDomain.equals(descriptor.commitDomainId())) {
                throw new IllegalArgumentException(
                        "PROVIDER_COMPOSITION_UNSUPPORTED");
            }
        }
    }

    private static void requireExactBuiltin(
            PlanGraphEnvelopeScanner.DurableSinkEnvelope.Kind kind,
            BatchSinkDescriptor descriptor) {
        boolean valid = kind
                == PlanGraphEnvelopeScanner.DurableSinkEnvelope.Kind.DISCARD
                ? "rowweave.discard".equals(descriptor.connectorType())
                && descriptor.connectorVersion() == 1
                && descriptor.capabilityProfile()
                == BatchSinkDescriptor.CapabilityProfile.SIDE_EFFECT_FREE_V1
                && "none:discard".equals(descriptor.destinationId())
                && "none:side-effect-free".equals(
                descriptor.commitDomainId())
                : "rowweave.manifest-file".equals(descriptor.connectorType())
                && descriptor.connectorVersion() == 1
                && descriptor.capabilityProfile()
                == BatchSinkDescriptor.CapabilityProfile
                .MANIFEST_ATOMIC_GROUP_V1;
        if (!valid) {
            throw new IllegalArgumentException(
                    "INVALID_DURABLE_SINK_CAPABILITY");
        }
    }

    /**
     * Driver-side equivalent of the byte scanner. It reads only frozen fields
     * from exact final core carrier types, and therefore can run before any OIS
     * writer callback in a source, function, or Sink.
     */
    public static void validateClientGraph(
            PlanGraph graph, DurableBatchSinkProviderRegistry registry) {
        if (graph == null || registry == null) {
            throw new IllegalArgumentException(
                    "provider Plan graph and registry are required");
        }
        DurableSinkProviderKey groupKey = null;
        String groupDomain = null;
        Set<String> destinations = new HashSet<>();
        for (int i = 0; i < graph.nodeCount(); i++) {
            PlanNode node = graph.node(i);
            if (node.kind() != PlanNode.Kind.SINK) continue;
            Sink output = node.output();
            if (output == null) {
                throw new IllegalArgumentException(
                        "UNSUPPORTED_DURABLE_SINK_PROVIDER: SINK node=" + i);
            }
            Class<?> type = output.getClass();
            BatchSinkDescriptor descriptor = node.batchSinkDescriptor();
            final DurableSinkProviderKey key;
            if (type == ProviderBatchSink.class) {
                ProviderBatchSink provider =
                        (ProviderBatchSink) output;
                key = requireProviderBinding(descriptor,
                        provider.configurationCodec(),
                        provider.configurationSize(), registry);
            } else if (type == ManifestFileSink.class
                    || type == DiscardSink.class) {
                key = new DurableSinkProviderKey(
                        descriptor.connectorType(), descriptor.connectorVersion(),
                        descriptor.capabilityProfile());
                registry.requireContract(key);
            } else {
                throw new IllegalArgumentException(
                        "UNSUPPORTED_DURABLE_SINK_PROVIDER: SINK node=" + i);
            }
            if (descriptor.capabilityProfile()
                    != BatchSinkDescriptor.CapabilityProfile.SIDE_EFFECT_FREE_V1
                    && !destinations.add(descriptor.destinationId())) {
                throw new IllegalArgumentException(
                        "PROVIDER_COMPOSITION_UNSUPPORTED");
            }
            if (groupKey == null) {
                groupKey = key;
                groupDomain = descriptor.commitDomainId();
            } else if (!groupKey.equals(key)
                    || !groupDomain.equals(descriptor.commitDomainId())) {
                throw new IllegalArgumentException(
                        "PROVIDER_COMPOSITION_UNSUPPORTED");
            }
        }
    }

    public static void validateProviderConfiguration(
            BatchSinkDescriptor descriptor,
            io.rowweave.DurableSinkCodecDescriptor configurationCodec,
            int configurationLength,
            DurableBatchSinkProviderRegistry registry) {
        requireProviderBinding(descriptor, configurationCodec,
                configurationLength, registry);
    }

    private static DurableSinkProviderKey requireProviderBinding(
            BatchSinkDescriptor descriptor,
            io.rowweave.DurableSinkCodecDescriptor configurationCodec,
            int configurationLength,
            DurableBatchSinkProviderRegistry registry) {
        DurableSinkProviderKey key = new DurableSinkProviderKey(
                descriptor.connectorType(), descriptor.connectorVersion(),
                descriptor.capabilityProfile());
        DurableBatchSinkProviderContract contract = registry.requireContract(key);
        if (!key.matches(descriptor)
                || !contract.configurationCodec().equals(configurationCodec)) {
            throw new IllegalArgumentException("PROVIDER_CODEC_UNSUPPORTED");
        }
        if (configurationLength < 0
                || configurationLength
                > contract.configurationCodec().maxPayloadBytes()) {
            throw new IllegalArgumentException("PROVIDER_BUDGET_INVALID");
        }
        return key;
    }
}
