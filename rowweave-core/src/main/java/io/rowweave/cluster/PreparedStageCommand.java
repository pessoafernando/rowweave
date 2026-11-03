package io.rowweave.cluster;

import io.rowweave.CoGroupFunction;
import io.rowweave.Source;
import io.rowweave.Sink;
import io.rowweave.ProviderBatchSink;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.artifact.PortablePlanArtifact;
import io.rowweave.deployment.DurableBatchSinkProviderRegistry;
import io.rowweave.plan.DurableSinkProviderPlanPreflight;
import io.rowweave.plan.ObjectTrustContext;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.TrustedObjectCodec;
import io.rowweave.portable.PortableBindingVerifier;
import io.rowweave.portable.PortableRuntimeResolver;

import java.util.function.LongConsumer;

/**
 * only bounded routing metadata and byte slices are retained by preflight.
 * User-controlled Java objects are materialized explicitly after durable acceptance.
 */
public final class PreparedStageCommand {
    static final class Slice {
        final int offset;
        final int length;

        Slice(int offset, int length) {
            this.offset = offset;
            this.length = length;
        }
    }

    private final byte[] envelope;
    private final int bodyLength;
    private final StageCommand.WireFields fields;
    private final Slice fragment;
    private final Slice sinkFragment;
    private final Slice input;
    private final Slice output;
    private final Slice coGroupFn;
    private final Slice portableArtifact;

    PreparedStageCommand(byte[] envelope, int bodyLength, StageCommand.WireFields fields,
                         Slice fragment, Slice sinkFragment, Slice input,
                         Slice output, Slice coGroupFn,
                         Slice portableArtifact) {
        this.envelope = envelope;
        this.bodyLength = bodyLength;
        this.fields = fields;
        this.fragment = fragment;
        this.sinkFragment = sinkFragment;
        this.input = input;
        this.output = output;
        this.coGroupFn = coGroupFn;
        this.portableArtifact = portableArtifact;
    }

    public io.rowweave.run.RunAttemptFence runAttemptFence() {
        return fields.runAttemptFence;
    }

    public StageCommand.Role role() { return fields.role; }

    public io.rowweave.run.SourceSplitAssignment.Context sourceSplitAssignment() {
        return fields.sourceSplitAssignment;
    }

    public int inboundAuthorizationCount() {
        return fields.inboundAuthorizations == null ? 0 : fields.inboundAuthorizations.size();
    }

    public boolean hasPortableArtifact() {
        return portableArtifact != null;
    }

    PortableStageSpec portableStageSpec() {
        return fields.portableStageSpec;
    }

    /** Static Worker admission; no provider/function class is loaded. */
    public void preflightPortableArtifact(
            PortableBindingVerifier verifier, boolean durable,
            LongConsumer reservation) {
        PlanArtifact artifact = readPortableArtifact(reservation);
        if (durable) {
            PortablePlanArtifact.preflightDurableValidated(
                    artifact, verifier, reservation);
        } else {
            PortablePlanArtifact.preflightValidated(
                    artifact, verifier, reservation);
        }
    }

    /** Post-acceptance Worker materialization through exact signed runtime bindings. */
    public StageCommand materializePortableArtifact(
            PortableBindingVerifier verifier, PortableRuntimeResolver resolver,
            LongConsumer reservation) {
        PlanArtifact artifact = readPortableArtifact(reservation);
        PlanGraph graph = PortablePlanArtifact.compileValidated(
                artifact, verifier, resolver, reservation);
        if (fields.portableStageSpec != null) {
            return PortableStageMaterializer.materialize(
                    graph, fields, fields.portableStageSpec);
        }
        StageCommand command = StageCommand.fromWire(
                fields, null, null, null, graph, null);
        command.attachPortableRuntimeGraph(graph);
        return command;
    }

    /** Worker durable prefix; runs before assignment acceptance or OIS. */
    public void validateDurableSinkProvider(
            DurableBatchSinkProviderRegistry registry, LongConsumer reservation) {
        providerOutputForAdmission(registry, reservation);
    }

    /** Decodes only the native provider proxy and scanned root schema; never enters OIS. */
    public ProviderBatchSink providerOutputForAdmission(
            DurableBatchSinkProviderRegistry registry, LongConsumer reservation) {
        if (registry == null || reservation == null) {
            throw new IllegalArgumentException(
                    "provider registry and reservation are required");
        }
        StageCommandCodecV2.verifyDigest(envelope, bodyLength);
        io.rowweave.plan.PlanGraphEnvelopeScanner.Scan fragmentScan =
                scanDurablePlan(fragment, registry, reservation);
        io.rowweave.plan.PlanGraphEnvelopeScanner.Scan sinkScan =
                scanDurablePlan(sinkFragment, registry, reservation);
        if (output == null) {
            return providerRootOutput(fragmentScan);
        }
        if (fields.outputEncoding
                == StageCommandCodecV2.OUTPUT_ENCODING_PROVIDER_NATIVE) {
            ProviderSinkStageEnvelopeCodec.View provider =
                    ProviderSinkStageEnvelopeCodec.decode(
                            envelope, output.offset, output.length, reservation);
            if (sinkScan == null || sinkScan.operatorDescriptors().isEmpty()
                    || !provider.descriptor().schemaFingerprint().equals(
                    sinkScan.operatorDescriptors().get(
                            sinkScan.operatorDescriptors().size() - 1)
                            .schemaFingerprint())) {
                throw new IllegalArgumentException(
                        "PROVIDER_CONFIG_NONCANONICAL: provider Sink schema mismatch");
            }
            DurableSinkProviderPlanPreflight.validateProviderConfiguration(
                    provider.descriptor(), provider.configurationCodec(),
                    provider.configurationBytes().length, registry);
            return provider.materialize(sinkScan.rootSchema());
        } else {
            io.rowweave.plan.PlanGraphEnvelopeScanner
                    .requireExactDurableLegacyOutputRoot(
                            envelope, output.offset, output.length);
            return null;
        }
    }

    private ProviderBatchSink providerRootOutput(
            io.rowweave.plan.PlanGraphEnvelopeScanner.Scan scan) {
        if (fields.role != StageCommand.Role.WHOLE_GRAPH || scan == null) {
            return null;
        }
        for (io.rowweave.plan.PlanGraphEnvelopeScanner.DurableSinkEnvelope sink
                : scan.durableSinkEnvelopes()) {
            if (sink.nodeIndex() == scan.nodeCount() - 1
                    && sink.kind()
                    == io.rowweave.plan.PlanGraphEnvelopeScanner
                    .DurableSinkEnvelope.Kind.PROVIDER) {
                return new ProviderBatchSink(
                        scan.rootSchema(), sink.descriptor(),
                        sink.configurationCodec(), sink.configurationBytes());
            }
        }
        return null;
    }

    public io.rowweave.run.SinkAttemptIdentity sinkAttemptIdentity() {
        return fields.sinkAttemptIdentity;
    }

    public String publicationId() { return fields.publicationId; }

    public long publicationGeneration() { return fields.publicationGeneration; }

    /** Revalidates the digest so a mutable RPC byte array cannot be changed after acceptance. */
    public StageCommand materialize(ObjectTrustContext trustContext, LongConsumer reservation) {
        if (trustContext == null || reservation == null) {
            throw new IllegalArgumentException("trust context and reservation are required");
        }
        StageCommandCodecV2.verifyDigest(envelope, bodyLength);
        if (portableArtifact != null) {
            throw new IllegalStateException(
                    "portable StageCommand requires Worker runtime bindings");
        }
        PlanGraph decodedFragment = readPlan(fragment, trustContext, reservation);
        PlanGraph decodedSink = readPlan(sinkFragment, trustContext, reservation);
        Source decodedInput = readObject(input, Source.class, trustContext, reservation);
        Sink decodedOutput = fields.outputEncoding
                == StageCommandCodecV2.OUTPUT_ENCODING_PROVIDER_NATIVE
                ? readProviderOutput(output, decodedSink, reservation)
                : readObject(output, Sink.class, trustContext, reservation);
        CoGroupFunction decodedCoGroup = readObject(
                coGroupFn, CoGroupFunction.class, trustContext, reservation);
        return StageCommand.fromWire(fields, decodedInput, decodedOutput,
                decodedCoGroup, decodedFragment, decodedSink);
    }

    byte[] portableArtifactBytes() {
        if (portableArtifact == null) return null;
        byte[] copy = new byte[portableArtifact.length];
        System.arraycopy(envelope, portableArtifact.offset,
                copy, 0, portableArtifact.length);
        return copy;
    }

    private PlanArtifact readPortableArtifact(LongConsumer reservation) {
        if (reservation == null) {
            throw new IllegalArgumentException("reservation is required");
        }
        StageCommandCodecV2.verifyDigest(envelope, bodyLength);
        if (portableArtifact == null) {
            throw new IllegalStateException(
                    "portable artifact is not present");
        }
        reservation.accept(portableArtifact.length);
        byte[] copy = portableArtifactBytes();
        return PlanArtifactCodec.decode(copy, reservation);
    }

    private PlanGraph readPlan(Slice slice, ObjectTrustContext trustContext,
                               LongConsumer reservation) {
        return slice == null ? null : PlanGraph.fromBytes(
                envelope, slice.offset, slice.length, trustContext, reservation);
    }

    private ProviderBatchSink readProviderOutput(
            Slice slice, PlanGraph decodedSink, LongConsumer reservation) {
        if (slice == null) return null;
        if (decodedSink == null || decodedSink.nodeCount() == 0) {
            throw new IllegalArgumentException(
                    "PROVIDER_CONFIG_NONCANONICAL: provider Sink fragment is missing");
        }
        return ProviderSinkStageEnvelopeCodec.decode(
                envelope, slice.offset, slice.length, reservation)
                .materialize(decodedSink.root().outMeta());
    }

    private io.rowweave.plan.PlanGraphEnvelopeScanner.Scan scanDurablePlan(
            Slice slice, DurableBatchSinkProviderRegistry registry,
            LongConsumer reservation) {
        if (slice == null) return null;
        io.rowweave.plan.PlanGraphEnvelopeScanner.Scan scan =
                io.rowweave.plan.PlanGraphEnvelopeScanner.scanDurable(
                        envelope, slice.offset, slice.length, reservation);
        DurableSinkProviderPlanPreflight.validate(scan, registry);
        return scan;
    }

    private <T> T readObject(Slice slice, Class<T> expectedRoot,
                             ObjectTrustContext trustContext, LongConsumer reservation) {
        return slice == null ? null : TrustedObjectCodec.readSlice(
                envelope, slice.offset, slice.length, expectedRoot, trustContext, reservation);
    }
}
