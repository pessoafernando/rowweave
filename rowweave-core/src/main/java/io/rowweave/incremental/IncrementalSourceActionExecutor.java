package io.rowweave.incremental;

import io.rowweave.IncrementalSourceProvider;
import io.rowweave.internal.ProviderActionSupervisor;

import java.security.MessageDigest;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;

/** Worker-only gateway for all incremental provider capture/materialize code. */
public final class IncrementalSourceActionExecutor
        implements IncrementalSourceActions {
    public static final String CAPTURE_CODEC_ID =
            "rowweave.incremental-capture-response";
    public static final String MATERIALIZATION_CODEC_ID =
            "rowweave.incremental-materialization-response";

    private final IncrementalSourceProviderContract contract;
    private final IncrementalSourceConfig configuration;
    private final IncrementalSourceProvider provider;
    private final ProviderActionSupervisor supervisor;
    private final byte[] contractDigest;

    public IncrementalSourceActionExecutor(
            IncrementalSourceProviderContract contract,
            IncrementalSourceConfig configuration,
            IncrementalSourceProvider provider,
            ProviderActionSupervisor supervisor) {
        if (contract == null || configuration == null
                || provider == null || supervisor == null) {
            throw new IllegalArgumentException(
                    "incremental source action fields are required");
        }
        byte[] expectedDigest =
                IncrementalSourceProviderContractCodec.digest(contract);
        if (!contract.providerKey().equals(configuration.providerKey())
                || !MessageDigest.isEqual(
                expectedDigest, configuration.contractDigest())) {
            throw new IllegalArgumentException(
                    "incremental source configuration contract differs");
        }
        if (configuration.configurationSize()
                > contract.configurationCodec().maxPayloadBytes()) {
            throw new IllegalArgumentException(
                    "incremental source configuration exceeds declared codec");
        }
        requireCoreCodec(contract.captureResponseCodec(), CAPTURE_CODEC_ID,
                IncrementalCaptureResponseCodec.MAX_FRAME_BYTES);
        requireCoreCodec(contract.materializationResponseCodec(),
                MATERIALIZATION_CODEC_ID,
                IncrementalMaterializationResponseCodec.MAX_FRAME_BYTES);
        this.contract = contract;
        this.configuration = configuration;
        this.provider = provider;
        this.supervisor = supervisor;
        this.contractDigest = expectedDigest;
    }

    @Override
    public CapturedTargetReceipt capture(
            IncrementalCaptureRequest request, long deadlineNanos) {
        if (request == null) {
            throw new IllegalArgumentException(
                    "incremental capture request is required");
        }
        return supervisor.call("incremental-source-capture",
                boundedDeadline(deadlineNanos), () -> {
                    requireRuntimeBinding();
                    IncrementalCaptureResponse response =
                            provider.captureTarget(configuration, request);
                    byte[] encoded = IncrementalCaptureResponseCodec.encode(
                            response);
                    if (encoded.length > contract.captureResponseCodec()
                            .maxPayloadBytes()) {
                        throw new IllegalArgumentException(
                                "incremental capture response exceeds declared codec");
                    }
                    IncrementalCaptureResponse canonical =
                            IncrementalCaptureResponseCodec.decode(encoded);
                    byte[] target = IncrementalFrontierCodec.encode(
                            canonical.target());
                    if (canonical.target().kind() != contract.frontierKind()
                            || target.length > contract.maxTargetBytes()) {
                        throw new IllegalArgumentException(
                                "incremental capture target differs from contract");
                    }
                    return request.bind(contractDigest, canonical);
                });
    }

    @Override
    public IncrementalMaterializationResponse materialize(
            CapturedTargetReceipt capturedTarget, long deadlineNanos,
            LongConsumer reservation) {
        if (capturedTarget == null || reservation == null) {
            throw new IllegalArgumentException(
                    "incremental materialization fields are required");
        }
        if (!MessageDigest.isEqual(contractDigest,
                capturedTarget.sourceContractDigest())
                || capturedTarget.target().kind() != contract.frontierKind()) {
            throw new IllegalArgumentException(
                    "incremental materialization capture differs from contract");
        }
        return supervisor.call("incremental-source-materialize",
                boundedDeadline(deadlineNanos), () -> {
                    requireRuntimeBinding();
                    IncrementalMaterializationResponse response =
                            provider.materializeInterval(
                                    configuration, capturedTarget);
                    byte[] encoded =
                            IncrementalMaterializationResponseCodec.encode(
                                    response);
                    if (encoded.length > contract.materializationResponseCodec()
                            .maxPayloadBytes()) {
                        throw new IllegalArgumentException(
                                "incremental materialization response exceeds declared codec");
                    }
                    IncrementalMaterializationResponse canonical =
                            IncrementalMaterializationResponseCodec.decode(
                                    encoded, reservation);
                    if (!contract.providerKey().equals(canonical.providerKey())
                            || !capturedTarget.equals(
                            canonical.capturedTarget())) {
                        throw new IllegalArgumentException(
                                "incremental materialization response binding differs");
                    }
                    return canonical;
                });
    }

    private void requireRuntimeBinding() {
        IncrementalSourceProviderContract runtime = provider.contract();
        if (!contract.equals(runtime)
                || !MessageDigest.isEqual(contractDigest,
                IncrementalSourceProviderContractCodec.digest(runtime))) {
            throw new IllegalArgumentException(
                    "incremental source runtime contract differs");
        }
        byte[] canonical = provider.canonicalizeConfiguration(configuration);
        if (canonical == null
                || canonical.length
                > contract.configurationCodec().maxPayloadBytes()
                || !MessageDigest.isEqual(
                canonical, configuration.configurationBytes())) {
            throw new IllegalArgumentException(
                    "incremental source configuration is not canonical");
        }
    }

    private long boundedDeadline(long requestedDeadlineNanos) {
        long now = System.nanoTime();
        long allowance = TimeUnit.MILLISECONDS.toNanos(
                contract.actionBudget().actionTimeoutMillis());
        long maximum = now > Long.MAX_VALUE - allowance
                ? Long.MAX_VALUE : now + allowance;
        return Math.min(requestedDeadlineNanos, maximum);
    }

    private static void requireCoreCodec(
            IncrementalSourceCodecDescriptor codec,
            String codecId, int maximumBytes) {
        if (!codecId.equals(codec.codecId()) || codec.codecVersion() != 1
                || codec.maxPayloadBytes() > maximumBytes) {
            throw new IllegalArgumentException(
                    "incremental source response codec is unsupported");
        }
    }
}
