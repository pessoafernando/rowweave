package io.rowweave.cluster;

import io.rowweave.incremental.CapturedTargetReceipt;
import io.rowweave.incremental.IncrementalCaptureRequest;
import io.rowweave.incremental.IncrementalSourceConfig;
import io.rowweave.incremental.IncrementalSourceProviderContract;
import io.rowweave.incremental.IncrementalSourceProviderContractCodec;

import java.security.MessageDigest;
import java.util.Objects;

/** Core-owned immutable command; it contains no provider object or authority token. */
final class IncrementalSourceActionCommand {
    enum Action { CAPTURE, MATERIALIZE }

    private final Action action;
    private final IncrementalSourceProviderContract contract;
    private final IncrementalSourceConfig configuration;
    private final IncrementalCaptureRequest captureRequest;
    private final CapturedTargetReceipt capturedTarget;

    private IncrementalSourceActionCommand(
            Action action, IncrementalSourceProviderContract contract,
            IncrementalSourceConfig configuration,
            IncrementalCaptureRequest captureRequest,
            CapturedTargetReceipt capturedTarget) {
        if (action == null || contract == null || configuration == null
                || (action == Action.CAPTURE) != (captureRequest != null)
                || (action == Action.MATERIALIZE) != (capturedTarget != null)) {
            throw new IllegalArgumentException(
                    "incremental source action command fields are invalid");
        }
        byte[] digest = IncrementalSourceProviderContractCodec.digest(contract);
        if (!contract.providerKey().equals(configuration.providerKey())
                || !MessageDigest.isEqual(
                digest, configuration.contractDigest())) {
            throw new IllegalArgumentException(
                    "incremental source action configuration binding differs");
        }
        if (capturedTarget != null
                && (!MessageDigest.isEqual(digest,
                capturedTarget.sourceContractDigest())
                || capturedTarget.target().kind() != contract.frontierKind())) {
            throw new IllegalArgumentException(
                    "incremental source action capture binding differs");
        }
        this.action = action;
        this.contract = contract;
        this.configuration = configuration;
        this.captureRequest = captureRequest;
        this.capturedTarget = capturedTarget;
    }

    static IncrementalSourceActionCommand capture(
            IncrementalSourceProviderContract contract,
            IncrementalSourceConfig configuration,
            IncrementalCaptureRequest request) {
        return new IncrementalSourceActionCommand(
                Action.CAPTURE, contract, configuration, request, null);
    }

    static IncrementalSourceActionCommand materialize(
            IncrementalSourceProviderContract contract,
            IncrementalSourceConfig configuration,
            CapturedTargetReceipt capturedTarget) {
        return new IncrementalSourceActionCommand(
                Action.MATERIALIZE, contract, configuration,
                null, capturedTarget);
    }

    Action action() { return action; }
    IncrementalSourceProviderContract contract() { return contract; }
    IncrementalSourceConfig configuration() { return configuration; }
    IncrementalCaptureRequest captureRequest() { return captureRequest; }
    CapturedTargetReceipt capturedTarget() { return capturedTarget; }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof IncrementalSourceActionCommand)) return false;
        IncrementalSourceActionCommand that =
                (IncrementalSourceActionCommand) other;
        return action == that.action
                && contract.equals(that.contract)
                && configuration.equals(that.configuration)
                && Objects.equals(captureRequest, that.captureRequest)
                && Objects.equals(capturedTarget, that.capturedTarget);
    }

    @Override
    public int hashCode() {
        return Objects.hash(action, contract, configuration,
                captureRequest, capturedTarget);
    }
}
