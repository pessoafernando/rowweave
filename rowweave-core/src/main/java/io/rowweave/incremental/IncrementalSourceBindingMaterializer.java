package io.rowweave.incremental;

import io.rowweave.BatchSourceSplitBinding;
import io.rowweave.IncrementalSourceSplit;
import io.rowweave.DataSchema;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Builds and verifies the inert source bindings carried by an interval Artifact. */
public final class IncrementalSourceBindingMaterializer {
    private IncrementalSourceBindingMaterializer() {
    }

    public static List<BatchSourceSplitBinding> materialize(
            DataSchema dataSchema, IncrementalSourceConfig configuration,
            IncrementalMaterializationResponse response) {
        return materialize(dataSchema, configuration, response, null);
    }

    public static List<BatchSourceSplitBinding> materialize(
            DataSchema dataSchema, IncrementalSourceConfig configuration,
            IncrementalMaterializationResponse response,
            IncrementalPlanDescriptor planDescriptor) {
        if (dataSchema == null || configuration == null || response == null) {
            throw new IllegalArgumentException(
                    "incremental source materialization fields are required");
        }
        if (!configuration.providerKey().equals(response.providerKey())
                || !MessageDigest.isEqual(configuration.contractDigest(),
                response.capturedTarget().sourceContractDigest())) {
            throw new IllegalArgumentException(
                    "incremental source materialization binding differs");
        }
        List<BatchSourceSplitBinding> bindings =
                new ArrayList<>(response.splits().size());
        for (int index = 0; index < response.splits().size(); index++) {
            IncrementalMaterializedSplit split = response.splits().get(index);
            IncrementalSourceSplit input =
                    new IncrementalSourceSplit(
                            dataSchema, configuration, response.capturedTarget(),
                            response.partitionSetDigest(), split,
                            planDescriptor, index);
            bindings.add(new BatchSourceSplitBinding(
                    input, split.descriptor()));
        }
        return Collections.unmodifiableList(bindings);
    }

    /**
     * Worker-side final check immediately before resolving provider code and
     * opening a handle. Every authority and split identity must still be exact.
     */
    public static IncrementalSourceSplit verifyPreOpen(
            BatchSourceSplitBinding binding,
            IncrementalSourceProviderContract contract,
            IncrementalSourceConfig configuration,
            CapturedTargetReceipt capturedTarget,
            byte[] partitionSetDigest) {
        if (binding == null || contract == null || configuration == null
                || capturedTarget == null || partitionSetDigest == null
                || partitionSetDigest.length != 32
                || binding.input().getClass()
                != IncrementalSourceSplit.class) {
            throw new IllegalArgumentException(
                    "incremental source pre-open binding is invalid");
        }
        IncrementalSourceSplit input =
                (IncrementalSourceSplit) binding.input();
        byte[] contractDigest =
                IncrementalSourceProviderContractCodec.digest(contract);
        if (!contract.providerKey().equals(configuration.providerKey())
                || !MessageDigest.isEqual(
                contractDigest, configuration.contractDigest())
                || !configuration.equals(input.configuration())
                || !capturedTarget.equals(input.capturedTarget())
                || !MessageDigest.isEqual(
                contractDigest, capturedTarget.sourceContractDigest())
                || !MessageDigest.isEqual(
                partitionSetDigest, input.partitionSetDigest())
                || !binding.descriptor().equals(input.batchSourceSplit())
                || !contract.providerKey().connectorType().equals(
                binding.descriptor().connectorType())
                || contract.providerKey().connectorVersion()
                != binding.descriptor().connectorVersion()) {
            throw new IllegalArgumentException(
                    "incremental source pre-open identity differs");
        }
        return input;
    }
}
