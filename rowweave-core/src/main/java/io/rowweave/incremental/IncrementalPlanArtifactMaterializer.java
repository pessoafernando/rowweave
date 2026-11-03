package io.rowweave.incremental;

import io.rowweave.BatchSourceSplitBinding;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactDeploymentBinding;
import io.rowweave.plan.ObjectTrustContext;
import io.rowweave.plan.PlanGraph;

import java.security.MessageDigest;
import java.util.List;
import java.util.function.LongConsumer;

/** Materializes one captured interval by replacing only the template SOURCE bindings. */
public final class IncrementalPlanArtifactMaterializer {
    private IncrementalPlanArtifactMaterializer() {
    }

    public static PlanArtifact materialize(
            IncrementalDefinition definition, PlanGraph template,
            IncrementalSourceProviderContract sourceContract,
            IncrementalSourceConfig configuration,
            IncrementalMaterializationResponse response,
            ObjectTrustContext trustContext, LongConsumer reservation) {
        return materialize(
                definition, template, sourceContract, configuration,
                response, null, trustContext, reservation);
    }

    public static PlanArtifact materialize(
            IncrementalDefinition definition, PlanGraph template,
            IncrementalSourceProviderContract sourceContract,
            IncrementalSourceConfig configuration,
            IncrementalMaterializationResponse response,
            String deploymentSetId,
            ObjectTrustContext trustContext, LongConsumer reservation) {
        if (definition == null || template == null || sourceContract == null
                || configuration == null || response == null
                || trustContext == null || reservation == null) {
            throw new IllegalArgumentException(
                    "incremental Artifact materialization fields are required");
        }
        int sourceIndex =
                IncrementalPlanTemplateFingerprint.uniquePlaceholderSource(template);
        IncrementalDefinition derived = IncrementalPlanTemplateFingerprint.derive(
                definition.definitionId(), template, trustContext, reservation);
        CapturedTargetReceipt receipt = response.capturedTarget();
        byte[] contractDigest =
                IncrementalSourceProviderContractCodec.digest(sourceContract);
        if (!definition.equals(derived)
                || !MessageDigest.isEqual(
                definition.fingerprint(), receipt.definitionDigest())
                || !sourceContract.providerKey().equals(
                configuration.providerKey())
                || !sourceContract.providerKey().equals(response.providerKey())
                || !MessageDigest.isEqual(
                contractDigest, configuration.contractDigest())
                || !MessageDigest.isEqual(
                contractDigest, receipt.sourceContractDigest())
                || receipt.target().kind() != sourceContract.frontierKind()
                || !MessageDigest.isEqual(
                definition.planDescriptor().partitionSetDigest(),
                response.partitionSetDigest())) {
            throw new IllegalArgumentException(
                    "incremental Artifact identity differs from template or capture");
        }
        List<BatchSourceSplitBinding> bindings =
                IncrementalSourceBindingMaterializer.materialize(
                        template.node(sourceIndex).outMeta(),
                        configuration, response,
                        definition.planDescriptor());
        PlanGraph materialized = template.replaceSourceBindings(
                sourceIndex, bindings);
        PlanArtifact materializedArtifact = LegacyPlanArtifactAdapter.wrapDurable(
                definitionIdentity(definition.fingerprint()), materialized,
                trustContext, reservation);
        return deploymentSetId == null ? materializedArtifact
                : PlanArtifactDeploymentBinding.bind(
                        materializedArtifact, deploymentSetId);
    }

    /** Uses the same frozen definition fingerprint stored by the pending Run. */
    public static String definitionIdentity(byte[] fingerprint) {
        byte[] digest = IncrementalAuthorityValue.digest(
                fingerprint, "definition fingerprint");
        StringBuilder value = new StringBuilder(digest.length * 2);
        for (byte octet : digest) {
            value.append(Character.forDigit((octet >>> 4) & 0xf, 16));
            value.append(Character.forDigit(octet & 0xf, 16));
        }
        return value.toString();
    }
}
