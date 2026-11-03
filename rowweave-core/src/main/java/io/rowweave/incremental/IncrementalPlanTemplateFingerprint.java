package io.rowweave.incremental;

import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.OperatorDescriptor;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.plan.ObjectTrustContext;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongConsumer;

/** Derives the interval-independent definition fingerprint from one durable template. */
public final class IncrementalPlanTemplateFingerprint {
    private static final byte[] OPERATOR_DOMAIN =
            "rowweave-incremental-template-operator-v1".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SINK_DOMAIN =
            "rowweave-incremental-template-sink-v1".getBytes(StandardCharsets.UTF_8);

    private IncrementalPlanTemplateFingerprint() {
    }

    public static IncrementalDefinition derive(
            String definitionId, PlanGraph template,
            ObjectTrustContext trustContext, LongConsumer reservation) {
        if (template == null || trustContext == null || reservation == null) {
            throw new IllegalArgumentException(
                    "incremental template fingerprint fields are required");
        }
        int sourceIndex = uniquePlaceholderSource(template);
        validateOperatorClosure(template);
        IncrementalPlanDescriptor planDescriptor =
                template.node(sourceIndex).incrementalPlanDescriptor();
        PlanArtifact normalized = LegacyPlanArtifactAdapter.wrapDurable(
                definitionId, template, trustContext, reservation);
        byte[] templateDigest = sha256(normalized.payload());
        List<IncrementalDefinition.OperatorBinding> operators =
                new ArrayList<>(normalized.operators().size());
        List<byte[]> sinkBindings = new ArrayList<>();
        for (int index = 0; index < normalized.operators().size(); index++) {
            OperatorDescriptor operator = normalized.operators().get(index);
            IncrementalDefinition.OperatorKind kind = kind(operator);
            operators.add(new IncrementalDefinition.OperatorBinding(
                    operator.operatorId(), kind, operator.typeId(),
                    hexToBytes(operator.schemaFingerprint()),
                    operator.inputOperatorIds(),
                    scopedDigest(OPERATOR_DOMAIN, templateDigest, index)));
            if (kind == IncrementalDefinition.OperatorKind.SINK_V1) {
                sinkBindings.add(scopedDigest(
                        SINK_DOMAIN, templateDigest, index));
            }
        }
        return new IncrementalDefinition(
                definitionId, planDescriptor, operators, sinkBindings);
    }

    static int uniquePlaceholderSource(PlanGraph template) {
        int sourceIndex = -1;
        for (int index = 0; index < template.nodeCount(); index++) {
            PlanNode node = template.node(index);
            if (node.kind() != PlanNode.Kind.SOURCE) continue;
            if (sourceIndex >= 0 || !node.inputs().isEmpty()
                    || !node.hasIncrementalPlanDescriptor()) {
                throw new IllegalArgumentException(
                        "incremental template requires one placeholder SOURCE");
            }
            sourceIndex = index;
        }
        if (sourceIndex < 0) {
            throw new IllegalArgumentException(
                    "incremental template SOURCE is missing");
        }
        return sourceIndex;
    }

    private static void validateOperatorClosure(PlanGraph template) {
        for (int index = 0; index < template.nodeCount(); index++) {
            PlanNode node = template.node(index);
            if (node.kind() == PlanNode.Kind.BINARY) {
                throw new IllegalArgumentException(
                        "incremental template does not permit binary operators");
            }
            if (node.kind() == PlanNode.Kind.UNARY
                    && (!"incrementalMap".equals(node.typeId())
                    || !(node.userFunction()
                    instanceof io.rowweave.IncrementalMapFunction))) {
                throw new IllegalArgumentException(
                        "incremental template only permits IncrementalMapFunction");
            }
        }
    }

    private static IncrementalDefinition.OperatorKind kind(
            OperatorDescriptor operator) {
        switch (operator.kind()) {
            case SOURCE:
                return IncrementalDefinition.OperatorKind.SOURCE_V1;
            case UNARY:
                if (!"incrementalMap".equals(operator.typeId())) {
                    throw new IllegalArgumentException(
                            "incremental template only permits IncrementalMapFunction");
                }
                return IncrementalDefinition.OperatorKind.MAP_V1;
            case SINK:
                return IncrementalDefinition.OperatorKind.SINK_V1;
            case BINARY:
            default:
                throw new IllegalArgumentException(
                        "incremental template does not permit binary operators");
        }
    }

    private static byte[] scopedDigest(
            byte[] domain, byte[] templateDigest, int index) {
        MessageDigest digest = digest();
        digest.update(domain);
        digest.update(templateDigest);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(index).array());
        return digest.digest();
    }

    private static byte[] sha256(byte[] value) {
        return digest().digest(value);
    }

    private static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static byte[] hexToBytes(String value) {
        if (value == null || !value.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException(
                    "incremental template schema fingerprint is invalid");
        }
        byte[] result = new byte[value.length() / 2];
        for (int index = 0; index < result.length; index++) {
            result[index] = (byte) Integer.parseInt(
                    value.substring(index * 2, index * 2 + 2), 16);
        }
        return result;
    }
}
