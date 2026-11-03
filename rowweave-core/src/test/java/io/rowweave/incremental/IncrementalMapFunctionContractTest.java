package io.rowweave.incremental;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.IncrementalMapFunction;
import io.rowweave.ManifestFileSink;
import io.rowweave.MapFunction;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.plan.ObjectTrustContext;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** only identity-preserving dedicated maps are changelog-safe. */
class IncrementalMapFunctionContractTest {
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.LongType),
            new Field("op", DataType.StringType),
            new Field("sequence", DataType.LongType),
            new Field("payload", DataType.StringType));

    @Test
    void templateAdmitsOnlyDedicatedIncrementalMap(@TempDir Path root)
            throws Exception {
        IncrementalPlanDescriptor descriptor = descriptor();
        PlanNode source = source(descriptor);
        PlanNode ordinaryMap = PlanNode.unaryFn(
                "map", META, 0, (MapFunction) DataRow::copy)
                .withParam(PlanNode.OPERATOR_UID_PARAM, "ordinary-map");
        PlanNode filter = PlanNode.unary(
                "filter", META, 0).param("expr", "id > 0")
                .param(PlanNode.OPERATOR_UID_PARAM, "filter").build();

        assertThrows(IllegalArgumentException.class,
                () -> derive(root, source, ordinaryMap));
        assertThrows(IllegalArgumentException.class,
                () -> derive(root, source, filter));

        PlanNode incrementalMap = PlanNode.unaryFn(
                "incrementalMap", META, 0,
                (IncrementalMapFunction) DataRow::copy)
                .withParam(PlanNode.OPERATOR_UID_PARAM, "incremental-map");
        IncrementalDefinition definition = derive(
                root, source, incrementalMap);
        assertEquals(IncrementalDefinition.OperatorKind.MAP_V1,
                definition.operators().get(1).kind());
        assertEquals("incrementalMap",
                definition.operators().get(1).typeId());
    }

    @Test
    void runtimeGuardAllowsPayloadChangeButRejectsIdentityMutation()
            throws Exception {
        IncrementalPlanDescriptor descriptor = descriptor();
        DataRow original = row(7L, "UPSERT", 11L, "before");
        IncrementalMapFunction payloadOnly = input -> {
            DataRow output = input.copy();
            output.set(3, "after");
            return output;
        };
        DataRow output = IncrementalMapIdentityGuard.guard(
                payloadOnly, META, descriptor).map(original);
        assertEquals(7L, output.get(0));
        assertEquals("after", output.get(3));

        assertIdentityMutationRejected(descriptor, input -> {
            DataRow changed = input.copy();
            changed.set(0, 8L);
            return changed;
        });
        assertIdentityMutationRejected(descriptor, input -> {
            DataRow changed = input.copy();
            changed.set(1, "DELETE");
            return changed;
        });
        assertIdentityMutationRejected(descriptor, input -> {
            DataRow changed = input.copy();
            changed.set(2, 12L);
            return changed;
        });
        assertIdentityMutationRejected(descriptor, input -> null);
    }

    private static void assertIdentityMutationRejected(
            IncrementalPlanDescriptor descriptor,
            IncrementalMapFunction function) {
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalMapIdentityGuard.guard(
                        function, META, descriptor).map(
                        row(7L, "UPSERT", 11L, "before")));
    }

    private static IncrementalDefinition derive(
            Path root, PlanNode source, PlanNode unary) {
        ManifestFileSink output = new ManifestFileSink(
                META, root.resolve("manifest").toString(), "changes");
        PlanNode sink = PlanNode.sink(META, 1, output.toProviderSink())
                .param(PlanNode.OPERATOR_UID_PARAM, "sink").build();
        return IncrementalPlanTemplateFingerprint.derive(
                "definition", new PlanGraph(
                        Arrays.asList(source, unary, sink), 1),
                ObjectTrustContext.localTrusted(), ignored -> { });
    }

    private static PlanNode source(IncrementalPlanDescriptor descriptor) {
        return PlanNode.placeholderSource(META)
                .withParam(PlanNode.OPERATOR_UID_PARAM, "source")
                .withIncrementalPlanDescriptor(descriptor);
    }

    private static IncrementalPlanDescriptor descriptor() throws Exception {
        return new IncrementalPlanDescriptor(
                "source", digest("frontier-contract"),
                Collections.singletonList(0), 1,
                Collections.singletonList(2), schemaDigest(), true,
                digest("partitions"),
                IncrementalPlanDescriptor.SinkCommitProfile.MANIFEST_ATOMIC_V1);
    }

    private static DataRow row(
            Long id, String operation, Long sequence, String payload) {
        DataRow row = new DataRow(4);
        row.set(0, id);
        row.set(1, operation);
        row.set(2, sequence);
        row.set(3, payload);
        return row;
    }

    private static byte[] schemaDigest() {
        String fingerprint = SchemaFingerprint.of(META);
        byte[] decoded = new byte[fingerprint.length() / 2];
        for (int index = 0; index < decoded.length; index++) {
            decoded[index] = (byte) Integer.parseInt(
                    fingerprint.substring(index * 2, index * 2 + 2), 16);
        }
        return decoded;
    }

    private static byte[] digest(String value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(
                value.getBytes(StandardCharsets.UTF_8));
    }
}
