package io.rowweave.stream.contract;

import io.rowweave.portable.PortableDataType;
import io.rowweave.portable.PortableField;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.stream.api.StreamExpression;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** CALC binding is typed, ordered and canonically portable. */
class StreamCalcBindingContractTest {

    @Test
    void predicateAndOrderedProjectionsRoundTripInThePlanCodec() {
        StreamExpression amount = StreamExpression.input(
                "amount", PortableDataType.BIG_DECIMAL, true);
        StreamCalcBinding binding = new StreamCalcBinding(
                StreamExpression.call(StreamExpression.Function.GREATER_THAN,
                        amount, StreamExpression.decimalLiteral(BigDecimal.ZERO)),
                Arrays.asList(
                        StreamCalcBinding.projection("id",
                                StreamExpression.input("id",
                                        PortableDataType.LONG, false)),
                        StreamCalcBinding.decimalProjection("total",
                                StreamExpression.call(
                                        StreamExpression.Function.COALESCE,
                                        amount,
                                        StreamExpression.decimalLiteral(
                                                BigDecimal.ZERO)))));

        PortableStreamPlan plan = plan(binding, outputSchema());
        PortableStreamPlan decoded = PortableStreamPlanCodec.decode(
                PortableStreamPlanCodec.encode(plan));
        StreamCalcBinding restored = (StreamCalcBinding)
                decoded.nodes().get(1).binding();

        assertEquals(binding, restored);
        assertEquals(StreamOperatorCode.BindingKind.CALC, restored.kind());
        assertEquals("id", restored.projections().get(0).outputFieldId());
        assertFalse(restored.projections().get(0).decimalOutputBoundary());
        assertEquals(PortableDataType.BIG_DECIMAL,
                restored.projections().get(1).expression().dataType());
    }

    @Test
    void invalidPredicateProjectionAndDeclaredSchemaFailClosed() {
        StreamCalcBinding valid = new StreamCalcBinding(null, Arrays.asList(
                StreamCalcBinding.projection("id", StreamExpression.input(
                        "id", PortableDataType.LONG, false)),
                StreamCalcBinding.decimalProjection("total",
                        StreamExpression.decimalLiteral(BigDecimal.ZERO))));

        assertFailure("EXPRESSION_TYPE_MISMATCH", () ->
                new StreamCalcBinding(StreamExpression.longLiteral(1L),
                        valid.projections()));
        assertFailure("DUPLICATE_PROJECTION", () -> new StreamCalcBinding(null,
                Arrays.asList(
                        StreamCalcBinding.projection("id",
                                StreamExpression.longLiteral(1L)),
                        StreamCalcBinding.projection("id",
                                StreamExpression.longLiteral(2L)))));
        assertFailure("DECIMAL_BOUNDARY_TYPE_MISMATCH", () ->
                StreamCalcBinding.decimalProjection("id",
                        StreamExpression.longLiteral(1L)));

        PortableRowSchema wrong = new PortableRowSchema(Arrays.asList(
                new PortableField("id", "id", "id",
                        PortableDataType.LONG, false),
                new PortableField("total", "total", "total",
                        PortableDataType.BIG_DECIMAL, true)));
        assertFailure("CALC_SCHEMA_MISMATCH", () -> plan(valid, wrong));

        assertFailure("BINDING_KIND_MISMATCH", () ->
                new PortableStreamNode("calc", StreamOperatorCode.CALC,
                        Collections.singletonList("source"),
                        Collections.singletonList(inputSchema().fingerprint()),
                        outputSchema(), Collections.singletonList("id"),
                        Collections.emptyList(), Collections.emptyMap(),
                        new StreamNodeBinding() {
                            @Override public StreamOperatorCode.BindingKind kind() {
                                return StreamOperatorCode.BindingKind.CALC;
                            }
                        }));
    }

    private static PortableStreamPlan plan(StreamCalcBinding binding,
            PortableRowSchema output) {
        PortableRowSchema input = inputSchema();
        PortableStreamNode source = new PortableStreamNode(
                "source", StreamOperatorCode.SOURCE,
                Collections.emptyList(), Collections.emptyList(), input,
                Collections.singletonList("id"), Collections.emptyList(),
                Collections.emptyMap(), sourceBinding());
        PortableStreamNode calc = new PortableStreamNode(
                "calc", StreamOperatorCode.CALC,
                Collections.singletonList("source"),
                Collections.singletonList(input.fingerprint()), output,
                Collections.singletonList("id"), Collections.emptyList(),
                Collections.emptyMap(), binding);
        PortableStreamNode sink = new PortableStreamNode(
                "sink", StreamOperatorCode.SINK,
                Collections.singletonList("calc"),
                Collections.singletonList(output.fingerprint()), output,
                Collections.singletonList("id"), Collections.emptyList(),
                Collections.emptyMap(), sinkBinding());
        return new PortableStreamPlan(PortableStreamPlan.CURRENT_VERSION,
                PortableStreamPlan.DeliveryGuarantee.AT_LEAST_ONCE,
                1, 128, PortableStreamPlan.ChangelogMode.APPEND,
                PortableStreamPlan.TimeMode.PROCESSING_TIME,
                Collections.singletonList("sink"),
                Arrays.asList(source, calc, sink),
                Arrays.asList(edge("source", "calc"), edge("calc", "sink")),
                Arrays.asList(PortableStreamPlan.STREAM_CAPABILITY,
                        PortableStreamPlan.CALC_CAPABILITY));
    }

    private static PortableRowSchema inputSchema() {
        return new PortableRowSchema(Arrays.asList(
                new PortableField("id", "id", "id",
                        PortableDataType.LONG, false),
                new PortableField("amount", "amount", "amount",
                        PortableDataType.BIG_DECIMAL, true)));
    }

    private static PortableRowSchema outputSchema() {
        return new PortableRowSchema(Arrays.asList(
                new PortableField("id", "id", "id",
                        PortableDataType.LONG, false),
                new PortableField("total", "total", "total",
                        PortableDataType.BIG_DECIMAL, false)));
    }

    private static StreamEdge edge(String source, String target) {
        return new StreamEdge(source, target, 0,
                repeat(source.charAt(0), 64), StreamEdge.Partitioning.FORWARD,
                1024, 1024);
    }

    private static StreamSourceBinding sourceBinding() {
        return new StreamSourceBinding("source", repeat('a', 64),
                sha256(new byte[] {1}), new byte[] {1},
                Collections.singletonList(new StreamSourceSplit(
                        repeat('c', 64), "position-v1", new byte[] {2})),
                StreamSourceBinding.ReplayGuarantee.EXACT_ORDERED);
    }

    private static StreamSinkBinding sinkBinding() {
        return new StreamSinkBinding("sink", repeat('d', 64),
                sha256(new byte[] {3}), new byte[] {3},
                StreamSinkBinding.VisibilityProfile.EAGER_AT_LEAST_ONCE);
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        java.util.Arrays.fill(chars, value);
        return new String(chars);
    }

    private static String sha256(byte[] value) {
        try {
            byte[] digest = java.security.MessageDigest.getInstance("SHA-256")
                    .digest(value);
            StringBuilder result = new StringBuilder(64);
            for (byte current : digest) result.append(String.format(
                    java.util.Locale.ROOT, "%02x", current & 0xff));
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static void assertFailure(String code, Runnable action) {
        StreamContractException failure = assertThrows(
                StreamContractException.class, action::run);
        assertEquals(code, failure.code());
    }
}
