package io.rowweave;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.portable.PortableDataType;
import io.rowweave.stream.api.StreamApiException;
import io.rowweave.stream.api.StreamCalcSpec;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamExpression;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.contract.PortableStreamJob;
import io.rowweave.stream.contract.PortableStreamJobArtifact;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.StreamCalcBinding;
import io.rowweave.stream.contract.StreamOperatorCode;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** public CALC declarations compile to typed portable IR. */
class StreamPublicCalcCompilerContractTest {

    @Test
    void calcCompilesPredicateProjectionCapabilityAndIdentityKey() {
        StreamExpression id = StreamExpression.input(
                "id", PortableDataType.LONG, false);
        StreamExpression amount = StreamExpression.input(
                "amount", PortableDataType.BIG_DECIMAL, true);
        StreamCalcSpec calc = StreamCalcSpec.of(
                StreamExpression.call(StreamExpression.Function.GREATER_THAN,
                        amount, StreamExpression.decimalLiteral(BigDecimal.ZERO)),
                Arrays.asList(
                        StreamCalcSpec.projection("id", id),
                        StreamCalcSpec.decimalProjection("total",
                                StreamExpression.call(
                                        StreamExpression.Function.COALESCE,
                                        amount,
                                        StreamExpression.decimalLiteral(
                                                BigDecimal.ZERO)))));

        StreamSession session = RowWeave.createStreamSession("calc");
        session.fromSource(source()).uid("source")
                .keyBy("id").uid("key")
                .calc(calc, outputSchema()).uid("calc")
                .sinkTo("sink", sink());

        PlanArtifact artifact = session.toPlanArtifact();
        PortableStreamJob job = PortableStreamJobArtifact.decodeValidated(artifact);
        PortableStreamNode node = job.plan().nodes().get(2);
        assertEquals(StreamOperatorCode.CALC, node.opcode());
        assertEquals(Collections.singletonList("id"), node.stableKeyFields());
        StreamCalcBinding binding = (StreamCalcBinding) node.binding();
        assertEquals(calc.projections().size(), binding.projections().size());
        assertEquals(calc.projections().get(1).outputFieldId(),
                binding.projections().get(1).outputFieldId());
        assertEquals(calc.projections().get(1).expression(),
                binding.projections().get(1).expression());
        assertEquals(calc.projections().get(1).decimalOutputBoundary(),
                binding.projections().get(1).decimalOutputBoundary());
        assertTrue(job.plan().requiredCapabilities().contains(
                PortableStreamPlan.CALC_CAPABILITY));
        assertTrue(artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_CALC_V1));
    }

    @Test
    void calcClearsKeyWhenProjectionIsNotAnIdentity() {
        StreamCalcSpec calc = StreamCalcSpec.of(null, Arrays.asList(
                StreamCalcSpec.projection("renamed-id", StreamExpression.input(
                        "id", PortableDataType.LONG, false)),
                StreamCalcSpec.projection("total",
                        StreamExpression.decimalLiteral(BigDecimal.ZERO))));
        StreamSession session = RowWeave.createStreamSession("calc-key-clear");
        session.fromSource(source()).uid("source")
                .keyBy("id").uid("key")
                .calc(calc, renamedSchema()).uid("calc")
                .sinkTo("sink", sink());

        PortableStreamNode node = PortableStreamJobArtifact.decodeValidated(
                session.toPlanArtifact()).plan().nodes().get(2);
        assertTrue(node.stableKeyFields().isEmpty());
    }

    @Test
    void calcRejectsUnknownInputAndDeclaredSchemaDrift() {
        assertCode(StreamApiException.Code.INVALID_EXPRESSION, () -> compile(
                StreamCalcSpec.of(null, Collections.singletonList(
                        StreamCalcSpec.projection("id", StreamExpression.input(
                                "missing", PortableDataType.LONG, false)))),
                new StreamSchema(Collections.singletonList(new StreamField(
                        "id", "id", "id", DataType.LongType, false)))));

        assertCode(StreamApiException.Code.SCHEMA_MISMATCH, () -> compile(
                StreamCalcSpec.of(null, Collections.singletonList(
                        StreamCalcSpec.projection("id",
                                StreamExpression.longLiteral(1L)))),
                new StreamSchema(Collections.singletonList(new StreamField(
                        "id", "id", "id", DataType.LongType, true)))));
    }

    private static void compile(StreamCalcSpec spec, StreamSchema output) {
        StreamSession session = RowWeave.createStreamSession("invalid-calc");
        session.fromSource(source()).uid("source")
                .calc(spec, output).uid("calc").sinkTo("sink", sink());
        session.toPlanArtifact();
    }

    private static void assertCode(StreamApiException.Code code,
            Runnable action) {
        StreamApiException failure = assertThrows(
                StreamApiException.class, action::run);
        assertEquals(code, failure.code());
    }

    private static StreamSchema inputSchema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("id", "id", "id", DataType.LongType, false),
                new StreamField("amount", "amount", "amount",
                        DataType.BigDecimalType, true)));
    }

    private static StreamSchema outputSchema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("id", "id", "id", DataType.LongType, false),
                new StreamField("total", "total", "total",
                        DataType.BigDecimalType, false)));
    }

    private static StreamSchema renamedSchema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("renamed-id", "id", "id",
                        DataType.LongType, false),
                new StreamField("total", "total", "total",
                        DataType.BigDecimalType, false)));
    }

    private static StreamSourceSpec source() {
        return StreamSourceSpec.exactOrdered(inputSchema(), "source-v1",
                sha256("source-contract"), new byte[] {1},
                Collections.singletonList(new StreamSplitSpec(
                        sha256("split"), "sequence-v1", new byte[] {2})));
    }

    private static StreamSinkSpec sink() {
        return StreamSinkSpec.eagerAtLeastOnce(
                "sink-v1", sha256("sink-contract"), new byte[] {3});
    }

    private static String sha256(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                    value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(64);
            for (byte current : digest) result.append(String.format(
                    java.util.Locale.ROOT, "%02x", current & 0xff));
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
