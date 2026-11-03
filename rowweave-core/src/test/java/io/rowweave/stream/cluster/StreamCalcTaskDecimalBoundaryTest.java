package io.rowweave.stream.cluster;

import io.rowweave.DataRow;
import io.rowweave.portable.PortableDataType;
import io.rowweave.portable.PortableField;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.stream.api.StreamExpression;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.StreamCalcBinding;
import io.rowweave.stream.contract.StreamOperatorCode;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** CALC rounds only projections marked as decimal boundaries. */
class StreamCalcTaskDecimalBoundaryTest {

    @Test
    void preservesIntermediatePrecisionAndRoundsOnceAtDecimalProjection() {
        PortableRowSchema input = schema("amount");
        StreamExpression sum = StreamExpression.call(
                StreamExpression.Function.ADD,
                StreamExpression.input(
                        "amount", PortableDataType.BIG_DECIMAL, false),
                StreamExpression.decimalLiteral(
                        new BigDecimal("0.000000000002")));
        PortableRowSchema output = new PortableRowSchema(Arrays.asList(
                field("raw"), field("posted")));
        StreamCalcBinding binding = new StreamCalcBinding(null, Arrays.asList(
                StreamCalcBinding.projection("raw", sum),
                StreamCalcBinding.decimalProjection("posted", sum)));
        PortableStreamNode node = new PortableStreamNode(
                "calc", StreamOperatorCode.CALC,
                Collections.singletonList("source"),
                Collections.singletonList(input.fingerprint()), output,
                Collections.emptyList(), Collections.emptyList(),
                Collections.emptyMap(), binding);
        StreamOperatorTask task = StreamOperatorTaskFactory.create(
                node, input,
                ignored -> { throw new AssertionError("no function expected"); });
        DataRow source = new DataRow(1);
        source.set(0, new BigDecimal("1.000000000009"));
        DataRow[] emitted = new DataRow[1];

        task.process(source, 42L, (row, timestamp) -> emitted[0] = row);

        assertEquals(new BigDecimal("1.000000000011"), emitted[0].get(0));
        assertEquals(new BigDecimal("1.0000000000"), emitted[0].get(1));
    }

    private static PortableRowSchema schema(String fieldId) {
        return new PortableRowSchema(Collections.singletonList(field(fieldId)));
    }

    private static PortableField field(String fieldId) {
        return new PortableField(fieldId, fieldId, fieldId,
                PortableDataType.BIG_DECIMAL, false);
    }
}
