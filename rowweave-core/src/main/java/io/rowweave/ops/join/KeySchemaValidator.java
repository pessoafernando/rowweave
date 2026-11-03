package io.rowweave.ops.join;

import io.rowweave.DataType;
import io.rowweave.DataSchema;

/** Shared graph-construction validation for binary keyed operators. */
final class KeySchemaValidator {

    private KeySchemaValidator() {
    }

    static void validate(
            String operator, DataSchema left, DataSchema right,
            int[] leftIndices, int[] rightIndices) {
        for (int i = 0; i < leftIndices.length; i++) {
            DataType leftType = left.getField(leftIndices[i]).getDataType();
            DataType rightType = right.getField(rightIndices[i]).getDataType();
            if (leftType != rightType && !(isNumeric(leftType) && isNumeric(rightType))) {
                throw new IllegalArgumentException(operator + " key type mismatch at position " + i
                        + ": left=" + leftType + ", right=" + rightType);
            }
        }
    }

    private static boolean isNumeric(DataType type) {
        return type == DataType.IntegerType
                || type == DataType.LongType
                || type == DataType.BigDecimalType;
    }
}
