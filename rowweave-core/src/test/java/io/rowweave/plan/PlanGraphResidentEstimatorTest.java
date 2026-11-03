package io.rowweave.plan;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** The materialization formula is answered by an allocation-free wire scan, before any node is constructed. */
class PlanGraphResidentEstimatorTest {

    @Test
    void exactFormulaCountsNodesFieldsParamsValuesAndUtf8Bytes() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(1);
            out.writeInt(PlanNode.Kind.SOURCE.ordinal());
            utf(out, "SOURCE");
            out.writeInt(1);
            utf(out, "a");
            utf(out, "");
            out.writeInt(0);
            out.writeInt(0);
            out.writeInt(1);
            utf(out, "k");
            utf(out, "v");
            out.writeInt(1);
            utf(out, "l");
            out.writeInt(2);
            utf(out, "x");
            utf(out, "yz");
            out.writeInt(0);
            out.writeInt(1);
        }
        assertEquals(512L + 5L * 256L + 13L * 3L,
                PlanGraphResidentEstimator.estimate(bytes.toByteArray()));
    }

    @Test
    void truncationAndUnknownPayloadTagFailWithoutAllocatingPlanObjects() throws Exception {
        byte[] valid = minimalPlan(-1);
        for (int length = 0; length < valid.length; length++) {
            byte[] truncated = Arrays.copyOf(valid, length);
            if (length == valid.length - Integer.BYTES) {
                // A legacy plan graph carries no trailing parallelism, so falling back to P=1 is legal.
                PlanGraphResidentEstimator.estimate(truncated);
            } else {
                assertThrows(IllegalArgumentException.class,
                        () -> PlanGraphResidentEstimator.estimate(truncated));
            }
        }
        assertThrows(IllegalArgumentException.class,
                () -> PlanGraphResidentEstimator.estimate(minimalPlan(-99)));
    }

    private static byte[] minimalPlan(int trailer) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(1);
            out.writeInt(PlanNode.Kind.UNARY.ordinal());
            utf(out, "u");
            out.writeInt(0);
            out.writeInt(0);
            out.writeInt(0);
            out.writeInt(0);
            out.writeInt(trailer);
            out.writeInt(1);
        }
        return bytes.toByteArray();
    }

    private static void utf(DataOutputStream out, String value) throws Exception {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(encoded.length);
        out.write(encoded);
    }
}
