package io.rowweave.plan;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A plan graph must pass a structural preflight that instantiates no user object. */
class PlanGraphEnvelopeScannerTest {

    @Test
    void validLegacyAndVersionWithParallelismProduceSameBoundedScan() throws Exception {
        byte[] legacy = unaryPlan(false, 1, 0, false, false);
        byte[] current = unaryPlan(true, 4, 0, false, false);

        assertEquals(2, PlanGraphEnvelopeScanner.scan(legacy).nodeCount());
        assertEquals(1, PlanGraphEnvelopeScanner.scan(legacy).parallelism());
        assertEquals(4, PlanGraphEnvelopeScanner.scan(current).parallelism());
    }

    @Test
    void invalidKindDataTypeAndChildTopologyFailPreflight() throws Exception {
        assertRejected(singleNode(99, 0, 0, -1, 1), "kind");
        assertRejected(singleNode(PlanNode.Kind.SOURCE.ordinal(), 99, 1, 0, 1), "data type");
        assertRejected(unaryPlan(true, 1, 1, false, false), "child");
        assertRejected(singleNode(PlanNode.Kind.BINARY.ordinal(), 0, 1, -1, 1), "child count");
    }

    @Test
    void duplicateParamsInvalidUtf8AndTrailingBytesFailPreflight() throws Exception {
        assertRejected(unaryPlan(true, 1, 0, true, false), "duplicate param");
        assertRejected(unaryPlan(true, 1, 0, false, true), "UTF-8");
        byte[] valid = unaryPlan(true, 1, 0, false, false);
        byte[] trailing = java.util.Arrays.copyOf(valid, valid.length + 1);
        assertRejected(trailing, "trailing");
    }

    @Test
    void countBlobAndParallelismBoundsFailBeforeAllocation() throws Exception {
        assertRejected(singleNode(PlanNode.Kind.SOURCE.ordinal(), 0, 4097, 0, 1), "field count");
        assertRejected(singleNode(PlanNode.Kind.SOURCE.ordinal(), 0, 0, 4097, 1), "input count");
        assertRejected(sourceWithClaimedBlob(16 * 1024 * 1024 + 1), "object blob");
        assertRejected(singleNode(PlanNode.Kind.SOURCE.ordinal(), 0, 0, 0, 0), "parallelism");
        assertRejected(singleNode(PlanNode.Kind.SOURCE.ordinal(), 0, 0, 0, 65_537), "parallelism");
    }

    @Test
    void encoderCannotProducePlanThatTheScannerRejects() {
        io.rowweave.DataSchema meta = new io.rowweave.DataSchema(
                new io.rowweave.Field("id", io.rowweave.DataType.IntegerType));
        PlanGraph tooParallel = new PlanGraph(Collections.singletonList(
                PlanNode.placeholderSource(meta)), 65_537);
        assertThrows(IllegalArgumentException.class, tooParallel::toBytes);

        String oversized = String.join("", Collections.nCopies(65_537, "x"));
        PlanGraph tooLong = new PlanGraph(Collections.singletonList(
                PlanNode.placeholderSource(meta).withParam("oversized", oversized)));
        assertThrows(IllegalArgumentException.class, tooLong::toBytes);
    }

    @Test
    void duplicateKeyScratchArraysAreReservedBeforeAllocation() throws Exception {
        byte[] plan = unaryPlan(true, 1, 0, true, false);
        AtomicLong charged = new AtomicLong();

        assertThrows(IllegalArgumentException.class,
                () -> PlanGraphEnvelopeScanner.scan(plan, charged::addAndGet));

        assertEquals(64L, charged.get());
        assertThrows(IllegalStateException.class,
                () -> PlanGraphEnvelopeScanner.scan(plan, bytes -> {
                    throw new IllegalStateException("budget denied before scratch allocation");
                }));
    }

    private static void assertRejected(byte[] bytes, String expected) {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> PlanGraphEnvelopeScanner.scan(bytes));
        assertTrue(failure.getMessage().contains(expected), failure.getMessage());
    }

    private static byte[] unaryPlan(boolean parallelism, int value, int child,
                                    boolean duplicateParam, boolean malformedUtf8) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(2);
            node(out, PlanNode.Kind.SOURCE.ordinal(), 0, new int[0], 0,
                    false, malformedUtf8);
            node(out, PlanNode.Kind.UNARY.ordinal(), 0, new int[]{child}, -1,
                    duplicateParam, false);
            if (parallelism) out.writeInt(value);
        }
        return bytes.toByteArray();
    }

    private static byte[] singleNode(int kind, int dataType, int fields,
                                     int trailer, int parallelism) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(1);
            out.writeInt(kind);
            utf(out, "source");
            out.writeInt(fields);
            for (int i = 0; i < fields; i++) {
                utf(out, "f" + i);
                utf(out, "");
                out.writeInt(dataType);
            }
            out.writeInt(kind == PlanNode.Kind.BINARY.ordinal() ? 1 : 0);
            if (kind == PlanNode.Kind.BINARY.ordinal()) out.writeInt(0);
            out.writeInt(0);
            out.writeInt(0);
            out.writeInt(trailer);
            out.writeInt(parallelism);
        }
        return bytes.toByteArray();
    }

    private static byte[] sourceWithClaimedBlob(int length) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(1);
            nodePrefix(out, PlanNode.Kind.SOURCE.ordinal(), new int[0], false);
            out.writeInt(0);
            out.writeInt(0);
            out.writeInt(1);
            out.writeInt(length);
        }
        return bytes.toByteArray();
    }

    private static void node(DataOutputStream out, int kind, int dataType, int[] children,
                             int trailer, boolean duplicateParam, boolean malformedUtf8)
            throws Exception {
        nodePrefix(out, kind, children, malformedUtf8);
        out.writeInt(duplicateParam ? 2 : 0);
        if (duplicateParam) {
            utf(out, "k"); utf(out, "v1");
            utf(out, "k"); utf(out, "v2");
        }
        out.writeInt(0);
        out.writeInt(trailer);
    }

    private static void nodePrefix(DataOutputStream out, int kind, int[] children,
                                   boolean malformedUtf8) throws Exception {
        out.writeInt(kind);
        if (malformedUtf8) {
            out.writeInt(1);
            out.writeByte(0xff);
        } else {
            utf(out, "operator");
        }
        out.writeInt(0);
        out.writeInt(children.length);
        for (int child : children) out.writeInt(child);
    }

    private static void utf(DataOutputStream out, String value) throws Exception {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(encoded.length);
        out.write(encoded);
    }
}
