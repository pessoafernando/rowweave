package io.rowweave.plan;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** bounded canonical job telemetry summary. */
class ExecutionTelemetrySummaryCodecTest {

    @Test
    void roundTripPreservesPartialResourceFactsAndPerStageDistributions() {
        List<PartitionDistribution> distributions = Arrays.asList(
                new PartitionDistribution(3, PartitionRole.KEYED_OP, 3,
                        TelemetryCoverage.COMPLETE,
                        new boolean[]{true, true, true}, new long[]{0, 7, 9}),
                new PartitionDistribution(8, PartitionRole.JOIN_CONSUMER, 2,
                        TelemetryCoverage.PARTIAL,
                        new boolean[]{true, false}, new long[]{11, 0}));
        ExecutionTelemetrySummary summary = new ExecutionTelemetrySummary(
                TelemetryCoverage.PARTIAL, 101, 202, 3, 4, 505, 606,
                TelemetryCoverage.PARTIAL, distributions);

        byte[] encoded = ExecutionTelemetrySummaryCodec.encode(summary);
        ExecutionTelemetrySummary decoded = ExecutionTelemetrySummaryCodec.decode(encoded);

        assertEquals(summary, decoded);
        assertArrayEquals(encoded, ExecutionTelemetrySummaryCodec.encode(decoded));
        ExecutionReport report = decoded.toExecutionReport(17L);
        assertEquals(17L, report.rowsOut());
        assertSame(decoded.partitionDistributions(), report.partitionDistributions(),
                "decoded summary conversion must retain its already-accounted immutable list");
        assertEquals(TelemetryCoverage.PARTIAL, report.resourceTelemetryCoverage());
        assertEquals(TelemetryCoverage.PARTIAL, report.partitionTelemetryCoverage());
        assertEquals(distributions, report.partitionDistributions());
    }

    @Test
    void singleCompleteDistributionReusesAccountedLegacyPartitionArray() {
        PartitionDistribution distribution = new PartitionDistribution(
                3, PartitionRole.KEYED_OP, 4, TelemetryCoverage.COMPLETE,
                new boolean[]{true, true, true, true}, new long[]{1, 2, 3, 4});
        ExecutionTelemetrySummary decoded = ExecutionTelemetrySummaryCodec.decode(
                ExecutionTelemetrySummaryCodec.encode(new ExecutionTelemetrySummary(
                        TelemetryCoverage.COMPLETE, 10, 20, 0, 0, 0, 0,
                        TelemetryCoverage.COMPLETE,
                        Collections.singletonList(distribution))));

        ExecutionReport report = decoded.toExecutionReport(10L);

        assertSame(decoded.partitionDistributions(), report.partitionDistributions());
        assertSame(decoded.partitionDistributions().get(0).borrowedRowCounts(),
                report.borrowedPartitionRowCounts(),
                "decoded summary conversion must not clone the accounted partition array");
        assertNotSame(report.borrowedPartitionRowCounts(), report.partitionRowCounts(),
                "the public API must remain copy-out immutable");
    }

    @Test
    void maximumCanonicalShapeStaysBelowIndependentFortyEightKibLimit() {
        List<PartitionDistribution> distributions = new ArrayList<>();
        for (int stage = 0; stage < ExecutionTelemetrySummary.MAX_DISTRIBUTIONS; stage++) {
            boolean[] present = new boolean[8];
            Arrays.fill(present, true);
            distributions.add(new PartitionDistribution(stage, PartitionRole.KEYED_OP, 8,
                    TelemetryCoverage.COMPLETE, present, new long[8]));
        }
        ExecutionTelemetrySummary summary = new ExecutionTelemetrySummary(
                TelemetryCoverage.COMPLETE, 1, 2, 3, 4, 5, 6,
                TelemetryCoverage.COMPLETE, distributions);

        byte[] encoded = ExecutionTelemetrySummaryCodec.encode(summary);

        AtomicLong resident = new AtomicLong();
        ExecutionTelemetrySummary decoded = ExecutionTelemetrySummaryCodec.decode(
                encoded, resident::addAndGet);

        int expected = 12 + 2 + 6 * Long.BYTES + Integer.BYTES
                + ExecutionTelemetrySummary.MAX_DISTRIBUTIONS * 14
                + ExecutionTelemetrySummary.MAX_DISTRIBUTIONS
                + ExecutionTelemetrySummary.MAX_PARTITION_SLOTS * Long.BYTES + 32;
        assertEquals(expected, encoded.length, "codec layout must match the frozen fixed-width arithmetic");
        assertTrue(encoded.length < ExecutionTelemetrySummaryCodec.MAX_FRAME_BYTES);
        assertEquals(summary, decoded);
        assertTrue(resident.get() > encoded.length,
                "decoded arrays/objects need an explicit resident charge beyond wire bytes");
        assertTrue(resident.get()
                        <= ExecutionTelemetrySummaryCodec.MAX_MATERIALIZED_RESIDENT_BYTES,
                "maximum legal summary must stay inside its independent resident bound");
    }

    @Test
    void codecHasAStablePackagePrivateBorrowWithoutWeakeningPublicImmutability() {
        PartitionDistribution distribution = new PartitionDistribution(
                1, PartitionRole.KEYED_OP, 2, TelemetryCoverage.COMPLETE,
                new boolean[]{true, true}, new long[]{3, 5});

        assertSame(distribution.borrowedPresenceBitmap(),
                distribution.borrowedPresenceBitmap());
        assertSame(distribution.borrowedRowCounts(), distribution.borrowedRowCounts());
        assertNotSame(distribution.borrowedPresenceBitmap(),
                distribution.presenceBitmap());
        assertNotSame(distribution.borrowedRowCounts(), distribution.rowCounts());
    }

    @Test
    void constructorAndCodecRejectAmbiguousOrUnboundedSummaries() {
        PartitionDistribution distribution = new PartitionDistribution(
                1, PartitionRole.KEYED_OP, 2, TelemetryCoverage.COMPLETE,
                new boolean[]{true, true}, new long[]{1, 2});
        assertThrows(IllegalArgumentException.class, () -> new ExecutionTelemetrySummary(
                TelemetryCoverage.UNAVAILABLE, 1, 0, 0, 0, 0, 0,
                TelemetryCoverage.COMPLETE, Collections.singletonList(distribution)));
        assertThrows(IllegalArgumentException.class, () -> new ExecutionTelemetrySummary(
                TelemetryCoverage.COMPLETE, 0, 0, 0, 0, 0, 0,
                TelemetryCoverage.NOT_APPLICABLE, Collections.singletonList(distribution)));
        assertThrows(IllegalArgumentException.class, () -> new ExecutionTelemetrySummary(
                TelemetryCoverage.COMPLETE, 0, 0, 0, 0, 0, 0,
                TelemetryCoverage.COMPLETE, Arrays.asList(distribution, distribution)));

        ExecutionTelemetrySummary valid = new ExecutionTelemetrySummary(
                TelemetryCoverage.COMPLETE, 1, 2, 3, 4, 5, 6,
                TelemetryCoverage.NOT_APPLICABLE, Collections.emptyList());
        byte[] corrupt = ExecutionTelemetrySummaryCodec.encode(valid);
        corrupt[20] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> ExecutionTelemetrySummaryCodec.decode(corrupt));
        assertThrows(IllegalArgumentException.class,
                () -> ExecutionTelemetrySummaryCodec.decode(
                        Arrays.copyOf(corrupt, corrupt.length + 1)));
    }

    @Test
    void unavailableSummaryPreservesRowsAndNeverPretendsZeroWasMeasured() {
        ExecutionReport report = ExecutionTelemetrySummary.unavailable()
                .toExecutionReport(29L);

        assertEquals(29L, report.rowsOut());
        assertEquals(TelemetryCoverage.UNAVAILABLE,
                report.resourceTelemetryCoverage());
        assertEquals(TelemetryCoverage.UNAVAILABLE,
                report.partitionTelemetryCoverage());
    }

    @Test
    void validChecksumCannotAuthorizeIllegalEnumsCountsDuplicatesOrBitmapPadding()
            throws Exception {
        PartitionDistribution first = new PartitionDistribution(
                4, PartitionRole.KEYED_OP, 2, TelemetryCoverage.COMPLETE,
                new boolean[]{true, true}, new long[]{3, 5});
        PartitionDistribution second = new PartitionDistribution(
                9, PartitionRole.JOIN_CONSUMER, 2, TelemetryCoverage.COMPLETE,
                new boolean[]{true, true}, new long[]{7, 11});
        ExecutionTelemetrySummary summary = new ExecutionTelemetrySummary(
                TelemetryCoverage.COMPLETE, 1, 2, 3, 4, 5, 6,
                TelemetryCoverage.COMPLETE, Arrays.asList(first, second));

        assertInvalid(summary, bytes -> bytes[12] = 4,
                "resource coverage cannot be NOT_APPLICABLE");
        assertInvalid(summary, bytes -> bytes[13] = 99,
                "unknown partition coverage must be rejected");
        assertInvalid(summary, bytes -> ByteBuffer.wrap(bytes).putInt(62, -1),
                "negative distribution count must be rejected");
        assertInvalid(summary, bytes -> bytes[70] = 99,
                "unknown partition role must be rejected");
        assertInvalid(summary, bytes -> bytes[71] = 4,
                "a distribution cannot be NOT_APPLICABLE");
        assertInvalid(summary, bytes -> bytes[80] |= 4,
                "unused bitmap bits must stay zero");

        // One parallelism=2 distribution occupies 14 header + 1 bitmap + 16 rows.
        int secondDistribution = 66 + 14 + 1 + 2 * Long.BYTES;
        assertInvalid(summary,
                bytes -> ByteBuffer.wrap(bytes).putInt(secondDistribution, 4),
                "logical stage identity must be unique");
        assertInvalid(summary,
                bytes -> ByteBuffer.wrap(bytes).putLong(81, -1L),
                "row counts must remain non-negative");
    }

    @Test
    void exactLengthRejectsRechecksummedTrailingBodyBytes() throws Exception {
        byte[] canonical = ExecutionTelemetrySummaryCodec.encode(
                new ExecutionTelemetrySummary(
                        TelemetryCoverage.COMPLETE, 0, 0, 0, 0, 0, 0,
                        TelemetryCoverage.NOT_APPLICABLE, Collections.emptyList()));
        int checksumOffset = canonical.length - 32;
        byte[] extended = new byte[canonical.length + 1];
        System.arraycopy(canonical, 0, extended, 0, checksumOffset);
        extended[checksumOffset] = 1;
        ByteBuffer.wrap(extended).putInt(8, extended.length);
        rewriteChecksum(extended);

        assertThrows(IllegalArgumentException.class,
                () -> ExecutionTelemetrySummaryCodec.decode(extended));
    }

    private static void assertInvalid(
            ExecutionTelemetrySummary summary, Mutation mutation, String message)
            throws Exception {
        byte[] bytes = ExecutionTelemetrySummaryCodec.encode(summary);
        mutation.apply(bytes);
        rewriteChecksum(bytes);
        assertThrows(IllegalArgumentException.class,
                () -> ExecutionTelemetrySummaryCodec.decode(bytes), message);
    }

    private static void rewriteChecksum(byte[] bytes) throws Exception {
        int bodyLength = bytes.length - 32;
        byte[] digest = MessageDigest.getInstance("SHA-256")
                .digest(Arrays.copyOf(bytes, bodyLength));
        System.arraycopy(digest, 0, bytes, bodyLength, digest.length);
    }

    private interface Mutation {
        void apply(byte[] bytes);
    }
}
