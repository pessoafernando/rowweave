package io.rowweave.incremental;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.commit.SchemaFingerprint;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** per-split changelog identity is validated with O(1) state. */
class IncrementalChangelogRowValidatorContractTest {
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.LongType),
            new Field("op", DataType.StringType),
            new Field("sequence", DataType.LongType),
            new Field("payload", DataType.StringType));

    @Test
    void incrementalRowsRequireNonNullKeyClosedOperationAndStrictPosition()
            throws Exception {
        CapturedTargetReceipt receipt = normal(
                IncrementalRunMode.INCREMENTAL_V1, 10L, 20L);
        IncrementalChangelogRowValidator validator = validator(
                receipt, false);
        DataRow first = row(1L, "UPSERT", 11L, "a");
        DataRow second = row(1L, "UPSERT", 20L, "b");
        assertSame(first, validator.validate(first));
        assertSame(second, validator.validate(second));

        assertThrows(IllegalArgumentException.class,
                () -> validator(receipt, false).validate(
                        row(null, "UPSERT", 11L, "a")));
        assertThrows(IllegalArgumentException.class,
                () -> validator(receipt, false).validate(
                        row(1L, "RETRACT", 11L, "a")));
        assertThrows(IllegalArgumentException.class,
                () -> validator(receipt, false).validate(
                        row(1L, "DELETE", 11L, null)));
        assertThrows(IllegalArgumentException.class,
                () -> validator(receipt, false).validate(
                        row(1L, "UPSERT", 10L, "a")));
        assertThrows(IllegalArgumentException.class,
                () -> validator(receipt, false).validate(
                        row(1L, "UPSERT", 21L, "a")));
    }

    @Test
    void duplicateOrDescendingPositionIsRejectedPerSplit() throws Exception {
        CapturedTargetReceipt receipt = normal(
                IncrementalRunMode.INCREMENTAL_V1, 10L, 20L);
        IncrementalChangelogRowValidator duplicate = validator(receipt, false);
        duplicate.validate(row(1L, "UPSERT", 12L, "a"));
        assertThrows(IllegalArgumentException.class,
                () -> duplicate.validate(row(1L, "UPSERT", 12L, "b")));

        IncrementalChangelogRowValidator descending = validator(receipt, false);
        descending.validate(row(1L, "UPSERT", 13L, "a"));
        assertThrows(IllegalArgumentException.class,
                () -> descending.validate(row(2L, "UPSERT", 12L, "b")));
    }

    @Test
    void snapshotUsesStableRowOrdinalAndNeverAcceptsDelete() throws Exception {
        CapturedTargetReceipt receipt = normal(
                IncrementalRunMode.SNAPSHOT_V1, -1L, 20L);
        IncrementalChangelogRowValidator validator = validator(
                receipt, true);
        validator.validate(row(1L, "UPSERT", 0L, "a"));
        validator.validate(row(2L, "UPSERT", 1L, "b"));
        assertThrows(IllegalArgumentException.class,
                () -> validator(receipt, true).validate(
                        row(1L, "DELETE", 0L, null)));
    }

    @Test
    void incrementalDeleteNeedsFrozenTombstoneCapability() throws Exception {
        CapturedTargetReceipt receipt = normal(
                IncrementalRunMode.INCREMENTAL_V1, 10L, 20L);
        DataRow tombstone = row(1L, "DELETE", 11L, null);
        assertSame(tombstone, validator(receipt, true).validate(tombstone));
    }

    private static IncrementalChangelogRowValidator validator(
            CapturedTargetReceipt receipt, boolean deleteCapable)
            throws Exception {
        byte[] partitions = digest("partitions");
        IncrementalPlanDescriptor descriptor = new IncrementalPlanDescriptor(
                "source", digest("frontier-contract"),
                Collections.singletonList(0), 1,
                Collections.singletonList(2), schemaDigest(), deleteCapable,
                partitions,
                IncrementalPlanDescriptor.SinkCommitProfile.MANIFEST_ATOMIC_V1);
        return new IncrementalChangelogRowValidator(
                META, descriptor, receipt, partitions, 0);
    }

    private static CapturedTargetReceipt normal(
            IncrementalRunMode mode, long from, long target) throws Exception {
        IncrementalFrontier fromFrontier = mode == IncrementalRunMode.SNAPSHOT_V1
                ? IncrementalFrontier.begin()
                : IncrementalFrontier.int64Sequence(from);
        return CapturedTargetReceipt.normal(
                1L, digest("definition"), digest("source-contract"),
                0L, fromFrontier, "trigger-" + mode.name(), mode,
                IncrementalFrontier.int64Sequence(target), bytes("snapshot"));
    }

    private static DataRow row(Long id, String op, Long sequence, String payload) {
        DataRow row = new DataRow(4);
        row.set(0, id);
        row.set(1, op);
        row.set(2, sequence);
        row.set(3, payload);
        return row;
    }

    private static byte[] schemaDigest() {
        String fingerprint = SchemaFingerprint.of(META);
        byte[] bytes = new byte[fingerprint.length() / 2];
        for (int index = 0; index < bytes.length; index++) {
            bytes[index] = (byte) Integer.parseInt(
                    fingerprint.substring(index * 2, index * 2 + 2), 16);
        }
        return bytes;
    }

    private static byte[] digest(String value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(bytes(value));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
