package io.rowweave.incremental;

import java.util.Arrays;

/** Strict total order for records from one exact interval split. */
public final class IncrementalRecordPositionOrder {
    private IncrementalRecordPositionOrder() {
    }

    public static int compareWithinSplit(
            IncrementalRecordPosition left, IncrementalRecordPosition right) {
        if (left == null || right == null) throw new IllegalArgumentException("positions are required");
        if (left.kind() != right.kind()
                || left.partitionOrdinal() != right.partitionOrdinal()
                || !Arrays.equals(left.intervalDigest(), right.intervalDigest())) {
            throw new IllegalArgumentException("record positions do not belong to one split");
        }
        int compared;
        switch (left.kind()) {
            case SEQUENCE_V1:
                compared = Long.compare(left.sequence(), right.sequence());
                break;
            case TIMESTAMP_KEY_V1:
                compared = Long.compare(left.epochMicros(), right.epochMicros());
                if (compared == 0) {
                    compared = OrderedFrontierKey.compare(
                            left.orderedKey(), right.orderedKey());
                }
                break;
            case MANIFEST_ENTRY_V1:
                compared = Long.compare(left.generation(), right.generation());
                if (compared == 0) {
                    compared = Long.compare(left.entryOrdinal(), right.entryOrdinal());
                }
                break;
            case SNAPSHOT_ROW_V1:
                compared = Long.compare(left.rowOrdinal(), right.rowOrdinal());
                break;
            default:
                throw new IllegalArgumentException("unsupported record position kind");
        }
        return compared != 0 ? compared : unsignedCompare(left.keyDigest(), right.keyDigest());
    }

    private static int unsignedCompare(byte[] left, byte[] right) {
        for (int i = 0; i < left.length; i++) {
            int compared = Integer.compare(left[i] & 0xff, right[i] & 0xff);
            if (compared != 0) return compared;
        }
        return 0;
    }
}
