package io.rowweave.mem;

import io.rowweave.DataSchema;
import io.rowweave.DataRow;

/**
 * Public credit calculator needed to transfer external-sort ownership. A fast path holding a DataRow can reserve, from the
 * first row, at the worst-case external-sort layout, then pass that same amount through {@link EscrowedExternalSortBuffer} on downgrade, which avoids a release and re-reserve deadlock inside one MemoryManager.
 */
public final class ExternalSortEscrow {

    private final RowSerializer serializer;

    public ExternalSortEscrow(DataSchema dataSchema) {
        this.serializer = new RowSerializer(dataSchema);
    }

    public Credit credit(DataRow row) {
        int serializedSize = serializer.serializedSize(row);
        long rowEstimate = RowSizeEstimator.estimateRow(row);
        long scratch = serializer.serializerScratchCredit(row);
        return new Credit(serializedSize, rowEstimate, scratch,
                ExternalSortMemory.rowCredit(serializedSize, rowEstimate, scratch),
                Math.multiplyExact(2L, rowEstimate),
                ExternalSortMemory.finalHeadCredit(serializedSize, rowEstimate));
    }

    public static long batchFixedCredit() {
        return ExternalSortMemory.BATCH_FIXED_CREDIT;
    }

    /** External-sort layout credit for one row; the value can be passed to EscrowedExternalSortBuffer directly. */
    public static final class Credit {
        private final int serializedSize;
        private final long rowEstimate;
        private final long scratch;
        private final long rowCredit;
        private final long comparatorTarget;
        private final long finalHeadTarget;

        private Credit(int serializedSize, long rowEstimate, long scratch, long rowCredit,
                       long comparatorTarget, long finalHeadTarget) {
            this.serializedSize = serializedSize;
            this.rowEstimate = rowEstimate;
            this.scratch = scratch;
            this.rowCredit = rowCredit;
            this.comparatorTarget = comparatorTarget;
            this.finalHeadTarget = finalHeadTarget;
        }

        public int serializedSize() { return serializedSize; }
        public long rowEstimate() { return rowEstimate; }
        public long scratch() { return scratch; }
        public long rowCredit() { return rowCredit; }
        public long comparatorTarget() { return comparatorTarget; }
        public long finalHeadTarget() { return finalHeadTarget; }
    }
}
