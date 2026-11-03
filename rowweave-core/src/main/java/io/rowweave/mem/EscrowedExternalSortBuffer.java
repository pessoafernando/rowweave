package io.rowweave.mem;

import io.rowweave.DataRow;

/** Hands managed-memory ownership from TwoTier to a synchronous or pipelined external sort without re-reserving it. */
public interface EscrowedExternalSortBuffer extends SortBuffer {

    void adoptAuxEscrow(long comparatorCredit, long headCredit);

    void importEscrowedRow(DataRow row, int serializedSize, long rowEstimate, long scratch,
                           long credit, boolean batchStart, boolean batchEnd);
}
