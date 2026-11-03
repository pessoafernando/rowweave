package io.rowweave.stream.cluster;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** A legal task graph must be rejected before starting if its proof cannot fit. */
class StreamCheckpointTransferBudgetTest {
    @Test
    void wholeGraphReceiptsAndPerTaskMembersHaveIndependentWireBudgets() {
        assertDoesNotThrow(() -> StreamCheckpointTransferBudget
                .validateFrameSizes(55_000L, 2));
        assertDoesNotThrow(() -> StreamCheckpointTransferBudget
                .validateFrameSizes(55_000L, 256));
        assertThrows(IllegalArgumentException.class,
                () -> StreamCheckpointTransferBudget.validateFrameSizes(
                        16L * 1024L * 1024L + 1L, 1));
        assertThrows(IllegalArgumentException.class,
                () -> StreamCheckpointTransferBudget.validateFrameSizes(
                        750_000L, 32));
    }
}
