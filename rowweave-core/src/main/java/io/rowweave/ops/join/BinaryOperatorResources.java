package io.rowweave.ops.join;

import io.rowweave.mem.SortBuffer;
import io.rowweave.mem.spill.SpillStore;

/** all-close with first-cause retention, for the single owner of a binary breaker's resources. */
final class BinaryOperatorResources {

    private BinaryOperatorResources() {
    }

    static void close(Throwable primary, SortBuffer left, SortBuffer right, SpillStore store) {
        io.rowweave.mem.ResourceCloser.close(primary, left, right, store);
    }
}
