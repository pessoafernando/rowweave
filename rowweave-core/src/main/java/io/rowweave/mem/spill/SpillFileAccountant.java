package io.rowweave.mem.spill;

import java.io.IOException;

/** Per-job view over the process spill reservation ledger. */
interface SpillFileAccountant {
    default void runOpened() { }
    default void runClosed() { }
    void reserve(long bytes);
    void release(long bytes);
    default void written(long logicalBytes) { }
    RuntimeException writeFailure(IOException cause);
}
