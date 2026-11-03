package io.rowweave.ops.join;

import io.rowweave.mem.MemoryManager;

/** Internal AUTO replay capability used only at the root-MM to child-MM handoff barrier. */
interface MemoryRebindableRowStream {

    void rebindMemory(MemoryManager memoryManager);
}
