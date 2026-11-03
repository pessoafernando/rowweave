package io.rowweave;

import io.rowweave.run.PreparedSinkLocator;

/** Worker-local, bounded write handle; rows enter synchronously through push. */
public interface DurableBatchSinkWriteAttempt extends AutoCloseable {

    void write(DataRow row);

    PreparedSinkLocator prepare();

    /** Resource release only; it must not publish or abort external state. */
    @Override
    void close();
}
