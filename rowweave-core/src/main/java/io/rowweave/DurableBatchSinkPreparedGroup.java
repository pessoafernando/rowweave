package io.rowweave;

import io.rowweave.commit.BatchSinkOutcome;

/** Rebuildable Worker-side handle for one bounded prepared publication group. */
public interface DurableBatchSinkPreparedGroup extends AutoCloseable {

    void publish();

    void abort();

    void finalizeAttempt();

    BatchSinkOutcome queryOutcome();

    /** Resource release only; it must not alter publication state. */
    @Override
    void close();
}
