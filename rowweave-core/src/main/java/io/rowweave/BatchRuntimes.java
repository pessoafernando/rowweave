package io.rowweave;

import io.rowweave.cluster.JobClient;

/** Access to reusable Batch execution runtimes. */
public final class BatchRuntimes {

    private BatchRuntimes() {
    }

    /**
     * Returns the process-wide, lazily started in-process Master/Worker runtime.
 *
     * <p>The returned client does not transfer ownership of the shared runtime;
     * callers submit jobs but must not attempt to close process-wide role resources.
     */
    public static JobClient processWideInProcess() {
        return InProcessBatchRuntime.processWide();
    }
}
