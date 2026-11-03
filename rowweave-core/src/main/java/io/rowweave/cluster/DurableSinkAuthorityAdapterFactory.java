package io.rowweave.cluster;

import io.rowweave.ProviderBatchSink;
import io.rowweave.commit.BatchSinkOutcome;
import io.rowweave.run.PreparedSinkLocator;
import io.rowweave.run.RunAttemptFence;

import java.util.List;

/** Test-injectable authority seam for a signed conformance provider. */
interface DurableSinkAuthorityAdapterFactory {
    Reservation reserve(
            RunAttemptFence fence, String publicationId,
            List<ProviderBatchSink> outputs);

    BatchSinkOutcome query(RunAttemptFence fence, String publicationId);

    interface Reservation extends AutoCloseable {
        long generation();
        DurableSinkAuthorityAdapter bind(List<PreparedSinkLocator> locators);
        @Override void close();
    }
}
