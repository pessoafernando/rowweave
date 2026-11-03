package io.rowweave;

import io.rowweave.run.PreparedSinkLocator;
import io.rowweave.run.SinkAttemptIdentity;

import java.util.List;

/** Versioned Worker-side runtime capability for a durable bounded-batch Sink. */
public interface DurableBatchSinkProvider {

    DurableBatchSinkProviderContract contract();

    /**
     * Worker-only semantic decode/re-encode boundary. The returned bytes must
     * be byte-exact canonical configuration for the same frozen proxy.
     */
    byte[] canonicalizeConfiguration(ProviderBatchSink output);

    /** Worker-only locator payload decode/re-encode boundary. */
    byte[] canonicalizeLocator(PreparedSinkLocator locator);

    DurableBatchSinkWriteAttempt openWriteAttempt(
            ProviderBatchSink output, SinkAttemptIdentity attemptIdentity,
            DurableSinkActionContext context);

    DurableBatchSinkPreparedGroup openPreparedGroup(
            List<ProviderBatchSink> outputs,
            List<PreparedSinkLocator> preparedLocators,
            DurableSinkActionContext context);
}
