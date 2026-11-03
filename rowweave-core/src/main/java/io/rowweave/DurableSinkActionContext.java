package io.rowweave;

import io.rowweave.run.SinkAttemptIdentity;

/** Engine-owned fence, deadline and resource view supplied to every provider action. */
public interface DurableSinkActionContext {

    String actionId();

    DurableSinkProviderKey providerKey();

    SinkAttemptIdentity attemptIdentity();

    String publicationId();

    long publicationGeneration();

    /** Empty before a durable decision; otherwise the exact authority SHA-256 digest. */
    byte[] authorityDecisionDigest();

    /** Absolute monotonic {@link System#nanoTime()} deadline. */
    long deadlineNanos();

    boolean isCancellationRequested();

    long maxResidentBytes();

    int maxOpenHandles();

    int maxRequestBytes();

    int maxResponseBytes();

    DurableSinkResourceLease acquireResidentLease(long bytes);

    DurableSinkResourceLease acquireHandleLease();
}
