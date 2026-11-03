package io.rowweave.cluster;

import io.rowweave.run.RunSnapshot;

/** Structured failure of a durable submit; UNKNOWN explicitly forbids resubmitting under a new identity. */
public final class DurableRunSubmitException extends RuntimeException {
    public enum Category { REJECTED, TERMINAL_FAILED, UNKNOWN }

    private final String runId;
    private final Category category;
    private final RunSnapshot lastSnapshot;

    public DurableRunSubmitException(String runId, Category category,
                                     RunSnapshot lastSnapshot, String message,
                                     Throwable cause) {
        super(message, cause);
        if (runId == null || runId.isEmpty()) throw new IllegalArgumentException("runId is required");
        if (category == null) throw new IllegalArgumentException("category is required");
        this.runId = runId;
        this.category = category;
        this.lastSnapshot = lastSnapshot;
    }

    public String runId() { return runId; }
    public Category category() { return category; }
    public RunSnapshot lastSnapshot() { return lastSnapshot; }
}
