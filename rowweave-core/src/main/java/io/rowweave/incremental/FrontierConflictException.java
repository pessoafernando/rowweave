package io.rowweave.incremental;

/** A canonical frontier identity is internally inconsistent and must freeze the feed. */
public final class FrontierConflictException extends IllegalArgumentException {
    public FrontierConflictException(String message) {
        super(message);
    }
}
