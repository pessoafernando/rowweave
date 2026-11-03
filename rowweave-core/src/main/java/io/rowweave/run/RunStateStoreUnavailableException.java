package io.rowweave.run;

/** The authority backend cannot confirm the outcome of a read or write; the caller retries with the same commandId. */
public final class RunStateStoreUnavailableException extends RuntimeException {
    private final String commandId;

    public RunStateStoreUnavailableException(String commandId, String message, Throwable cause) {
        super(message, cause);
        this.commandId = commandId;
    }

    public String commandId() { return commandId; }
}
