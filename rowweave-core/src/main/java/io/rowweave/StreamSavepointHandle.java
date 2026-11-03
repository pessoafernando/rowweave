package io.rowweave;

/** Typed stop-with-savepoint command handle. */
public interface StreamSavepointHandle extends StreamOperationHandle {
    String savepointId();
}
