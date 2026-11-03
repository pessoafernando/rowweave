package io.rowweave;

/** Typed checkpoint command handle. */
public interface StreamCheckpointHandle extends StreamOperationHandle {
    String checkpointId();
}
