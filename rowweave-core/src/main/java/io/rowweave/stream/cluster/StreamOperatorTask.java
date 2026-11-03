package io.rowweave.stream.cluster;

import io.rowweave.DataRow;
import io.rowweave.stream.contract.CheckpointDecision;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamSourceSplit;
import io.rowweave.stream.runtime.FileCheckpointPayloadStore;
import io.rowweave.stream.runtime.StreamCheckpointAuthority;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** One admitted portable operator instance in the Worker-local push graph. */
interface StreamOperatorTask {
    String operatorUid();

    default void process(DataRow input, Long eventTimestamp, Sink output) {
        throw new UnsupportedOperationException(
                "mailbox-owned operator does not accept inline callbacks");
    }

    default void start() { start(System.nanoTime()); }

    default void cancel() { close(); }

    default void awaitCompletion(long timeoutMillis)
            throws InterruptedException { }

    default Throwable failure() { return null; }

    default boolean completed() { return false; }

    default StreamTaskResourceUsage resourceUsage() {
        return StreamTaskResourceUsage.unavailable();
    }

    default CheckpointIdentity preparedCheckpoint() { return null; }

    default boolean terminalInputSeen() { return false; }

    default void reconcile(CheckpointDecision decision,
            StreamCheckpointAuthority authority) {
        throw new UnsupportedOperationException(
                "operator has no checkpoint publication lifecycle");
    }

    default CompletableFuture<List<StreamCheckpointMember>> checkpoint(
            CheckpointIdentity checkpoint) {
        return CompletableFuture.completedFuture(Collections.emptyList());
    }

    default CompletableFuture<Void> checkpointCompleted(
            CheckpointIdentity checkpoint) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        result.completeExceptionally(new UnsupportedOperationException(
                "operator has no checkpoint-complete lifecycle"));
        return result;
    }

    default void assignSourceSplit(StreamSourceSplit split) {
        throw new UnsupportedOperationException(
                "operator task does not accept source split assignments");
    }

    default CompletableFuture<Void> revokeSourceSplit(String splitId) {
        throw new UnsupportedOperationException(
                "operator task does not revoke source splits");
    }

    default StreamSourceSplit completedSourceSplit() { return null; }

    default boolean hasIdleTimer() { return false; }

    default void start(long nowNanos) { }

    default long idleDelayMillis(long nowNanos) { return Long.MAX_VALUE; }

    default void onIdleTimer(long nowNanos, Sink output) { }

    default boolean onWatermark(long timestamp, Sink output) {
        return true;
    }

    default boolean onWatermarkStatus(
            io.rowweave.stream.contract.StreamEnvelope.WatermarkStatus status,
            long generation, Sink output) {
        return true;
    }

    default boolean onEnd(Sink output) { return true; }

    default boolean hasCheckpointState() { return false; }

    default StreamCheckpointMember snapshot(CheckpointIdentity checkpoint,
            FileCheckpointPayloadStore payloadStore) { return null; }

    default void restore(StreamCheckpointMember member,
            FileCheckpointPayloadStore payloadStore) {
        if (member != null) {
            throw new UnsupportedOperationException(
                    "operator has no checkpoint state");
        }
    }

    default void close() { }

    @FunctionalInterface
    interface Sink {
        void emit(DataRow row, Long eventTimestamp);

        default void emitTagged(String outputTagId, DataRow row,
                Long eventTimestamp) {
            throw new UnsupportedOperationException(
                    "operator does not emit tagged records");
        }

        default void emitWatermark(long timestamp) {
            throw new UnsupportedOperationException(
                    "operator does not emit watermarks");
        }

        default void emitWatermarkStatus(
                io.rowweave.stream.contract.StreamEnvelope.WatermarkStatus status,
                long generation) {
            throw new UnsupportedOperationException(
                    "operator does not emit watermark status");
        }

        default void fail(Throwable cause) {
            throw new UnsupportedOperationException(
                    "operator output does not expose attempt failure", cause);
        }

        default void emitEnd() {
            throw new UnsupportedOperationException(
                    "operator output does not expose end-of-input");
        }
    }
}
