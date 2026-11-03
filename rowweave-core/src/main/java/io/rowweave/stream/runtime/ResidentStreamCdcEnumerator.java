package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.StreamCdcEnumeratorState;
import io.rowweave.stream.contract.StreamCdcEnumeratorStateCodec;
import io.rowweave.stream.contract.StreamEdge;
import io.rowweave.stream.contract.StreamUpsertRecord;
import io.rowweave.stream.contract.StreamUpsertRecordCodec;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Versioned, single-owner resident CDC snapshot-to-log state machine.
 *
 * <p>The snapshot is a consistent image at {@code snapshotCutSequence}; the
 * first log record must therefore be exactly cut+1. Progress is checkpointed
 * only at completed split boundaries or exact log positions and never through
 * the bounded CDC frontier authority.
 */
public final class ResidentStreamCdcEnumerator {
    @FunctionalInterface
    public interface Sink {
        void emit(byte[] canonicalUpsertFrame) throws Exception;
    }

    private final int maximumFrameBytes;
    private final Sink output;
    private final Thread ownerThread;

    private StreamCdcEnumeratorState state;
    private String activeSplit;
    private long activeSplitRecords;
    private MessageDigest activeSplitDigest;
    private boolean logTransactionOpen;

    public ResidentStreamCdcEnumerator(
            StreamCdcEnumeratorState restoredState,
            int maximumFrameBytes, Sink output) {
        if (restoredState == null || maximumFrameBytes < 1
                || maximumFrameBytes > StreamEdge.MAX_FRAME_BYTES
                || output == null) {
            throw failure(StreamCdcException.Code.INVALID_CONFIGURATION,
                    "resident CDC state, frame bound and output are required");
        }
        this.state = restoredState;
        this.maximumFrameBytes = maximumFrameBytes;
        this.output = output;
        this.ownerThread = Thread.currentThread();
    }

    public void onSnapshotRecord(
            String splitId, StreamUpsertRecord afterImage) {
        checkOwner();
        requirePhase(StreamCdcEnumeratorState.Phase.SNAPSHOT);
        String expected = nextSplit();
        if (splitId == null || !expected.equals(splitId)
                || (activeSplit != null && !activeSplit.equals(splitId))) {
            throw failure(StreamCdcException.Code.SPLIT_ORDER,
                    "resident CDC snapshot splits must run in canonical order");
        }
        if (afterImage == null || !afterImage.hasAfterImage()) {
            throw failure(StreamCdcException.Code.INVALID_SNAPSHOT_RECORD,
                    "resident CDC snapshot accepts only after-images");
        }
        byte[] frame = StreamUpsertRecordCodec.encode(afterImage);
        requireFrame(frame);
        emit(frame);
        if (activeSplit == null) {
            activeSplit = splitId;
            activeSplitDigest = sha256Digest();
        }
        updateLengthAndBytes(activeSplitDigest, frame);
        if (activeSplitRecords == Long.MAX_VALUE) {
            throw failure(StreamCdcException.Code.FRAME_LIMIT,
                    "resident CDC snapshot split record count overflow");
        }
        activeSplitRecords++;
    }

    public StreamCdcEnumeratorState.CompletedSplit completeSnapshotSplit(
            String splitId) {
        checkOwner();
        requirePhase(StreamCdcEnumeratorState.Phase.SNAPSHOT);
        String expected = nextSplit();
        if (splitId == null || !expected.equals(splitId)
                || (activeSplit != null && !activeSplit.equals(splitId))) {
            throw failure(StreamCdcException.Code.SPLIT_ORDER,
                    "resident CDC snapshot split completion is out of order");
        }
        if (activeSplitDigest == null) activeSplitDigest = sha256Digest();
        StreamCdcEnumeratorState.CompletedSplit completed =
                new StreamCdcEnumeratorState.CompletedSplit(
                        splitId, activeSplitRecords,
                        hex(activeSplitDigest.digest()));
        state = state.withCompletedSplit(completed);
        activeSplit = null;
        activeSplitRecords = 0L;
        activeSplitDigest = null;
        return completed;
    }

    public StreamCdcEnumeratorState completeHandoff() {
        checkOwner();
        requirePhase(StreamCdcEnumeratorState.Phase.SNAPSHOT);
        if (activeSplit != null
                || state.completedSplits().size()
                != state.expectedSplitIds().size()) {
            throw failure(StreamCdcException.Code.SNAPSHOT_INCOMPLETE,
                    "resident CDC handoff requires every snapshot split receipt");
        }
        String handoffDigest = hex(sha256Digest().digest(
                StreamCdcEnumeratorStateCodec.encode(state)));
        state = state.toLog(handoffDigest);
        return state;
    }

    public StreamCdcEnumeratorState completeSnapshotAndHandoff(
            String splitId, byte[] externalPositionFrame) {
        checkOwner();
        if (state.version() != StreamCdcEnumeratorState.CURRENT_VERSION) {
            throw failure(StreamCdcException.Code.INVALID_CONFIGURATION,
                    "resident CDC v2 handoff requires a v2 state");
        }
        if (externalPositionFrame == null
                || externalPositionFrame.length < 1
                || externalPositionFrame.length
                > StreamCdcEnumeratorState.MAX_EXTERNAL_POSITION_BYTES) {
            throw failure(StreamCdcException.Code.FRAME_LIMIT,
                    "resident CDC external position exceeds its byte bound");
        }
        int completed = state.completedSplits().size();
        if (completed + 1 != state.expectedSplitIds().size()
                || !state.expectedSplitIds().get(completed).equals(splitId)) {
            throw failure(StreamCdcException.Code.SNAPSHOT_INCOMPLETE,
                    "resident CDC atomic handoff requires the final snapshot split");
        }
        completeSnapshotSplit(splitId);
        String handoffDigest = hex(sha256Digest().digest(
                StreamCdcEnumeratorStateCodec.encode(state)));
        state = state.toLog(handoffDigest, externalPositionFrame);
        return state;
    }

    public void onLogRecord(long sequence, StreamUpsertRecord record) {
        checkOwner();
        requirePhase(StreamCdcEnumeratorState.Phase.LOG);
        requireNextLogSequence(sequence);
        if (state.version() != StreamCdcEnumeratorState.LEGACY_VERSION) {
            throw failure(StreamCdcException.Code.INVALID_CONFIGURATION,
                    "resident CDC v2 log records require external boundaries");
        }
        acceptLogRecord(sequence, record,
                state.withLastLogSequence(sequence), false);
    }

    public void onLogRecord(long sequence, StreamUpsertRecord record,
            byte[] completedExternalPositionFrame) {
        checkOwner();
        requirePhase(StreamCdcEnumeratorState.Phase.LOG);
        requireNextLogSequence(sequence);
        if (state.version() != StreamCdcEnumeratorState.CURRENT_VERSION) {
            throw failure(StreamCdcException.Code.INVALID_CONFIGURATION,
                    "resident CDC v1 log records cannot carry external boundaries");
        }
        StreamCdcEnumeratorState next = completedExternalPositionFrame == null
                ? state.withLastLogSequence(sequence)
                : state.withLastLogBoundary(
                        sequence, completedExternalPositionFrame);
        acceptLogRecord(sequence, record, next,
                completedExternalPositionFrame == null);
    }

    public void onLogPosition(byte[] completedExternalPositionFrame) {
        checkOwner();
        requirePhase(StreamCdcEnumeratorState.Phase.LOG);
        if (state.version() != StreamCdcEnumeratorState.CURRENT_VERSION
                || logTransactionOpen) {
            throw failure(StreamCdcException.Code.CHECKPOINT_BOUNDARY,
                    "resident CDC position requires a completed transaction");
        }
        state = state.withExternalPosition(completedExternalPositionFrame);
    }

    public StreamCdcEnumeratorState checkpointState() {
        checkOwner();
        if (activeSplit != null || logTransactionOpen) {
            throw failure(StreamCdcException.Code.CHECKPOINT_BOUNDARY,
                    "resident CDC snapshot checkpoints require a completed split boundary");
        }
        return state;
    }

    private void acceptLogRecord(long sequence, StreamUpsertRecord record,
            StreamCdcEnumeratorState next, boolean transactionOpen) {
        checkOwner();
        requirePhase(StreamCdcEnumeratorState.Phase.LOG);
        requireNextLogSequence(sequence);
        byte[] frame = StreamUpsertRecordCodec.encode(record);
        requireFrame(frame);
        emit(frame);
        state = next;
        logTransactionOpen = transactionOpen;
    }

    private void requireNextLogSequence(long sequence) {
        long previous = state.lastLogSequence();
        if (previous == Long.MAX_VALUE || sequence != previous + 1L) {
            throw failure(StreamCdcException.Code.LOG_SEQUENCE,
                    "resident CDC log sequence must advance exactly once");
        }
    }

    public StreamCdcStatusView status() {
        checkOwner();
        return new StreamCdcStatusView(state);
    }

    private String nextSplit() {
        int completed = state.completedSplits().size();
        if (completed >= state.expectedSplitIds().size()) {
            throw failure(StreamCdcException.Code.SNAPSHOT_INCOMPLETE,
                    "resident CDC snapshot split set is already complete");
        }
        return state.expectedSplitIds().get(completed);
    }

    private void requirePhase(StreamCdcEnumeratorState.Phase required) {
        if (state.phase() != required) {
            throw failure(StreamCdcException.Code.INVALID_PHASE,
                    "resident CDC operation is illegal in the current phase");
        }
    }

    private void requireFrame(byte[] frame) {
        if (frame.length > maximumFrameBytes) {
            throw failure(StreamCdcException.Code.FRAME_LIMIT,
                    "resident CDC UPSERT frame exceeds its configured bound");
        }
    }

    private void emit(byte[] frame) {
        try {
            output.emit(frame.clone());
        } catch (Throwable failure) {
            throw failure(StreamCdcException.Code.OUTPUT_FAILURE,
                    "resident CDC output rejected a record", failure);
        }
    }

    private void checkOwner() {
        if (Thread.currentThread() != ownerThread) {
            throw failure(StreamCdcException.Code.WRONG_THREAD,
                    "resident CDC enumerator is owned by one mailbox thread");
        }
    }

    private static MessageDigest sha256Digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static void updateLengthAndBytes(
            MessageDigest digest, byte[] frame) {
        digest.update((byte) (frame.length >>> 24));
        digest.update((byte) (frame.length >>> 16));
        digest.update((byte) (frame.length >>> 8));
        digest.update((byte) frame.length);
        digest.update(frame);
    }

    private static String hex(byte[] digest) {
        StringBuilder result = new StringBuilder(digest.length * 2);
        for (byte current : digest) {
            result.append(Character.forDigit((current >>> 4) & 0xf, 16));
            result.append(Character.forDigit(current & 0xf, 16));
        }
        return result.toString();
    }

    private static StreamCdcException failure(
            StreamCdcException.Code code, String message) {
        return new StreamCdcException(code, message);
    }

    private static StreamCdcException failure(
            StreamCdcException.Code code, String message, Throwable cause) {
        return new StreamCdcException(code, message, cause);
    }
}
