package io.rowweave.stream.spi;

import io.rowweave.stream.contract.StreamCdcEnumeratorState;

/** Exact resident CDC position whose engine checkpoint is authoritative. */
public final class StreamCdcCheckpointCompletion {
    private final long checkpointSequence;
    private final long lastLogSequence;
    private final byte[] externalPosition;

    public StreamCdcCheckpointCompletion(long checkpointSequence,
            long lastLogSequence, byte[] externalPositionFrame) {
        if (checkpointSequence < 1L || lastLogSequence < 0L
                || externalPositionFrame == null
                || externalPositionFrame.length < 1
                || externalPositionFrame.length
                > StreamCdcEnumeratorState.MAX_EXTERNAL_POSITION_BYTES) {
            throw new IllegalArgumentException(
                    "invalid resident CDC checkpoint completion");
        }
        this.checkpointSequence = checkpointSequence;
        this.lastLogSequence = lastLogSequence;
        this.externalPosition = externalPositionFrame.clone();
    }

    public long checkpointSequence() { return checkpointSequence; }
    public long lastLogSequence() { return lastLogSequence; }
    public byte[] externalPositionFrame() {
        return externalPosition.clone();
    }
}
