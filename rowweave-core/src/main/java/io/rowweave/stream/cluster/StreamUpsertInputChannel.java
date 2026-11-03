package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamUpsertRecord;
import io.rowweave.stream.contract.StreamUpsertRecordCodec;
import io.rowweave.stream.runtime.StreamChannelInput;

/** Presents UPSERT after-images as rows to an operator mailbox. */
final class StreamUpsertInputChannel implements StreamChannelInput {
    private final StreamChannelInput input;
    private final int maximumFrameBytes;

    StreamUpsertInputChannel(StreamChannelInput input,
            int maximumFrameBytes) {
        if (input == null || maximumFrameBytes <= 0) {
            throw new IllegalArgumentException(
                    "Stream UPSERT input contract is incomplete");
        }
        this.input = input;
        this.maximumFrameBytes = maximumFrameBytes;
    }

    @Override public StreamChannelIdentity channelIdentity() {
        return input.channelIdentity();
    }

    @Override public StreamEnvelope receive() throws InterruptedException {
        return afterImage(input.receive());
    }

    @Override public StreamEnvelope poll(long timeoutMillis)
            throws InterruptedException {
        return afterImage(input.poll(timeoutMillis));
    }

    @Override public void cancel() { input.cancel(); }

    @Override public void fail(Throwable cause) { input.fail(cause); }

    private StreamEnvelope afterImage(StreamEnvelope envelope) {
        if (envelope == null
                || envelope.kind() != StreamEnvelope.Kind.RECORD) {
            return envelope;
        }
        StreamUpsertRecord record = StreamUpsertRecordCodec.decode(
                envelope.recordFrame(), maximumFrameBytes,
                maximumFrameBytes);
        if (!record.hasAfterImage()) {
            throw new UnsupportedOperationException(
                    "UPSERT tombstone cannot enter a stateful Stream operator");
        }
        return envelope.hasEventTimestamp()
                ? StreamEnvelope.record(envelope.channelIdentity(),
                envelope.sequence(), envelope.timestamp(),
                record.afterImageFrame())
                : StreamEnvelope.record(envelope.channelIdentity(),
                envelope.sequence(), record.afterImageFrame());
    }
}
