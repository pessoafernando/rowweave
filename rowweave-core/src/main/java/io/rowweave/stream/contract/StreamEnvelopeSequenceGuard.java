package io.rowweave.stream.contract;

/** Single-channel admission guard; failed events do not advance accepted state. */
public final class StreamEnvelopeSequenceGuard {
    private final StreamChannelIdentity channelIdentity;
    private long lastSequence;
    private long lastWatermark = Long.MIN_VALUE;
    private long lastWatermarkStatusGeneration;
    private boolean terminated;

    public StreamEnvelopeSequenceGuard(StreamChannelIdentity channelIdentity) {
        if (channelIdentity == null) throw new IllegalArgumentException("channel is required");
        this.channelIdentity = channelIdentity;
    }

    public void accept(StreamEnvelope event) {
        if (event == null || !channelIdentity.equals(event.channelIdentity())) {
            throw StreamContractSupport.failure("STALE_CHANNEL", "envelope.channel",
                    "event channel is not the active channel identity");
        }
        if (terminated) {
            throw StreamContractSupport.failure("CHANNEL_ALREADY_TERMINATED",
                    "envelope.kind", "ordered channel already accepted a terminal event");
        }
        if (event.sequence() <= lastSequence) {
            throw StreamContractSupport.failure("CHANNEL_SEQUENCE_REPLAY",
                    "envelope.sequence", "event sequence was already accepted");
        }
        if (event.sequence() != lastSequence + 1L) {
            throw StreamContractSupport.failure("CHANNEL_SEQUENCE_GAP",
                    "envelope.sequence", "event sequence has a gap");
        }
        if (event.kind() == StreamEnvelope.Kind.WATERMARK
                && event.timestamp() < lastWatermark) {
            throw StreamContractSupport.failure("WATERMARK_REGRESSION",
                    "envelope.watermark", "watermark must not regress");
        }
        if (event.kind() == StreamEnvelope.Kind.WATERMARK_STATUS
                && event.watermarkStatusGeneration()
                <= lastWatermarkStatusGeneration) {
            throw StreamContractSupport.failure(
                    "WATERMARK_STATUS_GENERATION_REPLAY",
                    "envelope.watermarkStatusGeneration",
                    "watermark status generation must increase");
        }
        lastSequence = event.sequence();
        if (event.kind() == StreamEnvelope.Kind.WATERMARK) {
            lastWatermark = event.timestamp();
        }
        if (event.kind() == StreamEnvelope.Kind.WATERMARK_STATUS) {
            lastWatermarkStatusGeneration = event.watermarkStatusGeneration();
        }
        if (event.kind() == StreamEnvelope.Kind.END_OF_INPUT
                || event.kind() == StreamEnvelope.Kind.CANCEL
                || event.kind() == StreamEnvelope.Kind.ERROR) {
            terminated = true;
        }
    }

    public long lastSequence() { return lastSequence; }
    public long lastWatermark() { return lastWatermark; }
    public long lastWatermarkStatusGeneration() {
        return lastWatermarkStatusGeneration;
    }
}
