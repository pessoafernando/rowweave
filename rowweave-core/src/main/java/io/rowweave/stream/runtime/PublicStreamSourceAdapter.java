package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamSourceSplit;
import io.rowweave.stream.spi.StreamCdcSourceContext;
import io.rowweave.stream.spi.StreamCdcSourceProvider;
import io.rowweave.stream.spi.StreamCdcSourceReader;
import io.rowweave.stream.spi.StreamSourceContext;
import io.rowweave.stream.spi.StreamDynamicSourceProvider;
import io.rowweave.stream.spi.StreamSourceEnumerator;
import io.rowweave.stream.spi.StreamSourceEnumeratorContext;
import io.rowweave.stream.spi.StreamSourceEvent;
import io.rowweave.stream.spi.StreamSourceProvider;
import io.rowweave.stream.spi.StreamSecretAccess;

/** Worker-local bridge from the public source SDK to the Stream kernel. */
public final class PublicStreamSourceAdapter {
    private PublicStreamSourceAdapter() { }

    public static DurableStreamSourceProvider adapt(
            StreamSourceProvider provider) {
        return adapt(provider, StreamSecretAccess.unavailable());
    }

    public static DurableStreamSourceProvider adapt(
            StreamSourceProvider provider, StreamSecretAccess secrets) {
        if (provider == null || empty(provider.providerKey())
                || empty(provider.contractDigest()) || secrets == null) {
            throw new IllegalArgumentException(
                    "public Stream source provider is invalid");
        }
        return provider instanceof StreamDynamicSourceProvider
                ? new DynamicAdapter(
                (StreamDynamicSourceProvider) provider, secrets)
                : new Adapter(provider, secrets);
    }

    private static class Adapter
            implements ResidentCdcStreamSourceProvider {
        private final StreamSourceProvider delegate;
        protected final StreamSecretAccess secrets;

        private Adapter(StreamSourceProvider delegate,
                StreamSecretAccess secrets) {
            this.delegate = delegate;
            this.secrets = secrets;
        }

        @Override public String providerKey() {
            return delegate.providerKey();
        }

        @Override public String contractDigest() {
            return delegate.contractDigest();
        }

        @Override public StreamSourceReader open(StreamSourceOpenRequest request) {
            StreamSourceSplit split = request.split();
            io.rowweave.stream.spi.StreamSourceReader reader;
            try {
                reader = delegate.open(new StreamSourceContext(
                        request.sourceOperatorUid(),
                        request.binding().configurationFrame(),
                        split.splitId(), split.positionCodecId(),
                        split.descriptorFrame(),
                        request.hasRestoredPosition()
                                ? request.restoredPositionFrame() : null,
                        request.hasRestoredPosition()
                                ? request.restoredLogicalPosition() : -1L,
                        secrets));
            } catch (Exception failure) {
                throw providerFailure("open", failure);
            }
            if (reader == null) {
                throw new IllegalArgumentException(
                        "public Stream source returned no reader");
            }
            return bridge(reader, request.binding().dynamicDiscovery());
        }

        @Override public StreamCdcSourceReader openCdc(
                StreamCdcSourceContext context) {
            if (!(delegate instanceof StreamCdcSourceProvider)) {
                throw new UnsupportedOperationException(
                        "public Stream source has no resident CDC SPI");
            }
            StreamCdcSourceReader reader;
            try {
                reader = ((StreamCdcSourceProvider) delegate).openCdc(
                        context.withSecrets(secrets));
            } catch (Exception failure) {
                throw providerFailure("open CDC", failure);
            }
            if (reader == null) {
                throw new IllegalArgumentException(
                        "public resident CDC source returned no reader");
            }
            return reader;
        }
    }

    private static final class DynamicAdapter extends Adapter
            implements DynamicDurableStreamSourceProvider {
        private final StreamDynamicSourceProvider dynamic;

        private DynamicAdapter(StreamDynamicSourceProvider delegate,
                StreamSecretAccess secrets) {
            super(delegate, secrets);
            this.dynamic = delegate;
        }

        @Override public StreamSourceEnumerator openEnumerator(
                StreamSourceEnumeratorOpenRequest request) {
            io.rowweave.stream.contract.StreamSourceDiscoveryDescriptor
                    limits = request.binding().discoveryDescriptor();
            try {
                StreamSourceEnumerator enumerator = dynamic.openEnumerator(
                        new StreamSourceEnumeratorContext(
                                request.sourceOperatorUid(),
                                request.binding().configurationFrame(),
                                new io.rowweave.stream.api
                                        .StreamSourceDiscoverySpec(
                                        limits.discoveryIntervalMillis(),
                                        limits.discoveryTimeoutMillis(),
                                        limits.maximumKnownSplits(),
                                        limits.maximumPendingSplits(),
                                        limits.maximumChangesPerDiscovery(),
                                        limits.maximumCursorBytes(),
                                        limits.maximumDescriptorBytes()),
                                secrets));
                if (enumerator == null) {
                    throw new IllegalArgumentException(
                            "public Stream source returned no enumerator");
                }
                return enumerator;
            } catch (Exception failure) {
                throw providerFailure("open enumerator", failure);
            }
        }
    }

    private static StreamSourceReader bridge(
            io.rowweave.stream.spi.StreamSourceReader reader,
            boolean rejectEndOfInput) {
        return new StreamSourceReader() {
            @Override public StreamSourceRecord next() throws Exception {
                io.rowweave.stream.spi.StreamSourceRecord value = reader.next();
                return value == null ? null : record(value);
            }

            @Override public io.rowweave.stream.runtime.StreamSourceEvent
                    nextEvent() throws Exception {
                StreamSourceEvent value = reader.nextEvent();
                if (value == null) return null;
                switch (value.kind()) {
                    case RECORD:
                        return io.rowweave.stream.runtime.StreamSourceEvent
                                .record(record(value.record()));
                    case WATERMARK:
                        return io.rowweave.stream.runtime.StreamSourceEvent
                                .watermark(value.watermark());
                    case WATERMARK_STATUS:
                        return io.rowweave.stream.runtime.StreamSourceEvent
                                .watermarkStatus(
                                        value.watermarkStatus()
                                                == StreamSourceEvent
                                                .WatermarkStatus.ACTIVE
                                                ? StreamEnvelope.WatermarkStatus
                                                .ACTIVE
                                                : StreamEnvelope.WatermarkStatus
                                                .IDLE,
                                        value.watermarkStatusGeneration());
                    case END_OF_INPUT:
                        if (rejectEndOfInput) {
                            throw new IllegalStateException(
                                    "public dynamic Source emitted undeclared END_OF_INPUT");
                        }
                        return io.rowweave.stream.runtime.StreamSourceEvent
                                .endOfInput();
                    default:
                        throw new AssertionError(value.kind());
                }
            }

            @Override public byte[] checkpointPositionFrame() {
                byte[] frame = reader.checkpointPositionFrame();
                return frame == null ? null : frame.clone();
            }

            @Override public void wakeUp() {
                reader.wakeUp();
            }

            @Override public void cancel() {
                reader.cancel();
            }

            @Override public void close() throws Exception {
                reader.close();
            }
        };
    }

    private static StreamSourceRecord record(
            io.rowweave.stream.spi.StreamSourceRecord value) {
        return value.hasEventTimestamp()
                ? StreamSourceRecord.of(value.rowFrame(), value.positionFrame(),
                value.eventTimestamp())
                : StreamSourceRecord.of(value.rowFrame(), value.positionFrame());
    }

    private static IllegalStateException providerFailure(
            String action, Exception failure) {
        return new IllegalStateException(
                "public Stream source " + action + " failed", failure);
    }

    private static boolean empty(String value) {
        return value == null || value.isEmpty();
    }
}
