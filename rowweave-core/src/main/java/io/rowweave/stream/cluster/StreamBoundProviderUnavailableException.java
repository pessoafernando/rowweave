package io.rowweave.stream.cluster;

/** Bound provider absent from the current Worker registry snapshot. */
final class StreamBoundProviderUnavailableException
        extends UnsupportedOperationException {
    StreamBoundProviderUnavailableException(String message) {
        super(message);
    }
}
