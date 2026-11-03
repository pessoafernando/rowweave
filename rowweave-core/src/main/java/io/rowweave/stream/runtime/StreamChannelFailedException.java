package io.rowweave.stream.runtime;

/** First-poison-wins failure observed by either side of a local Stream channel. */
public final class StreamChannelFailedException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public StreamChannelFailedException(Throwable cause) {
        super("local Stream channel failed", cause);
    }
}
