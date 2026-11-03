package io.rowweave.stream.runtime;

/** Terminal local Stream channel cancellation; used to wake both blocked sides. */
public final class StreamChannelCancelledException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public StreamChannelCancelledException() {
        super("local Stream channel was cancelled");
    }
}
