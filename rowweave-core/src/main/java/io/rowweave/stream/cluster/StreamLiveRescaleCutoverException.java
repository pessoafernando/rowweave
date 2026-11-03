package io.rowweave.stream.cluster;

/** Signals that run-level cutover is durable although target launch must retry. */
final class StreamLiveRescaleCutoverException extends RuntimeException {
    StreamLiveRescaleCutoverException(Throwable cause) {
        super("live rescale cutover is durable and requires recovery", cause);
    }
}
