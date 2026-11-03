package io.rowweave.sched;

/**
 * The channel was poisoned: a downstream or opposite stage failed, channel.fail poisoned the channel, and an
 * upstream thread parked on send with a full queue wakes to throw this. It is a <b>cooperative cancellation
 * signal</b>, meaning the upstream was terminated as collateral, and not a root failure. A Task body catches it
 * and exits, which lets the caller join every task instead of deadlocking. The root cause is recorded by the
 * stage that failed first, and the fail-fast reports that one rather than this collateral exception.
 */
public final class ChannelPoisonedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ChannelPoisonedException(String message) {
        super(message);
    }

    /** Carries the first poison cause from the channel back to the producer thread being woken or refused. */
    public ChannelPoisonedException(String message, Throwable cause) {
        super(message, cause);
    }
}
