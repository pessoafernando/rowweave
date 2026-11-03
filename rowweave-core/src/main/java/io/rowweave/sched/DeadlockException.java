package io.rowweave.sched;

/**
 * Thrown when the deadlock watchdog confirms a pipeline deadlock: every live stage thread is parked while
 * channel throughput stops advancing, repeatedly past the timeout threshold. **It fails fast with an explicit
 * error instead of hanging forever** — a silent hang is a third failure mode beyond a wrong result and an OOM.
 *
 * <p>It is the **backstop** for the fan-out materialization decision: even if the reachability analysis misses a
 * deadlock shape, the job reports this exception rather than hanging quietly.
 */
public final class DeadlockException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public DeadlockException(String message) {
        super(message);
    }
}
