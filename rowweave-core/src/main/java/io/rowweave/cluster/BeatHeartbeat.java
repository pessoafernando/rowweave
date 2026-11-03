package io.rowweave.cluster;

/**
 * A {@link Heartbeat} whose liveness comes from the freshness of an independent beat timestamp.
 * The worker beats periodically from its own mechanism, a beat thread or a timer, so it keeps
 * beating even when the main working thread is parked in a send or a poll under backpressure.
 * {@link #isAlive()} is true while the most recent beat is no more than stalenessMillis
 * old.<p>Beating is decoupled from row progress. A slow worker under backpressure still beats
 * and is therefore alive, so it is never killed by mistake; only a genuinely dead thread, or
 * one left unscheduled so long that its beating stopped too, goes stale and then dead.
 */
public final class BeatHeartbeat implements Heartbeat {

    private final long stalenessNanos;
    private volatile long lastBeatNanos;
    private volatile boolean dead;  // explicit marker that the thread terminated, so death is known at once rather than by timeout

    public BeatHeartbeat(long stalenessMillis) {
        if (stalenessMillis <= 0) {
            throw new IllegalArgumentException("stalenessMillis must be > 0");
        }
        // Monotonic nanoTime, so an NTP step or a leap second cannot make a healthy worker look stale and get killed.
        this.stalenessNanos = stalenessMillis * 1_000_000L;
        this.lastBeatNanos = System.nanoTime();
    }

    /** Called periodically by a separate worker thread, which backpressure does not affect; refreshes the liveness timestamp. */
    public void beat() {
        lastBeatNanos = System.nanoTime();
    }

    /** Declares the thread dead explicitly, from the finally block of process exit or thread termination: dead at once, without waiting for a timeout. */
    public void markDead() {
        dead = true;
    }

    @Override
    public boolean isAlive() {
        if (dead) {
            return false;
        }
        return System.nanoTime() - lastBeatNanos <= stalenessNanos;
    }
}
