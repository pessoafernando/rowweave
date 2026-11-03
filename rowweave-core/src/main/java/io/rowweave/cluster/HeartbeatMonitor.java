package io.rowweave.cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;

/**
 * Monitors a group of {@link Heartbeat} instances, with a daemon thread probing once per intervalMs. The
 * first alive→dead transition of an entity fires {@code onLost} **exactly once** (deduplicated per
 * entity). It backs job fail-fast: onLost poisons every channel of the job, so losing a worker fails the
 * whole job and **does not reschedule** it, because re-running a partially published job could duplicate
 * its output. {@link #stop()} stops idempotently.
 *
 * <p><b>Success path</b>: a job that completes successfully must call {@code stop()} before any final
 * probe runs, so a finished entity cannot be probed dead and poison the job after it already succeeded.
 */
public final class HeartbeatMonitor {

    private final List<Heartbeat> beats;
    private final Set<Heartbeat> reportedLost = new CopyOnWriteArraySet<>();
    private final Object dispatchLock = new Object();  // makes the monitoring check and onLost atomic; probing needs only this lock and never this, so joining cannot deadlock
    private volatile Thread monitorThread;
    private volatile boolean monitoring;

    public HeartbeatMonitor(List<? extends Heartbeat> beats) {
        Objects.requireNonNull(beats, "beats");
        for (Heartbeat b : beats) {
            Objects.requireNonNull(b, "beats element must not be null");
        }
        this.beats = new ArrayList<>(beats);
    }

    /** Starts background probing on a daemon thread; the first time an entity goes from alive to dead, onLost is called for it, once per entity. Calling again stops the previous monitor first. */
    public synchronized void startMonitoring(long intervalMs, Consumer<Heartbeat> onLost) {
        if (intervalMs <= 0) {
            throw new IllegalArgumentException("intervalMs must be > 0");
        }
        Objects.requireNonNull(onLost, "onLost");
        stop();
        monitoring = true;
        Thread t = new Thread(() -> {
            while (monitoring) {
                for (Heartbeat b : beats) {
                    // Checking the flag and dispatching onLost are atomic under the same lock that stop
                    // uses, so once stop sets monitoring to false no further onLost can be dispatched: after
                    // a joined thread nothing is in flight, and the success path cannot poison by accident.
                    dispatchIfLost(b, onLost);
                    if (!monitoring) {
                        return;
                    }
                }
                try {
                    Thread.sleep(intervalMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }, "rowweave-heartbeat");
        t.setDaemon(true);
        monitorThread = t;
        t.start();
    }

    /**
     * Probes one entity, atomically checking the monitoring flag and calling onLost on the
     * first sight of deadunder dispatchLock, which is mutually exclusive with {@link #stop}
     * setting monitoring to false, so noonLost follows a stop. An exception thrown by the
     * callback is swallowed rather than killing the probe thread.
     */
    private void dispatchIfLost(Heartbeat b, Consumer<Heartbeat> onLost) {
        synchronized (dispatchLock) {
            if (!monitoring) {
                return;
            }
            if (!reportedLost.contains(b) && !b.isAlive()) {
                reportedLost.add(b);
                try {
                    onLost.accept(b);
                } catch (Throwable t2) {
                    // A callback exception must not kill the probe thread, or later workers would go unchecked and hang in silence.
                }
            }
        }
    }

    /**
     * Stops background probing idempotently: monitoring is set to false under dispatchLock,
     * which excludesonLost, and the probe thread is joined so that no callback is in flight
     * when this returns. The successpath stops before declaring success, so it cannot poison
     * afterwards. The probe thread itself does not joinwhen it calls this.
     */
    public synchronized void stop() {
        synchronized (dispatchLock) {
            monitoring = false;  // mutually exclusive with dispatchIfLost: no new onLost gets in after this
        }
        Thread t = monitorThread;
        if (t != null) {
            monitorThread = null;
            t.interrupt();
            if (t != Thread.currentThread()) {
                try {
                    t.join(2000);  // happens-before: once the join returns the probe thread has exited and no onLost is in flight
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
