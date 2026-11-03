package io.rowweave.cluster;

/**
 * Something that can be probed for liveness. {@link #isAlive} asks whether the threads of the
 * node arestill being scheduled and responding, and never about row progress: a worker parked
 * on a send or a spillunder backpressure makes no row progress and is nonetheless alive, which
 * is healthy but blocked, and mustnot be read as dead.<p>The local path probes the freshness of
 * a worker's independent beat timestamp; acluster pings the process as well as checking
 * beat frames. The interface is the same.
 */
public interface Heartbeat {

    /** Whether the node is alive, judged by responsiveness rather than by row progress. */
    boolean isAlive();
}
