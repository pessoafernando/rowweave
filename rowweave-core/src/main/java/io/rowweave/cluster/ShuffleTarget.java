package io.rowweave.cluster;

import io.rowweave.sched.net.ShuffleEndpoint;

import java.io.Serializable;

/**
 * The transport target of one upstream instance fanning out to one inbound edge of one
 * downstream instance. It is exactly one of two kinds: a socket target, where {@link #endpoint}
 * is not null and the edge crosses workers using a serialized {@code SocketChannelSink} over
 * TCP, or a local target, where {@link #localKey} is not null and the edge stays inside one
 * worker using {@code LocalChannelSink}, which passes a DataRow reference in process.<p>Because
 * the decision is per instance pair, round-robin placement makes a mixture the norm rather than
 * the exception: among the N downstream partitions one upstream instance writes to, some sit on
 * its own worker and some do not, which is exactly what a keyed shuffle from M to N produces.
 * So every upstream instance gets its own downstream target list: the master builds it per
 * instance, and a fan-out sink creates a socket or a local sink per partition according to the
 * kind of the target.<p>{@code localKey} is {@code
 * jobId#downStageIndex#downInstanceIndex#edge}, naming the pre-built inbound channel of that
 * edge of that instance, awaited in the {@code LocalShuffleRegistry} of the worker holding the
 * downstream instance. It is only meaningful within one worker, which is to say one JVM and one
 * registry.
 */
public final class ShuffleTarget implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ShuffleEndpoint endpoint;  // socket target across workers; null when the target is local
    private final String localKey;  // local rendezvous key within one worker; null when the target is a socket

    private ShuffleTarget(ShuffleEndpoint endpoint, String localKey) {
        this.endpoint = endpoint;
        this.localKey = localKey;
    }

    /** Target across workers, so a socket. */
    public static ShuffleTarget socket(ShuffleEndpoint endpoint) {
        if (endpoint == null) {
            throw new IllegalArgumentException("socket endpoint must not be null");
        }
        return new ShuffleTarget(endpoint, null);
    }

    /** Target inside one worker, so an in-process rendezvous key. */
    public static ShuffleTarget local(String localKey) {
        if (localKey == null) {
            throw new IllegalArgumentException("local key must not be null");
        }
        return new ShuffleTarget(null, localKey);
    }

    public boolean isLocal() {
        return localKey != null;
    }

    public ShuffleEndpoint endpoint() {
        return endpoint;
    }

    public String localKey() {
        return localKey;
    }

    @Override
    public String toString() {
        return isLocal() ? "local(" + localKey + ")" : "socket(" + endpoint + ")";
    }
}
