package io.rowweave.sched.net;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The shuffle endpoint registry: a thread-safe map from {@code shuffleEdgeId} to the list of consumer
 * endpoints in partition order. Once {@link MultiSocketChannelSource} on the consumer side has its
 * ServerSocket up it calls {@link #register} with its own {@link ShuffleEndpoint}, and on the producer side
 * {@link #awaitPartitions} waits until the M consumer endpoints are all present, then {@link #lookup} connects
 * to them.
 *
 * <p>In the local in-process cluster this is a singleton over loopback. A real multi-process deployment
 * supplies a registry service over the network behind the same interface contract. It holds control metadata,
 * the endpoints, and never touches data rows.
 */
public final class ShuffleRegistry {

    private static final class EdgeSlots {
        final ShuffleEndpoint[] endpoints;  // indexed by partition
        int registered;  // how many have registered
        final ReentrantLock lock = new ReentrantLock();
        final Condition allRegistered = lock.newCondition();

        EdgeSlots(int expectedPartitions) {
            this.endpoints = new ShuffleEndpoint[expectedPartitions];
        }
    }

    private final Map<String, EdgeSlots> edges = new ConcurrentHashMap<>();

    /**
     * Consumer registration: the endpoint at partitionIndex of edgeId becomes this endpoint. expectedPartitions
     * is the total partition count M of the edge, fixed by the first registration and required to agree
     * afterwards. Registering the same edge and partition again overwrites idempotently.
     */
    public void register(String edgeId, int partitionIndex, int expectedPartitions, ShuffleEndpoint endpoint) {
        EdgeSlots slots = edges.computeIfAbsent(edgeId, k -> new EdgeSlots(expectedPartitions));
        if (slots.endpoints.length != expectedPartitions) {
            throw new IllegalArgumentException("edge " + edgeId + " expectedPartitions mismatch: "
                    + slots.endpoints.length + " vs " + expectedPartitions);
        }
        if (partitionIndex < 0 || partitionIndex >= expectedPartitions) {
            throw new IllegalArgumentException("partitionIndex " + partitionIndex + " out of range [0,"
                    + expectedPartitions + ")");
        }
        slots.lock.lock();
        try {
            if (slots.endpoints[partitionIndex] == null) {
                slots.registered++;
            }
            slots.endpoints[partitionIndex] = endpoint;
            if (slots.registered == slots.endpoints.length) {
                slots.allRegistered.signalAll();
            }
        } finally {
            slots.lock.unlock();
        }
    }

    /**
     * The producer blocks until all M consumers of edgeId, that is expectedPartitions, have registered, and
     * returns the endpoints in partition order. Timing out before they are all present throws rather than
     * hanging, the same rule as the accept guard.
     */
    public List<ShuffleEndpoint> awaitPartitions(String edgeId, int expectedPartitions, long timeoutMillis) {
        EdgeSlots slots = edges.computeIfAbsent(edgeId, k -> new EdgeSlots(expectedPartitions));
        if (slots.endpoints.length != expectedPartitions) {
            throw new IllegalArgumentException("edge " + edgeId + " expectedPartitions mismatch");
        }
        long deadline = System.currentTimeMillis() + timeoutMillis;
        slots.lock.lock();
        try {
            while (slots.registered < slots.endpoints.length) {
                long remain = deadline - System.currentTimeMillis();
                if (remain <= 0) {
                    throw new IllegalStateException("edge " + edgeId + " only " + slots.registered + "/"
                            + slots.endpoints.length + " partitions registered within timeout (not hang)");
                }
                try {
                    slots.allRegistered.await(remain, java.util.concurrent.TimeUnit.MILLISECONDS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("interrupted awaiting shuffle partitions", ie);
                }
            }
            return new ArrayList<>(java.util.Arrays.asList(slots.endpoints));
        } finally {
            slots.lock.unlock();
        }
    }

    /** Immediate lookup with no waiting; before all are registered it returns the current snapshot, which may contain null slots. For a case already known to be complete. */
    public List<ShuffleEndpoint> lookup(String edgeId) {
        EdgeSlots slots = edges.get(edgeId);
        if (slots == null) {
            return new ArrayList<>();
        }
        slots.lock.lock();
        try {
            return new ArrayList<>(java.util.Arrays.asList(slots.endpoints));
        } finally {
            slots.lock.unlock();
        }
    }
}
