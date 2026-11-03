package io.rowweave.sched.net;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/** Immutable consumer-side authorization closure for one remote shuffle target. */
public final class ShuffleInboundAuthorization implements Serializable {

    private static final long serialVersionUID = 1L;
    private final UUID targetId;
    private final int expectedPoolCount;
    private final List<ShuffleAuthorizationRecord> records;

    private ShuffleInboundAuthorization(UUID targetId, int expectedPoolCount,
                                        List<ShuffleAuthorizationRecord> records) {
        if (targetId == null || Frame.NO_TARGET.equals(targetId)) {
            throw new IllegalArgumentException("non-zero inbound authorization target is required");
        }
        if (records == null || records.isEmpty()
                || records.size() > ShuffleProducerGrant.MAX_PRODUCERS_PER_TARGET) {
            throw new IllegalArgumentException(
                    "inbound authorization record count must be in [1,4096]");
        }
        if (expectedPoolCount < 1 || expectedPoolCount > records.size()) {
            throw new IllegalArgumentException("invalid expected connection pool count");
        }
        Set<Integer> ordinals = new HashSet<>();
        Set<WorkerDataIdentity> owners = new HashSet<>();
        for (ShuffleAuthorizationRecord record : records) {
            if (record == null || !targetId.equals(record.targetId())) {
                throw new IllegalArgumentException("authorization record target mismatch");
            }
            if (!ordinals.add(record.producerOrdinal())) {
                throw new IllegalArgumentException("duplicate producer ordinal");
            }
            owners.add(record.expectedSourceIdentity());
        }
        for (int ordinal = 1; ordinal <= records.size(); ordinal++) {
            if (!ordinals.contains(ordinal)) {
                throw new IllegalArgumentException("producer ordinals must be contiguous from 1");
            }
        }
        if (owners.size() != expectedPoolCount) {
            throw new IllegalArgumentException(
                    "expected pool count does not match distinct source owners");
        }
        this.targetId = targetId;
        this.expectedPoolCount = expectedPoolCount;
        this.records = Collections.unmodifiableList(new ArrayList<>(records));
    }

    public static ShuffleInboundAuthorization of(
            UUID targetId, int expectedPoolCount,
            List<ShuffleAuthorizationRecord> records) {
        return new ShuffleInboundAuthorization(targetId, expectedPoolCount, records);
    }

    public UUID targetId() { return targetId; }
    public int expectedPoolCount() { return expectedPoolCount; }
    public List<ShuffleAuthorizationRecord> records() { return records; }

    @Override
    public String toString() {
        return "ShuffleInboundAuthorization{redacted, producers=" + records.size()
                + ", pools=" + expectedPoolCount + '}';
    }
}
