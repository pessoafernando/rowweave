package io.rowweave.stream.contract;

import io.rowweave.stream.api.StreamTimerSpec;

import java.util.Objects;

/** Portable named timer-family contract with hard count and byte ceilings. */
public final class StreamTimerDescriptor {
    public enum TimeDomain {
        PROCESSING_TIME(1), EVENT_TIME(2);
        private final int wireId;
        TimeDomain(int wireId) { this.wireId = wireId; }
        int wireId() { return wireId; }
        static TimeDomain fromWireId(int wireId) {
            for (TimeDomain value : values()) {
                if (value.wireId == wireId) return value;
            }
            throw StreamContractSupport.failure("UNKNOWN_TIMER_DOMAIN",
                    "nodes.process.timers", "unknown timer time domain");
        }
    }

    private final String timerFamilyId;
    private final TimeDomain timeDomain;
    private final int maximumTimersPerKey;
    private final long maximumTotalTimers;
    private final long maximumTotalBytes;

    public StreamTimerDescriptor(String timerFamilyId, TimeDomain timeDomain,
            int maximumTimersPerKey, long maximumTotalTimers,
            long maximumTotalBytes) {
        this.timerFamilyId = StreamContractSupport.text(timerFamilyId,
                "nodes.process.timers.id", 256);
        if (timeDomain == null || maximumTimersPerKey <= 0
                || maximumTimersPerKey > StreamTimerSpec.MAX_TIMERS_PER_KEY
                || maximumTotalTimers < maximumTimersPerKey
                || maximumTotalTimers > StreamTimerSpec.MAX_TOTAL_TIMERS
                || maximumTotalBytes <= 0L
                || maximumTotalBytes > StreamTimerSpec.MAX_TOTAL_BYTES) {
            throw StreamContractSupport.failure("INVALID_TIMER_RESOURCE_LIMIT",
                    "nodes.process.timers", "timer resource limits are invalid");
        }
        this.timeDomain = timeDomain;
        this.maximumTimersPerKey = maximumTimersPerKey;
        this.maximumTotalTimers = maximumTotalTimers;
        this.maximumTotalBytes = maximumTotalBytes;
    }

    public String timerFamilyId() { return timerFamilyId; }
    public TimeDomain timeDomain() { return timeDomain; }
    public int maximumTimersPerKey() { return maximumTimersPerKey; }
    public long maximumTotalTimers() { return maximumTotalTimers; }
    public long maximumTotalBytes() { return maximumTotalBytes; }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamTimerDescriptor)) return false;
        StreamTimerDescriptor that = (StreamTimerDescriptor) other;
        return maximumTimersPerKey == that.maximumTimersPerKey
                && maximumTotalTimers == that.maximumTotalTimers
                && maximumTotalBytes == that.maximumTotalBytes
                && timerFamilyId.equals(that.timerFamilyId)
                && timeDomain == that.timeDomain;
    }

    @Override public int hashCode() {
        return Objects.hash(timerFamilyId, timeDomain, maximumTimersPerKey,
                maximumTotalTimers, maximumTotalBytes);
    }
}
