package io.rowweave.stream.api;

import java.util.Objects;

/** Named timer family with a fixed time domain and hard resource ceilings. */
public final class StreamTimerSpec {
    public static final int MAX_TIMERS_PER_KEY = 1_000_000;
    public static final long MAX_TOTAL_TIMERS = 1_000_000_000L;
    public static final long MAX_TOTAL_BYTES = 1L << 38;

    public enum TimeDomain { PROCESSING_TIME, EVENT_TIME }

    private final String timerFamilyId;
    private final TimeDomain timeDomain;
    private final int maximumTimersPerKey;
    private final long maximumTotalTimers;
    private final long maximumTotalBytes;

    private StreamTimerSpec(String timerFamilyId, TimeDomain timeDomain,
            int maximumTimersPerKey, long maximumTotalTimers,
            long maximumTotalBytes) {
        if (timerFamilyId == null || timerFamilyId.isEmpty()
                || timeDomain == null || maximumTimersPerKey <= 0
                || maximumTimersPerKey > MAX_TIMERS_PER_KEY
                || maximumTotalTimers < maximumTimersPerKey
                || maximumTotalTimers > MAX_TOTAL_TIMERS
                || maximumTotalBytes <= 0L
                || maximumTotalBytes > MAX_TOTAL_BYTES) {
            throw new IllegalArgumentException("timer bounds are invalid");
        }
        this.timerFamilyId = timerFamilyId;
        this.timeDomain = timeDomain;
        this.maximumTimersPerKey = maximumTimersPerKey;
        this.maximumTotalTimers = maximumTotalTimers;
        this.maximumTotalBytes = maximumTotalBytes;
    }

    public static StreamTimerSpec processingTime(String timerFamilyId,
            int maximumTimersPerKey, long maximumTotalTimers,
            long maximumTotalBytes) {
        return new StreamTimerSpec(timerFamilyId, TimeDomain.PROCESSING_TIME,
                maximumTimersPerKey, maximumTotalTimers, maximumTotalBytes);
    }

    public static StreamTimerSpec eventTime(String timerFamilyId,
            int maximumTimersPerKey, long maximumTotalTimers,
            long maximumTotalBytes) {
        return new StreamTimerSpec(timerFamilyId, TimeDomain.EVENT_TIME,
                maximumTimersPerKey, maximumTotalTimers, maximumTotalBytes);
    }

    public String timerFamilyId() { return timerFamilyId; }
    public TimeDomain timeDomain() { return timeDomain; }
    public int maximumTimersPerKey() { return maximumTimersPerKey; }
    public long maximumTotalTimers() { return maximumTotalTimers; }
    public long maximumTotalBytes() { return maximumTotalBytes; }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamTimerSpec)) return false;
        StreamTimerSpec that = (StreamTimerSpec) other;
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
