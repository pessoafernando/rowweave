package io.rowweave.memory;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.function.LongConsumer;

/** General pre-allocation resident reservation bridge for the reference and array counts JEP 290 observes. */
public class ResidentObjectInputStream extends ObjectInputStream {
    private volatile RuntimeException residentReservationFailure;

    public ResidentObjectInputStream(InputStream input) throws IOException {
        super(input);
    }

    public ResidentObjectInputStream(InputStream input, LongConsumer reservation) throws IOException {
        this(input, reservation, null);
    }

    /** A subclass may layer a safety filter on top; resident accounting stays in this class. */
    protected ResidentObjectInputStream(InputStream input, LongConsumer reservation,
                                        StreamFilter securityFilter) throws IOException {
        this(input, reservation, securityFilter, false);
    }

    protected ResidentObjectInputStream(InputStream input, LongConsumer reservation,
                                        StreamFilter securityFilter,
                                        boolean filterRuntimeRequired) throws IOException {
        super(input);
        if (reservation == null) throw new IllegalArgumentException("reservation is required");
        ObjectInputFilterBridge.install(
                this, new ResidentFilter(reservation, securityFilter), filterRuntimeRequired);
    }

    public RuntimeException residentReservationFailure() {
        return residentReservationFailure;
    }

    public static void requireFilterRuntime() {
        if (!ObjectInputFilterBridge.available()) {
            throw new IllegalStateException(
                    "JEP 290 ObjectInputFilter runtime is required for cluster object loading");
        }
    }

    public enum StreamDecision { UNDECIDED, ALLOWED, REJECTED }

    @FunctionalInterface
    public interface StreamFilter {
        StreamDecision check(StreamInfo info);
    }

    public static final class StreamInfo {
        private final long depth;
        private final long references;
        private final long streamBytes;
        private final long arrayLength;
        private final Class<?> serialClass;

        StreamInfo(long depth, long references, long streamBytes, long arrayLength,
                   Class<?> serialClass) {
            this.depth = depth;
            this.references = references;
            this.streamBytes = streamBytes;
            this.arrayLength = arrayLength;
            this.serialClass = serialClass;
        }

        public long depth() { return depth; }
        public long references() { return references; }
        public long streamBytes() { return streamBytes; }
        public long arrayLength() { return arrayLength; }
        public Class<?> serialClass() { return serialClass; }
    }

    private final class ResidentFilter implements StreamFilter {
        private final LongConsumer reservation;
        private final StreamFilter securityFilter;
        private long chargedReferences;

        private ResidentFilter(LongConsumer reservation, StreamFilter securityFilter) {
            this.reservation = reservation;
            this.securityFilter = securityFilter;
        }

        @Override
        public StreamDecision check(StreamInfo info) {
            try {
                StreamDecision security = securityFilter == null
                        ? StreamDecision.UNDECIDED : securityFilter.check(info);
                if (security == StreamDecision.REJECTED) {
                    return StreamDecision.REJECTED;
                }
                long references = info.references();
                if (references > chargedReferences) {
                    reservation.accept(multiply(references - chargedReferences, 128L));
                    chargedReferences = references;
                }
                long arrayLength = info.arrayLength();
                if (arrayLength >= 0L) {
                    reservation.accept(align8(add(16L, multiply(arrayLength, 8L))));
                }
                return security;
            } catch (RuntimeException rejected) {
                residentReservationFailure = rejected;
                return StreamDecision.REJECTED;
            }
        }
    }

    private static long multiply(long left, long right) {
        if (left < 0L || right < 0L || (left != 0L && right > Long.MAX_VALUE / left)) {
            throw new IllegalArgumentException("resident object charge overflow");
        }
        return left * right;
    }

    private static long add(long left, long right) {
        if (right > Long.MAX_VALUE - left) {
            throw new IllegalArgumentException("resident object charge overflow");
        }
        return left + right;
    }

    private static long align8(long value) {
        if (value > Long.MAX_VALUE - 7L) {
            throw new IllegalArgumentException("resident object charge overflow");
        }
        return (value + 7L) & ~7L;
    }
}
