package io.rowweave.plan;

import io.rowweave.memory.ResidentObjectInputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectStreamClass;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.LongConsumer;

/** Object reader with quantified bounds, no proxies, a narrow JDK set, and code source binding. */
public final class TrustedObjectInputStream extends ResidentObjectInputStream {
    public static final long MAX_DEPTH = 64L;
    public static final long MAX_REFERENCES = 100_000L;
    public static final long MAX_STREAM_BYTES = 16L * 1024L * 1024L;
    public static final long MAX_ARRAY_LENGTH = 1_000_000L;

    private static final Set<String> ALLOWED_JDK_CLASSES = Collections.unmodifiableSet(
            new HashSet<String>(Arrays.asList(
                    Object.class.getName(), String.class.getName(), Boolean.class.getName(), Byte.class.getName(),
                    Short.class.getName(), Integer.class.getName(), Long.class.getName(),
                    Float.class.getName(), Double.class.getName(), Character.class.getName(),
                    Number.class.getName(), Enum.class.getName(),
                    BigDecimal.class.getName(), BigInteger.class.getName(), Timestamp.class.getName(),
                    java.util.Date.class.getName(), java.sql.Date.class.getName(),
                    LocalDate.class.getName(), LocalTime.class.getName(), LocalDateTime.class.getName(),
                    Instant.class.getName(), Duration.class.getName(), Period.class.getName(),
                    ZoneId.class.getName(), ZoneOffset.class.getName(), ZonedDateTime.class.getName(),
                    OffsetDateTime.class.getName(), OffsetTime.class.getName(),
                    java.util.ArrayList.class.getName(), java.util.LinkedList.class.getName(),
                    java.util.HashMap.class.getName(), java.util.LinkedHashMap.class.getName(),
                    java.util.HashSet.class.getName(), java.util.LinkedHashSet.class.getName(),
                    java.util.TreeMap.class.getName(), java.util.TreeSet.class.getName(),
                    "java.util.Arrays$ArrayList", "java.util.Collections$EmptyList",
                    "java.util.Collections$SingletonList", "java.util.Collections$UnmodifiableList",
                    "java.util.Collections$EmptyMap", "java.util.Collections$SingletonMap",
                    "java.util.Collections$UnmodifiableMap", "java.lang.invoke.SerializedLambda")));

    private final ObjectTrustContext trustContext;

    private TrustedObjectInputStream(ByteArrayInputStream input, LongConsumer reservation,
                                     ObjectTrustContext trustContext) throws IOException {
        super(input, reservation, new QuantitativeFilter(), requireFilter(trustContext));
        if (trustContext == null) throw new IllegalArgumentException("object trust context is required");
        this.trustContext = trustContext;
    }

    private static boolean requireFilter(ObjectTrustContext trustContext) {
        if (trustContext == null) throw new IllegalArgumentException("object trust context is required");
        return trustContext.quantitativeFilterRequired();
    }

    public static <T> T read(byte[] blob, Class<T> expectedRoot,
                             ObjectTrustContext trustContext, LongConsumer reservation)
            throws IOException, ClassNotFoundException {
        return read(blob, expectedRoot, trustContext, reservation, false);
    }

    /** Used when the plan scanner has already reserved the 3x baseline for every blob, so nothing is charged twice. */
    static <T> T readReserved(byte[] blob, Class<T> expectedRoot,
                              ObjectTrustContext trustContext, LongConsumer reservation)
            throws IOException, ClassNotFoundException {
        return read(blob, expectedRoot, trustContext, reservation, true);
    }

    private static <T> T read(byte[] blob, Class<T> expectedRoot,
                              ObjectTrustContext trustContext, LongConsumer reservation,
                              boolean baselineReserved)
            throws IOException, ClassNotFoundException {
        return read(blob, 0, blob == null ? 0 : blob.length, expectedRoot,
                trustContext, reservation, baselineReserved);
    }

    static <T> T readSlice(byte[] blob, int offset, int length, Class<T> expectedRoot,
                           ObjectTrustContext trustContext, LongConsumer reservation)
            throws IOException, ClassNotFoundException {
        return read(blob, offset, length, expectedRoot, trustContext, reservation, false);
    }

    private static <T> T read(byte[] blob, int offset, int length, Class<T> expectedRoot,
                              ObjectTrustContext trustContext, LongConsumer reservation,
                              boolean baselineReserved)
            throws IOException, ClassNotFoundException {
        if (blob == null || offset < 0 || length <= 0 || offset > blob.length - length
                || length > MAX_STREAM_BYTES) {
            throw ObjectTrustContext.rejected(expectedRoot == null ? "null" : expectedRoot.getName(),
                    "object stream length out of bounds");
        }
        if (expectedRoot == null || reservation == null || trustContext == null) {
            throw new IllegalArgumentException("blob, expected root, trust context and reservation are required");
        }
        if (!baselineReserved) reservation.accept(multiply(length, 3L));
        ByteArrayInputStream raw = new ByteArrayInputStream(blob, offset, length);
        try (TrustedObjectInputStream in = new TrustedObjectInputStream(raw, reservation, trustContext)) {
            Object value;
            try {
                value = in.readObject();
            } catch (IOException rejected) {
                if (in.residentReservationFailure() != null) {
                    throw in.residentReservationFailure();
                }
                throw rejected;
            }
            if (!expectedRoot.isInstance(value)) {
                throw ObjectTrustContext.rejected(value == null ? "null" : value.getClass().getName(),
                        "root object must implement " + expectedRoot.getName());
            }
            if (raw.available() != 0) {
                throw ObjectTrustContext.rejected(value.getClass().getName(),
                        "object stream has trailing bytes");
            }
            return expectedRoot.cast(value);
        }
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass descriptor)
            throws IOException, ClassNotFoundException {
        String name = descriptor.getName();
        Class<?> resolved = trustContext.resolveApplicationClass(name);
        verifyClassPolicy(resolved, trustContext);
        return resolved;
    }

    @Override
    protected Class<?> resolveProxyClass(String[] interfaces) throws IOException {
        throw ObjectTrustContext.rejected(Proxy.class.getName(),
                "dynamic proxy is forbidden in trusted object streams");
    }

    static void verifyClassPolicy(Class<?> type, ObjectTrustContext trustContext)
            throws InvalidClassException {
        if (type.isArray()) {
            Class<?> component = type;
            while (component.isArray()) component = component.getComponentType();
            if (!component.isPrimitive()) verifyClassPolicy(component, trustContext);
            return;
        }
        String name = type.getName();
        if (name.startsWith("java.") || name.startsWith("javax.")
                || name.startsWith("jdk.") || name.startsWith("sun.")) {
            if (!ALLOWED_JDK_CLASSES.contains(name)) {
                throw ObjectTrustContext.rejected(name, "JDK class is outside the narrow allowlist");
            }
            return;
        }
        trustContext.verifyNonJdkClass(type);
    }

    private static long multiply(long left, long right) {
        if (left < 0L || right < 0L || (left != 0L && right > Long.MAX_VALUE / left)) {
            throw new IllegalArgumentException("object stream charge overflow");
        }
        return left * right;
    }

    private static final class QuantitativeFilter
            implements io.rowweave.memory.ResidentObjectInputStream.StreamFilter {
        @Override
        public io.rowweave.memory.ResidentObjectInputStream.StreamDecision check(
                io.rowweave.memory.ResidentObjectInputStream.StreamInfo info) {
            if (info.depth() > MAX_DEPTH || info.references() > MAX_REFERENCES
                    || info.streamBytes() > MAX_STREAM_BYTES
                    || info.arrayLength() > MAX_ARRAY_LENGTH) {
                return io.rowweave.memory.ResidentObjectInputStream.StreamDecision.REJECTED;
            }
            if (info.serialClass() != null && Proxy.isProxyClass(info.serialClass())) {
                return io.rowweave.memory.ResidentObjectInputStream.StreamDecision.REJECTED;
            }
            return io.rowweave.memory.ResidentObjectInputStream.StreamDecision.UNDECIDED;
        }
    }
}
