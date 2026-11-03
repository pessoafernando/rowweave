package io.rowweave.cluster;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Properties;

/**
 * immutable proof of the JVM caps supplied on the process command line.
 * This class only parses values and performs exact arithmetic; it creates no files,
 * sockets, registries, TLS state, or threads.
 */
public final class JvmCapacityEnvelope {

    public static final String PROCESS_MEMORY_LIMIT_BYTES =
            "rowweave.process.memoryLimitBytes";
    public static final String HEAP_HEADROOM_BYTES =
            "rowweave.process.heapHeadroomBytes";
    public static final String NATIVE_HEADROOM_BYTES =
            "rowweave.process.nativeHeadroomBytes";

    private static final long KIB = 1024L;
    private static final long MIB = 1024L * KIB;
    private static final long MIN_STACK_BYTES = 512L * KIB;
    private static final long MAX_STACK_BYTES = 2L * MIB;
    private static final long MIN_HEAP_HEADROOM_BYTES = 256L * MIB;
    private static final long MIN_NATIVE_HEADROOM_BYTES = 512L * MIB;

    private static final String XMS = "-Xms";
    private static final String XMX = "-Xmx";
    private static final String DIRECT = "-XX:MaxDirectMemorySize=";
    private static final String METASPACE = "-XX:MaxMetaspaceSize=";
    private static final String CODE_CACHE = "-XX:ReservedCodeCacheSize=";
    private static final String XSS = "-Xss";
    private static final String PROCESSORS = "-XX:ActiveProcessorCount=";

    private final long initialHeapBytes;
    private final long maxHeapBytes;
    private final long maxDirectMemoryBytes;
    private final long maxMetaspaceBytes;
    private final long reservedCodeCacheBytes;
    private final long threadStackBytes;
    private final int activeProcessorCount;
    private final long processMemoryLimitBytes;
    private final long heapHeadroomBytes;
    private final long nativeHeadroomBytes;

    private JvmCapacityEnvelope(long initialHeapBytes, long maxHeapBytes,
                                long maxDirectMemoryBytes, long maxMetaspaceBytes,
                                long reservedCodeCacheBytes, long threadStackBytes,
                                int activeProcessorCount, long processMemoryLimitBytes,
                                long heapHeadroomBytes, long nativeHeadroomBytes) {
        this.initialHeapBytes = initialHeapBytes;
        this.maxHeapBytes = maxHeapBytes;
        this.maxDirectMemoryBytes = maxDirectMemoryBytes;
        this.maxMetaspaceBytes = maxMetaspaceBytes;
        this.reservedCodeCacheBytes = reservedCodeCacheBytes;
        this.threadStackBytes = threadStackBytes;
        this.activeProcessorCount = activeProcessorCount;
        this.processMemoryLimitBytes = processMemoryLimitBytes;
        this.heapHeadroomBytes = heapHeadroomBytes;
        this.nativeHeadroomBytes = nativeHeadroomBytes;
    }

    /** Capture only public runtime arguments; never infer missing ergonomic defaults. */
    public static JvmCapacityEnvelope capture(Properties properties) {
        return from(ManagementFactory.getRuntimeMXBean().getInputArguments(), properties);
    }

    static JvmCapacityEnvelope from(List<String> inputArguments,
                                    Properties properties) {
        if (inputArguments == null) {
            throw optionFailure("JVM input arguments are required");
        }
        if (properties == null) {
            throw configurationFailure("capacity properties are required");
        }

        long xms = requiredSizeOption(inputArguments, XMS, "-Xms");
        long xmx = requiredSizeOption(inputArguments, XMX, "-Xmx");
        long direct = requiredSizeOption(inputArguments, DIRECT,
                "-XX:MaxDirectMemorySize");
        long metaspace = requiredSizeOption(inputArguments, METASPACE,
                "-XX:MaxMetaspaceSize");
        long codeCache = requiredSizeOption(inputArguments, CODE_CACHE,
                "-XX:ReservedCodeCacheSize");
        long stack = requiredSizeOption(inputArguments, XSS, "-Xss");
        int processors = requiredIntOption(inputArguments, PROCESSORS,
                "-XX:ActiveProcessorCount", 1, 64);

        if (xms > xmx) {
            throw optionFailure("-Xms must not exceed -Xmx");
        }
        if (stack < MIN_STACK_BYTES || stack > MAX_STACK_BYTES) {
            throw optionFailure("-Xss must be in [512k,2m]");
        }

        long processLimit = requiredDecimalProperty(
                properties, PROCESS_MEMORY_LIMIT_BYTES);
        long defaultHeapHeadroom = Math.max(MIN_HEAP_HEADROOM_BYTES,
                ceilDiv(xmx, 10L));
        long defaultNativeHeadroom = Math.max(MIN_NATIVE_HEADROOM_BYTES,
                ceilDiv(xmx, 8L));
        long heapHeadroom = optionalHeadroom(properties, HEAP_HEADROOM_BYTES,
                defaultHeapHeadroom);
        long nativeHeadroom = optionalHeadroom(properties, NATIVE_HEADROOM_BYTES,
                defaultNativeHeadroom);

        return new JvmCapacityEnvelope(xms, xmx, direct, metaspace,
                codeCache, stack, processors, processLimit,
                heapHeadroom, nativeHeadroom);
    }

    public long rssRequiredBytes(long javaThreadUpperBound) {
        if (javaThreadUpperBound < 0L) {
            throw rssFailure("Java thread upper bound must not be negative", null);
        }
        try {
            long threadStacks = Math.multiplyExact(
                    javaThreadUpperBound, threadStackBytes);
            long required = Math.addExact(maxHeapBytes, maxDirectMemoryBytes);
            required = Math.addExact(required, maxMetaspaceBytes);
            required = Math.addExact(required, reservedCodeCacheBytes);
            required = Math.addExact(required, threadStacks);
            return Math.addExact(required, nativeHeadroomBytes);
        } catch (ArithmeticException overflow) {
            throw rssFailure("RSS capacity arithmetic overflow", overflow);
        }
    }

    public void requireRssCapacity(long javaThreadUpperBound) {
        long required = rssRequiredBytes(javaThreadUpperBound);
        if (required > processMemoryLimitBytes) {
            throw rssFailure("declared process memory limit is smaller than required RSS", null);
        }
    }

    public long initialHeapBytes() { return initialHeapBytes; }
    public long maxHeapBytes() { return maxHeapBytes; }
    public long maxDirectMemoryBytes() { return maxDirectMemoryBytes; }
    public long maxMetaspaceBytes() { return maxMetaspaceBytes; }
    public long reservedCodeCacheBytes() { return reservedCodeCacheBytes; }
    public long threadStackBytes() { return threadStackBytes; }
    public int activeProcessorCount() { return activeProcessorCount; }
    public long processMemoryLimitBytes() { return processMemoryLimitBytes; }
    public long heapHeadroomBytes() { return heapHeadroomBytes; }
    public long nativeHeadroomBytes() { return nativeHeadroomBytes; }

    private static long requiredSizeOption(List<String> arguments,
                                           String prefix, String displayName) {
        String value = uniqueOptionValue(arguments, prefix, displayName);
        return parseSize(value, displayName);
    }

    private static int requiredIntOption(List<String> arguments, String prefix,
                                         String displayName, int minimum, int maximum) {
        String value = uniqueOptionValue(arguments, prefix, displayName);
        long parsed = parseCanonicalDecimal(value, displayName,
                ProcessCapacityException.Code.JVM_OPTION);
        if (parsed < minimum || parsed > maximum) {
            throw optionFailure(displayName + " must be in [" + minimum
                    + ',' + maximum + ']');
        }
        return (int) parsed;
    }

    private static String uniqueOptionValue(List<String> arguments,
                                            String prefix, String displayName) {
        String result = null;
        int matches = 0;
        for (String argument : arguments) {
            if (argument != null && argument.startsWith(prefix)) {
                matches++;
                result = argument.substring(prefix.length());
            }
        }
        if (matches == 0) {
            throw optionFailure(displayName + " must be explicit");
        }
        if (matches != 1) {
            throw optionFailure(displayName + " must appear exactly once");
        }
        return result;
    }

    private static long parseSize(String value, String displayName) {
        if (value == null || value.isEmpty()) {
            throw optionFailure(displayName + " must have a size");
        }
        long multiplier = 1L;
        char last = value.charAt(value.length() - 1);
        String digits = value;
        if (last == 'k' || last == 'm' || last == 'g') {
            digits = value.substring(0, value.length() - 1);
            multiplier = last == 'k' ? KIB : last == 'm' ? MIB : 1024L * MIB;
        } else if (last < '0' || last > '9') {
            throw optionFailure(displayName
                    + " must use a canonical binary size with optional k/m/g suffix");
        }
        long base = parseCanonicalDecimal(digits, displayName,
                ProcessCapacityException.Code.JVM_OPTION);
        try {
            return Math.multiplyExact(base, multiplier);
        } catch (ArithmeticException overflow) {
            throw new ProcessCapacityException(ProcessCapacityException.Code.JVM_OPTION,
                    displayName + " size overflows", overflow);
        }
    }

    private static long requiredDecimalProperty(Properties properties, String name) {
        String value = properties.getProperty(name);
        if (value == null || value.isEmpty()) {
            throw configurationFailure(name + " must be explicit");
        }
        return parseCanonicalDecimal(value, name,
                ProcessCapacityException.Code.CONFIGURATION);
    }

    private static long optionalHeadroom(Properties properties, String name,
                                         long minimum) {
        String value = properties.getProperty(name);
        if (value == null || value.isEmpty()) return minimum;
        long parsed = parseCanonicalDecimal(value, name,
                ProcessCapacityException.Code.CONFIGURATION);
        if (parsed < minimum) {
            throw configurationFailure(name + " cannot reduce the certified default");
        }
        return parsed;
    }

    private static long parseCanonicalDecimal(String value, String name,
                                              ProcessCapacityException.Code code) {
        if (value == null || value.isEmpty() || value.charAt(0) == '0') {
            throw failure(code, name + " must be a positive canonical decimal", null);
        }
        for (int i = 0; i < value.length(); i++) {
            char current = value.charAt(i);
            if (current < '0' || current > '9') {
                throw failure(code, name + " must be a positive canonical decimal", null);
            }
        }
        try {
            long parsed = Long.parseLong(value);
            if (parsed <= 0L) {
                throw failure(code, name + " must be positive", null);
            }
            return parsed;
        } catch (NumberFormatException invalid) {
            throw failure(code, name + " is outside the signed long range", invalid);
        }
    }

    private static long ceilDiv(long dividend, long divisor) {
        return dividend / divisor + (dividend % divisor == 0L ? 0L : 1L);
    }

    private static ProcessCapacityException optionFailure(String message) {
        return new ProcessCapacityException(
                ProcessCapacityException.Code.JVM_OPTION, message);
    }

    private static ProcessCapacityException configurationFailure(String message) {
        return new ProcessCapacityException(
                ProcessCapacityException.Code.CONFIGURATION, message);
    }

    private static ProcessCapacityException rssFailure(String message, Throwable cause) {
        return failure(ProcessCapacityException.Code.RSS_CAPACITY, message, cause);
    }

    private static ProcessCapacityException failure(ProcessCapacityException.Code code,
                                                    String message, Throwable cause) {
        return cause == null ? new ProcessCapacityException(code, message)
                : new ProcessCapacityException(code, message, cause);
    }
}
