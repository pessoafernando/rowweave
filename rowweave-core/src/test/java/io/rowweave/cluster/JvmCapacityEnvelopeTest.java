package io.rowweave.cluster;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JvmCapacityEnvelopeTest {

    private static final long MIB = 1024L * 1024L;
    private static final long GIB = 1024L * MIB;

    @Test
    void parsesAllExplicitCapsAndComputesRssWithoutDoubleCountingXms() {
        JvmCapacityEnvelope envelope = JvmCapacityEnvelope.from(
                validArguments(), properties(8L * GIB));

        assertEquals(512L * MIB, envelope.initialHeapBytes());
        assertEquals(4L * GIB, envelope.maxHeapBytes());
        assertEquals(768L * MIB, envelope.maxDirectMemoryBytes());
        assertEquals(384L * MIB, envelope.maxMetaspaceBytes());
        assertEquals(128L * MIB, envelope.reservedCodeCacheBytes());
        assertEquals(MIB, envelope.threadStackBytes());
        assertEquals(4, envelope.activeProcessorCount());
        assertEquals(8L * GIB, envelope.processMemoryLimitBytes());
        assertEquals(512L * MIB, envelope.nativeHeadroomBytes());
        assertEquals(429_496_730L, envelope.heapHeadroomBytes());

        long expected = 4L * GIB + 768L * MIB + 384L * MIB
                + 128L * MIB + 100L * MIB + 512L * MIB;
        assertEquals(expected, envelope.rssRequiredBytes(100));
        envelope.requireRssCapacity(100);
    }

    @Test
    void rejectsEachMissingJvmOptionWithStableCode() {
        for (int i = 0; i < validArguments().size(); i++) {
            List<String> missing = new ArrayList<>(validArguments());
            missing.remove(i);
            ProcessCapacityException failure = assertThrows(
                    ProcessCapacityException.class,
                    () -> JvmCapacityEnvelope.from(missing, properties(8L * GIB)));
            assertEquals(ProcessCapacityException.Code.JVM_OPTION, failure.code());
            assertEquals("ROWWEAVE-CAP-001", failure.stableCode());
        }
    }

    @Test
    void rejectsDuplicateNonCanonicalOverflowAndInvalidRelationships() {
        List<String> duplicate = new ArrayList<>(validArguments());
        duplicate.add("-Xmx4g");
        assertJvmOptionFailure(duplicate);

        List<String> uppercaseSuffix = replacing("-Xmx4g", "-Xmx4G");
        assertJvmOptionFailure(uppercaseSuffix);

        List<String> overflow = replacing("-Xmx4g", "-Xmx9223372036854775807g");
        assertJvmOptionFailure(overflow);

        List<String> xmsAboveXmx = replacing("-Xms512m", "-Xms5g");
        assertJvmOptionFailure(xmsAboveXmx);

        List<String> tinyStack = replacing("-Xss1m", "-Xss256k");
        assertJvmOptionFailure(tinyStack);

        List<String> tooManyProcessors = replacing(
                "-XX:ActiveProcessorCount=4", "-XX:ActiveProcessorCount=65");
        assertJvmOptionFailure(tooManyProcessors);
    }

    @Test
    void requiresCanonicalProcessLimitAndDoesNotAllowHeadroomReduction() {
        ProcessCapacityException missing = assertThrows(
                ProcessCapacityException.class,
                () -> JvmCapacityEnvelope.from(validArguments(), new Properties()));
        assertEquals(ProcessCapacityException.Code.CONFIGURATION, missing.code());

        Properties leadingZero = properties(8L * GIB);
        leadingZero.setProperty(JvmCapacityEnvelope.PROCESS_MEMORY_LIMIT_BYTES, "08589934592");
        assertConfigurationFailure(leadingZero);

        Properties lowHeapHeadroom = properties(8L * GIB);
        lowHeapHeadroom.setProperty(JvmCapacityEnvelope.HEAP_HEADROOM_BYTES,
                Long.toString(256L * MIB));
        assertConfigurationFailure(lowHeapHeadroom);

        Properties lowNativeHeadroom = properties(8L * GIB);
        lowNativeHeadroom.setProperty(JvmCapacityEnvelope.NATIVE_HEADROOM_BYTES,
                Long.toString(511L * MIB));
        assertConfigurationFailure(lowNativeHeadroom);
    }

    @Test
    void rejectsRssLimitByOneByteAndArithmeticOverflow() {
        JvmCapacityEnvelope roomy = JvmCapacityEnvelope.from(
                validArguments(), properties(8L * GIB));
        long required = roomy.rssRequiredBytes(100);
        JvmCapacityEnvelope shortByOne = JvmCapacityEnvelope.from(
                validArguments(), properties(required - 1L));
        ProcessCapacityException insufficient = assertThrows(
                ProcessCapacityException.class,
                () -> shortByOne.requireRssCapacity(100));
        assertEquals(ProcessCapacityException.Code.RSS_CAPACITY, insufficient.code());
        assertEquals("ROWWEAVE-CAP-004", insufficient.stableCode());

        ProcessCapacityException overflow = assertThrows(
                ProcessCapacityException.class,
                () -> roomy.rssRequiredBytes(Long.MAX_VALUE));
        assertEquals(ProcessCapacityException.Code.RSS_CAPACITY, overflow.code());
    }

    private static void assertJvmOptionFailure(List<String> arguments) {
        ProcessCapacityException failure = assertThrows(
                ProcessCapacityException.class,
                () -> JvmCapacityEnvelope.from(arguments, properties(8L * GIB)));
        assertEquals(ProcessCapacityException.Code.JVM_OPTION, failure.code());
    }

    private static void assertConfigurationFailure(Properties properties) {
        ProcessCapacityException failure = assertThrows(
                ProcessCapacityException.class,
                () -> JvmCapacityEnvelope.from(validArguments(), properties));
        assertEquals(ProcessCapacityException.Code.CONFIGURATION, failure.code());
        assertEquals("ROWWEAVE-CAP-002", failure.stableCode());
    }

    private static List<String> replacing(String before, String after) {
        List<String> arguments = new ArrayList<>(validArguments());
        arguments.set(arguments.indexOf(before), after);
        return arguments;
    }

    private static List<String> validArguments() {
        return new ArrayList<>(Arrays.asList(
                "-Xms512m",
                "-Xmx4g",
                "-XX:MaxDirectMemorySize=768m",
                "-XX:MaxMetaspaceSize=384m",
                "-XX:ReservedCodeCacheSize=128m",
                "-Xss1m",
                "-XX:ActiveProcessorCount=4"));
    }

    private static Properties properties(long memoryLimitBytes) {
        Properties properties = new Properties();
        properties.setProperty(JvmCapacityEnvelope.PROCESS_MEMORY_LIMIT_BYTES,
                Long.toString(memoryLimitBytes));
        return properties;
    }
}
