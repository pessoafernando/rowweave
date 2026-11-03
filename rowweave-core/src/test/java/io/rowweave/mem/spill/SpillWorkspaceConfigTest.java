package io.rowweave.mem.spill;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SpillWorkspaceConfigTest {

    private static final String PREFIX = "rowweave.worker.spill.";

    @AfterEach
    void clearProperties() {
        System.clearProperty(PREFIX + "dir");
        System.clearProperty(PREFIX + "maxBytes");
        System.clearProperty(PREFIX + "jobMaxBytes");
        System.clearProperty(PREFIX + "minFreeBytes");
        System.clearProperty(PREFIX + "orphanGraceMs");
        System.clearProperty(PREFIX + "scanMaxEntries");
        System.clearProperty(PREFIX + "deleteMaxEntries");
        System.clearProperty(PREFIX + "maintenanceIntervalMs");
    }

    @Test
    void defaultsAreFiniteAndUseAnAbsoluteDedicatedBase() {
        SpillWorkspaceConfig config = SpillWorkspaceConfig.fromSystemProperties();

        Path expected;
        try {
            expected = Paths.get(System.getProperty("java.io.tmpdir")).toRealPath()
                    .resolve("rowweave-spill-v1").normalize();
        } catch (java.io.IOException e) {
            throw new AssertionError(e);
        }
        assertEquals(expected, config.baseDirectory());
        assertEquals(64L * 1024 * 1024 * 1024, config.workerMaxBytes());
        assertEquals(16L * 1024 * 1024 * 1024, config.jobMaxBytes());
        assertEquals(1024L * 1024 * 1024, config.minFreeBytes());
        assertEquals(60L * 60 * 1000, config.orphanGraceMillis());
        assertEquals(4096, config.scanMaxEntries());
        assertEquals(1024, config.deleteMaxEntries());
        assertEquals(60_000L, config.maintenanceIntervalMillis());
    }

    @Test
    void acceptsTrimmedStrictProductionOverrides() {
        Path base = Paths.get(System.getProperty("java.io.tmpdir"), "rowweave-spill-config-test")
                .toAbsolutePath().normalize();
        System.setProperty(PREFIX + "dir", "  " + base + "  ");
        System.setProperty(PREFIX + "maxBytes", " 8388608 ");
        System.setProperty(PREFIX + "jobMaxBytes", "4194304");
        System.setProperty(PREFIX + "minFreeBytes", "0");
        System.setProperty(PREFIX + "orphanGraceMs", "60000");
        System.setProperty(PREFIX + "scanMaxEntries", "7");
        System.setProperty(PREFIX + "deleteMaxEntries", "3");
        System.setProperty(PREFIX + "maintenanceIntervalMs", "1000");

        SpillWorkspaceConfig config = SpillWorkspaceConfig.fromSystemProperties();

        assertEquals(base, config.baseDirectory());
        assertEquals(8_388_608L, config.workerMaxBytes());
        assertEquals(4_194_304L, config.jobMaxBytes());
        assertEquals(0L, config.minFreeBytes());
        assertEquals(60_000L, config.orphanGraceMillis());
        assertEquals(7, config.scanMaxEntries());
        assertEquals(3, config.deleteMaxEntries());
        assertEquals(1000L, config.maintenanceIntervalMillis());
    }

    @Test
    void rejectsRelativeOrBlankDirectory() {
        System.setProperty(PREFIX + "dir", "relative/spill");
        assertConfigFailure(PREFIX + "dir");

        System.setProperty(PREFIX + "dir", "   ");
        assertConfigFailure(PREFIX + "dir");
    }

    @Test
    void rejectsMalformedOverflowAndOutOfRangeNumbersInsteadOfFallingBack() {
        System.setProperty(PREFIX + "maxBytes", "not-a-number");
        assertConfigFailure(PREFIX + "maxBytes");

        System.setProperty(PREFIX + "maxBytes", "9223372036854775808");
        assertConfigFailure(PREFIX + "maxBytes");

        System.setProperty(PREFIX + "maxBytes", Long.toString(1024L * 1024 - 1));
        assertConfigFailure(PREFIX + "maxBytes");

        System.setProperty(PREFIX + "maxBytes", Long.toString(16L * 1024 * 1024 * 1024 * 1024 + 1));
        assertConfigFailure(PREFIX + "maxBytes");
    }

    @Test
    void rejectsCrossFieldQuotaAndMaintenanceViolations() {
        System.setProperty(PREFIX + "maxBytes", Long.toString(4L * 1024 * 1024));
        System.setProperty(PREFIX + "jobMaxBytes", Long.toString(5L * 1024 * 1024));
        assertConfigFailure(PREFIX + "jobMaxBytes");

        clearProperties();
        System.setProperty(PREFIX + "deleteMaxEntries", "8");
        System.setProperty(PREFIX + "scanMaxEntries", "7");
        assertConfigFailure(PREFIX + "deleteMaxEntries");

        clearProperties();
        System.setProperty(PREFIX + "orphanGraceMs", "59999");
        assertConfigFailure(PREFIX + "orphanGraceMs");

        clearProperties();
        System.setProperty(PREFIX + "maintenanceIntervalMs", "999");
        assertConfigFailure(PREFIX + "maintenanceIntervalMs");

        clearProperties();
        System.setProperty(PREFIX + "scanMaxEntries", "3");
        assertConfigFailure(PREFIX + "scanMaxEntries");

        clearProperties();
        System.setProperty(PREFIX + "deleteMaxEntries", "2");
        assertConfigFailure(PREFIX + "deleteMaxEntries");
    }

    @Test
    void explicitTestFactoryAllowsZeroGraceWithoutWeakeningProductionParser() {
        Path base = Paths.get(System.getProperty("java.io.tmpdir"), "rowweave-spill-test-factory")
                .toAbsolutePath().normalize();

        SpillWorkspaceConfig config = SpillWorkspaceConfig.forTests(
                base, 4L * 1024 * 1024, 2L * 1024 * 1024,
                0, 0, 8, 4, 0);

        assertEquals(0L, config.orphanGraceMillis());
        assertEquals(0L, config.maintenanceIntervalMillis());
    }

    private static void assertConfigFailure(String key) {
        SpillDiskException failure = assertThrows(
                SpillDiskException.class, SpillWorkspaceConfig::fromSystemProperties);
        assertEquals(SpillFailureCode.SPILL_CONFIG_INVALID, failure.code());
        assertTrue(failure.getMessage().contains(key));
        assertTrue(failure.getMessage().length() <= 256, "configuration diagnostics remain bounded");
    }
}
