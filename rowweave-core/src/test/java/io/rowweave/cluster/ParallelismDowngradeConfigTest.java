package io.rowweave.cluster;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParallelismDowngradeConfigTest {

    @Test
    void downgradeIsEnabledWhenPropertyIsAbsentOrBlank() {
        assertTrue(ParallelismDowngradeConfig.enabled(new Properties()));
        Properties blank = new Properties();
        blank.setProperty(ParallelismDowngradeConfig.DOWNGRADE_PROPERTY, "  ");
        assertTrue(ParallelismDowngradeConfig.enabled(blank));
    }

    @Test
    void explicitBooleanSelectsDowngradeOrLegacyAdmission() {
        Properties enabled = new Properties();
        enabled.setProperty(ParallelismDowngradeConfig.DOWNGRADE_PROPERTY, " TrUe ");
        assertTrue(ParallelismDowngradeConfig.enabled(enabled));

        Properties disabled = new Properties();
        disabled.setProperty(ParallelismDowngradeConfig.DOWNGRADE_PROPERTY, " FaLsE ");
        assertFalse(ParallelismDowngradeConfig.enabled(disabled));
    }

    @Test
    void invalidBooleanFailsFast() {
        Properties invalid = new Properties();
        invalid.setProperty(ParallelismDowngradeConfig.DOWNGRADE_PROPERTY, "on");
        assertThrows(IllegalArgumentException.class,
                () -> ParallelismDowngradeConfig.enabled(invalid));
    }

    @Test
    void graceUsesBoundedDefaultAndAcceptsZero() {
        assertEquals(5000L, ParallelismDowngradeConfig.graceMs(new Properties(), 60_000L));
        assertEquals(250L, ParallelismDowngradeConfig.graceMs(new Properties(), 1000L));
        Properties blank = new Properties();
        blank.setProperty(ParallelismDowngradeConfig.GRACE_PROPERTY, "  ");
        assertEquals(250L, ParallelismDowngradeConfig.graceMs(blank, 1000L));
        Properties zero = new Properties();
        zero.setProperty(ParallelismDowngradeConfig.GRACE_PROPERTY, "0");
        assertEquals(0L, ParallelismDowngradeConfig.graceMs(zero, 60_000L));
    }

    @Test
    void invalidOrNegativeGraceFailsFast() {
        Properties invalid = new Properties();
        invalid.setProperty(ParallelismDowngradeConfig.GRACE_PROPERTY, "soon");
        assertThrows(IllegalArgumentException.class,
                () -> ParallelismDowngradeConfig.graceMs(invalid, 60_000L));

        Properties negative = new Properties();
        negative.setProperty(ParallelismDowngradeConfig.GRACE_PROPERTY, "-1");
        assertThrows(IllegalArgumentException.class,
                () -> ParallelismDowngradeConfig.graceMs(negative, 60_000L));
    }

    @Test
    void masterCannotGrowIndependentDowngradeParsers() throws Exception {
        String master = source("MasterServer.java");
        assertFalse(master.contains("System.getProperty(\"rowweave.cluster.downgrade\""));
        assertFalse(master.contains("Long.getLong(\"rowweave.cluster.downgradeGraceMs\""));
    }

    private static String source(String fileName) throws Exception {
        Path cwd = Paths.get("").toAbsolutePath();
        Path source = cwd.resolve("src/main/java/io/rowweave/cluster").resolve(fileName);
        if (!Files.exists(source)) {
            source = cwd.resolve("rowweave-core/src/main/java/io/rowweave/cluster").resolve(fileName);
        }
        return new String(Files.readAllBytes(source), StandardCharsets.UTF_8);
    }
}

