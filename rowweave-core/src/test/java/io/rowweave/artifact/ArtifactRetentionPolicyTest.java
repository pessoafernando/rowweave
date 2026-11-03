package io.rowweave.artifact;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArtifactRetentionPolicyTest {

    @Test
    void productionDefaultsAndPropertiesRemainWithinHardBounds() {
        ArtifactRetentionPolicy defaults =
                ArtifactRetentionPolicy.fromProperties(new Properties());
        assertEquals(256, defaults.maxEntriesPerShard());
        assertEquals(1L << 30, defaults.maxBytesPerShard());
        assertEquals(24L * 60L * 60L * 1000L, defaults.orphanGraceMs());
        assertEquals(128, defaults.scanPageSize());
        assertEquals(16, defaults.maxDeletesPerPass());
        assertEquals(60_000L, defaults.maintenanceIntervalMs());

        Properties configured = new Properties();
        configured.setProperty("rowweave.master.artifactRetention.maxEntriesPerShard", "17");
        configured.setProperty("rowweave.master.artifactRetention.maxBytesPerShard",
                Long.toString(PlanArtifactCodec.MAX_FRAME_BYTES));
        configured.setProperty("rowweave.master.artifactRetention.orphanGraceMs", "300000");
        configured.setProperty("rowweave.master.artifactRetention.scanPageSize", "11");
        configured.setProperty("rowweave.master.artifactRetention.maxDeletesPerPass", "7");
        configured.setProperty("rowweave.master.artifactRetention.maintenanceIntervalMs", "100");
        ArtifactRetentionPolicy policy =
                ArtifactRetentionPolicy.fromProperties(configured);
        assertEquals(17, policy.maxEntriesPerShard());
        assertEquals(PlanArtifactCodec.MAX_FRAME_BYTES, policy.maxBytesPerShard());
        assertEquals(300_000L, policy.orphanGraceMs());
        assertEquals(11, policy.scanPageSize());
        assertEquals(7, policy.maxDeletesPerPass());
        assertEquals(100L, policy.maintenanceIntervalMs());
    }

    @Test
    void productionParserCannotBypassCapacityAgeOrWorkBounds() {
        assertInvalid("rowweave.master.artifactRetention.maxEntriesPerShard", "0");
        assertInvalid("rowweave.master.artifactRetention.maxEntriesPerShard", "4097");
        assertInvalid("rowweave.master.artifactRetention.maxBytesPerShard",
                Long.toString(PlanArtifactCodec.MAX_FRAME_BYTES - 1L));
        assertInvalid("rowweave.master.artifactRetention.maxBytesPerShard",
                Long.toString((1L << 40) + 1L));
        assertInvalid("rowweave.master.artifactRetention.orphanGraceMs", "299999");
        assertInvalid("rowweave.master.artifactRetention.orphanGraceMs",
                Long.toString(10L * 365L * 24L * 60L * 60L * 1000L + 1L));
        assertInvalid("rowweave.master.artifactRetention.scanPageSize", "1001");
        assertInvalid("rowweave.master.artifactRetention.maxDeletesPerPass", "0");
        assertInvalid("rowweave.master.artifactRetention.maintenanceIntervalMs", "99");
    }

    @Test
    void dueNeverExpiresFutureCandidateAndHandlesLongRangeWithoutOverflow() {
        ArtifactRetentionPolicy policy = ArtifactRetentionPolicy.testing(
                4, PlanArtifactCodec.MAX_FRAME_BYTES, 10L, 2, 1, 1L);
        assertFalse(policy.due(99L, 100L));
        assertFalse(policy.due(109L, 100L));
        assertTrue(policy.due(110L, 100L));
        assertTrue(policy.due(Long.MAX_VALUE, 1L));
        assertThrows(IllegalArgumentException.class, () -> policy.due(-1L, 1L));
        assertThrows(IllegalArgumentException.class, () -> policy.due(1L, 0L));
    }

    private static void assertInvalid(String key, String value) {
        Properties properties = new Properties();
        properties.setProperty(key, value);
        assertThrows(IllegalArgumentException.class,
                () -> ArtifactRetentionPolicy.fromProperties(properties));
    }
}
