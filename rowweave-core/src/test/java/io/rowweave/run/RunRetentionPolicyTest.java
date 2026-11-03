package io.rowweave.run;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RunRetentionPolicyTest {

    @Test
    void productionDefaultsAndPropertiesRemainWithinHardBounds() {
        RunRetentionPolicy defaults = RunRetentionPolicy.fromProperties(new Properties());
        assertEquals(30L * 24L * 60L * 60L * 1000L,
                defaults.terminalRetentionMs());
        assertEquals(4096, defaults.maxEntriesPerShard());
        assertEquals(128, defaults.scanPageSize());
        assertEquals(32, defaults.maxDeletesPerPass());
        assertEquals(60_000L, defaults.maintenanceIntervalMs());

        Properties configured = new Properties();
        configured.setProperty("rowweave.master.runRetention.terminalMs", "300000");
        configured.setProperty("rowweave.master.runRetention.maxEntriesPerShard", "17");
        configured.setProperty("rowweave.master.runRetention.scanPageSize", "11");
        configured.setProperty("rowweave.master.runRetention.maxDeletesPerPass", "7");
        configured.setProperty("rowweave.master.runRetention.maintenanceIntervalMs", "100");
        RunRetentionPolicy policy = RunRetentionPolicy.fromProperties(configured);
        assertEquals(300_000L, policy.terminalRetentionMs());
        assertEquals(17, policy.maxEntriesPerShard());
        assertEquals(11, policy.scanPageSize());
        assertEquals(7, policy.maxDeletesPerPass());
        assertEquals(100L, policy.maintenanceIntervalMs());
    }

    @Test
    void productionParserCannotBypassRetentionOrWorkBounds() {
        assertInvalid("rowweave.master.runRetention.terminalMs", "299999");
        assertInvalid("rowweave.master.runRetention.terminalMs",
                Long.toString(10L * 365L * 24L * 60L * 60L * 1000L + 1L));
        assertInvalid("rowweave.master.runRetention.maxEntriesPerShard", "0");
        assertInvalid("rowweave.master.runRetention.maxEntriesPerShard", "4097");
        assertInvalid("rowweave.master.runRetention.scanPageSize", "1001");
        assertInvalid("rowweave.master.runRetention.maxDeletesPerPass", "0");
        assertInvalid("rowweave.master.runRetention.maintenanceIntervalMs", "99");
    }

    @Test
    void authorityTimeNeverExpiresFutureOrRolledBackTerminal() {
        RunRetentionPolicy policy = RunRetentionPolicy.testing(10L, 4, 2, 1, 1L);
        assertFalse(policy.due(99L, 100L));
        assertFalse(policy.due(109L, 100L));
        assertTrue(policy.due(110L, 100L));
        assertTrue(policy.due(Long.MAX_VALUE, 1L));
        assertEquals(100L, RunRetentionPolicy.clampAuthorityTime(90L, 100L));
        assertEquals(101L, RunRetentionPolicy.clampAuthorityTime(101L, 100L));
        assertThrows(IllegalArgumentException.class,
                () -> policy.due(-1L, 1L));
        assertThrows(IllegalArgumentException.class,
                () -> policy.due(1L, 0L));
    }

    private static void assertInvalid(String key, String value) {
        Properties properties = new Properties();
        properties.setProperty(key, value);
        assertThrows(IllegalArgumentException.class,
                () -> RunRetentionPolicy.fromProperties(properties));
    }
}
