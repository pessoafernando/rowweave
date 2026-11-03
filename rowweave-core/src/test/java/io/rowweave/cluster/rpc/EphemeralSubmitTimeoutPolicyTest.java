package io.rowweave.cluster.rpc;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** submit execution/cleanup budgets are strict and share one checked policy. */
class EphemeralSubmitTimeoutPolicyTest {

    @Test
    void clientAndLegacyServerDefaultsAreFiniteAndClientWaitsForCleanup() {
        EphemeralSubmitTimeoutPolicy client =
                EphemeralSubmitTimeoutPolicy.clientFromProperties(new Properties());
        EphemeralSubmitTimeoutPolicy master =
                EphemeralSubmitTimeoutPolicy.masterFromProperties(new Properties());

        assertBudget(client.budget(), 600_000L, 60_000L);
        assertEquals(665_000, client.clientReadTimeoutMillis());
        assertBudget(master.resolve(null), 600_000L, 60_000L,
                "legacy request must receive the fail-closed server defaults");
    }

    @Test
    void configuredClientBudgetUsesExactCheckedReadDeadline() {
        Properties properties = new Properties();
        properties.setProperty("rowweave.cluster.submitTimeoutMs", "12000");
        properties.setProperty("rowweave.cluster.submitCleanupGraceMs", "3000");

        EphemeralSubmitTimeoutPolicy policy =
                EphemeralSubmitTimeoutPolicy.clientFromProperties(properties);

        assertBudget(policy.budget(), 12_000L, 3_000L);
        assertEquals(20_000, policy.clientReadTimeoutMillis());
    }

    @Test
    void masterAppliesLegacyHardLimitToDefaultsAndExplicitRequests() {
        Properties properties = new Properties();
        properties.setProperty("rowweave.cluster.ephemeralJobTimeoutMs", "9000");
        properties.setProperty("rowweave.cluster.ephemeralCleanupTimeoutMs", "3000");
        properties.setProperty("rowweave.cluster.jobHardLimitMs", "5000");
        EphemeralSubmitTimeoutPolicy policy =
                EphemeralSubmitTimeoutPolicy.masterFromProperties(properties);

        assertBudget(policy.resolve(null), 5_000L, 3_000L);
        assertBudget(policy.resolve(
                EphemeralSubmitTimeoutPolicy.Budget.of(7_000L, 2_000L)),
                5_000L, 2_000L);
        assertBudget(policy.resolve(
                EphemeralSubmitTimeoutPolicy.Budget.of(3_000L, 2_000L)),
                3_000L, 2_000L);
    }

    @Test
    void acceptsLargestPairThatStillFitsSocketTimeout() {
        EphemeralSubmitTimeoutPolicy.Budget budget =
                EphemeralSubmitTimeoutPolicy.Budget.of(2_147_000_000L, 1_000L);
        assertBudget(budget, 2_147_000_000L, 1_000L);
        assertEquals(2_147_006_000,
                EphemeralSubmitTimeoutPolicy.clientReadTimeoutMillis(budget));
    }

    @Test
    void rejectsNonDecimalOutOfRangeAndCrossFieldOverflow() {
        String[] malformed = {"", " 1000", "+1000", "1_000", "-1000", "0", "999",
                "2147000001", "9223372036854775808"};
        for (String value : malformed) {
            Properties properties = new Properties();
            properties.setProperty("rowweave.cluster.submitTimeoutMs", value);
            assertThrows(IllegalArgumentException.class,
                    () -> EphemeralSubmitTimeoutPolicy.clientFromProperties(properties), value);
        }

        Properties overflow = new Properties();
        overflow.setProperty("rowweave.cluster.submitTimeoutMs", "2147000000");
        overflow.setProperty("rowweave.cluster.submitCleanupGraceMs", "2147000000");
        assertThrows(IllegalArgumentException.class,
                () -> EphemeralSubmitTimeoutPolicy.clientFromProperties(overflow));

        Properties badHardLimit = new Properties();
        badHardLimit.setProperty("rowweave.cluster.jobHardLimitMs", "1");
        assertThrows(IllegalArgumentException.class,
                () -> EphemeralSubmitTimeoutPolicy.masterFromProperties(badHardLimit));
    }

    private static void assertBudget(EphemeralSubmitTimeoutPolicy.Budget budget,
                                     long execution, long cleanup) {
        assertEquals(execution, budget.executionTimeoutMs());
        assertEquals(cleanup, budget.cleanupTimeoutMs());
    }

    private static void assertBudget(EphemeralSubmitTimeoutPolicy.Budget budget,
                                     long execution, long cleanup, String message) {
        assertEquals(execution, budget.executionTimeoutMs(), message);
        assertEquals(cleanup, budget.cleanupTimeoutMs(), message);
    }
}
