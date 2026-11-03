package io.rowweave.deployment;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** control and deployment resident budgets share a heap/2 ceiling. */
class DeploymentRuntimeConfigTest {

    @Test
    void defaultAndExplicitDeploymentBudgetAreValidatedWithControlBudget() {
        long mib = 1024L * 1024L;
        DeploymentRuntimeConfig defaults = DeploymentRuntimeConfig.fromProperties(
                new Properties(), 256L * mib, 1024L * mib);
        assertEquals(256L * mib, defaults.deploymentBytes());

        Properties explicit = new Properties();
        explicit.setProperty("rowweave.deployment.memoryBytes", Long.toString(64L * mib));
        assertEquals(64L * mib, DeploymentRuntimeConfig.fromProperties(
                explicit, 224L * mib, 1024L * mib).deploymentBytes());
    }

    @Test
    void invalidOrCombinedOverHalfHeapFailsClosedWithoutOverflow() {
        long mib = 1024L * 1024L;
        Properties tooLarge = new Properties();
        tooLarge.setProperty("rowweave.deployment.memoryBytes", Long.toString(289L * mib));
        assertThrows(IllegalArgumentException.class,
                () -> DeploymentRuntimeConfig.fromProperties(
                        tooLarge, 224L * mib, 1024L * mib));

        Properties invalid = new Properties();
        invalid.setProperty("rowweave.deployment.memoryBytes", "0");
        assertThrows(IllegalArgumentException.class,
                () -> DeploymentRuntimeConfig.fromProperties(invalid, 1L, Long.MAX_VALUE));
        invalid.setProperty("rowweave.deployment.memoryBytes", Long.toString(Long.MAX_VALUE));
        assertThrows(IllegalArgumentException.class,
                () -> DeploymentRuntimeConfig.fromProperties(invalid, Long.MAX_VALUE, Long.MAX_VALUE));
    }
}
