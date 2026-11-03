package io.rowweave.deployment;

import java.util.Properties;

/** Process-level deployment budget validated together with the control resident budget. */
public final class DeploymentRuntimeConfig {
    public static final String MEMORY_BYTES_PROPERTY = "rowweave.deployment.memoryBytes";

    private final long deploymentBytes;

    private DeploymentRuntimeConfig(long deploymentBytes) {
        this.deploymentBytes = deploymentBytes;
    }

    public static DeploymentRuntimeConfig fromProperties(
            Properties properties, long controlResidentBytes, long maxHeapBytes) {
        DeploymentRuntimeConfig parsed = fromProcessProperties(properties);
        if (controlResidentBytes < 0L || maxHeapBytes <= 0L) {
            throw new IllegalArgumentException("resident budgets and max heap are invalid");
        }
        long ceiling = maxHeapBytes / 2L;
        if (controlResidentBytes > ceiling
                || parsed.deploymentBytes > ceiling - controlResidentBytes) {
            throw new IllegalArgumentException("control resident plus deployment memory must be <= half JVM max heap");
        }
        return parsed;
    }

    /** aggregate-process parser; the unified heap ledger owns the upper bound. */
    public static DeploymentRuntimeConfig fromProcessProperties(Properties properties) {
        if (properties == null) {
            throw new IllegalArgumentException("deployment properties are required");
        }
        String configured = properties.getProperty(MEMORY_BYTES_PROPERTY);
        long deploymentBytes = configured == null || configured.trim().isEmpty()
                ? DeploymentMemoryBudget.DEFAULT_BYTES : parsePositive(configured.trim());
        return new DeploymentRuntimeConfig(deploymentBytes);
    }

    public static DeploymentRuntimeConfig fromSystemProperties(long controlResidentBytes) {
        return fromProperties(System.getProperties(), controlResidentBytes,
                Runtime.getRuntime().maxMemory());
    }

    public long deploymentBytes() { return deploymentBytes; }

    public DeploymentMemoryBudget newBudget() {
        return DeploymentMemoryBudget.withCapacity(deploymentBytes);
    }

    private static long parsePositive(String value) {
        try {
            long parsed = Long.parseLong(value);
            if (parsed <= 0L) throw new NumberFormatException("non-positive");
            return parsed;
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException(MEMORY_BYTES_PROPERTY
                    + " must be a positive byte count", failure);
        }
    }
}
