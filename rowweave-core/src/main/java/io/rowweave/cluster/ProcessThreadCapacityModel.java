package io.rowweave.cluster;

import io.rowweave.cluster.rpc.RpcAdmissionConfig;

import java.util.Properties;

/** pure, conservative upper bound for RowWeave-created Java threads. */
final class ProcessThreadCapacityModel {

    static final String MAX_STAGE_THREADS = "rowweave.worker.maxStageThreads";
    static final String MONITOR_IO_THREADS = "rowweave.monitor.ioThreads";
    static final String ASSIGN_PARALLELISM = "rowweave.cluster.assign.parallelism";
    static final String PROVIDER_ACTION_PARALLELISM =
            "rowweave.provider.action.parallelism";
    static final int CURATOR_SESSION_THREAD_RESERVE = 16;

    private ProcessThreadCapacityModel() {
    }

    static Snapshot estimate(Properties properties,
                             boolean masterEnabled,
                             boolean workerEnabled,
                             boolean zkEnabled,
                             RpcAdmissionConfig rpcAdmission,
                             DataPlaneConfig dataPlaneConfig,
                             int workerSlots,
                             int maxStageThreads,
                             int activeProcessorCount) {
        if (properties == null || rpcAdmission == null) {
            throw configurationFailure("thread capacity inputs are required", null);
        }
        if (activeProcessorCount < 1 || activeProcessorCount > 64) {
            throw configurationFailure("active processor count must be in [1,64]", null);
        }
        if (workerEnabled && (dataPlaneConfig == null || workerSlots < 1
                || maxStageThreads < 1)) {
            throw configurationFailure(
                    "worker thread capacity requires data plane, slots and maxStageThreads", null);
        }

        // ClusterMain/main, role shutdown hook and the lazily registered spill-workspace hook.
        long process = 3L;
        long assignmentParallelism = strictInt(
                properties, ASSIGN_PARALLELISM, 4, 1, 100_000);
        long master = masterEnabled
                ? exactAdd(exactAdd(rpcAdmission.maxConnections(), 5L),
                        exactMultiply(2L, assignmentParallelism))
                : 0L;

        long workerExecution = workerEnabled
                ? exactMultiply(workerSlots, exactAdd(2L, maxStageThreads))
                : 0L;
        long workerFacilities = 0L;
        long handshake = 0L;
        if (workerEnabled) {
            // RPC accept/connections + independent heartbeat and maintenance
            // timers + the process-wide fixed provider action lanes + one
            // single-shot poison-fence closer.
            long providerActions = strictInt(properties,
                    PROVIDER_ACTION_PARALLELISM,
                    Math.min(workerSlots,
                            io.rowweave.internal.ProviderActionSupervisor
                                    .MAX_CAPACITY),
                    1,
                    io.rowweave.internal.ProviderActionSupervisor
                            .MAX_CAPACITY);
            workerFacilities = exactAdd(
                    exactAdd(rpcAdmission.maxConnections(), 3L),
                    exactAdd(providerActions, 1L));
            if (dataPlaneConfig.muxEnabled()) {
                // data boss + explicit I/O + ingress + mux client + Netty global executor.
                workerFacilities = exactAdd(workerFacilities,
                        exactAdd(dataPlaneConfig.ioThreads(),
                                exactAdd(dataPlaneConfig.ingressAdmission()
                                                .maxIngressHandlers(), 3L)));
                if (dataPlaneConfig.protocol()
                        == DataPlaneConfig.Protocol.SHUFFLE_MTLS_V1) {
                    // inbound TLS setup plus both delegated-task pools.
                    handshake = exactAdd(dataPlaneConfig.inboundHandshakeSlots(),
                            dataPlaneConfig.maxConcurrentHandshakes());
                }
            }
        }

        long monitor = masterEnabled && strictBoolean(
                properties, "rowweave.monitor.enable", false)
                ? exactAdd(1L, strictInt(properties, MONITOR_IO_THREADS,
                        Math.max(2, activeProcessorCount / 2), 1, 64))
                : 0L;
        long zk = zkEnabled
                ? exactMultiply((masterEnabled ? 1L : 0L)
                                + (workerEnabled ? 1L : 0L),
                        CURATOR_SESSION_THREAD_RESERVE)
                : 0L;
        long deploymentProbe = deploymentCount(properties) > 0 ? 1L : 0L;

        return new Snapshot(process, master, workerFacilities,
                workerExecution, handshake, monitor, zk, deploymentProbe);
    }

    private static int deploymentCount(Properties properties) {
        String configured = properties.getProperty("rowweave.deployment.count");
        if (configured == null) return 0;
        return strictInt(properties, "rowweave.deployment.count", 0, 0, 4096);
    }

    private static boolean strictBoolean(Properties properties, String name,
                                         boolean defaultValue) {
        String configured = properties.getProperty(name);
        if (configured == null) return defaultValue;
        if ("true".equals(configured)) return true;
        if ("false".equals(configured)) return false;
        throw configurationFailure(name + " must be exactly true or false", null);
    }

    private static int strictInt(Properties properties, String name,
                                 int defaultValue, int minimum, int maximum) {
        String configured = properties.getProperty(name);
        if (configured == null) return defaultValue;
        if (configured.isEmpty() || !configured.equals(configured.trim())
                || (configured.length() > 1 && configured.charAt(0) == '0')) {
            throw configurationFailure(name + " must be a canonical decimal", null);
        }
        for (int i = configured.charAt(0) == '-' ? 1 : 0;
             i < configured.length(); i++) {
            char value = configured.charAt(i);
            if (value < '0' || value > '9') {
                throw configurationFailure(name + " must be a canonical decimal", null);
            }
        }
        try {
            long parsed = Long.parseLong(configured);
            if (parsed < minimum || parsed > maximum) {
                throw configurationFailure(name + " must be in [" + minimum
                        + ',' + maximum + ']', null);
            }
            return (int) parsed;
        } catch (NumberFormatException invalid) {
            throw configurationFailure(name + " is outside the signed long range", invalid);
        }
    }

    private static long exactAdd(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException overflow) {
            throw configurationFailure("thread capacity arithmetic overflow", overflow);
        }
    }

    private static long exactMultiply(long left, long right) {
        try {
            return Math.multiplyExact(left, right);
        } catch (ArithmeticException overflow) {
            throw configurationFailure("thread capacity arithmetic overflow", overflow);
        }
    }

    private static ProcessCapacityException configurationFailure(
            String message, Throwable cause) {
        return cause == null
                ? new ProcessCapacityException(
                        ProcessCapacityException.Code.CONFIGURATION, message)
                : new ProcessCapacityException(
                        ProcessCapacityException.Code.CONFIGURATION, message, cause);
    }

    static final class Snapshot {
        private final long processThreads;
        private final long masterThreads;
        private final long workerFacilityThreads;
        private final long workerExecutionThreads;
        private final long workerHandshakeThreads;
        private final long monitorThreads;
        private final long zkVendorThreads;
        private final long deploymentProbeThreads;
        private final long totalThreads;

        private Snapshot(long processThreads, long masterThreads,
                         long workerFacilityThreads, long workerExecutionThreads,
                         long workerHandshakeThreads, long monitorThreads,
                         long zkVendorThreads, long deploymentProbeThreads) {
            this.processThreads = processThreads;
            this.masterThreads = masterThreads;
            this.workerFacilityThreads = workerFacilityThreads;
            this.workerExecutionThreads = workerExecutionThreads;
            this.workerHandshakeThreads = workerHandshakeThreads;
            this.monitorThreads = monitorThreads;
            this.zkVendorThreads = zkVendorThreads;
            this.deploymentProbeThreads = deploymentProbeThreads;
            long total = exactAdd(processThreads, masterThreads);
            total = exactAdd(total, workerFacilityThreads);
            total = exactAdd(total, workerExecutionThreads);
            total = exactAdd(total, workerHandshakeThreads);
            total = exactAdd(total, monitorThreads);
            total = exactAdd(total, zkVendorThreads);
            this.totalThreads = exactAdd(total, deploymentProbeThreads);
        }

        long processThreads() { return processThreads; }
        long masterThreads() { return masterThreads; }
        long workerFacilityThreads() { return workerFacilityThreads; }
        long workerExecutionThreads() { return workerExecutionThreads; }
        long workerHandshakeThreads() { return workerHandshakeThreads; }
        long monitorThreads() { return monitorThreads; }
        long zkVendorThreads() { return zkVendorThreads; }
        long deploymentProbeThreads() { return deploymentProbeThreads; }
        long totalThreads() { return totalThreads; }
    }
}
