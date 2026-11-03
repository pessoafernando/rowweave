package io.rowweave.monitor;

/**
 * Monitor service launcher SPI:core defines the interface,**core does not depend on the monitor module**(zero-HTTP invariant).
 * rowweave-monitor module provides the implementation and registers via {@code META-INF/services/io.rowweave.monitor.MonitorLauncher};
 * ClusterMain (inside core) uses {@link java.util.ServiceLoader} to discover and start; starts only when the monitor JAR is on classpath, skipped when absent.
 */
public interface MonitorLauncher {

    /**
     * Start monitor HTTP server,binding to {@code port}(0=random),reading from {@code registry} for observation data rendering.
     * @param registry observation data source (same instance injected by in-process Master or local execution)
     * @param port listening port (0=random)
     * @param leaderLocator resolves current leader monitor address in cluster mode (standby redirect; null for single-machine)
     * @return actual listening port
     */
    int start(JobMonitorRegistry registry, int port, LeaderAddressResolver leaderLocator);

    /** ClusterMain path with a process-preflighted, explicit Netty worker count. */
    default int start(JobMonitorRegistry registry, int port,
                      LeaderAddressResolver leaderLocator, int ioThreads) {
        if (ioThreads < 1 || ioThreads > 64) {
            throw new IllegalArgumentException(
                    "monitor ioThreads must be in [1,64]");
        }
        return start(registry, port, leaderLocator);
    }

    /** Stops the monitor service (releases its port and threads). */
    void stop();

    /** Cluster leader address resolution(standby monitor redirects to leader). null for single-machine mode (no redirect).  */
    interface LeaderAddressResolver {
        /** whether currently leader (true -> local has data; false -> needs redirect).  */
        boolean isLeader();
        /** Current leader host (the redirect target); null when there is no leader. */
        String leaderHost();
    }
}
