package io.rowweave.monitor.http;

import io.rowweave.monitor.JobMonitorRegistry;
import io.rowweave.monitor.MonitorLauncher;

/**
 * {@link MonitorLauncher} Netty implementation. Registered via {@code META-INF/services},
 * ClusterMain (core) uses {@link java.util.ServiceLoader} to discover and start; **core does not depend on the monitor module**,
 * monitoring starts only when the rowweave-monitor JAR is on classpath, skipped when absent.
 */
public final class NettyMonitorLauncher implements MonitorLauncher {

    private MonitorHttpServer server;

    @Override
    public int start(JobMonitorRegistry registry, int port, LeaderAddressResolver leaderLocator) {
        server = new MonitorHttpServer(registry, port, leaderLocator);
        return server.start();
    }

    @Override
    public int start(JobMonitorRegistry registry, int port,
                     LeaderAddressResolver leaderLocator, int ioThreads) {
        server = new MonitorHttpServer(registry, port, leaderLocator, ioThreads);
        return server.start();
    }

    @Override
    public void stop() {
        if (server != null) {
            server.stop();
        }
    }
}
