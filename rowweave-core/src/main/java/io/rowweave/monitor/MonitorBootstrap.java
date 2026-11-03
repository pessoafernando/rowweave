package io.rowweave.monitor;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * Monitor bootstrap:core uses {@link ServiceLoader} to discover {@link MonitorLauncher} implementations
 * (provided by the rowweave-monitor module), **core does not import the monitor module**(zero-HTTP invariant). starts only when monitor JAR is on classpath,null when absent.
 * ClusterMain optionally starts monitoring via this class: with the monitor dependency it starts the HTTP server; without it, silently skipped (no error).
 */
public final class MonitorBootstrap {

    private MonitorBootstrap() { }

    /** Discovers a MonitorLauncher implementation; null when absent, meaning the monitor JAR is not on the classpath. */
    public static MonitorLauncher discover() {
        try {
            ServiceLoader<MonitorLauncher> loader = ServiceLoader.load(MonitorLauncher.class);
            Iterator<MonitorLauncher> it = loader.iterator();
            if (it.hasNext()) {
                return it.next();
            }
        } catch (Throwable ignored) {
            // Discovery failure (missing class / load exception) treated as no monitoring, silently skipped
        }
        return null;
    }
}
