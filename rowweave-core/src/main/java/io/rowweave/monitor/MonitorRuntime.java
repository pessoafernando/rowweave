package io.rowweave.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Process-wide registry used by public client factories; NOOP unless enabled. */
public final class MonitorRuntime {

    private static final Logger LOG = LoggerFactory.getLogger(MonitorRuntime.class);
    private static final Object LOCK = new Object();
    private static volatile JobMonitorRegistry shared = JobMonitorRegistry.NOOP;
    private static volatile MonitorLauncher ownedLauncher;
    private static volatile int ownedPort;

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(
                MonitorRuntime::resetForTesting,
                "rowweave-monitor-runtime-shutdown"));
    }

    private MonitorRuntime() { }

    public static JobMonitorRegistry shared() {
        JobMonitorRegistry current = shared;
        if (current != JobMonitorRegistry.NOOP) return current;
        if (!Boolean.getBoolean("rowweave.monitor.enable")) {
            return JobMonitorRegistry.NOOP;
        }
        synchronized (LOCK) {
            if (shared != JobMonitorRegistry.NOOP) return shared;
            startOwned();
            return shared;
        }
    }

    public static void useForTesting(JobMonitorRegistry registry) {
        if (registry == null) {
            throw new IllegalArgumentException("registry is required");
        }
        resetForTesting();
        shared = registry;
    }

    public static void resetForTesting() {
        synchronized (LOCK) {
            MonitorLauncher launcher = ownedLauncher;
            ownedLauncher = null;
            ownedPort = 0;
            shared = JobMonitorRegistry.NOOP;
            if (launcher != null) {
                try {
                    launcher.stop();
                } catch (RuntimeException failure) {
                    LOG.debug("shared monitor HTTP stop failed", failure);
                }
            }
        }
    }

    private static void startOwned() {
        MonitorLauncher launcher = MonitorBootstrap.discover();
        if (launcher == null) {
            LOG.warn("rowweave.monitor.enable=true but no MonitorLauncher "
                    + "implementation is on the classpath; monitoring is disabled");
            return;
        }
        try {
            InMemoryJobMonitor registry = new InMemoryJobMonitor(
                    Integer.getInteger("rowweave.monitor.history", 200));
            ownedPort = launcher.start(
                    registry,
                    Integer.getInteger("rowweave.monitor.port", 0),
                    null,
                    Integer.getInteger("rowweave.monitor.ioThreads",
                            Math.max(2,
                                    Runtime.getRuntime().availableProcessors() / 2)));
            ownedLauncher = launcher;
            shared = registry;
        } catch (RuntimeException | LinkageError failure) {
            try {
                launcher.stop();
            } catch (RuntimeException | LinkageError closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            LOG.warn("shared monitor HTTP failed to start; monitoring is disabled", failure);
        }
    }
}
