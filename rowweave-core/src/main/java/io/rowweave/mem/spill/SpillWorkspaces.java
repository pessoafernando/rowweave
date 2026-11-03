package io.rowweave.mem.spill;

/** lazy process-wide workspace authority shared by local and Worker execution. */
public final class SpillWorkspaces {

    private static final Object LOCK = new Object();
    private static volatile SpillWorkspaceManager processManager;

    private SpillWorkspaces() {
    }

    /** Opens one job workspace, initializing the process manager only on the first breaker job. */
    public static SpillJobWorkspace openJob() {
        SpillWorkspaceManager manager = processManager;
        if (manager == null) {
            synchronized (LOCK) {
                manager = processManager;
                if (manager == null) {
                    manager = SpillWorkspaceManager.open(
                            SpillWorkspaceConfig.fromSystemProperties());
                    final SpillWorkspaceManager owned = manager;
                    try {
                        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                            try {
                                owned.close();
                            } catch (RuntimeException | Error ignored) {
                                // Best effort only. Crash recovery must never rely on this hook.
                            }
                        }, "rowweave-spill-workspace-close"));
                    } catch (RuntimeException | Error hookFailure) {
                        try {
                            manager.close();
                        } catch (RuntimeException | Error cleanup) {
                            hookFailure.addSuppressed(cleanup);
                        }
                        throw hookFailure;
                    }
                    processManager = manager;
                }
            }
        }
        return manager.openJob();
    }
}
