package io.rowweave.run;

import java.nio.file.Path;
import java.nio.file.Paths;

/** Cuts off File retention purge publication inside a real child JVM. */
public final class FileRunPurgeProcessCrashProbe {
    private FileRunPurgeProcessCrashProbe() { }

    public static void main(String[] args) {
        Path base = Paths.get(args[0]);
        String cut = args[1];
        FileRunStateStore store = new FileRunStateStore(base, () -> { },
                RunLifecycleCatalogKernel.MAX_ENTRIES_PER_SHARD,
                System::currentTimeMillis, new FileRunStateStore.PurgeObserver() {
                    @Override
                    public void afterMove() {
                        if ("move".equals(cut)) Runtime.getRuntime().halt(92);
                    }

                    @Override
                    public void afterLifecycleDelete() {
                        if ("lifecycle".equals(cut)) Runtime.getRuntime().halt(93);
                    }
                });
        RunLifecycleRef ref = store.lifecycle("run");
        store.purge(ref.retentionToken(), 1);
        throw new AssertionError("purge crash cut was not reached");
    }
}
