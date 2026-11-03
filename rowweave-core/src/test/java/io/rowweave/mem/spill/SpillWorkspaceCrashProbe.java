package io.rowweave.mem.spill;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.function.Supplier;

/** Child-JVM fixture for proving OS lease behavior and shutdown-hook-independent recovery. */
public final class SpillWorkspaceCrashProbe {

    private static final long MIB = 1024L * 1024;
    private static volatile SpillWorkspaceManager liveManager;

    private SpillWorkspaceCrashProbe() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) throw new IllegalArgumentException("expected base/mode/instance/job");
        Path base = Paths.get(args[0]).toAbsolutePath().normalize();
        SpillWorkspaceConfig config = SpillWorkspaceConfig.forTests(
                base, 8 * MIB, 4 * MIB, 0, 0, 64, 32, 0);
        SpillWorkspaceManager manager = SpillWorkspaceManager.openForTests(
                config, System::currentTimeMillis, identities(args[2], args[3]),
                ignored -> Long.MAX_VALUE);
        SpillJobWorkspace job = manager.openJob();
        BoundedReplayRun run = job.newSpillStore().createRun();
        run.append(new byte[]{1, 2, 3, 4});
        run.finishWriting();

        if ("live".equals(args[1])) liveManager = manager;
        System.out.println("INSTANCE=" + manager.instanceDirectory().getFileName());
        System.out.flush();
        if ("halt".equals(args[1])) {
            Runtime.getRuntime().halt(47);
        }
        if (!"live".equals(args[1])) throw new IllegalArgumentException("unknown probe mode");
        try {
            Thread.sleep(60_000L);
        } finally {
            liveManager.close();
            liveManager = null;
        }
    }

    private static Supplier<String> identities(String... values) {
        Queue<String> queue = new ArrayDeque<>(Arrays.asList(values));
        return () -> {
            String value = queue.poll();
            if (value == null) throw new AssertionError("identity supplier exhausted");
            return value;
        };
    }
}
