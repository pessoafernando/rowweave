package io.rowweave.run;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/** Probe that contends from a separate JVM for the last slot of one lifecycle shard. */
public final class FileRunLifecycleContenderProbe {
    private FileRunLifecycleContenderProbe() { }

    public static void main(String[] args) throws Exception {
        Path base = Paths.get(args[0]);
        Path gate = Paths.get(args[1]);
        String runId = args[2];
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15L);
        while (!Files.exists(gate)) {
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException("start gate timeout");
            }
            Thread.sleep(5L);
        }
        FileRunStateStore store = new FileRunStateStore(
                base, () -> { }, 1, () -> 1_000L);
        RunDecision decision = store.apply(RunCommand.createRun(
                "create-" + runId, runId, "definition", "artifact-" + runId,
                1, 0L, 1L));
        System.out.println(decision.code().name());
    }
}
