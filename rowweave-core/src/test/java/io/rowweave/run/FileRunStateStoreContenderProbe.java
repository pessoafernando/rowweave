package io.rowweave.run;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/** Probe that contends from a separate JVM for the same expected revision. */
public final class FileRunStateStoreContenderProbe {
    private FileRunStateStoreContenderProbe() { }

    public static void main(String[] args) throws Exception {
        Path base = Paths.get(args[0]);
        Path gate = Paths.get(args[1]);
        int number = Integer.parseInt(args[2]);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15L);
        while (!Files.exists(gate)) {
            if (System.nanoTime() >= deadline) throw new IllegalStateException("start gate timeout");
            Thread.sleep(5L);
        }
        RunDecision decision = new FileRunStateStore(base).apply(RunCommand.createAttempt(
                "process-attempt-" + number, "run", 1L,
                "attempt-" + number, "publication-" + number, 2L));
        System.out.println(decision.code().name());
    }
}
