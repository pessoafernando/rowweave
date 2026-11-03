package io.rowweave.run;

import java.nio.file.Paths;

/** Force succeeds and the process exits hard before the caller receives a response. */
public final class FileRunStateStoreCrashProbe {
    private FileRunStateStoreCrashProbe() { }

    public static void main(String[] args) {
        FileRunStateStore store = new FileRunStateStore(
                Paths.get(args[0]), () -> Runtime.getRuntime().halt(91));
        store.apply(RunCommand.createRun(
                "command-create", "run", "definition", "artifact", 2, 0L, 1L));
        throw new AssertionError("halt injection did not run");
    }
}
