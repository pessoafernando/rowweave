package io.rowweave.incremental;

import java.nio.file.Paths;

/** Hard process-loss probe for the File authority force-to-ACK window. */
public final class IncrementalFileAuthorityCrashProbe {
    private static final int HALT_CODE = 96;

    private IncrementalFileAuthorityCrashProbe() {
    }

    public static void main(String[] args) throws Exception {
        java.nio.file.Path root = Paths.get(args[0]);
        IncrementalFileAuthorityJointTerminalContractTest.Fixture fixture =
                IncrementalFileAuthorityJointTerminalContractTest.prepared(root);
        IncrementalAuthorityMutation mutation =
                IncrementalFileAuthorityJointTerminalContractTest.successMutation(
                        fixture, "process-crash-joint-success");
        FileIncrementalAuthorityStore crashing =
                new FileIncrementalAuthorityStore(
                        root.resolve("authority"), phase -> {
                    if (phase
                            == IncrementalJointSuccessPhase.AFTER_COMMIT_BEFORE_ACK) {
                        Runtime.getRuntime().halt(HALT_CODE);
                    }
                });
        crashing.apply(mutation);
        throw new AssertionError("halt injection did not run");
    }
}
