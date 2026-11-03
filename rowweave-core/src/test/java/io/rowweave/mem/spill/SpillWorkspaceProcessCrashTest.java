package io.rowweave.mem.spill;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SpillWorkspaceProcessCrashTest {

    private static final long MIB = 1024L * 1024;
    private static final String CHILD = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    private static final String CHILD_JOB = "11111111111111111111111111111111";
    private static final String REAPER = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    @TempDir
    Path temp;

    @Test
    void realRuntimeHaltLeavesTrustedOrphanThatNextProcessReclaims() throws Exception {
        Child child = launch("halt");
        try {
            Path orphan = child.awaitInstance();

            assertTrue(child.process.waitFor(15, TimeUnit.SECONDS));
            assertEquals(47, child.process.exitValue());
            assertTrue(Files.exists(orphan));

            SpillWorkspaceManager reaper = openReaper();

            assertFalse(Files.exists(orphan));
            assertTrue(reaper.lastScavengeReport().deletedEntries() >= 6);
            reaper.close();
        } finally {
            if (child.process.isAlive()) {
                child.process.destroyForcibly();
                child.process.waitFor(10, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void liveChildLeaseCannotBeCrossedButBecomesReclaimableAfterDeath() throws Exception {
        Child child = launch("live");
        try {
            Path liveInstance = child.awaitInstance();
            assertTrue(child.process.isAlive(), "live child exited before observation");
            assertTrue(Files.exists(liveInstance), "live child did not retain its instance");
            SpillWorkspaceManager observer = openReaper();

            assertTrue(Files.exists(liveInstance));
            assertTrue(observer.lastScavengeReport().liveInstances() >= 1);

            child.process.destroyForcibly();
            assertTrue(child.process.waitFor(10, TimeUnit.SECONDS));
            for (int pass = 0; pass < 3 && Files.exists(liveInstance); pass++) {
                observer.scavengeNowForTests();
            }

            assertFalse(Files.exists(liveInstance));
            observer.close();
        } finally {
            if (child.process.isAlive()) {
                child.process.destroyForcibly();
                child.process.waitFor(10, TimeUnit.SECONDS);
            }
        }
    }

    private Child launch(String mode) throws Exception {
        Path base = temp.toRealPath().resolve("spill");
        Process process = new ProcessBuilder(io.rowweave.test.ForkedJvm.command(
                SpillWorkspaceCrashProbe.class.getName(), base.toString(), mode,
                CHILD, CHILD_JOB))
                .redirectErrorStream(true).start();
        return new Child(process, base);
    }

    private SpillWorkspaceManager openReaper() throws Exception {
        Path base = temp.toRealPath().resolve("spill");
        SpillWorkspaceConfig config = SpillWorkspaceConfig.forTests(
                base, 8 * MIB, 4 * MIB, 0, 0, 64, 32, 0);
        return SpillWorkspaceManager.openForTests(config, System::currentTimeMillis,
                identities(REAPER), ignored -> Long.MAX_VALUE);
    }

    private static Supplier<String> identities(String... values) {
        Queue<String> queue = new ArrayDeque<>(Arrays.asList(values));
        return () -> {
            String value = queue.poll();
            if (value == null) throw new AssertionError("identity supplier exhausted");
            return value;
        };
    }

    private static final class Child {
        private final Process process;
        private final Path base;
        private final LinkedBlockingQueue<String> lines = new LinkedBlockingQueue<>();

        private Child(Process process, Path base) {
            this.process = process;
            this.base = base;
            Thread drainer = new Thread(() -> {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                        process.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) lines.offer(line);
                } catch (Exception ignored) {
                    // Assertions below retain the child exit/output boundary.
                }
            }, "spill-crash-probe-output");
            drainer.setDaemon(true);
            drainer.start();
        }

        private Path awaitInstance() throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            String line;
            do {
                long remaining = deadline - System.nanoTime();
                line = remaining <= 0 ? null : lines.poll(remaining, TimeUnit.NANOSECONDS);
                if (line != null && line.matches("INSTANCE=instance-[0-9a-f]{32}")) {
                    return base.resolve("instances")
                            .resolve(line.substring("INSTANCE=".length()));
                }
            } while (line != null);
            assertNotNull(line, "child did not publish its owned instance");
            throw new AssertionError("invalid child instance line: " + line);
        }
    }
}
