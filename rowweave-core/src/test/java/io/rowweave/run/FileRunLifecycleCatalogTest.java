package io.rowweave.run;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FileRunLifecycleCatalogTest {
    @TempDir Path temp;

    @Test
    void lifecycleSurvivesReopenAndUsesAuthorityClock() {
        AtomicLong clock = new AtomicLong(1_000L);
        FileRunStateStore first = store(16, clock);
        assertEquals(RunDecision.Code.ACCEPTED,
                create(first, "run", Long.MAX_VALUE - 1L).code());
        assertEquals(RunDecision.Code.ACCEPTED,
                first.apply(RunCommand.cancelReadyRun(
                        "cancel", "run", 1L, Long.MAX_VALUE - 1L,
                        "CANCELED", "done")).code());

        RunLifecycleRef terminal = ((RunLifecycleCatalog) first).lifecycle("run");
        assertEquals(RunLifecycleState.TERMINAL, terminal.state());
        assertEquals(1_000L, terminal.terminalAtMillis());
        assertEquals(2L, terminal.revision());

        clock.set(500L);
        FileRunStateStore reopened = store(16, clock);
        RunLifecycleRef restored = ((RunLifecycleCatalog) reopened).lifecycle("run");
        assertEquals(RunLifecycleState.TERMINAL, restored.state());
        assertEquals(1_000L, restored.terminalAtMillis());
        assertEquals(RunStatus.CANCELED, reopened.get("run").status());
    }

    @Test
    void sameShardConcurrentCreateNeverExceedsCapacity() throws Exception {
        List<String> ids = sameShardIds(2);
        FileRunStateStore left = store(1, new AtomicLong(1_000L));
        FileRunStateStore right = store(1, new AtomicLong(1_000L));
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<RunDecision> a = pool.submit(() -> {
                start.await();
                return create(left, ids.get(0), 1L);
            });
            Future<RunDecision> b = pool.submit(() -> {
                start.await();
                return create(right, ids.get(1), 1L);
            });
            start.countDown();
            int accepted = 0;
            int limited = 0;
            for (RunDecision decision : new RunDecision[] { a.get(), b.get() }) {
                if (decision.code() == RunDecision.Code.ACCEPTED) accepted++;
                if (decision.code() == RunDecision.Code.LIMIT_EXCEEDED) limited++;
            }
            assertEquals(1, accepted);
            assertEquals(1, limited);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void independentProcessesAlsoShareTheLifecycleShardCapacityLock() throws Exception {
        List<String> ids = sameShardIds(2);
        Path gate = temp.resolve("lifecycle-start.gate");
        Path firstOutput = temp.resolve("lifecycle-first.out");
        Path secondOutput = temp.resolve("lifecycle-second.out");
        Process first = lifecycleContender(gate, firstOutput, ids.get(0));
        Process second = lifecycleContender(gate, secondOutput, ids.get(1));
        Files.write(gate, new byte[] { 1 }, StandardOpenOption.CREATE_NEW);

        org.junit.jupiter.api.Assertions.assertTrue(first.waitFor(20, TimeUnit.SECONDS));
        org.junit.jupiter.api.Assertions.assertTrue(second.waitFor(20, TimeUnit.SECONDS));
        assertEquals(0, first.exitValue(), read(firstOutput));
        assertEquals(0, second.exitValue(), read(secondOutput));
        List<String> outcomes = Arrays.asList(
                read(firstOutput).trim(), read(secondOutput).trim());
        assertEquals(1L, outcomes.stream().filter("ACCEPTED"::equals).count(),
                outcomes.toString());
        assertEquals(1L, outcomes.stream().filter("LIMIT_EXCEEDED"::equals).count(),
                outcomes.toString());
    }

    @Test
    void terminalKeepsCapacityAndMissingOrCorruptLifecycleFailsClosed() throws Exception {
        List<String> ids = sameShardIds(2);
        FileRunStateStore store = store(1, new AtomicLong(1_000L));
        assertEquals(RunDecision.Code.ACCEPTED, create(store, ids.get(0), 1L).code());
        assertEquals(RunDecision.Code.ACCEPTED,
                store.apply(RunCommand.cancelReadyRun(
                        "cancel", ids.get(0), 1L, 2L, "CANCELED", "done")).code());
        assertEquals(RunDecision.Code.LIMIT_EXCEEDED,
                create(store, ids.get(1), 1L).code());
        assertNull(store.get(ids.get(1)));

        Path lifecycle = store.lifecyclePathForTest(ids.get(0));
        Files.delete(lifecycle);
        assertThrows(IllegalStateException.class, () -> store.get(ids.get(0)));

        FileRunStateStore corruptStore = store(2, new AtomicLong(1_000L));
        assertEquals(RunDecision.Code.ACCEPTED,
                create(corruptStore, "corrupt", 1L).code());
        Path corrupt = corruptStore.lifecyclePathForTest("corrupt");
        byte[] bytes = Files.readAllBytes(corrupt);
        bytes[8] ^= 1;
        Files.write(corrupt, bytes);
        assertThrows(IllegalStateException.class,
                () -> corruptStore.get("corrupt"));
    }

    @Test
    void scanReturnsOnlyTerminalRefsAcrossACompleteCycle() {
        FileRunStateStore store = store(16, new AtomicLong(1_000L));
        create(store, "active", 1L);
        create(store, "terminal", 1L);
        store.apply(RunCommand.cancelReadyRun(
                "cancel-terminal", "terminal", 1L, 2L, "CANCELED", "done"));

        RunLifecycleCatalog catalog = store;
        String cursor = null;
        List<String> found = new ArrayList<>();
        boolean complete = false;
        while (!complete) {
            RunLifecyclePage page = catalog.scanLifecycle(cursor, 1);
            for (RunLifecycleRef ref : page.runs()) found.add(ref.runId());
            cursor = page.nextCursor();
            complete = page.cycleComplete();
        }
        assertEquals(java.util.Collections.singletonList("terminal"), found);
    }

    private FileRunStateStore store(int maxLifecyclePerShard, AtomicLong clock) {
        return new FileRunStateStore(temp, () -> { }, maxLifecyclePerShard, clock::get);
    }

    private Process lifecycleContender(Path gate, Path output,
                                       String runId) throws Exception {
        return new ProcessBuilder(io.rowweave.test.ForkedJvm.command(
                FileRunLifecycleContenderProbe.class.getName(), temp.toString(),
                gate.toString(), runId))
                .redirectErrorStream(true).redirectOutput(output.toFile()).start();
    }

    private static String read(Path path) throws Exception {
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }

    private static RunDecision create(FileRunStateStore store, String runId, long eventTime) {
        return store.apply(RunCommand.createRun(
                "create-" + runId, runId, "definition", "artifact-" + runId,
                1, 0L, eventTime));
    }

    private static List<String> sameShardIds(int count) {
        List<String> ids = new ArrayList<>();
        int target = -1;
        for (int value = 0; ids.size() < count; value++) {
            String id = "file-capacity-run-" + value;
            int shard = RunLifecycleCatalogKernel.shard(
                    RunLifecycleCatalogKernel.digestHex(id));
            if (target < 0) target = shard;
            if (shard == target) ids.add(id);
        }
        return ids;
    }
}
