package io.rowweave.run;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileRunStateStoreTest {

    @TempDir
    Path temporary;

    @Test
    void fileBackendDoesNotClaimSharedRecoveryOwnershipCapability() {
        RunStateStore store = new FileRunStateStore(temporary);
        assertFalse(store instanceof RunRecoveryOwnership);
    }

    @Test
    void reopenRecoversSnapshotEventsAndAckLossReplay() {
        RunCommand create = RunCommand.createRun(
                "command-create", "run", "definition", "artifact", 2, 0L, 1L);
        FileRunStateStore first = new FileRunStateStore(temporary);
        RunDecision created = first.apply(create);
        assertEquals(RunDecision.Code.ACCEPTED, created.code());

        FileRunStateStore reopened = new FileRunStateStore(temporary);
        assertEquals(1L, reopened.get("run").revision());
        assertEquals(1, reopened.events("run", 0L, 100).size());
        RunDecision replay = reopened.apply(create);
        assertEquals(RunDecision.Code.ALREADY_APPLIED, replay.code());
        assertNotNull(replay.event());
        assertEquals(1L, replay.event().sequence());
        assertEquals(1, reopened.events("run", 0L, 100).size());
    }

    @Test
    void truncatedTailIsRemovedWithoutLosingCommittedPrefix() throws Exception {
        FileRunStateStore store = new FileRunStateStore(temporary);
        RunDecision created = store.apply(RunCommand.createRun(
                "command-create", "run", "definition", "artifact", 2, 0L, 1L));
        RunCommand attempt = RunCommand.createAttempt(
                "command-attempt", "run", created.snapshot().revision(),
                "attempt-1", "publication-1", 2L);
        InMemoryRunStateStore oracle = new InMemoryRunStateStore();
        oracle.apply(RunCommand.createRun(
                "command-create", "run", "definition", "artifact", 2, 0L, 1L));
        RunDecision next = oracle.apply(attempt);
        byte[] second = RunStateCodec.encodeRecord(
                new RunJournalRecord(attempt, next.snapshot(), next.event()));
        Path journal = store.journalPath("run");
        long committedLength = Files.size(journal);
        Files.write(journal, java.util.Arrays.copyOf(second, second.length / 2),
                StandardOpenOption.APPEND);

        FileRunStateStore reopened = new FileRunStateStore(temporary);
        assertEquals(1L, reopened.get("run").revision());
        assertEquals(committedLength, Files.size(journal));
        assertEquals(RunDecision.Code.ACCEPTED, reopened.apply(attempt).code());
        assertEquals(2L, reopened.get("run").revision());
    }

    @Test
    void everyByteOfAnIncompleteNextFrameIsRemovedAsTornTail() throws Exception {
        InMemoryRunStateStore oracle = new InMemoryRunStateStore();
        RunCommand create = RunCommand.createRun(
                "command-create", "run", "definition", "artifact", 2, 0L, 1L);
        RunDecision created = oracle.apply(create);
        byte[] firstFrame = RunStateCodec.encodeRecord(
                new RunJournalRecord(create, created.snapshot(), created.event()));
        RunCommand attempt = RunCommand.createAttempt(
                "command-attempt", "run", 1L, "attempt-1", "publication-1", 2L);
        RunDecision attempted = oracle.apply(attempt);
        byte[] secondFrame = RunStateCodec.encodeRecord(
                new RunJournalRecord(attempt, attempted.snapshot(), attempted.event()));

        for (int cut = 1; cut < secondFrame.length; cut++) {
            Path base = temporary.resolve("cut-" + cut);
            FileRunStateStore layout = new FileRunStateStore(base);
            assertEquals(RunDecision.Code.ACCEPTED, layout.apply(create).code());
            Path journal = layout.journalPath("run");
            Files.write(journal, Arrays.copyOf(secondFrame, cut),
                    StandardOpenOption.APPEND);

            FileRunStateStore recovered = new FileRunStateStore(base);
            assertEquals(1L, recovered.get("run").revision(), "cut=" + cut);
            assertEquals(firstFrame.length, Files.size(journal), "cut=" + cut);
        }
    }

    @Test
    void completeCorruptFrameFailsClosedInsteadOfRollingBack() throws Exception {
        FileRunStateStore store = new FileRunStateStore(temporary);
        store.apply(RunCommand.createRun(
                "command-create", "run", "definition", "artifact", 2, 0L, 1L));
        Path journal = store.journalPath("run");
        byte[] bytes = Files.readAllBytes(journal);
        bytes[20] ^= 1;
        Files.write(journal, bytes, StandardOpenOption.TRUNCATE_EXISTING);

        FileRunStateStore reopened = new FileRunStateStore(temporary);
        assertThrows(IllegalStateException.class, () -> reopened.get("run"));
        assertThrows(IllegalStateException.class, () -> reopened.events("run", 0L, 100));
    }

    @Test
    void twoStoreInstancesCompetingOnSameRevisionHaveOneWinner() throws Exception {
        FileRunStateStore first = new FileRunStateStore(temporary);
        FileRunStateStore second = new FileRunStateStore(temporary);
        first.apply(RunCommand.createRun(
                "command-create", "run", "definition", "artifact", 2, 0L, 1L));
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        List<RunDecision> decisions = new ArrayList<>();
        Object monitor = new Object();
        for (int i = 1; i <= 2; i++) {
            final int number = i;
            final FileRunStateStore target = i == 1 ? first : second;
            pool.submit(() -> {
                start.await();
                RunDecision decision = target.apply(RunCommand.createAttempt(
                        "command-attempt-" + number, "run", 1L,
                        "attempt-" + number, "publication-" + number, 2L));
                synchronized (monitor) {
                    decisions.add(decision);
                }
                return null;
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        assertEquals(1L, decisions.stream().filter(RunDecision::accepted).count());
        assertEquals(1L, decisions.stream()
                .filter(d -> d.code() == RunDecision.Code.STALE_REVISION).count());
        assertEquals(2L, new FileRunStateStore(temporary).get("run").revision());
    }

    @Test
    void processHaltAfterForceBeforeReturnReplaysAsAlreadyApplied() throws Exception {
        Process process = new ProcessBuilder(io.rowweave.test.ForkedJvm.command(
                FileRunStateStoreCrashProbe.class.getName(), temporary.toString()))
                .redirectErrorStream(true).start();
        assertTrue(process.waitFor(15, TimeUnit.SECONDS));
        assertEquals(91, process.exitValue());

        FileRunStateStore reopened = new FileRunStateStore(temporary);
        RunDecision replay = reopened.apply(RunCommand.createRun(
                "command-create", "run", "definition", "artifact", 2, 0L, 1L));
        assertEquals(RunDecision.Code.ALREADY_APPLIED, replay.code());
        assertEquals(1L, replay.snapshot().revision());
        assertEquals(1L, replay.event().sequence());
        assertEquals(RunLifecycleState.ACTIVE,
                ((RunLifecycleCatalog) reopened).lifecycle("run").state());
    }

    @Test
    void twoIndependentProcessesCompetingOnSameRevisionHaveOneWinner() throws Exception {
        FileRunStateStore store = new FileRunStateStore(temporary);
        assertEquals(RunDecision.Code.ACCEPTED, store.apply(RunCommand.createRun(
                "command-create", "run", "definition", "artifact", 2, 0L, 1L)).code());
        Path gate = temporary.resolve("start.gate");
        Path firstOutput = temporary.resolve("first.out");
        Path secondOutput = temporary.resolve("second.out");
        Process first = contender(gate, firstOutput, 1);
        Process second = contender(gate, secondOutput, 2);
        Files.write(gate, new byte[] { 1 }, StandardOpenOption.CREATE_NEW);

        assertTrue(first.waitFor(20, TimeUnit.SECONDS));
        assertTrue(second.waitFor(20, TimeUnit.SECONDS));
        assertEquals(0, first.exitValue(), read(firstOutput));
        assertEquals(0, second.exitValue(), read(secondOutput));
        List<String> outcomes = Arrays.asList(read(firstOutput).trim(), read(secondOutput).trim());
        assertEquals(1L, outcomes.stream().filter("ACCEPTED"::equals).count(), outcomes.toString());
        assertEquals(1L, outcomes.stream().filter("STALE_REVISION"::equals).count(), outcomes.toString());
        assertEquals(2L, new FileRunStateStore(temporary).get("run").revision());
        assertEquals(2, new FileRunStateStore(temporary).events("run", 0L, 100).size());
    }

    private Process contender(Path gate, Path output, int number) throws Exception {
        return new ProcessBuilder(io.rowweave.test.ForkedJvm.command(
                FileRunStateStoreContenderProbe.class.getName(), temporary.toString(),
                gate.toString(), Integer.toString(number)))
                .redirectErrorStream(true).redirectOutput(output.toFile()).start();
    }

    private static String read(Path path) throws Exception {
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }
}
