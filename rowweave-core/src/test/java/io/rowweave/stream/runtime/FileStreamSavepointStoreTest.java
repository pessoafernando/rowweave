package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.CheckpointAuthorityReference;
import io.rowweave.stream.contract.CheckpointDecision;
import io.rowweave.stream.contract.CheckpointEvidence;
import io.rowweave.stream.contract.CheckpointEvidenceKey;
import io.rowweave.stream.contract.CheckpointExpectation;
import io.rowweave.stream.contract.CheckpointExpectedTask;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.CheckpointManifest;
import io.rowweave.stream.contract.CheckpointTaskReceipt;
import io.rowweave.stream.contract.StreamSavepoint;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** durable savepoints are immutable completed checkpoint copies. */
class FileStreamSavepointStoreTest {
    private static final String A = digest('a');
    private static final String B = digest('b');
    private static final String C = digest('c');

    @Test
    void completedCheckpointBecomesCanonicalReopenableSavepoint(
            @TempDir Path root) {
        Cut cut = cut(1L, CheckpointDecision.Status.COMPLETED);
        FileCheckpointAuthority checkpoints = new FileCheckpointAuthority(
                root.resolve("checkpoints"));
        publish(checkpoints, cut);

        FileStreamSavepointStore store = new FileStreamSavepointStore(
                root.resolve("savepoints"));
        StreamSavepoint created = store.create(
                "quarter-close-2026-q3", cut.identity, checkpoints);

        assertEquals("quarter-close-2026-q3", created.savepointId());
        assertEquals(cut.manifest, created.checkpointManifest());
        assertEquals(created, store.create(
                "quarter-close-2026-q3", cut.identity, checkpoints),
                "lost create ACK must resolve to the exact durable savepoint");
        assertEquals(created, new FileStreamSavepointStore(
                root.resolve("savepoints")).get("quarter-close-2026-q3"));
    }

    @Test
    void onlyCompletedCheckpointCanCreateSavepoint(@TempDir Path root) {
        Cut decided = cut(1L, CheckpointDecision.Status.COMMIT_DECIDED);
        FileCheckpointAuthority decidedAuthority = new FileCheckpointAuthority(
                root.resolve("decided"));
        publish(decidedAuthority, decided);
        FileStreamSavepointStore store = new FileStreamSavepointStore(
                root.resolve("savepoints"));

        assertCode(StreamCheckpointException.Code.CHECKPOINT_NOT_COMPLETED,
                () -> store.create("not-complete", decided.identity,
                        decidedAuthority));

        Cut aborted = cut(2L, CheckpointDecision.Status.ABORTED);
        FileCheckpointAuthority abortedAuthority = new FileCheckpointAuthority(
                root.resolve("aborted"));
        abortedAuthority.publishDecision(aborted.decision, null);
        assertCode(StreamCheckpointException.Code.CHECKPOINT_NOT_COMPLETED,
                () -> store.create("aborted", aborted.identity,
                        abortedAuthority));
        assertFalse(Files.exists(store.savepointPath("not-complete")));
        assertFalse(Files.exists(store.savepointPath("aborted")));
    }

    @Test
    void savepointNameIsFirstReferenceWins(@TempDir Path root) {
        Cut first = cut(1L, CheckpointDecision.Status.COMPLETED);
        Cut second = cut(2L, CheckpointDecision.Status.COMPLETED);
        FileCheckpointAuthority checkpoints = new FileCheckpointAuthority(
                root.resolve("checkpoints"));
        publish(checkpoints, first);
        publish(checkpoints, second);
        FileStreamSavepointStore store = new FileStreamSavepointStore(
                root.resolve("savepoints"));

        StreamSavepoint saved = store.create("stable-name", first.identity,
                checkpoints);
        assertEquals(saved, store.create("stable-name", first.identity,
                checkpoints));
        assertCode(StreamCheckpointException.Code.SAVEPOINT_CONFLICT,
                () -> store.create("stable-name", second.identity,
                        checkpoints));
        assertEquals(saved, store.get("stable-name"));
    }

    @Test
    void durablePublicationSurvivesAckLossAndCorruptionFailsClosed(
            @TempDir Path root) throws Exception {
        Cut cut = cut(1L, CheckpointDecision.Status.COMPLETED);
        FileCheckpointAuthority checkpoints = new FileCheckpointAuthority(
                root.resolve("checkpoints"));
        publish(checkpoints, cut);
        RuntimeException lostAck = new RuntimeException("lost create ACK");
        FileStreamSavepointStore crashing = new FileStreamSavepointStore(
                root.resolve("savepoints"), (step, ignored) -> {
                    if (step == FileStreamSavepointStore.PublicationStep.SAVEPOINT_DURABLE) {
                        throw lostAck;
                    }
                });

        assertSame(lostAck, assertThrows(RuntimeException.class,
                () -> crashing.create("durable", cut.identity, checkpoints)));
        FileStreamSavepointStore reopened = new FileStreamSavepointStore(
                root.resolve("savepoints"));
        StreamSavepoint recovered = reopened.create(
                "durable", cut.identity, checkpoints);
        assertEquals(cut.manifest, recovered.checkpointManifest());
        assertEquals(recovered, reopened.create(
                "durable", cut.identity, new FileCheckpointAuthority(
                        root.resolve("checkpoint-authority-retired"))),
                "durable savepoint ACK recovery must not depend on the source checkpoint authority");

        Path path = reopened.savepointPath("durable");
        byte[] bytes = Files.readAllBytes(path);
        bytes[bytes.length - 1] ^= 1;
        Files.write(path, bytes);
        assertCode(StreamCheckpointException.Code.CORRUPT_SAVEPOINT,
                () -> reopened.get("durable"));
    }

    private static void publish(FileCheckpointAuthority authority, Cut cut) {
        if (cut.decision.status() == CheckpointDecision.Status.ABORTED) {
            authority.publishDecision(cut.decision, null);
            return;
        }
        authority.publishDecision(cut.commit, cut.manifest);
        if (cut.decision.status() == CheckpointDecision.Status.COMPLETED) {
            authority.complete(cut.decision);
        }
    }

    private static Cut cut(long sequence, CheckpointDecision.Status status) {
        CheckpointIdentity identity = checkpoint(sequence);
        CheckpointExpectation expectation = new CheckpointExpectation(
                identity, Collections.singletonList(new CheckpointExpectedTask(
                "task#0", 2L, "worker-1", Collections.singletonList(
                new CheckpointEvidenceKey(CheckpointEvidence.Kind.STATE,
                        "state")))));
        CheckpointAuthorityReference reference =
                new CheckpointAuthorityReference(expectation);
        if (status == CheckpointDecision.Status.ABORTED) {
            return new Cut(identity, reference.abort(), null, null);
        }
        reference.acknowledge(new CheckpointTaskReceipt(
                identity, "task#0", 2L, "worker-1",
                Collections.singletonList(new CheckpointEvidence(
                        CheckpointEvidence.Kind.STATE, "state", A, 4096L))));
        CheckpointDecision commit = reference.decideCommit();
        CheckpointManifest manifest = reference.manifest();
        if (status == CheckpointDecision.Status.COMMIT_DECIDED) {
            return new Cut(identity, commit, commit, manifest);
        }
        reference.complete(commit);
        return new Cut(identity, reference.snapshot(), commit, manifest);
    }

    private static CheckpointIdentity checkpoint(long sequence) {
        return new CheckpointIdentity(
                "definition-1", A, "run-1", "attempt-1", 1L,
                4L, "coordinator-1", sequence, B, C, 1, 1);
    }

    private static void assertCode(StreamCheckpointException.Code code,
                                   org.junit.jupiter.api.function.Executable action) {
        StreamCheckpointException failure = assertThrows(
                StreamCheckpointException.class, action);
        assertEquals(code, failure.code());
    }

    private static String digest(char value) {
        char[] chars = new char[64];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static final class Cut {
        private final CheckpointIdentity identity;
        private final CheckpointDecision decision;
        private final CheckpointDecision commit;
        private final CheckpointManifest manifest;

        private Cut(CheckpointIdentity identity, CheckpointDecision decision,
                    CheckpointDecision commit, CheckpointManifest manifest) {
            this.identity = identity;
            this.decision = decision;
            this.commit = commit;
            this.manifest = manifest;
        }
    }
}
