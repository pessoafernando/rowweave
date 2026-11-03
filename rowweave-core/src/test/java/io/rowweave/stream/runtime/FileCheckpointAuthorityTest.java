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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** durable File first-decision-wins checkpoint authority. */
class FileCheckpointAuthorityTest {
    private static final String A = digest('a');
    private static final String B = digest('b');
    private static final String C = digest('c');

    @Test
    void commitAndCompletionSurviveReopenAndAckLoss(@TempDir Path root) {
        Cut cut = committedCut(A);
        FileCheckpointAuthority authority = new FileCheckpointAuthority(root);

        assertEquals(cut.commit,
                authority.publishDecision(cut.commit, cut.manifest));
        assertEquals(cut.commit,
                authority.publishDecision(cut.commit, cut.manifest));

        FileCheckpointAuthority reopened = new FileCheckpointAuthority(root);
        assertEquals(cut.commit,
                reopened.decision(cut.commit.checkpointIdentity()));
        assertEquals(cut.manifest,
                reopened.manifest(cut.commit.manifestDigest()));

        assertEquals(cut.completed, reopened.complete(cut.completed));
        assertEquals(cut.completed, reopened.complete(cut.completed));
        assertEquals(cut.completed,
                new FileCheckpointAuthority(root)
                        .decision(cut.commit.checkpointIdentity()));
    }

    @Test
    void firstAbortOrCommitDecisionCannotBeRewritten(@TempDir Path root) {
        Cut committed = committedCut(A);
        CheckpointAuthorityReference abortReference = reference(A);
        CheckpointDecision aborted = abortReference.abort();
        FileCheckpointAuthority abortFirst =
                new FileCheckpointAuthority(root.resolve("abort-first"));

        assertEquals(aborted, abortFirst.publishDecision(aborted, null));
        StreamCheckpointException commitConflict = assertThrows(
                StreamCheckpointException.class,
                () -> abortFirst.publishDecision(
                        committed.commit, committed.manifest));
        assertEquals(StreamCheckpointException.Code.FIRST_DECISION_CONFLICT,
                commitConflict.code());
        assertNull(abortFirst.manifest(committed.commit.manifestDigest()),
                "sequential conflict must not publish an orphan manifest");

        FileCheckpointAuthority commitFirst =
                new FileCheckpointAuthority(root.resolve("commit-first"));
        commitFirst.publishDecision(committed.commit, committed.manifest);
        StreamCheckpointException abortConflict = assertThrows(
                StreamCheckpointException.class,
                () -> commitFirst.publishDecision(aborted, null));
        assertEquals(StreamCheckpointException.Code.FIRST_DECISION_CONFLICT,
                abortConflict.code());
        assertEquals(committed.commit,
                commitFirst.decision(committed.commit.checkpointIdentity()));
    }

    @Test
    void crashBeforeAndAfterDecisionLinearizationAreRecoverable(
            @TempDir Path root) {
        Cut cut = committedCut(A);
        RuntimeException crash = new RuntimeException("simulated hard crash");
        FileCheckpointAuthority beforeDecision = new FileCheckpointAuthority(
                root.resolve("before-decision"), (step, ignored) -> {
                    if (step == FileCheckpointAuthority.PublicationStep.MANIFEST_DURABLE) {
                        throw crash;
                    }
                });

        assertSame(crash, assertThrows(RuntimeException.class,
                () -> beforeDecision.publishDecision(
                        cut.commit, cut.manifest)));
        FileCheckpointAuthority retryBefore = new FileCheckpointAuthority(
                root.resolve("before-decision"));
        assertNull(retryBefore.decision(cut.commit.checkpointIdentity()));
        assertEquals(cut.manifest,
                retryBefore.manifest(cut.commit.manifestDigest()));
        assertEquals(cut.commit,
                retryBefore.publishDecision(cut.commit, cut.manifest));

        FileCheckpointAuthority afterDecision = new FileCheckpointAuthority(
                root.resolve("after-decision"), (step, ignored) -> {
                    if (step == FileCheckpointAuthority.PublicationStep.DECISION_DURABLE) {
                        throw crash;
                    }
                });
        assertSame(crash, assertThrows(RuntimeException.class,
                () -> afterDecision.publishDecision(
                        cut.commit, cut.manifest)));
        FileCheckpointAuthority retryAfter = new FileCheckpointAuthority(
                root.resolve("after-decision"));
        assertEquals(cut.commit,
                retryAfter.decision(cut.commit.checkpointIdentity()));
        assertEquals(cut.commit,
                retryAfter.publishDecision(cut.commit, cut.manifest));
    }

    @Test
    void corruptDecisionOrManifestFailsClosed(@TempDir Path root)
            throws Exception {
        Cut cut = committedCut(A);
        FileCheckpointAuthority authority = new FileCheckpointAuthority(root);
        authority.publishDecision(cut.commit, cut.manifest);

        byte[] decision = Files.readAllBytes(
                authority.decisionPath(cut.commit.checkpointIdentity()));
        decision[decision.length - 1] ^= 1;
        Files.write(authority.decisionPath(cut.commit.checkpointIdentity()),
                decision);
        assertCode(StreamCheckpointException.Code.CORRUPT_AUTHORITY,
                () -> authority.decision(cut.commit.checkpointIdentity()));

        Path otherRoot = root.resolve("manifest-corrupt");
        FileCheckpointAuthority manifestAuthority =
                new FileCheckpointAuthority(otherRoot);
        manifestAuthority.publishDecision(cut.commit, cut.manifest);
        byte[] manifest = Files.readAllBytes(
                manifestAuthority.manifestPath(cut.commit.manifestDigest()));
        manifest[manifest.length - 1] ^= 1;
        Files.write(manifestAuthority.manifestPath(
                cut.commit.manifestDigest()), manifest);
        assertCode(StreamCheckpointException.Code.CORRUPT_AUTHORITY,
                () -> manifestAuthority.manifest(
                        cut.commit.manifestDigest()));

        Path missingRoot = root.resolve("manifest-missing");
        FileCheckpointAuthority missingAuthority =
                new FileCheckpointAuthority(missingRoot);
        missingAuthority.publishDecision(cut.commit, cut.manifest);
        Files.delete(missingAuthority.manifestPath(
                cut.commit.manifestDigest()));
        assertCode(StreamCheckpointException.Code.CORRUPT_AUTHORITY,
                () -> missingAuthority.decision(
                        cut.commit.checkpointIdentity()));
    }

    private static Cut committedCut(String evidenceDigest) {
        CheckpointAuthorityReference reference = reference(evidenceDigest);
        CheckpointIdentity checkpoint = checkpoint();
        reference.acknowledge(new CheckpointTaskReceipt(
                checkpoint, "task#0", 2L, "worker-1",
                Collections.singletonList(new CheckpointEvidence(
                        CheckpointEvidence.Kind.STATE, "state",
                        evidenceDigest, 4096L))));
        CheckpointDecision commit = reference.decideCommit();
        CheckpointManifest manifest = reference.manifest();
        reference.complete(commit);
        return new Cut(commit, reference.snapshot(), manifest);
    }

    private static CheckpointAuthorityReference reference(String ignored) {
        CheckpointIdentity checkpoint = checkpoint();
        return new CheckpointAuthorityReference(new CheckpointExpectation(
                checkpoint, Collections.singletonList(
                        new CheckpointExpectedTask(
                                "task#0", 2L, "worker-1",
                                Collections.singletonList(
                                        new CheckpointEvidenceKey(
                                                CheckpointEvidence.Kind.STATE,
                                                "state"))))));
    }

    private static CheckpointIdentity checkpoint() {
        return new CheckpointIdentity(
                "definition-1", A, "run-1", "attempt-1", 1L,
                4L, "coordinator-1", 1L, B, C, 1, 1);
    }

    private static void assertCode(
            StreamCheckpointException.Code code,
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
        private final CheckpointDecision commit;
        private final CheckpointDecision completed;
        private final CheckpointManifest manifest;

        private Cut(CheckpointDecision commit, CheckpointDecision completed,
                    CheckpointManifest manifest) {
            this.commit = commit;
            this.completed = completed;
            this.manifest = manifest;
        }
    }
}
