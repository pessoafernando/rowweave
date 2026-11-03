package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.CheckpointDecision;
import io.rowweave.stream.contract.CheckpointEvidenceKey;
import io.rowweave.stream.contract.CheckpointExpectation;
import io.rowweave.stream.contract.CheckpointExpectedTask;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.CheckpointTaskReceipt;
import io.rowweave.stream.contract.StreamCheckpointMember;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** durable typed handle catalog for hard-crash restore. */
class FileCheckpointMemberStoreTest {
    private static final String A = digest('a');
    private static final String B = digest('b');
    private static final String C = digest('c');

    @TempDir
    Path temporaryDirectory;

    @Test
    void replacementResolvesEveryTypedHandleFromCommittedManifest() {
        Path root = temporaryDirectory.resolve("members");
        FileCheckpointMemberStore members = new FileCheckpointMemberStore(root);
        List<StreamCheckpointMember> expected = members();
        for (StreamCheckpointMember member : expected) members.publish(member);
        FileCheckpointAuthority authority = new FileCheckpointAuthority(
                temporaryDirectory.resolve("authority"));
        StreamCheckpointCoordinator coordinator = new StreamCheckpointCoordinator(
                expectation(expected), authority, members);
        coordinator.acknowledge(receipt(expected));
        CheckpointDecision commit = coordinator.decideCommit();

        FileCheckpointMemberStore replacement =
                new FileCheckpointMemberStore(root);
        assertEquals(expected, replacement.resolve(commit, authority));
    }

    @Test
    void coordinatorRejectsMissingHandleBeforeAuthorityMutation() {
        FileCheckpointMemberStore members = new FileCheckpointMemberStore(
                temporaryDirectory.resolve("missing-members"));
        List<StreamCheckpointMember> expected = members();
        members.publish(expected.get(0));
        FileCheckpointAuthority authority = new FileCheckpointAuthority(
                temporaryDirectory.resolve("missing-authority"));
        StreamCheckpointCoordinator coordinator = new StreamCheckpointCoordinator(
                expectation(expected), authority, members);

        StreamCheckpointException missing = assertThrows(
                StreamCheckpointException.class,
                () -> coordinator.acknowledge(receipt(expected)));
        assertEquals(StreamCheckpointException.Code.CORRUPT_MEMBER_CATALOG,
                missing.code());
        assertNull(authority.firstDecision(checkpoint()));
    }

    @Test
    void corruptCatalogEntryFailsClosedDuringReplacementRestore()
            throws Exception {
        Path root = temporaryDirectory.resolve("corrupt-members");
        FileCheckpointMemberStore members = new FileCheckpointMemberStore(root);
        List<StreamCheckpointMember> expected = members();
        for (StreamCheckpointMember member : expected) members.publish(member);
        FileCheckpointAuthority authority = new FileCheckpointAuthority(
                temporaryDirectory.resolve("corrupt-authority"));
        StreamCheckpointCoordinator coordinator = new StreamCheckpointCoordinator(
                expectation(expected), authority, members);
        coordinator.acknowledge(receipt(expected));
        CheckpointDecision commit = coordinator.decideCommit();
        Files.write(members.memberPath(expected.get(1)),
                new byte[] {0}, StandardOpenOption.TRUNCATE_EXISTING);

        StreamCheckpointException corrupt = assertThrows(
                StreamCheckpointException.class,
                () -> new FileCheckpointMemberStore(root)
                        .resolve(commit, authority));
        assertEquals(StreamCheckpointException.Code.CORRUPT_MEMBER_CATALOG,
                corrupt.code());
    }

    private static List<StreamCheckpointMember> members() {
        return Arrays.asList(
                StreamCheckpointMember.sourcePosition(
                        "source", 0, A, "position-v1", 42L, B, 128L),
                StreamCheckpointMember.stateHandle(
                        "aggregate", 0, 0, 127, "readout",
                        "decimal-v1", 1, C, 256L),
                StreamCheckpointMember.transactionalSink(
                        "sink", 0, "transaction-1", A, B, 64L));
    }

    private static CheckpointExpectation expectation(
            List<StreamCheckpointMember> members) {
        List<CheckpointEvidenceKey> keys = new ArrayList<>();
        for (StreamCheckpointMember member : members) {
            keys.add(new CheckpointEvidenceKey(
                    member.kind().evidenceKind(), member.memberId()));
        }
        return new CheckpointExpectation(
                checkpoint(), Collections.singletonList(
                        new CheckpointExpectedTask(
                                "task-0", 2L, "worker-1", keys)));
    }

    private static CheckpointTaskReceipt receipt(
            List<StreamCheckpointMember> members) {
        List<io.rowweave.stream.contract.CheckpointEvidence> evidence =
                new ArrayList<>();
        for (StreamCheckpointMember member : members) {
            evidence.add(member.toEvidence());
        }
        return new CheckpointTaskReceipt(
                checkpoint(), "task-0", 2L, "worker-1", evidence);
    }

    private static CheckpointIdentity checkpoint() {
        return new CheckpointIdentity(
                "definition-1", A, "run-1", "attempt-1", 1L,
                4L, "coordinator-1", 1L, A, B, 1, 1);
    }

    private static String digest(char value) {
        char[] chars = new char[64];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
