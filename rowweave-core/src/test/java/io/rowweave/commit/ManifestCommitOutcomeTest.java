package io.rowweave.commit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** After a lost ACK the outcome must be decided by the exact manifest. */
class ManifestCommitOutcomeTest {

    @Test
    void outcomeDistinguishesNotFoundPreparingCommittedAndConflict(@TempDir Path domain) throws Exception {
        ManifestFileStore store = new ManifestFileStore(domain);

        ManifestCommitOutcome missing = store.queryOutcome("attempt-a", null);
        assertEquals(ManifestCommitOutcome.State.NOT_FOUND, missing.state());
        assertNull(missing.generation());

        ManifestCommitOutcome preparing = store.queryOutcome("attempt-a", 7L);
        assertEquals(ManifestCommitOutcome.State.PREPARING, preparing.state());
        assertEquals(7L, preparing.generation());

        Path staged = store.stagingPath("attempt-a", "ledger");
        Files.write(staged, "7\n".getBytes(StandardCharsets.UTF_8));
        CommitManifest.Member member = store.installMember(
                7L, "ledger", "schema-a", staged);
        CommitManifest manifest = new CommitManifest(
                store.domainId(), 7L, "attempt-a", Collections.singletonList(member));
        store.publish(manifest);

        ManifestCommitOutcome committed = store.queryOutcome("attempt-a", 7L);
        assertEquals(ManifestCommitOutcome.State.COMMITTED, committed.state());
        assertEquals(7L, committed.generation());
        assertArrayEquals(CommitManifest.sha256(manifest.encode()), committed.manifestHash());

        ManifestCommitOutcome conflict = store.queryOutcome("attempt-b", 7L);
        assertEquals(ManifestCommitOutcome.State.CORRUPT_OR_CONFLICT, conflict.state());
        assertTrue(conflict.detail().contains("attemptId"), conflict.detail());
    }

    @Test
    void memberControlFrameIsDeterministicBoundedAndRoundTrips() {
        byte[] checksum = new byte[CommitManifest.SHA256_BYTES];
        checksum[0] = 41;
        CommitManifest.Member member = new CommitManifest.Member(
                "ledger", "data/9/ledger.data", "schema-9", 123L, checksum);

        byte[] first = ManifestCommitWire.encodeMember(member);
        byte[] second = ManifestCommitWire.encodeMember(member);
        assertArrayEquals(first, second);
        assertEquals(member, ManifestCommitWire.decodeMember(first));
    }
}
