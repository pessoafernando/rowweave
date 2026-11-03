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
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** ZooKeeper stores bounded checkpoint metadata only. */
class ZkStreamCheckpointAuthorityTest {
    private static final String A = digest('a');
    private static final String B = digest('b');
    private static final String C = digest('c');

    @Test
    void commitAndCompletionReconcileAcrossClients() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = connected(server);
             CuratorFramework secondClient = connected(server)) {
            Cut cut = cut(A);
            String root = "/rowweave/stream-checkpoints";
            StreamCheckpointAuthority first =
                    new ZkStreamCheckpointAuthority(firstClient, root);
            StreamCheckpointAuthority replacement =
                    new ZkStreamCheckpointAuthority(secondClient, root);

            assertEquals(cut.commit,
                    first.publishDecision(cut.commit, cut.manifest));
            assertEquals(cut.commit, replacement.decision(cut.identity));
            assertEquals(cut.manifest,
                    replacement.manifest(cut.commit.manifestDigest()));
            assertEquals(cut.commit,
                    replacement.publishDecision(cut.commit, cut.manifest));
            assertEquals(cut.completed,
                    replacement.complete(cut.completed));
            assertEquals(cut.completed, first.decision(cut.identity));
            assertEquals(Collections.singletonList(cut.completed),
                    first.decisionsForRun("run-1"));
            assertEquals(Collections.emptyList(),
                    replacement.decisionsForRun("another-run"));
        }
    }

    @Test
    void abortWinsAtomicallyWithoutOrphanManifest() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connected(server)) {
            String root = "/rowweave/stream-abort";
            Cut cut = cut(A);
            ZkStreamCheckpointAuthority authority =
                    new ZkStreamCheckpointAuthority(client, root);
            CheckpointDecision aborted = aborted(cut.identity);

            assertEquals(aborted, authority.publishDecision(aborted, null));
            StreamCheckpointException conflict = assertThrows(
                    StreamCheckpointException.class,
                    () -> authority.publishDecision(cut.commit, cut.manifest));
            assertEquals(StreamCheckpointException.Code.FIRST_DECISION_CONFLICT,
                    conflict.code());
            assertNull(authority.manifest(cut.commit.manifestDigest()),
                    "losing commit must not leave an orphan ZK manifest");
            assertNull(client.checkExists().forPath(
                    authority.manifestPath(cut.commit.manifestDigest())));
        }
    }

    @Test
    void corruptBoundedMetadataFailsClosed() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connected(server)) {
            Cut cut = cut(A);
            ZkStreamCheckpointAuthority authority =
                    new ZkStreamCheckpointAuthority(
                            client, "/rowweave/stream-corrupt");
            authority.publishDecision(cut.commit, cut.manifest);
            String path = authority.manifestPath(
                    cut.commit.manifestDigest());
            byte[] corrupt = client.getData().forPath(path);
            corrupt[corrupt.length - 1] ^= 1;
            client.setData().forPath(path, corrupt);

            StreamCheckpointException failure = assertThrows(
                    StreamCheckpointException.class,
                    () -> authority.manifest(cut.commit.manifestDigest()));
            assertEquals(StreamCheckpointException.Code.CORRUPT_AUTHORITY,
                    failure.code());
        }
    }

    private static Cut cut(String evidenceDigest) {
        CheckpointIdentity identity = checkpoint();
        CheckpointExpectation expectation = new CheckpointExpectation(
                identity, Collections.singletonList(new CheckpointExpectedTask(
                "task#0", 2L, "worker-1", Collections.singletonList(
                new CheckpointEvidenceKey(CheckpointEvidence.Kind.STATE,
                        "state")))));
        CheckpointAuthorityReference reference =
                new CheckpointAuthorityReference(expectation);
        reference.acknowledge(new CheckpointTaskReceipt(
                identity, "task#0", 2L, "worker-1",
                Collections.singletonList(new CheckpointEvidence(
                        CheckpointEvidence.Kind.STATE, "state",
                        evidenceDigest, 4096L))));
        CheckpointDecision commit = reference.decideCommit();
        CheckpointManifest manifest = reference.manifest();
        reference.complete(commit);
        return new Cut(identity, commit, reference.snapshot(), manifest);
    }

    private static CheckpointDecision aborted(CheckpointIdentity identity) {
        CheckpointExpectation expectation = new CheckpointExpectation(
                identity, Collections.singletonList(new CheckpointExpectedTask(
                "task#0", 2L, "worker-1", Collections.singletonList(
                new CheckpointEvidenceKey(CheckpointEvidence.Kind.STATE,
                        "state")))));
        return new CheckpointAuthorityReference(expectation).abort();
    }

    private static CheckpointIdentity checkpoint() {
        return new CheckpointIdentity(
                "definition-1", A, "run-1", "attempt-1", 1L,
                4L, "coordinator-1", 1L, B, C, 1, 1);
    }

    private static CuratorFramework connected(TestingServer server)
            throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                server.getConnectString(), new ExponentialBackoffRetry(25, 3));
        client.start();
        if (!client.blockUntilConnected(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Curator did not connect");
        }
        return client;
    }

    private static String digest(char value) {
        char[] chars = new char[64];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static final class Cut {
        private final CheckpointIdentity identity;
        private final CheckpointDecision commit;
        private final CheckpointDecision completed;
        private final CheckpointManifest manifest;

        private Cut(CheckpointIdentity identity, CheckpointDecision commit,
                    CheckpointDecision completed,
                    CheckpointManifest manifest) {
            this.identity = identity;
            this.commit = commit;
            this.completed = completed;
            this.manifest = manifest;
        }
    }
}
