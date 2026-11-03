package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.CheckpointAuthorityReference;
import io.rowweave.stream.contract.CheckpointDecision;
import io.rowweave.stream.contract.CheckpointExpectation;
import io.rowweave.stream.contract.CheckpointExpectedTask;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.CheckpointTaskReceipt;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamSavepoint;
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

/** replacement Masters share member/savepoint metadata. */
class ZkStreamCheckpointCatalogTest {

    @Test
    void retentionMutationsRemoveOnlyUnprotectedMetadata() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connected(server)) {
            String root = "/rowweave/stream-checkpoint-retention";
            ZkStreamCheckpointAuthority authority =
                    new ZkStreamCheckpointAuthority(client,
                            root + "/authority");
            ZkStreamCheckpointCatalog catalog =
                    new ZkStreamCheckpointCatalog(client,
                            root + "/catalog");
            PublishedCut first = publish(authority, catalog, 1L, 'd');
            PublishedCut second = publish(authority, catalog, 2L, 'e');
            catalog.create("stream-checkpoint:first",
                    first.identity, authority);
            catalog.create("stream-checkpoint:second",
                    second.identity, authority);
            catalog.create("stream-savepoint:user",
                    second.identity, authority);

            assertEquals(3, catalog.savepoints().size());

            catalog.removeCheckpointReference("stream-checkpoint:first");
            catalog.retainOnlyMembers(
                    Collections.singleton(second.member));
            authority.retainOnly(
                    Collections.singleton(second.identity),
                    Collections.singleton(
                            second.completed.manifestDigest()));

            assertNull(catalog.get("stream-checkpoint:first"));
            assertEquals(second.identity, catalog.get("stream-savepoint:user")
                    .checkpointManifest().checkpointIdentity());
            assertThrows(StreamCheckpointException.class,
                    () -> catalog.member(first.member.toEvidence()));
            assertEquals(second.member,
                    catalog.member(second.member.toEvidence()));
            assertNull(authority.decision(first.identity));
            assertEquals(second.completed,
                    authority.decision(second.identity));
        }
    }

    @Test
    void memberProofAndSavepointResolveAcrossMasterClients() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = connected(server);
             CuratorFramework replacementClient = connected(server)) {
            String root = "/rowweave/stream-checkpoint-catalog";
            StreamCheckpointAuthority firstAuthority =
                    new ZkStreamCheckpointAuthority(firstClient,
                            root + "/authority");
            StreamCheckpointAuthority replacementAuthority =
                    new ZkStreamCheckpointAuthority(replacementClient,
                            root + "/authority");
            StreamCheckpointCatalog first =
                    new ZkStreamCheckpointCatalog(firstClient,
                            root + "/catalog");
            StreamCheckpointCatalog replacement =
                    new ZkStreamCheckpointCatalog(replacementClient,
                            root + "/catalog");
            StreamCheckpointMember member =
                    StreamCheckpointMember.stateHandle(
                            "map", 0, 0, 7, "readout",
                            "decimal-state-v1", 1, digest('d'), 128L);
            CheckpointIdentity identity = checkpoint();
            CheckpointExpectation expectation = new CheckpointExpectation(
                    identity, Collections.singletonList(
                    new CheckpointExpectedTask("map#0", 1L, "worker-a",
                            Collections.singletonList(
                                    member.toEvidence().key()))));
            CheckpointAuthorityReference reference =
                    new CheckpointAuthorityReference(expectation);

            first.publish(member);
            CheckpointTaskReceipt receipt = new CheckpointTaskReceipt(
                    identity, "map#0", 1L, "worker-a",
                    Collections.singletonList(member.toEvidence()));
            first.verify(receipt);
            reference.acknowledge(receipt);
            CheckpointDecision commit = reference.decideCommit();
            firstAuthority.publishDecision(commit, reference.manifest());
            CheckpointDecision completed = firstAuthority.complete(
                    CheckpointDecision.completed(commit));

            assertEquals(Collections.singletonList(member),
                    replacement.resolve(completed, replacementAuthority));
            StreamSavepoint savepoint = first.create(
                    "savepoint-ha", identity, firstAuthority);
            assertEquals(savepoint, replacement.get("savepoint-ha"));
        }
    }

    private static CheckpointIdentity checkpoint() {
        return new CheckpointIdentity(
                "definition-1", digest('a'), "run-ha", "attempt-1", 1L,
                7L, "master-a", 1L, digest('b'), digest('c'), 1, 1);
    }

    private static PublishedCut publish(
            StreamCheckpointAuthority authority,
            StreamCheckpointCatalog catalog, long sequence,
            char content) {
        StreamCheckpointMember member =
                StreamCheckpointMember.stateHandle(
                        "map", 0, 0, 7, "readout",
                        "decimal-state-v1", 1, digest(content), 128L);
        CheckpointIdentity identity = new CheckpointIdentity(
                "definition-1", digest('a'), "run-retained", "attempt-1",
                1L, 7L, "master-a", sequence, digest('b'), digest('c'),
                1, 1);
        CheckpointExpectation expectation = new CheckpointExpectation(
                identity, Collections.singletonList(
                new CheckpointExpectedTask("map#0", 1L, "worker-a",
                        Collections.singletonList(
                                member.toEvidence().key()))));
        CheckpointAuthorityReference reference =
                new CheckpointAuthorityReference(expectation);
        catalog.publish(member);
        CheckpointTaskReceipt receipt = new CheckpointTaskReceipt(
                identity, "map#0", 1L, "worker-a",
                Collections.singletonList(member.toEvidence()));
        reference.acknowledge(receipt);
        CheckpointDecision commit = reference.decideCommit();
        authority.publishDecision(commit, reference.manifest());
        CheckpointDecision completed = authority.complete(
                CheckpointDecision.completed(commit));
        return new PublishedCut(identity, member, completed);
    }

    private static final class PublishedCut {
        private final CheckpointIdentity identity;
        private final StreamCheckpointMember member;
        private final CheckpointDecision completed;

        private PublishedCut(CheckpointIdentity identity,
                StreamCheckpointMember member,
                CheckpointDecision completed) {
            this.identity = identity;
            this.member = member;
            this.completed = completed;
        }
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
}
