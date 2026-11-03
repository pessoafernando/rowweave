package io.rowweave.run;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestCommitWire;
import org.junit.jupiter.api.Test;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ManifestCommitJournalCodecTest {

    @Test
    void sealedMembersAndIntentRoundTripToTheExactCanonicalManifest() throws Exception {
        StageAssignmentSnapshot assignments = terminalAssignments(65, true);
        ManifestCommitJournalSnapshot journal =
                ManifestCommitJournalSnapshot.initial(assignments);
        for (int i = 0; i < assignments.manifest().assignmentCount(); i++) {
            journal = journal.withMember(member(assignments, i, "member-" + i));
        }
        assertEquals(65, journal.memberCount());
        assertTrue(journal.allExpectedMembersSealed());

        ManifestCommitIntent intent = journal.buildIntent();
        journal = journal.withIntent(intent);
        List<byte[]> shards = memberShards(journal);
        byte[] encodedIntent = ManifestCommitJournalCodec.encodeIntent(intent);
        ManifestCommitJournalSnapshot decoded = ManifestCommitJournalCodec.decodeSnapshot(
                assignments, shards, encodedIntent);

        assertEquals(65, decoded.memberCount());
        assertEquals(intent, decoded.intent());
        CommitManifest manifest = decoded.intendedManifest();
        assertEquals("domain-journal", manifest.domainId());
        assertEquals(71L, manifest.generation());
        assertEquals("publication-journal", manifest.attemptId());
        assertEquals(65, manifest.members().size());
        assertArrayEquals(intent.manifestHash(), sha256(manifest.encode()));
        assertEquals(assignments.manifest().shardCount(), intent.memberShardCount());
    }

    @Test
    void memberRequiresSucceededOutputReceiptAndOneConsistentCommitIdentity() throws Exception {
        StageAssignmentSnapshot assignments = terminalAssignments(2, true);
        ManifestCommitJournalSnapshot journal =
                ManifestCommitJournalSnapshot.initial(assignments);
        ManifestMemberReceipt first = member(assignments, 0, "first");
        journal = journal.withMember(first);
        ManifestCommitJournalSnapshot firstJournal = journal;
        assertThrows(IllegalArgumentException.class,
                () -> firstJournal.withMember(member(assignments, 0, "first")));
        assertThrows(IllegalArgumentException.class,
                () -> firstJournal.withMember(ManifestMemberReceipt.sealed(
                        assignments.token(1), assignments.acceptance(1).commandDigest(),
                        "other-domain", 71L, "publication-journal",
                        descriptor("second", 1), 40L)));

        StageAssignmentSnapshot failed = failedAssignments();
        assertThrows(IllegalArgumentException.class,
                () -> ManifestCommitJournalSnapshot.initial(failed)
                        .withMember(member(failed, 0, "failed")));

        StageAssignmentSnapshot router = terminalAssignments(1, false);
        assertThrows(IllegalArgumentException.class,
                () -> ManifestCommitJournalSnapshot.initial(router)
                        .withMember(member(router, 0, "router")));
    }

    @Test
    void intentRequiresCompleteClosureAndFreezesMemberSet() throws Exception {
        StageAssignmentSnapshot assignments = terminalAssignments(2, true);
        ManifestCommitJournalSnapshot partial = ManifestCommitJournalSnapshot
                .initial(assignments).withMember(member(assignments, 0, "first"));
        assertFalse(partial.allExpectedMembersSealed());
        assertThrows(IllegalStateException.class, partial::buildIntent);

        ManifestCommitJournalSnapshot complete = partial
                .withMember(member(assignments, 1, "second"));
        ManifestCommitJournalSnapshot intended = complete.withIntent(complete.buildIntent());
        assertThrows(IllegalStateException.class,
                () -> intended.withMember(member(assignments, 1, "second")));
    }

    @Test
    void memberShardAndIntentRejectCorruptionTruncationAndWrongAssignmentEvidence()
            throws Exception {
        StageAssignmentSnapshot assignments = terminalAssignments(1, true);
        ManifestCommitJournalSnapshot journal = ManifestCommitJournalSnapshot
                .initial(assignments).withMember(member(assignments, 0, "only"));
        ManifestCommitIntent intent = journal.buildIntent();
        byte[] shard = ManifestCommitJournalCodec.encodeMemberShard(journal, 0);
        byte[] root = ManifestCommitJournalCodec.encodeIntent(intent);

        byte[] corrupt = shard.clone();
        corrupt[corrupt.length / 2] ^= 1;
        assertThrows(IllegalArgumentException.class, () ->
                ManifestCommitJournalCodec.decodeSnapshot(
                        assignments, Arrays.asList(corrupt), root));
        assertThrows(IllegalArgumentException.class, () ->
                ManifestCommitJournalCodec.decodeSnapshot(assignments,
                        Arrays.asList(Arrays.copyOf(shard, shard.length - 1)), root));

        StageAssignmentSnapshot different = failedAssignments();
        assertThrows(IllegalArgumentException.class, () ->
                ManifestCommitJournalCodec.decodeSnapshot(
                        different, Arrays.asList(shard), root));
    }

    @Test
    void fullMemberShardHasAHardFrameLimit() throws Exception {
        StageAssignmentSnapshot assignments = terminalAssignments(
                StageAssignmentManifest.MAX_RECORDS_PER_SHARD, true);
        ManifestCommitJournalSnapshot journal =
                ManifestCommitJournalSnapshot.initial(assignments);
        for (int i = 0; i < assignments.manifest().assignmentCount(); i++) {
            journal = journal.withMember(member(assignments, i, "member-" + i));
        }
        byte[] encoded = ManifestCommitJournalCodec.encodeMemberShard(journal, 0);
        assertTrue(encoded.length <=
                ManifestCommitJournalCodec.MAX_MEMBER_SHARD_FRAME_BYTES);
    }

    @Test
    void v2PreparedLocatorRoundTripsWhileLegacyReceiptsRemainReadable() throws Exception {
        StageAssignmentSnapshot assignments = terminalAssignments(1, true);
        CommitManifest.Member member = descriptor("located", 0);
        BatchSinkDescriptor sink = new BatchSinkDescriptor(
                1, "rowweave.manifest-file", 1, repeat('0', 64),
                "domain-journal:member:located", "domain-journal",
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);
        SinkAttemptIdentity identity = SinkAttemptIdentity.forAssignment(
                assignments.manifest(), assignments.manifest().assignment(0),
                "located-sink", sink);
        PreparedSinkLocator locator = new PreparedSinkLocator(
                1, identity,
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                "domain-journal", ManifestCommitWire.encodeMember(member),
                member.length(), member.sha256(), 40L);
        ManifestMemberReceipt receipt = ManifestMemberReceipt.sealed(
                assignments.token(0), assignments.acceptance(0).commandDigest(),
                "domain-journal", 71L, "publication-journal",
                member, 40L, locator);
        assertTrue(receipt.hasPreparedLocator());
        assertEquals(locator, receipt.preparedLocator());

        ManifestCommitJournalSnapshot journal = ManifestCommitJournalSnapshot
                .initial(assignments).withMember(receipt);
        byte[] shard = ManifestCommitJournalCodec.encodeMemberShard(journal, 0);
        ManifestCommitIntent intent = journal.buildIntent();
        ManifestCommitJournalSnapshot decoded =
                ManifestCommitJournalCodec.decodeSnapshot(
                        assignments, java.util.Collections.singletonList(shard),
                        ManifestCommitJournalCodec.encodeIntent(intent));
        assertEquals(locator, decoded.member(0).preparedLocator());
        assertEquals(intent, decoded.intent());

        ManifestMemberReceipt legacy = member(assignments, 0, "legacy");
        assertFalse(legacy.hasPreparedLocator());
        ManifestCommitJournalSnapshot legacyJournal = ManifestCommitJournalSnapshot
                .initial(assignments).withMember(legacy);
        byte[] legacyShard = ManifestCommitJournalCodec.encodeMemberShard(
                legacyJournal, 0);
        assertFalse(ManifestCommitJournalCodec.decodeSnapshot(
                assignments, java.util.Collections.singletonList(legacyShard), null)
                .member(0).hasPreparedLocator());
    }

    @Test
    void locatorMustMatchReceiptAssignmentDomainMemberAndSealTime() throws Exception {
        StageAssignmentSnapshot assignments = terminalAssignments(2, true);
        CommitManifest.Member member = descriptor("located", 0);
        BatchSinkDescriptor sink = new BatchSinkDescriptor(
                1, "rowweave.manifest-file", 1, repeat('0', 64),
                "domain-journal:member:located", "domain-journal",
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);
        SinkAttemptIdentity wrongAssignment = SinkAttemptIdentity.forAssignment(
                assignments.manifest(), assignments.manifest().assignment(1),
                "located-sink", sink);
        PreparedSinkLocator wrong = new PreparedSinkLocator(
                1, wrongAssignment,
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                "domain-journal", ManifestCommitWire.encodeMember(member),
                member.length(), member.sha256(), 40L);
        assertThrows(IllegalArgumentException.class,
                () -> ManifestMemberReceipt.sealed(
                        assignments.token(0), assignments.acceptance(0).commandDigest(),
                        "domain-journal", 71L, "publication-journal",
                        member, 40L, wrong));
    }

    private static StageAssignmentSnapshot terminalAssignments(
            int count, boolean outputRole) throws Exception {
        RunAttemptFence fence = new RunAttemptFence(
                "run-manifest-journal", "attempt-1", 1L);
        List<StageAssignmentRecord> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            records.add(StageAssignmentRecord.create(
                    fence, 1L, i, 100L + i, i, 0,
                    outputRole ? StageAssignmentRole.SINK : StageAssignmentRole.ROUTER,
                    "worker-" + i, "127.0.0.1", 7001 + i, -1,
                    "inc-" + i));
        }
        StageAssignmentSnapshot snapshot = StageAssignmentSnapshot.initial(
                new StageAssignmentManifest(fence, "artifact-journal", 1L, records));
        for (int i = 0; i < count; i++) {
            byte[] digest = sha256("command-" + i);
            StageAssignmentAcceptance acceptance = new StageAssignmentAcceptance(
                    snapshot.token(i), digest, 10L + i);
            snapshot = snapshot.withAcceptance(acceptance);
            snapshot = snapshot.withTerminal(StageTerminalReceipt.succeeded(
                    acceptance.token(), digest, i, i, 20L + i));
        }
        return snapshot;
    }

    private static StageAssignmentSnapshot failedAssignments() throws Exception {
        StageAssignmentSnapshot snapshot = terminalAssignments(1, true);
        StageAssignmentManifest manifest = snapshot.manifest();
        StageAssignmentSnapshot failed = StageAssignmentSnapshot.initial(manifest);
        StageAssignmentAcceptance acceptance = new StageAssignmentAcceptance(
                failed.token(0), sha256("failed-command"), 10L);
        failed = failed.withAcceptance(acceptance);
        return failed.withTerminal(StageTerminalReceipt.failed(
                acceptance.token(), acceptance.commandDigest(), 1L, 0L,
                "FAILED", sha256("failure"), 20L));
    }

    private static ManifestMemberReceipt member(
            StageAssignmentSnapshot assignments, int ordinal, String name) {
        return ManifestMemberReceipt.sealed(
                assignments.acceptance(ordinal).token(),
                assignments.acceptance(ordinal).commandDigest(),
                "domain-journal", 71L, "publication-journal",
                descriptor(name, ordinal), 40L + ordinal);
    }

    private static CommitManifest.Member descriptor(String name, int ordinal) {
        return new CommitManifest.Member(name,
                "data/71/" + name + ".data", "schema-" + ordinal,
                ordinal, filledDigest(ordinal + 1));
    }

    private static List<byte[]> memberShards(ManifestCommitJournalSnapshot journal) {
        List<byte[]> shards = new ArrayList<>();
        for (int i = 0; i < journal.assignments().manifest().shardCount(); i++) {
            shards.add(ManifestCommitJournalCodec.encodeMemberShard(journal, i));
        }
        return shards;
    }

    private static byte[] filledDigest(int value) {
        byte[] digest = new byte[CommitManifest.SHA256_BYTES];
        Arrays.fill(digest, (byte) value);
        return digest;
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static byte[] sha256(String value) throws Exception {
        return sha256(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static byte[] sha256(byte[] value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(value);
    }
}
