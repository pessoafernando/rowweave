package io.rowweave.run;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.BatchSinkDescriptorCodec;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestCommitWire;
import io.rowweave.commit.SchemaFingerprint;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** descriptor, locator and journal frame profile through 4096 Sinks. */
@Tag("profile")
class BatchSinkJournalScaleProfileTest {
    private static final int[] SINK_COUNTS = {1, 64, 512, 4096};
    private static final long CONTROL_RESIDENT_LIMIT = 256L * 1024L * 1024L;
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType));
    private static final String SCHEMA = SchemaFingerprint.of(META);
    private static final String DOMAIN = "sink-v2-profile-domain";

    @Test
    void descriptorLocatorAndJournalFramesRemainBoundedThrough4096Sinks()
            throws Exception {
        for (int sinkCount : SINK_COUNTS) {
            ProfileResult result = profile(sinkCount);
            System.out.printf("[SINK-V2-JOURNAL-PROFILE] sinks=%d "
                            + "descriptors=%dB maxLocator=%dB maxMemberShard=%dB "
                            + "intent=%dB resident=%dB%n",
                    sinkCount, result.descriptorBytes, result.maxLocatorBytes,
                    result.maxMemberShardBytes, result.intentBytes,
                    result.residentBytes);
        }
    }

    private static ProfileResult profile(int sinkCount) throws Exception {
        RunAttemptFence fence = new RunAttemptFence(
                "run-" + sinkCount, "attempt-1", 1L);
        List<StageAssignmentRecord> records = new ArrayList<>(sinkCount);
        for (int ordinal = 0; ordinal < sinkCount; ordinal++) {
            records.add(StageAssignmentRecord.create(
                    fence, 1L, ordinal, ordinal + 1L, ordinal, 0,
                    StageAssignmentRole.SINK, "worker", "127.0.0.1",
                    7100, -1, "incarnation"));
        }
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fence, "artifact", 1L, records);
        List<StageAssignmentAcceptance> acceptances =
                new ArrayList<>(sinkCount);
        List<StageTerminalReceipt> terminals = new ArrayList<>(sinkCount);
        List<ManifestMemberReceipt> members = new ArrayList<>(sinkCount);
        long descriptorBytes = 0L;
        long locatorBytes = 0L;
        int maxLocator = 0;
        for (int ordinal = 0; ordinal < sinkCount; ordinal++) {
            StageAssignmentRecord record = records.get(ordinal);
            StageAssignmentToken token = StageAssignmentToken.forAssignment(
                    manifest, record, 1L);
            byte[] commandDigest = digest(ordinal);
            acceptances.add(new StageAssignmentAcceptance(
                    token, commandDigest, ordinal));
            terminals.add(StageTerminalReceipt.succeeded(
                    token, commandDigest, 1L, 1L, ordinal));
            BatchSinkDescriptor descriptor = descriptor(ordinal);
            descriptorBytes += BatchSinkDescriptorCodec.encode(descriptor).length;
            CommitManifest.Member member = member(ordinal);
            SinkAttemptIdentity identity = SinkAttemptIdentity.forAssignment(
                    manifest, record, "output-" + ordinal, descriptor);
            PreparedSinkLocator locator = new PreparedSinkLocator(
                    PreparedSinkLocator.CURRENT_VERSION, identity,
                    BatchSinkDescriptor.CapabilityProfile
                            .MANIFEST_ATOMIC_GROUP_V1,
                    DOMAIN, ManifestCommitWire.encodeMember(member),
                    member.length(), member.sha256(), ordinal);
            byte[] locatorFrame = PreparedSinkLocatorCodec.encode(locator);
            locatorBytes += locatorFrame.length;
            maxLocator = Math.max(maxLocator, locatorFrame.length);
            members.add(ManifestMemberReceipt.sealed(
                    token, commandDigest, DOMAIN, 1L, "publication-1",
                    member, ordinal, locator));
        }
        assertTrue(descriptorBytes <= 4L * 1024L * 1024L);
        StageAssignmentSnapshot assignments = new StageAssignmentSnapshot(
                manifest, 1L, acceptances, terminals);
        ManifestCommitJournalSnapshot journal =
                new ManifestCommitJournalSnapshot(assignments, members, null);
        assertEquals(sinkCount, journal.expectedMemberCount());
        assertTrue(journal.allExpectedMembersSealed());
        long journalBytes = 0L;
        int maxMemberShard = 0;
        for (int shard = 0; shard < manifest.shardCount(); shard++) {
            byte[] frame = ManifestCommitJournalCodec.encodeMemberShard(
                    journal, shard);
            journalBytes += frame.length;
            maxMemberShard = Math.max(maxMemberShard, frame.length);
            assertTrue(frame.length
                    <= ManifestCommitJournalCodec.MAX_MEMBER_SHARD_FRAME_BYTES);
        }
        byte[] intent = ManifestCommitJournalCodec.encodeIntent(
                journal.buildIntent());
        assertTrue(intent.length
                <= ManifestCommitJournalCodec.MAX_INTENT_FRAME_BYTES);
        long resident = descriptorBytes + locatorBytes + journalBytes + intent.length;
        assertTrue(resident <= CONTROL_RESIDENT_LIMIT);
        return new ProfileResult(descriptorBytes, maxLocator,
                maxMemberShard, intent.length, resident);
    }

    private static BatchSinkDescriptor descriptor(int ordinal) {
        return new BatchSinkDescriptor(
                BatchSinkDescriptor.CURRENT_VERSION,
                "rowweave.manifest-file", 1, SCHEMA,
                "destination-" + ordinal, DOMAIN,
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);
    }

    private static CommitManifest.Member member(int ordinal) throws Exception {
        String name = "member-" + ordinal;
        return new CommitManifest.Member(
                name, "data/1/" + name + ".data", SCHEMA,
                1L, digest(ordinal));
    }

    private static byte[] digest(int value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(
                new byte[] {(byte) (value >>> 24), (byte) (value >>> 16),
                        (byte) (value >>> 8), (byte) value});
    }

    private static final class ProfileResult {
        final long descriptorBytes;
        final int maxLocatorBytes;
        final int maxMemberShardBytes;
        final int intentBytes;
        final long residentBytes;

        ProfileResult(long descriptorBytes, int maxLocatorBytes,
                      int maxMemberShardBytes, int intentBytes,
                      long residentBytes) {
            this.descriptorBytes = descriptorBytes;
            this.maxLocatorBytes = maxLocatorBytes;
            this.maxMemberShardBytes = maxMemberShardBytes;
            this.intentBytes = intentBytes;
            this.residentBytes = residentBytes;
        }
    }
}
