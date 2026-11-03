package io.rowweave.run;

import io.rowweave.QualityGateDecision;
import io.rowweave.QualityGateDecisionCodec;
import io.rowweave.commit.CommitManifest;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Canonical bounded wire/storage frames for the built-in Manifest commit journal. */
public final class ManifestCommitJournalCodec {
    public static final int MAX_MEMBER_SHARD_FRAME_BYTES = 512 * 1024;
    public static final int MAX_INTENT_FRAME_BYTES = 64 * 1024;
    public static final int MAX_QUALITY_INTENT_FRAME_BYTES = 192 * 1024;

    private static final int MAGIC = 0x414D4A31; // AMJ1
    private static final int VERSION_V1 = 1;
    private static final int VERSION_V2 = 2;
    private static final int MEMBER_SHARD = 1;
    private static final int INTENT = 2;
    private static final int QUALITY_DECIDED_PASS = 1;
    private static final int QUALITY_DECIDED_FAIL = 2;
    private static final int COMMIT_INTENT_WITH_QUALITY = 3;
    private static final int HEADER_BYTES = 12;
    private static final int CHECKSUM_BYTES = 32;

    private ManifestCommitJournalCodec() { }

    public static byte[] encodeMemberShard(
            ManifestCommitJournalSnapshot snapshot, int shardIndex) {
        if (snapshot == null) throw new IllegalArgumentException("snapshot is required");
        StageAssignmentManifest manifest = snapshot.assignments().manifest();
        int first = firstOrdinal(shardIndex);
        int count = recordCount(manifest.assignmentCount(), shardIndex);
        List<ManifestMemberReceipt> members = new ArrayList<>(count);
        for (int i = 0; i < count; i++) members.add(snapshot.member(first + i));
        return encodeMemberShard(manifest.fence(), manifest.artifactId(),
                manifest.generation(), manifest.assignmentCount(),
                manifest.shardCount(), shardIndex, members);
    }

    static byte[] encodeMemberShard(
            StageAssignmentManifest manifest, int shardIndex,
            List<ManifestMemberReceipt> members) {
        if (manifest == null) throw new IllegalArgumentException("manifest is required");
        return encodeMemberShard(manifest.fence(), manifest.artifactId(),
                manifest.generation(), manifest.assignmentCount(),
                manifest.shardCount(), shardIndex, members);
    }

    public static byte[] encodeMemberShard(
            MemberShardSnapshot snapshot, ManifestMemberReceipt receipt) {
        if (snapshot == null) throw new IllegalArgumentException("snapshot is required");
        if (receipt == null) throw new IllegalArgumentException("receipt is required");
        int offset = snapshot.offset(receipt.token().physicalOrdinal());
        if (snapshot.members.get(offset) != null) {
            throw new IllegalArgumentException("member receipt is already present");
        }
        List<ManifestMemberReceipt> copy = new ArrayList<>(snapshot.members);
        copy.set(offset, receipt);
        MemberShardSnapshot updated = new MemberShardSnapshot(
                snapshot.assignments, copy);
        return encodeMemberShard(updated.assignments.fence(),
                updated.assignments.artifactId(), updated.assignments.generation(),
                updated.assignments.assignmentCount(), updated.assignments.shardCount(),
                updated.assignments.shardIndex(), updated.members);
    }

    public static byte[] encodeIntent(ManifestCommitIntent intent) {
        if (intent == null) throw new IllegalArgumentException("intent is required");
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(4096);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            if (intent.hasQualityDecision()) {
                out.writeByte(COMMIT_INTENT_WITH_QUALITY);
                writeQualityDecision(out, intent.qualityDecisionBytes());
            }
            writeIntentBody(out, intent);
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        return frame(INTENT, bytes.toByteArray(), intent.hasQualityDecision()
                ? MAX_QUALITY_INTENT_FRAME_BYTES : MAX_INTENT_FRAME_BYTES,
                intent.hasQualityDecision() ? VERSION_V2 : VERSION_V1);
    }

    public static byte[] encodeQualityDecision(QualityGateDecision decision) {
        if (decision == null) {
            throw new IllegalArgumentException("quality decision is required");
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(4096);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeByte(decision.outcome() == QualityGateDecision.Outcome.PASS
                    ? QUALITY_DECIDED_PASS : QUALITY_DECIDED_FAIL);
            writeQualityDecision(out, QualityGateDecisionCodec.encode(decision));
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        return frame(INTENT, bytes.toByteArray(),
                MAX_QUALITY_INTENT_FRAME_BYTES, VERSION_V2);
    }

    public static ManifestCommitJournalSnapshot decodeSnapshot(
            StageAssignmentSnapshot assignments,
            List<byte[]> memberShardBytes, byte[] intentBytes) {
        if (assignments == null) throw new IllegalArgumentException("assignments are required");
        StageAssignmentManifest manifest = assignments.manifest();
        if (memberShardBytes == null
                || memberShardBytes.size() != manifest.shardCount()) {
            throw new IllegalArgumentException("member shard count mismatch");
        }
        List<ManifestMemberReceipt> members = new ArrayList<>(manifest.assignmentCount());
        for (int shard = 0; shard < manifest.shardCount(); shard++) {
            DecodedMemberShard decoded = decodeMemberShard(memberShardBytes.get(shard));
            requireIdentity(manifest, decoded, shard);
            members.addAll(decoded.members);
        }
        if (members.size() != manifest.assignmentCount()) {
            throw new IllegalArgumentException("member receipt count mismatch");
        }
        ManifestCommitJournalSnapshot snapshot =
                new ManifestCommitJournalSnapshot(assignments, members, null);
        if (intentBytes == null) return snapshot;
        DecodedSlot slot = decodeSlot(intentBytes);
        if (slot.decision != null) snapshot = snapshot.withQualityDecision(slot.decision);
        return slot.intent == null ? snapshot : snapshot.withIntent(slot.intent);
    }

    public static MemberShardSnapshot decodeMemberShard(
            StageAssignmentManifestCodec.ShardSnapshot assignments,
            byte[] memberShardBytes, int expectedShardIndex) {
        if (assignments == null) throw new IllegalArgumentException("assignments are required");
        DecodedMemberShard decoded = decodeMemberShard(memberShardBytes);
        if (decoded.shardIndex != expectedShardIndex
                || assignments.shardIndex() != expectedShardIndex
                || decoded.assignmentCount != assignments.assignmentCount()
                || decoded.shardCount != assignments.shardCount()
                || decoded.generation != assignments.generation()
                || !decoded.fence.equals(assignments.fence())
                || !decoded.artifactId.equals(assignments.artifactId())) {
            throw new IllegalArgumentException("member shard identity mismatch");
        }
        return new MemberShardSnapshot(assignments, decoded.members);
    }

    public static ManifestCommitIntent decodeIntent(byte[] encoded) {
        DecodedSlot slot = decodeSlot(encoded);
        if (slot.intent == null) {
            throw new IllegalArgumentException(
                    "manifest journal slot has no commit intent");
        }
        return slot.intent;
    }

    /** Decode the quality slot without materializing unrelated member shards. */
    public static QualitySlotSnapshot decodeQualitySlot(byte[] encoded) {
        if (encoded == null || encoded.length == 0) {
            return new QualitySlotSnapshot(null, null);
        }
        DecodedSlot slot = decodeSlot(encoded);
        if (slot.decision == null) {
            throw new IllegalArgumentException(
                    "quality manifest slot lacks a quality decision");
        }
        return new QualitySlotSnapshot(slot.decision, slot.intent);
    }

    private static ManifestCommitIntent decodeIntentBody(RunWire.Reader body) {
        RunAttemptFence fence = readFence(body, "intent.fence");
        long assignmentGeneration = body.readLong("intent.assignmentGeneration");
        String domainId = body.readText(
                "intent.domainId", ManifestMemberReceipt.MAX_DOMAIN_ID_BYTES);
        long generation = body.readLong("intent.generation");
        String publicationId = body.readText(
                "intent.publicationId", ManifestMemberReceipt.MAX_PUBLICATION_ID_BYTES);
        int memberCount = body.readInt("intent.memberCount");
        int shardCount = body.readInt("intent.shardCount");
        if (shardCount < 1 || shardCount > StageAssignmentManifest.MAX_SHARDS) {
            throw new IllegalArgumentException("intent shard count out of bounds");
        }
        List<byte[]> shardDigests = new ArrayList<>(shardCount);
        for (int i = 0; i < shardCount; i++) {
            shardDigests.add(body.readFixed(CHECKSUM_BYTES,
                    "intent.shardDigest[" + i + "]"));
        }
        byte[] manifestHash = body.readFixed(CHECKSUM_BYTES, "intent.manifestHash");
        return new ManifestCommitIntent(fence, assignmentGeneration,
                domainId, generation, publicationId, memberCount,
                shardDigests, manifestHash);
    }

    private static DecodedSlot decodeSlot(byte[] encoded) {
        FrameBody framed = body(
                encoded, INTENT, MAX_QUALITY_INTENT_FRAME_BYTES);
        if (framed.version == VERSION_V1) {
            if (encoded.length > MAX_INTENT_FRAME_BYTES) {
                throw new IllegalArgumentException(
                        "legacy manifest intent exceeds its frame bound");
            }
            ManifestCommitIntent intent = decodeIntentBody(framed.reader);
            framed.reader.finish("manifest commit intent body");
            return new DecodedSlot(null, intent);
        }
        RunWire.Reader body = framed.reader;
        int state = body.readUnsignedByte("qualitySlot.state");
        if (state < QUALITY_DECIDED_PASS || state > COMMIT_INTENT_WITH_QUALITY) {
            throw new IllegalArgumentException(
                    "quality manifest journal state is unsupported");
        }
        int decisionLength = body.readInt("qualitySlot.decisionLength");
        if (decisionLength < 1
                || decisionLength > QualityGateDecisionCodec.MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "quality decision length is out of bounds");
        }
        byte[] decisionBytes = body.readFixed(
                decisionLength, "qualitySlot.decision");
        QualityGateDecision decision = QualityGateDecisionCodec.decode(decisionBytes);
        byte[] policyDigest = body.readFixed(
                CHECKSUM_BYTES, "qualitySlot.policyDigest");
        byte[] decisionDigest = body.readFixed(
                CHECKSUM_BYTES, "qualitySlot.decisionDigest");
        if (!MessageDigest.isEqual(policyDigest, decision.policyDigest())
                || !MessageDigest.isEqual(decisionDigest,
                QualityGateDecisionCodec.digest(decision))) {
            throw new IllegalArgumentException(
                    "quality decision digest binding mismatch");
        }
        boolean pass = decision.outcome() == QualityGateDecision.Outcome.PASS;
        if ((state == QUALITY_DECIDED_PASS) != pass
                && state != COMMIT_INTENT_WITH_QUALITY) {
            throw new IllegalArgumentException(
                    "quality decision outcome and slot state disagree");
        }
        ManifestCommitIntent intent = null;
        if (state == COMMIT_INTENT_WITH_QUALITY) {
            if (!pass) {
                throw new IllegalArgumentException(
                        "failed quality decision cannot carry commit intent");
            }
            intent = ManifestCommitIntent.quality(
                    decodeIntentBody(body), decisionBytes);
        }
        body.finish("quality manifest journal slot");
        return new DecodedSlot(decision, intent);
    }

    private static void writeQualityDecision(
            DataOutputStream out, byte[] decisionBytes) throws IOException {
        if (decisionBytes == null || decisionBytes.length < 1
                || decisionBytes.length > QualityGateDecisionCodec.MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "quality decision bytes are out of bounds");
        }
        QualityGateDecision decision = QualityGateDecisionCodec.decode(decisionBytes);
        out.writeInt(decisionBytes.length);
        out.write(decisionBytes);
        out.write(decision.policyDigest());
        out.write(QualityGateDecisionCodec.digest(decision));
    }

    private static void writeIntentBody(
            DataOutputStream out, ManifestCommitIntent intent) throws IOException {
        writeFence(out, intent.fence());
        out.writeLong(intent.assignmentGeneration());
        RunWire.writeText(out, intent.domainId());
        out.writeLong(intent.generation());
        RunWire.writeText(out, intent.publicationId());
        out.writeInt(intent.memberCount());
        out.writeInt(intent.memberShardCount());
        for (int i = 0; i < intent.memberShardCount(); i++) {
            out.write(intent.memberShardDigest(i));
        }
        out.write(intent.manifestHash());
    }

    private static byte[] encodeMemberShard(
            RunAttemptFence fence, String artifactId, long assignmentGeneration,
            int assignmentCount, int shardCount, int shardIndex,
            List<ManifestMemberReceipt> members) {
        int expected = recordCount(assignmentCount, shardIndex);
        if (members == null || members.size() != expected || shardCount < 1
                || shardCount > StageAssignmentManifest.MAX_SHARDS) {
            throw new IllegalArgumentException("member shard shape mismatch");
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(32 * 1024);
        int version = VERSION_V1;
        for (ManifestMemberReceipt receipt : members) {
            if (receipt != null && receipt.hasPreparedLocator()) {
                version = VERSION_V2;
                break;
            }
        }
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            writeFence(out, fence);
            RunWire.writeText(out, artifactId);
            out.writeLong(assignmentGeneration);
            out.writeInt(assignmentCount);
            out.writeInt(shardCount);
            out.writeInt(shardIndex);
            out.writeInt(members.size());
            for (ManifestMemberReceipt receipt : members) {
                out.writeBoolean(receipt != null);
                if (receipt == null) continue;
                byte[] token = StageAssignmentManifestCodec.encodeToken(receipt.token());
                out.writeInt(token.length);
                out.write(token);
                out.write(receipt.commandDigest());
                RunWire.writeText(out, receipt.domainId());
                out.writeLong(receipt.generation());
                RunWire.writeText(out, receipt.publicationId());
                CommitManifest.Member member = receipt.member();
                RunWire.writeText(out, member.name());
                RunWire.writeText(out, member.relativeDataPath());
                RunWire.writeText(out, member.schemaFingerprint());
                out.writeLong(member.length());
                out.write(member.sha256());
                out.writeLong(receipt.sealedAtMillis());
                if (version >= VERSION_V2) {
                    out.writeBoolean(receipt.hasPreparedLocator());
                    if (receipt.hasPreparedLocator()) {
                        byte[] locator = PreparedSinkLocatorCodec.encode(
                                receipt.preparedLocator());
                        out.writeInt(locator.length);
                        out.write(locator);
                    }
                }
            }
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        return frame(MEMBER_SHARD, bytes.toByteArray(),
                MAX_MEMBER_SHARD_FRAME_BYTES, version);
    }

    private static DecodedMemberShard decodeMemberShard(byte[] encoded) {
        FrameBody framed = body(
                encoded, MEMBER_SHARD, MAX_MEMBER_SHARD_FRAME_BYTES);
        RunWire.Reader body = framed.reader;
        RunAttemptFence fence = readFence(body, "memberShard.fence");
        String artifactId = body.readText("memberShard.artifactId", RunText.MAX_ID_BYTES);
        long assignmentGeneration = body.readLong("memberShard.assignmentGeneration");
        int assignmentCount = body.readInt("memberShard.assignmentCount");
        int shardCount = body.readInt("memberShard.shardCount");
        int shardIndex = body.readInt("memberShard.shardIndex");
        int count = body.readInt("memberShard.recordCount");
        if (assignmentGeneration <= 0L
                || assignmentCount < 1
                || assignmentCount > StageAssignmentManifest.MAX_ASSIGNMENTS
                || shardCount < 1 || shardCount > StageAssignmentManifest.MAX_SHARDS
                || shardIndex < 0 || shardIndex >= shardCount
                || count != recordCount(assignmentCount, shardIndex)) {
            throw new IllegalArgumentException("member shard scalar fields out of bounds");
        }
        List<ManifestMemberReceipt> members = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            if (!body.readBoolean("memberShard.present[" + i + "]")) {
                members.add(null);
                continue;
            }
            int tokenLength = body.readInt("memberShard.tokenLength[" + i + "]");
            if (tokenLength < 1 || tokenLength > 2048) {
                throw new IllegalArgumentException("member token length out of bounds");
            }
            StageAssignmentToken token = StageAssignmentManifestCodec.decodeToken(
                    body.readFixed(tokenLength, "memberShard.token[" + i + "]"));
            byte[] commandDigest = body.readFixed(
                    CHECKSUM_BYTES, "memberShard.commandDigest[" + i + "]");
            String domainId = body.readText(
                    "memberShard.domainId[" + i + "]",
                    ManifestMemberReceipt.MAX_DOMAIN_ID_BYTES);
            long generation = body.readLong("memberShard.generation[" + i + "]");
            String publicationId = body.readText(
                    "memberShard.publicationId[" + i + "]",
                    ManifestMemberReceipt.MAX_PUBLICATION_ID_BYTES);
            String name = body.readText("memberShard.name[" + i + "]",
                    ManifestMemberReceipt.MAX_MEMBER_NAME_BYTES);
            String path = body.readText("memberShard.path[" + i + "]",
                    ManifestMemberReceipt.MAX_RELATIVE_PATH_BYTES);
            String schema = body.readText("memberShard.schema[" + i + "]",
                    ManifestMemberReceipt.MAX_SCHEMA_FINGERPRINT_BYTES);
            long length = body.readLong("memberShard.length[" + i + "]");
            byte[] hash = body.readFixed(CHECKSUM_BYTES,
                    "memberShard.hash[" + i + "]");
            long sealedAt = body.readLong("memberShard.sealedAt[" + i + "]");
            CommitManifest.Member member =
                    new CommitManifest.Member(name, path, schema, length, hash);
            PreparedSinkLocator locator = null;
            if (framed.version >= VERSION_V2
                    && body.readBoolean("memberShard.locatorPresent[" + i + "]")) {
                int locatorLength = body.readInt(
                        "memberShard.locatorLength[" + i + "]");
                if (locatorLength < 1
                        || locatorLength > PreparedSinkLocatorCodec.MAX_FRAME_BYTES) {
                    throw new IllegalArgumentException(
                            "prepared sink locator length out of bounds");
                }
                locator = PreparedSinkLocatorCodec.decode(body.readFixed(
                        locatorLength, "memberShard.locator[" + i + "]"));
            }
            members.add(locator == null
                    ? ManifestMemberReceipt.sealed(token, commandDigest,
                    domainId, generation, publicationId, member, sealedAt)
                    : ManifestMemberReceipt.sealed(token, commandDigest,
                    domainId, generation, publicationId, member, sealedAt, locator));
        }
        body.finish("manifest member shard body");
        return new DecodedMemberShard(fence, artifactId, assignmentGeneration,
                assignmentCount, shardCount, shardIndex, members);
    }

    private static void requireIdentity(StageAssignmentManifest manifest,
                                        DecodedMemberShard shard, int expectedIndex) {
        if (shard.shardIndex != expectedIndex
                || shard.assignmentCount != manifest.assignmentCount()
                || shard.shardCount != manifest.shardCount()
                || shard.generation != manifest.generation()
                || !shard.fence.equals(manifest.fence())
                || !shard.artifactId.equals(manifest.artifactId())) {
            throw new IllegalArgumentException(
                    "member shard identity mismatch at " + expectedIndex);
        }
    }

    private static int firstOrdinal(int shardIndex) {
        if (shardIndex < 0 || shardIndex >= StageAssignmentManifest.MAX_SHARDS) {
            throw new IllegalArgumentException("member shard index out of bounds");
        }
        return shardIndex * StageAssignmentManifest.MAX_RECORDS_PER_SHARD;
    }

    private static int recordCount(int assignmentCount, int shardIndex) {
        int first = firstOrdinal(shardIndex);
        int count = Math.min(StageAssignmentManifest.MAX_RECORDS_PER_SHARD,
                assignmentCount - first);
        if (assignmentCount < 1
                || assignmentCount > StageAssignmentManifest.MAX_ASSIGNMENTS
                || count < 1) {
            throw new IllegalArgumentException("member shard has no assignment range");
        }
        return count;
    }

    private static void writeFence(DataOutputStream out, RunAttemptFence fence)
            throws IOException {
        if (fence == null) throw new IllegalArgumentException("fence is required");
        RunWire.writeText(out, fence.runId());
        RunWire.writeText(out, fence.attemptId());
        out.writeLong(fence.fenceToken());
    }

    private static RunAttemptFence readFence(RunWire.Reader body, String path) {
        return new RunAttemptFence(
                body.readText(path + ".runId", RunText.MAX_ID_BYTES),
                body.readText(path + ".attemptId", RunText.MAX_ID_BYTES),
                body.readLong(path + ".fenceToken"));
    }

    private static byte[] frame(int kind, byte[] payload, int maxBytes, int version) {
        if (payload == null || HEADER_BYTES + payload.length + CHECKSUM_BYTES > maxBytes) {
            throw new IllegalArgumentException("manifest journal frame exceeds limit");
        }
        if (version < VERSION_V1 || version > VERSION_V2) {
            throw new IllegalArgumentException("manifest journal version is unsupported");
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(
                HEADER_BYTES + payload.length + CHECKSUM_BYTES);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC);
            out.writeShort(version);
            out.writeShort(kind);
            out.writeInt(payload.length);
            out.write(payload);
            out.flush();
            out.write(sha256(bytes.toByteArray()));
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        return bytes.toByteArray();
    }

    private static FrameBody body(byte[] encoded, int expectedKind, int maxBytes) {
        if (encoded == null || encoded.length < HEADER_BYTES + CHECKSUM_BYTES
                || encoded.length > maxBytes) {
            throw new IllegalArgumentException("manifest journal frame length out of bounds");
        }
        RunWire.Reader frame = new RunWire.Reader(encoded);
        if (frame.readInt("frame.magic") != MAGIC) {
            throw new IllegalArgumentException("invalid manifest journal magic");
        }
        int version = (frame.readUnsignedByte("frame.version.high") << 8)
                | frame.readUnsignedByte("frame.version.low");
        int kind = (frame.readUnsignedByte("frame.kind.high") << 8)
                | frame.readUnsignedByte("frame.kind.low");
        int bodyLength = frame.readInt("frame.bodyLength");
        if (version < VERSION_V1 || version > VERSION_V2
                || kind != expectedKind || bodyLength < 0
                || encoded.length != HEADER_BYTES + bodyLength + CHECKSUM_BYTES) {
            throw new IllegalArgumentException("invalid manifest journal frame header");
        }
        byte[] payload = frame.readFixed(bodyLength, "frame.body");
        byte[] checksum = frame.readFixed(CHECKSUM_BYTES, "frame.checksum");
        frame.finish("manifest journal frame");
        if (!MessageDigest.isEqual(sha256(
                Arrays.copyOf(encoded, HEADER_BYTES + bodyLength)), checksum)) {
            throw new IllegalArgumentException("manifest journal checksum mismatch");
        }
        return new FrameBody(version, new RunWire.Reader(payload));
    }

    private static final class FrameBody {
        final int version;
        final RunWire.Reader reader;
        FrameBody(int version, RunWire.Reader reader) {
            this.version = version;
            this.reader = reader;
        }
    }

    private static final class DecodedSlot {
        final QualityGateDecision decision;
        final ManifestCommitIntent intent;

        DecodedSlot(QualityGateDecision decision, ManifestCommitIntent intent) {
            this.decision = decision;
            this.intent = intent;
        }
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    /** Authenticated bounded view used by one-shard authority CAS. */
    public static final class MemberShardSnapshot {
        private final StageAssignmentManifestCodec.ShardSnapshot assignments;
        private final List<ManifestMemberReceipt> members;

        private MemberShardSnapshot(
                StageAssignmentManifestCodec.ShardSnapshot assignments,
                List<ManifestMemberReceipt> members) {
            int expected = recordCount(assignments.assignmentCount(), assignments.shardIndex());
            if (members == null || members.size() != expected) {
                throw new IllegalArgumentException("member shard record count mismatch");
            }
            List<ManifestMemberReceipt> copy = new ArrayList<>(members);
            for (int i = 0; i < copy.size(); i++) {
                ManifestMemberReceipt receipt = copy.get(i);
                if (receipt == null) continue;
                int ordinal = assignments.firstOrdinal() + i;
                StageAssignmentRecord record = assignments.record(ordinal);
                StageAssignmentAcceptance acceptance = assignments.acceptance(ordinal);
                StageTerminalReceipt terminal = assignments.terminal(ordinal);
                if (!ManifestCommitJournalSnapshot.outputRole(record.role())
                        || acceptance == null || terminal == null || !terminal.succeeded()
                        || !receipt.token().equals(acceptance.token())
                        || !receipt.token().equals(terminal.token())
                        || !MessageDigest.isEqual(receipt.commandDigest(),
                        acceptance.commandDigest())
                        || !MessageDigest.isEqual(receipt.commandDigest(),
                        terminal.commandDigest())) {
                    throw new IllegalArgumentException(
                            "member shard assignment evidence mismatch");
                }
            }
            this.assignments = assignments;
            this.members = Collections.unmodifiableList(copy);
        }

        public StageAssignmentManifestCodec.ShardSnapshot assignments() {
            return assignments;
        }
        public ManifestMemberReceipt member(int physicalOrdinal) {
            return members.get(offset(physicalOrdinal));
        }
        public MemberShardSnapshot withMember(ManifestMemberReceipt receipt) {
            int offset = offset(receipt.token().physicalOrdinal());
            if (members.get(offset) != null) {
                throw new IllegalArgumentException("member receipt is already present");
            }
            List<ManifestMemberReceipt> copy = new ArrayList<>(members);
            copy.set(offset, receipt);
            return new MemberShardSnapshot(assignments, copy);
        }
        private int offset(int ordinal) {
            int offset = ordinal - assignments.firstOrdinal();
            if (offset < 0 || offset >= members.size()) {
                throw new IllegalArgumentException("ordinal is outside member shard");
            }
            return offset;
        }
    }

    /** Bounded projection of EMPTY, QUALITY_DECIDED, or quality intent state. */
    public static final class QualitySlotSnapshot {
        private final QualityGateDecision decision;
        private final ManifestCommitIntent intent;

        private QualitySlotSnapshot(
                QualityGateDecision decision, ManifestCommitIntent intent) {
            this.decision = decision;
            this.intent = intent;
        }

        public QualityGateDecision decision() { return decision; }
        public ManifestCommitIntent intent() { return intent; }
    }

    private static final class DecodedMemberShard {
        final RunAttemptFence fence;
        final String artifactId;
        final long generation;
        final int assignmentCount;
        final int shardCount;
        final int shardIndex;
        final List<ManifestMemberReceipt> members;

        DecodedMemberShard(RunAttemptFence fence, String artifactId, long generation,
                           int assignmentCount, int shardCount, int shardIndex,
                           List<ManifestMemberReceipt> members) {
            this.fence = fence;
            this.artifactId = artifactId;
            this.generation = generation;
            this.assignmentCount = assignmentCount;
            this.shardCount = shardCount;
            this.shardIndex = shardIndex;
            this.members = members;
        }
    }
}
