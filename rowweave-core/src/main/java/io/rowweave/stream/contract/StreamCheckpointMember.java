package io.rowweave.stream.contract;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

public final class StreamCheckpointMember {
    public enum Kind {
        SOURCE_POSITION(1, CheckpointEvidence.Kind.SOURCE_POSITION),
        STATE(2, CheckpointEvidence.Kind.STATE),
        TRANSACTIONAL_SINK(3, CheckpointEvidence.Kind.TRANSACTIONAL_SINK),
        EAGER_SINK_CAPABILITY(4, CheckpointEvidence.Kind.EAGER_SINK_CAPABILITY),
        SOURCE_COORDINATOR(5, CheckpointEvidence.Kind.SOURCE_COORDINATOR),
        CHANNEL_STATE(6, CheckpointEvidence.Kind.CHANNEL_STATE);

        private final int wireId;
        private final CheckpointEvidence.Kind evidenceKind;

        Kind(int wireId, CheckpointEvidence.Kind evidenceKind) {
            this.wireId = wireId;
            this.evidenceKind = evidenceKind;
        }

        int wireId() { return wireId; }
        public CheckpointEvidence.Kind evidenceKind() { return evidenceKind; }

        static Kind fromWire(int wireId) {
            for (Kind value : values()) {
                if (value.wireId == wireId) return value;
            }
            throw StreamContractSupport.failure("UNKNOWN_CHECKPOINT_MEMBER_KIND",
                    "checkpoint.member.kind", "unknown checkpoint member kind");
        }
    }

    private final Kind kind;
    private final String operatorUid;
    private final int subtaskIndex;
    private final String subjectId;
    private final String codecId;
    private final long logicalPosition;
    private final int keyGroupStart;
    private final int keyGroupEnd;
    private final int schemaVersion;
    private final String contractDigest;
    private final String contentDigest;
    private final long contentBytes;
    private final String memberId;

    private StreamCheckpointMember(
            Kind kind, String operatorUid, int subtaskIndex, String subjectId,
            String codecId, long logicalPosition, int keyGroupStart,
            int keyGroupEnd, int schemaVersion, String contractDigest,
            String contentDigest, long contentBytes) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.operatorUid = StreamContractSupport.text(
                operatorUid, "checkpoint.member.operatorUid", 256);
        if (subtaskIndex < 0 || contentBytes < 0L) {
            throw StreamContractSupport.failure("INVALID_CHECKPOINT_MEMBER",
                    "checkpoint.member", "subtask and size must not be negative");
        }
        this.subtaskIndex = subtaskIndex;
        this.subjectId = StreamContractSupport.text(
                subjectId, "checkpoint.member.subjectId", 512);
        this.codecId = codecId == null ? null : StreamContractSupport.text(
                codecId, "checkpoint.member.codecId", 256);
        this.logicalPosition = logicalPosition;
        this.keyGroupStart = keyGroupStart;
        this.keyGroupEnd = keyGroupEnd;
        this.schemaVersion = schemaVersion;
        this.contractDigest = contractDigest == null ? null
                : StreamContractSupport.digest(
                        contractDigest, "checkpoint.member.contractDigest");
        this.contentDigest = StreamContractSupport.digest(
                contentDigest, "checkpoint.member.contentDigest");
        this.contentBytes = contentBytes;
        this.memberId = "stream-member:" + stableIdentityDigest();
    }

    public static StreamCheckpointMember sourcePosition(
            String sourceOperatorUid, int subtaskIndex,
            String splitId, String positionCodecId,
            long monotonicPosition, String positionDigest, long positionBytes) {
        if (monotonicPosition < 0L) {
            throw StreamContractSupport.failure("INVALID_SOURCE_POSITION",
                    "checkpoint.member.logicalPosition",
                    "source position must not be negative");
        }
        return new StreamCheckpointMember(
                Kind.SOURCE_POSITION, sourceOperatorUid, subtaskIndex, splitId,
                positionCodecId, monotonicPosition, -1, -1, 0,
                null, positionDigest, positionBytes);
    }

    public static StreamCheckpointMember stateHandle(
            String operatorUid, int subtaskIndex,
            int keyGroupStart, int keyGroupEnd, String stateName,
            String serializerContractId, int stateSchemaVersion,
            String contentDigest, long contentBytes) {
        if (keyGroupStart < 0 || keyGroupEnd < keyGroupStart
                || keyGroupEnd >= PortableStreamPlan.MAX_PARALLELISM) {
            throw StreamContractSupport.failure("INVALID_KEY_GROUP_RANGE",
                    "checkpoint.member.keyGroups", "invalid key-group range");
        }
        if (stateSchemaVersion <= 0) {
            throw StreamContractSupport.failure("INVALID_STATE_HANDLE",
                    "checkpoint.member.schemaVersion",
                    "positive state schema version is required");
        }
        return new StreamCheckpointMember(
                Kind.STATE, operatorUid, subtaskIndex, stateName,
                serializerContractId, -1L, keyGroupStart, keyGroupEnd,
                stateSchemaVersion, null, contentDigest, contentBytes);
    }

    public static StreamCheckpointMember transactionalSink(
            String sinkOperatorUid, int subtaskIndex, String transactionIdentity,
            String providerContractDigest, String preparedReceiptDigest,
            long receiptBytes) {
        return new StreamCheckpointMember(
                Kind.TRANSACTIONAL_SINK, sinkOperatorUid, subtaskIndex,
                transactionIdentity, null, -1L, -1, -1, 0,
                providerContractDigest, preparedReceiptDigest, receiptBytes);
    }

    public static StreamCheckpointMember eagerSinkCapability(
            String sinkOperatorUid, int subtaskIndex,
            String providerContractDigest, String capabilityDigest) {
        return new StreamCheckpointMember(
                Kind.EAGER_SINK_CAPABILITY, sinkOperatorUid, subtaskIndex,
                "eager-capability", null, -1L, -1, -1, 0,
                providerContractDigest, capabilityDigest, 0L);
    }

    public static StreamCheckpointMember sourceCoordinator(
            String sourceOperatorUid, String coordinatorCodecId,
            String contentDigest, long contentBytes) {
        return new StreamCheckpointMember(
                Kind.SOURCE_COORDINATOR, sourceOperatorUid, 0,
                "source-coordinator", coordinatorCodecId, -1L,
                -1, -1, 2, null, contentDigest, contentBytes);
    }

    public static StreamCheckpointMember channelState(
            String operatorUid, int subtaskIndex, String inputChannelId,
            long channelEpoch, String contentDigest, long contentBytes) {
        if (channelEpoch <= 0L) {
            throw StreamContractSupport.failure("INVALID_CHANNEL_STATE",
                    "checkpoint.member.logicalPosition",
                    "positive channel epoch is required");
        }
        return new StreamCheckpointMember(
                Kind.CHANNEL_STATE, operatorUid, subtaskIndex,
                inputChannelId, StreamChannelStateCodec.CODEC_ID,
                channelEpoch, -1, -1, 1, null,
                contentDigest, contentBytes);
    }

    public Kind kind() { return kind; }
    public String operatorUid() { return operatorUid; }
    public int subtaskIndex() { return subtaskIndex; }
    public String subjectId() { return subjectId; }
    public String codecId() { return codecId; }
    public long logicalPosition() { return logicalPosition; }
    public int keyGroupStart() { return keyGroupStart; }
    public int keyGroupEnd() { return keyGroupEnd; }
    public int schemaVersion() { return schemaVersion; }
    public String contractDigest() { return contractDigest; }
    public String contentDigest() { return contentDigest; }
    public long contentBytes() { return contentBytes; }
    public String memberId() { return memberId; }

    public CheckpointEvidence toEvidence() {
        return new CheckpointEvidence(kind.evidenceKind(), memberId,
                contentDigest, contentBytes);
    }

    private String stableIdentityDigest() {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.writeInt(kind.wireId());
                writeText(output, operatorUid);
                output.writeInt(subtaskIndex);
                writeText(output, subjectId);
                if (kind == Kind.SOURCE_POSITION) {
                    writeText(output, codecId);
                } else if (kind == Kind.SOURCE_COORDINATOR) {
                    writeText(output, codecId);
                    output.writeInt(schemaVersion);
                } else if (kind == Kind.CHANNEL_STATE) {
                    writeText(output, codecId);
                    output.writeLong(logicalPosition);
                    output.writeInt(schemaVersion);
                } else if (kind == Kind.STATE) {
                    writeText(output, codecId);
                    output.writeInt(keyGroupStart);
                    output.writeInt(keyGroupEnd);
                    output.writeInt(schemaVersion);
                } else {
                    writeText(output, contractDigest);
                }
            }
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(bytes.toByteArray());
            return CheckpointManifestCodec.hex(digest);
        } catch (IOException | NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(encoded.length);
        output.write(encoded);
    }

    private static void writeNullableText(DataOutputStream output, String value)
            throws IOException {
        output.writeBoolean(value != null);
        if (value != null) writeText(output, value);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof StreamCheckpointMember)) return false;
        StreamCheckpointMember that = (StreamCheckpointMember) other;
        return subtaskIndex == that.subtaskIndex
                && logicalPosition == that.logicalPosition
                && keyGroupStart == that.keyGroupStart
                && keyGroupEnd == that.keyGroupEnd
                && schemaVersion == that.schemaVersion
                && contentBytes == that.contentBytes
                && kind == that.kind
                && operatorUid.equals(that.operatorUid)
                && subjectId.equals(that.subjectId)
                && Objects.equals(codecId, that.codecId)
                && Objects.equals(contractDigest, that.contractDigest)
                && contentDigest.equals(that.contentDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, operatorUid, subtaskIndex, subjectId, codecId,
                logicalPosition, keyGroupStart, keyGroupEnd, schemaVersion,
                contractDigest, contentDigest, contentBytes);
    }
}
