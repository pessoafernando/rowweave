package io.rowweave.upgrade;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Canonical, checksummed and bounded UpgradeAuthorityRecord frame. */
public final class UpgradeAuthorityRecordCodec {
    public static final int MAX_FRAME_BYTES = 64 * 1024;

    private static final byte[] MAGIC =
            "RWXXUPA1".getBytes(StandardCharsets.US_ASCII);
    private static final int CODEC_VERSION = 1;

    private UpgradeAuthorityRecordCodec() {
    }

    public static byte[] encode(UpgradeAuthorityRecord record) {
        if (record == null) throw new IllegalArgumentException("authority record is required");
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(record.recordVersion());
            writeHeader(out, record.activeHeader());
            writeHeader(out, record.sourceHeader());
            writeHeader(out, record.targetHeader());
            out.writeLong(record.generation());
            out.writeInt(record.phase().wireId());
            UpgradeCodecSupport.writeString(out, record.sourceNamespace());
            UpgradeCodecSupport.writeDigest(out, record.sourceDigest());
            UpgradeCodecSupport.writeString(out, record.shadowNamespace());
            UpgradeCodecSupport.writeDigest(out, record.shadowDigest());
            writeOwner(out, record.owner());
            out.writeLong(record.expectedActivePointerVersion());
            UpgradeCodecSupport.writeString(out, record.activeNamespace());
            out.writeLong(record.activePointerVersion());
            out.writeLong(record.writerEpoch());
            out.writeLong(record.verifiedObjectCount());
            out.writeInt(record.verifiedShardCount());
            UpgradeCodecSupport.writeDigest(out, record.verifiedSourceDigest());
            UpgradeCodecSupport.writeDigest(out, record.verifiedTargetDigest());
            out.writeLong(record.rollbackDeadlineEpochMillis());
            out.writeBoolean(record.writesAllowed());
            out.writeBoolean(record.newSemanticsObserved());
            out.writeInt(record.commandEvidence().size());
            for (UpgradeCommandEvidence evidence : record.commandEvidence()) {
                UpgradeCodecSupport.writeString(out, evidence.commandId());
                UpgradeCodecSupport.writeDigest(out, evidence.commandDigest());
            }
            out.flush();
            return UpgradeCodecSupport.frame(
                    MAGIC, CODEC_VERSION, bytes.toByteArray(), MAX_FRAME_BYTES);
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory authority encoding failed", impossible);
        }
    }

    public static UpgradeAuthorityRecord decode(byte[] frame) {
        byte[] body = UpgradeCodecSupport.body(
                frame, MAGIC, CODEC_VERSION, MAX_FRAME_BYTES);
        try {
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(body));
            int version = input.readInt();
            StoreFormatHeader active = readHeader(input);
            StoreFormatHeader source = readHeader(input);
            StoreFormatHeader target = readHeader(input);
            long generation = input.readLong();
            UpgradePhase phase = UpgradePhase.fromWireId(input.readInt());
            String sourceNamespace = UpgradeCodecSupport.readString(input);
            String sourceDigest = UpgradeCodecSupport.readDigest(input);
            String shadowNamespace = UpgradeCodecSupport.readString(input);
            String shadowDigest = UpgradeCodecSupport.readDigest(input);
            UpgradeOwner owner = readOwner(input);
            long expectedPointerVersion = input.readLong();
            String activeNamespace = UpgradeCodecSupport.readString(input);
            long activePointerVersion = input.readLong();
            long writerEpoch = input.readLong();
            long objectCount = input.readLong();
            int shardCount = input.readInt();
            String verifiedSource = UpgradeCodecSupport.readDigest(input);
            String verifiedTarget = UpgradeCodecSupport.readDigest(input);
            long rollbackDeadline = input.readLong();
            boolean writesAllowed = input.readBoolean();
            boolean newSemanticsObserved = input.readBoolean();
            int evidenceCount = input.readInt();
            if (evidenceCount < 0
                    || evidenceCount > UpgradeAuthorityRecord.MAX_COMMAND_EVIDENCE) {
                throw new IllegalArgumentException("command evidence count is invalid");
            }
            List<UpgradeCommandEvidence> evidence =
                    new ArrayList<UpgradeCommandEvidence>(evidenceCount);
            for (int index = 0; index < evidenceCount; index++) {
                evidence.add(new UpgradeCommandEvidence(
                        UpgradeCodecSupport.readString(input),
                        UpgradeCodecSupport.readDigest(input)));
            }
            UpgradeCodecSupport.requireConsumed(input);
            UpgradeAuthorityRecord record = new UpgradeAuthorityRecord(
                    version, active, source, target, generation, phase,
                    sourceNamespace, sourceDigest, shadowNamespace, shadowDigest,
                    owner, expectedPointerVersion, activeNamespace,
                    activePointerVersion, writerEpoch, objectCount, shardCount,
                    verifiedSource, verifiedTarget, rollbackDeadline,
                    writesAllowed, newSemanticsObserved, evidence);
            if (!Arrays.equals(frame, encode(record))) {
                throw new IllegalArgumentException("authority frame is not canonical");
            }
            return record;
        } catch (IOException malformed) {
            throw UpgradeCodecSupport.malformed("authority record", malformed);
        }
    }

    private static void writeHeader(DataOutputStream out, StoreFormatHeader header)
            throws IOException {
        byte[] body = UpgradeCodecSupport.encodeHeaderBody(header);
        out.writeInt(body.length);
        out.write(body);
    }

    private static StoreFormatHeader readHeader(DataInputStream input)
            throws IOException {
        int length = input.readInt();
        if (length <= 0 || length > StoreFormatHeaderCodec.MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("nested store header length is invalid");
        }
        byte[] body = new byte[length];
        input.readFully(body);
        DataInputStream nested = new DataInputStream(new ByteArrayInputStream(body));
        StoreFormatHeader header = UpgradeCodecSupport.decodeHeaderBody(nested);
        UpgradeCodecSupport.requireConsumed(nested);
        return header;
    }

    private static void writeOwner(DataOutputStream out, UpgradeOwner owner)
            throws IOException {
        out.writeLong(owner.term());
        UpgradeCodecSupport.writeString(out, owner.masterId());
        UpgradeCodecSupport.writeString(out, owner.incarnationId());
        out.writeLong(owner.fence());
    }

    private static UpgradeOwner readOwner(DataInputStream input)
            throws IOException {
        return new UpgradeOwner(input.readLong(),
                UpgradeCodecSupport.readString(input),
                UpgradeCodecSupport.readString(input), input.readLong());
    }
}
