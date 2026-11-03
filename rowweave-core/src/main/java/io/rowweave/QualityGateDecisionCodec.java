package io.rowweave;

import io.rowweave.run.RunAttemptFence;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Canonical bounded wire representation for {@link QualityGateDecision}. */
public final class QualityGateDecisionCodec {
    public static final int MAX_FRAME_BYTES = 128 * 1024;
    public static final int MAX_EXEMPLAR_SECTION_BYTES = 32 * 1024;

    private static final int MAGIC = 0x52575144; // RWQD
    private static final int VERSION = 1;
    private static final int HEADER_BYTES = 10;
    private static final int CHECKSUM_BYTES = 32;

    private QualityGateDecisionCodec() {
    }

    public static byte[] encode(QualityGateDecision decision) {
        if (decision == null) throw invalid("decision is required");
        byte[] body = encodeBody(decision);
        int frameLength = checkedAdd(HEADER_BYTES, body.length, CHECKSUM_BYTES);
        if (frameLength > MAX_FRAME_BYTES) throw invalid("frame is too large");
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(frameLength);
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(MAGIC);
            out.writeShort(VERSION);
            out.writeInt(body.length);
            out.write(body);
            out.flush();
            byte[] unsigned = bytes.toByteArray();
            out.write(sha256(unsigned));
            out.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory decision encoding failed", impossible);
        }
    }

    public static QualityGateDecision decode(byte[] encoded) {
        if (encoded == null || encoded.length < HEADER_BYTES + CHECKSUM_BYTES
                || encoded.length > MAX_FRAME_BYTES) {
            throw invalid("frame length is invalid");
        }
        int checksumOffset = encoded.length - CHECKSUM_BYTES;
        byte[] expected = sha256(Arrays.copyOf(encoded, checksumOffset));
        byte[] actual = Arrays.copyOfRange(encoded, checksumOffset, encoded.length);
        if (!MessageDigest.isEqual(expected, actual)) {
            throw invalid("checksum mismatch");
        }
        Reader frame = new Reader(encoded, 0, checksumOffset);
        if (frame.readInt("magic") != MAGIC) throw invalid("magic mismatch");
        if (frame.readUnsignedShort("version") != VERSION) {
            throw invalid("unsupported version");
        }
        int bodyLength = frame.readInt("bodyLength");
        if (bodyLength < 0 || bodyLength != frame.remaining()) {
            throw invalid("body length mismatch");
        }

        String artifactId = frame.readText("artifactId", QualityGateDecision.MAX_ID_BYTES);
        String runId = frame.readText("fence.runId", QualityGateDecision.MAX_ID_BYTES);
        String attemptId = frame.readText(
                "fence.attemptId", QualityGateDecision.MAX_ID_BYTES);
        long fenceToken = frame.readLong("fence.token");
        String publicationId = frame.readText(
                "publicationId", QualityGateDecision.MAX_ID_BYTES);
        byte[] policyDigest = frame.readFixed(
                QualityGateDecision.DIGEST_BYTES, "policyDigest");
        long accepted = frame.readLong("acceptedTotal");
        long rejected = frame.readLong("rejectedTotal");

        int memberCount = frame.readUnsignedByte("memberCount");
        if (memberCount != 3) throw invalid("member count must be three");
        List<QualityPreparedMemberEvidence> members = new ArrayList<>(memberCount);
        String[] roles = {
                TypedQualityManifest.VALID_SINK_UID,
                TypedQualityManifest.REJECTED_SINK_UID,
                TypedQualityManifest.SUMMARY_SINK_UID
        };
        for (int i = 0; i < memberCount; i++) {
            int role = frame.readUnsignedByte("member.role");
            if (role != i) throw invalid("member roles are not canonical");
            String name = frame.readText("member.name", QualityGateDecision.MAX_ID_BYTES);
            long length = frame.readLong("member.length");
            byte[] hash = frame.readFixed(QualityGateDecision.DIGEST_BYTES,
                    "member.dataHash");
            byte[] locator = frame.readFixed(QualityGateDecision.DIGEST_BYTES,
                    "member.locatorDigest");
            members.add(new QualityPreparedMemberEvidence(
                    roles[i], name, length, hash, locator));
        }

        int observationCount = frame.readUnsignedShort("observationCount");
        if (observationCount > QualityGatePolicy.MAX_REASON_THRESHOLDS) {
            throw invalid("observation count exceeds its bound");
        }
        List<QualityGateDecision.ReasonObservation> observations =
                new ArrayList<>(observationCount);
        for (int i = 0; i < observationCount; i++) {
            QualityReasonKey reason = QualityReasonKey.of(
                    frame.readText("observation.errorCode", 64),
                    frame.readOptionalText("observation.ruleId", 64),
                    frame.readInt("observation.fieldIndex"),
                    frame.readText("observation.detail", 64));
            observations.add(new QualityGateDecision.ReasonObservation(
                    reason, frame.readLong("observation.maximum"),
                    frame.readLong("observation.observed")));
        }

        QualityGateDecision.Outcome outcome = enumValue(
                QualityGateDecision.Outcome.values(),
                frame.readUnsignedByte("outcome"), "outcome");
        int failureCount = frame.readUnsignedByte("failureCount");
        if (failureCount > QualityGateDecision.FailureCode.values().length) {
            throw invalid("failure count exceeds its closed set");
        }
        List<QualityGateDecision.FailureCode> failures = new ArrayList<>(failureCount);
        for (int i = 0; i < failureCount; i++) {
            failures.add(enumValue(QualityGateDecision.FailureCode.values(),
                    frame.readUnsignedByte("failureCode"), "failureCode"));
        }

        int exemplarSectionLength = frame.readInt("exemplarSectionLength");
        if (exemplarSectionLength < 0
                || exemplarSectionLength > MAX_EXEMPLAR_SECTION_BYTES
                || exemplarSectionLength > frame.remaining()) {
            throw invalid("exemplar section length is invalid");
        }
        Reader exemplarSection = frame.section(
                exemplarSectionLength, "exemplarSection");
        List<QualityExemplar> exemplars = decodeExemplars(exemplarSection);
        exemplarSection.finish("exemplar section");
        frame.finish("decision body");

        return new QualityGateDecision(artifactId,
                new RunAttemptFence(runId, attemptId, fenceToken), publicationId,
                policyDigest, accepted, rejected, members, observations,
                outcome, failures, exemplars);
    }

    public static byte[] digest(QualityGateDecision decision) {
        return sha256(encode(decision));
    }

    static int encodedExemplarSectionBytes(QualityGateDecision decision) {
        if (decision == null) throw invalid("decision is required");
        return encodeExemplars(decision.exemplars()).length;
    }

    private static byte[] encodeBody(QualityGateDecision decision) {
        byte[] exemplars = encodeExemplars(decision.exemplars());
        if (exemplars.length > MAX_EXEMPLAR_SECTION_BYTES) {
            throw invalid("exemplar section is too large");
        }
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(4096);
            DataOutputStream out = new DataOutputStream(bytes);
            writeText(out, decision.artifactId(), QualityGateDecision.MAX_ID_BYTES);
            writeText(out, decision.fence().runId(), QualityGateDecision.MAX_ID_BYTES);
            writeText(out, decision.fence().attemptId(), QualityGateDecision.MAX_ID_BYTES);
            out.writeLong(decision.fence().fenceToken());
            writeText(out, decision.publicationId(), QualityGateDecision.MAX_ID_BYTES);
            out.write(decision.policyDigest());
            out.writeLong(decision.acceptedTotal());
            out.writeLong(decision.rejectedTotal());
            out.writeByte(decision.members().size());
            for (int i = 0; i < decision.members().size(); i++) {
                QualityPreparedMemberEvidence member = decision.members().get(i);
                out.writeByte(i);
                writeText(out, member.memberName(), QualityGateDecision.MAX_ID_BYTES);
                out.writeLong(member.dataLength());
                out.write(member.dataHash());
                out.write(member.preparedLocatorDigest());
            }
            out.writeShort(decision.reasonObservations().size());
            for (QualityGateDecision.ReasonObservation observation
                    : decision.reasonObservations()) {
                writeText(out, observation.reason().errorCode(), 64);
                writeOptionalText(out, observation.reason().ruleId(), 64);
                out.writeInt(observation.reason().fieldIndex());
                writeText(out, observation.reason().detail(), 64);
                out.writeLong(observation.maximum());
                out.writeLong(observation.observed());
            }
            out.writeByte(decision.outcome().ordinal());
            out.writeByte(decision.failureCodes().size());
            for (QualityGateDecision.FailureCode failure : decision.failureCodes()) {
                out.writeByte(failure.ordinal());
            }
            out.writeInt(exemplars.length);
            out.write(exemplars);
            out.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory decision encoding failed", impossible);
        }
    }

    private static byte[] encodeExemplars(List<QualityExemplar> exemplars) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(1024);
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeByte(exemplars.size());
            for (QualityExemplar exemplar : exemplars) {
                writeText(out, exemplar.sourceOperatorUid(), QualityGateDecision.MAX_ID_BYTES);
                out.write(hexToBytes(exemplar.splitIdentitySha256()));
                out.writeLong(exemplar.splitLocalOrdinal());
                writeText(out, exemplar.qualityErrorCode(), 64);
            }
            out.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory exemplar encoding failed", impossible);
        }
    }

    private static List<QualityExemplar> decodeExemplars(Reader section) {
        int count = section.readUnsignedByte("exemplarCount");
        if (count > QualityGatePolicy.MAX_EXEMPLARS) {
            throw invalid("exemplar count exceeds its bound");
        }
        List<QualityExemplar> exemplars = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String uid = section.readText(
                    "exemplar.sourceOperatorUid", QualityGateDecision.MAX_ID_BYTES);
            String split = bytesToHex(section.readFixed(
                    QualityGateDecision.DIGEST_BYTES, "exemplar.splitIdentity"));
            long ordinal = section.readLong("exemplar.ordinal");
            String code = section.readText("exemplar.errorCode", 64);
            exemplars.add(QualityExemplar.of(uid, split, ordinal, code));
        }
        return exemplars;
    }

    private static void writeText(DataOutputStream out, String value, int maxBytes)
            throws IOException {
        QualityGateDecision.boundedText(value, "wire text", maxBytes);
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        out.writeShort(encoded.length);
        out.write(encoded);
    }

    private static void writeOptionalText(
            DataOutputStream out, String value, int maxBytes) throws IOException {
        if (value == null) throw invalid("optional wire text is null");
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        if (encoded.length > maxBytes) throw invalid("wire text exceeds its bound");
        out.writeShort(encoded.length);
        out.write(encoded);
    }

    private static byte[] hexToBytes(String value) {
        byte[] bytes = new byte[value.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) ((Character.digit(value.charAt(i * 2), 16) << 4)
                    | Character.digit(value.charAt(i * 2 + 1), 16));
        }
        return bytes;
    }

    private static String bytesToHex(byte[] bytes) {
        char[] alphabet = "0123456789abcdef".toCharArray();
        char[] encoded = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            encoded[i * 2] = alphabet[(bytes[i] >>> 4) & 0xf];
            encoded[i * 2 + 1] = alphabet[bytes[i] & 0xf];
        }
        return new String(encoded);
    }

    private static <T> T enumValue(T[] values, int ordinal, String field) {
        if (ordinal < 0 || ordinal >= values.length) {
            throw invalid(field + " is outside its closed set");
        }
        return values[ordinal];
    }

    private static int checkedAdd(int first, int second, int third) {
        long result = (long) first + second + third;
        if (result > Integer.MAX_VALUE) throw invalid("frame length overflow");
        return (int) result;
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static IllegalArgumentException invalid(String detail) {
        return new IllegalArgumentException("quality gate decision frame is invalid: " + detail);
    }

    private static final class Reader {
        private final ByteBuffer buffer;

        private Reader(byte[] value, int offset, int length) {
            this.buffer = ByteBuffer.wrap(value, offset, length).slice();
        }

        int remaining() { return buffer.remaining(); }

        int readUnsignedByte(String field) {
            require(1, field);
            return buffer.get() & 0xff;
        }

        int readUnsignedShort(String field) {
            require(2, field);
            return buffer.getShort() & 0xffff;
        }

        int readInt(String field) {
            require(4, field);
            return buffer.getInt();
        }

        long readLong(String field) {
            require(8, field);
            return buffer.getLong();
        }

        byte[] readFixed(int length, String field) {
            if (length < 0) throw invalid(field + " length is negative");
            require(length, field);
            byte[] value = new byte[length];
            buffer.get(value);
            return value;
        }

        String readText(String field, int maxBytes) {
            int length = readUnsignedShort(field + ".length");
            if (length < 1 || length > maxBytes) {
                throw invalid(field + " length is invalid");
            }
            return strictText(readFixed(length, field), field, maxBytes);
        }

        String readOptionalText(String field, int maxBytes) {
            int length = readUnsignedShort(field + ".length");
            if (length > maxBytes) throw invalid(field + " length is invalid");
            if (length == 0) return "";
            return strictText(readFixed(length, field), field, maxBytes);
        }

        Reader section(int length, String field) {
            require(length, field);
            byte[] value = new byte[length];
            buffer.get(value);
            return new Reader(value, 0, value.length);
        }

        void finish(String field) {
            if (buffer.hasRemaining()) throw invalid(field + " has trailing bytes");
        }

        private void require(int length, String field) {
            if (length < 0 || buffer.remaining() < length) {
                throw invalid(field + " is truncated");
            }
        }

        private static String strictText(byte[] value, String field, int maxBytes) {
            try {
                String decoded = StandardCharsets.UTF_8.newDecoder()
                        .onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT)
                        .decode(ByteBuffer.wrap(value)).toString();
                return QualityGateDecision.boundedText(decoded, field, maxBytes);
            } catch (CharacterCodingException malformed) {
                throw invalid(field + " is not strict UTF-8");
            }
        }
    }
}
