package io.rowweave;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/** Canonical bounded wire representation for {@link QualityGatePolicy}. */
public final class QualityGatePolicyCodec {

    public static final int MAX_FRAME_BYTES = 128 * 1024;
    private static final int MAGIC = 0x52575150; // RWQP
    private static final int CHECKSUM_BYTES = 32;
    private static final int MAX_TEXT_BYTES = 64;

    private QualityGatePolicyCodec() {
    }

    public static byte[] encode(QualityGatePolicy policy) {
        if (policy == null) throw invalid("policy is required");
        try {
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
            DataOutputStream body = new DataOutputStream(bodyBytes);
            int flags = (policy.maxRejectedRows() == null ? 0 : 1)
                    | (policy.maxRejectedPartsPerMillion() == null ? 0 : 2);
            body.writeByte(flags);
            if (policy.maxRejectedRows() != null) body.writeLong(policy.maxRejectedRows());
            if (policy.maxRejectedPartsPerMillion() != null) {
                body.writeInt(policy.maxRejectedPartsPerMillion());
            }
            body.writeShort(policy.reasonThresholds().size());
            for (QualityGatePolicy.ReasonThreshold threshold : policy.reasonThresholds()) {
                writeText(body, threshold.reason().errorCode());
                writeText(body, threshold.reason().ruleId());
                body.writeInt(threshold.reason().fieldIndex());
                writeText(body, threshold.reason().detail());
                body.writeLong(threshold.maximum());
            }
            body.writeByte(policy.exemplarLimit());
            body.flush();

            byte[] payload = bodyBytes.toByteArray();
            ByteArrayOutputStream frameBytes = new ByteArrayOutputStream();
            DataOutputStream frame = new DataOutputStream(frameBytes);
            frame.writeInt(MAGIC);
            frame.writeShort(QualityGatePolicy.CURRENT_VERSION);
            frame.writeInt(payload.length);
            frame.write(payload);
            frame.flush();
            byte[] withoutChecksum = frameBytes.toByteArray();
            frame.write(sha256(withoutChecksum));
            frame.flush();
            byte[] encoded = frameBytes.toByteArray();
            if (encoded.length > MAX_FRAME_BYTES) throw invalid("policy frame is too large");
            return encoded;
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory policy encoding failed", impossible);
        }
    }

    public static QualityGatePolicy decode(byte[] encoded) {
        if (encoded == null || encoded.length < 10 + CHECKSUM_BYTES
                || encoded.length > MAX_FRAME_BYTES) {
            throw invalid("policy frame length is invalid");
        }
        byte[] expected = sha256(Arrays.copyOf(encoded, encoded.length - CHECKSUM_BYTES));
        byte[] actual = Arrays.copyOfRange(encoded, encoded.length - CHECKSUM_BYTES,
                encoded.length);
        if (!MessageDigest.isEqual(expected, actual)) throw invalid("policy checksum mismatch");

        Reader reader = new Reader(encoded, encoded.length - CHECKSUM_BYTES);
        if (reader.readInt() != MAGIC) throw invalid("policy magic mismatch");
        int version = reader.readUnsignedShort();
        if (version != QualityGatePolicy.CURRENT_VERSION) {
            throw invalid("unsupported policy version");
        }
        int bodyLength = reader.readInt();
        if (bodyLength != reader.remaining()) throw invalid("policy body length mismatch");
        int flags = reader.readUnsignedByte();
        if ((flags & ~3) != 0) throw invalid("policy flags are invalid");
        QualityGatePolicy.Builder builder = QualityGatePolicy.builder();
        if ((flags & 1) != 0) builder.maxRejectedRows(reader.readLong());
        if ((flags & 2) != 0) builder.maxRejectedPartsPerMillion(reader.readInt());
        int count = reader.readUnsignedShort();
        if (count > QualityGatePolicy.MAX_REASON_THRESHOLDS) {
            throw invalid("too many quality reason thresholds");
        }
        QualityReasonKey previous = null;
        for (int i = 0; i < count; i++) {
            QualityReasonKey reason = QualityReasonKey.of(
                    reader.readText(), reader.readText(), reader.readInt(), reader.readText());
            if (previous != null && previous.compareTo(reason) >= 0) {
                throw invalid("quality reason thresholds are not canonical");
            }
            builder.reasonThreshold(reason, reader.readLong());
            previous = reason;
        }
        builder.exemplarLimit(reader.readUnsignedByte());
        if (reader.remaining() != 0) throw invalid("policy frame has trailing bytes");
        return builder.build();
    }

    public static byte[] digest(QualityGatePolicy policy) {
        return sha256(encode(policy));
    }

    static byte[] digestEncoded(byte[] encoded) {
        decode(encoded);
        return sha256(encoded);
    }

    private static void writeText(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_TEXT_BYTES) throw invalid("policy text is too large");
        out.writeByte(bytes.length);
        out.write(bytes);
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static IllegalArgumentException invalid(String detail) {
        return new IllegalArgumentException("quality gate policy is invalid: " + detail);
    }

    private static final class Reader {
        private final ByteBuffer buffer;

        private Reader(byte[] bytes, int limit) {
            this.buffer = ByteBuffer.wrap(bytes, 0, limit);
        }

        private int remaining() {
            return buffer.remaining();
        }

        private int readUnsignedByte() {
            require(1);
            return buffer.get() & 0xff;
        }

        private int readUnsignedShort() {
            require(2);
            return buffer.getShort() & 0xffff;
        }

        private int readInt() {
            require(4);
            return buffer.getInt();
        }

        private long readLong() {
            require(8);
            return buffer.getLong();
        }

        private String readText() {
            int length = readUnsignedByte();
            if (length > MAX_TEXT_BYTES) throw invalid("policy text is too large");
            require(length);
            byte[] value = new byte[length];
            buffer.get(value);
            return new String(value, StandardCharsets.UTF_8);
        }

        private void require(int bytes) {
            if (bytes < 0 || buffer.remaining() < bytes) throw invalid("policy frame is truncated");
        }
    }
}
