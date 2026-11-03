package io.rowweave.incremental;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** One-way mode-specific run, interval and backfill identities derived from a capture receipt. */
public final class IncrementalIntervalIdentity {
    private final String candidateRunId;
    private final String intervalId;
    private final String capturedTargetReceiptDigest;
    private final String backfillId;

    private IncrementalIntervalIdentity(
            String candidateRunId, String intervalId,
            String capturedTargetReceiptDigest, String backfillId) {
        this.candidateRunId = candidateRunId;
        this.intervalId = intervalId;
        this.capturedTargetReceiptDigest = capturedTargetReceiptDigest;
        this.backfillId = backfillId;
    }

    public static IncrementalIntervalIdentity from(CapturedTargetReceipt receipt) {
        if (receipt == null) throw new IllegalArgumentException("captured target receipt is required");
        byte[] receiptDigest = CapturedTargetReceiptCodec.digest(receipt);
        byte[] candidate;
        String backfill = null;
        if (receipt.mode() == IncrementalRunMode.BACKFILL_V1) {
            byte[] backfillBytes = hash(preimage("rowweave-backfill-v1", receipt, true));
            backfill = hex(backfillBytes);
            candidate = hash(join("rowweave-backfill-run-v1", backfillBytes));
            byte[] interval = hash(join("rowweave-backfill-interval-v1", backfillBytes));
            return new IncrementalIntervalIdentity(
                    hex(candidate), hex(interval), hex(receiptDigest), backfill);
        } else {
            candidate = hash(preimage("rowweave-normal-run-v1", receipt, false));
        }
        byte[] interval = hash(join("rowweave-interval-v1", candidate, receiptDigest));
        return new IncrementalIntervalIdentity(
                hex(candidate), hex(interval), hex(receiptDigest), backfill);
    }

    public String candidateRunId() {
        return candidateRunId;
    }

    public String intervalId() {
        return intervalId;
    }

    public String capturedTargetReceiptDigest() {
        return capturedTargetReceiptDigest;
    }

    public String backfillId() {
        if (backfillId == null) throw new IllegalStateException("normal interval has no backfill id");
        return backfillId;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof IncrementalIntervalIdentity)) return false;
        IncrementalIntervalIdentity that = (IncrementalIntervalIdentity) other;
        return candidateRunId.equals(that.candidateRunId)
                && intervalId.equals(that.intervalId)
                && capturedTargetReceiptDigest.equals(that.capturedTargetReceiptDigest)
                && equal(backfillId, that.backfillId);
    }

    @Override
    public int hashCode() {
        int result = candidateRunId.hashCode();
        result = 31 * result + intervalId.hashCode();
        result = 31 * result + capturedTargetReceiptDigest.hashCode();
        result = 31 * result + (backfillId == null ? 0 : backfillId.hashCode());
        return result;
    }

    private static byte[] preimage(
            String domain, CapturedTargetReceipt receipt, boolean includeBounds) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            writeBytes(out, domain.getBytes(StandardCharsets.US_ASCII));
            out.writeLong(receipt.feedIncarnation());
            out.write(receipt.definitionDigest());
            out.writeByte(receipt.mode().wireId());
            out.writeLong(receipt.expectedSequence());
            writeBytes(out, receipt.requestIdentityBytes());
            if (includeBounds) {
                writeBytes(out, IncrementalFrontierCodec.encode(receipt.from()));
                writeBytes(out, IncrementalFrontierCodec.encode(receipt.target()));
            }
            out.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory interval identity failed", impossible);
        }
    }

    private static byte[] join(String domain, byte[]... values) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            writeBytes(out, domain.getBytes(StandardCharsets.US_ASCII));
            for (byte[] value : values) writeBytes(out, value);
            out.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory interval identity failed", impossible);
        }
    }

    private static void writeBytes(DataOutputStream out, byte[] value) throws IOException {
        out.writeInt(value.length);
        out.write(value);
    }

    private static byte[] hash(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static String hex(byte[] value) {
        char[] chars = new char[value.length * 2];
        char[] digits = "0123456789abcdef".toCharArray();
        for (int i = 0; i < value.length; i++) {
            int unsigned = value[i] & 0xff;
            chars[i * 2] = digits[unsigned >>> 4];
            chars[i * 2 + 1] = digits[unsigned & 0xf];
        }
        return new String(chars);
    }

    private static boolean equal(Object left, Object right) {
        return left == null ? right == null : left.equals(right);
    }
}
