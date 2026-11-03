package io.rowweave.plan;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongConsumer;

/** Canonical, checksum-protected RPC-only codec for one job telemetry summary. */
public final class ExecutionTelemetrySummaryCodec {
    public static final int MAX_FRAME_BYTES = 48 * 1024;
    /** Conservative upper bound for decoded list/distribution/array ownership. */
    public static final int MAX_MATERIALIZED_RESIDENT_BYTES = 256 * 1024;
    private static final LongConsumer NO_RESERVATION = value -> { };
    private static final int MAGIC = 0x52574A31; // RWJ1
    private static final int VERSION = 1;
    private static final int HEADER_BYTES = 12;
    private static final int CHECKSUM_BYTES = 32;
    private static final int FIXED_BODY_BYTES = 2 + 6 * Long.BYTES + Integer.BYTES;
    private static final int DISTRIBUTION_HEADER_BYTES = 14;

    private ExecutionTelemetrySummaryCodec() { }

    public static byte[] encode(ExecutionTelemetrySummary summary) {
        if (summary == null) throw new IllegalArgumentException("summary is required");
        long size = HEADER_BYTES + FIXED_BODY_BYTES + CHECKSUM_BYTES;
        for (PartitionDistribution distribution : summary.partitionDistributions()) {
            size = add(size, DISTRIBUTION_HEADER_BYTES);
            size = add(size, bitmapBytes(distribution.parallelism()));
            size = add(size, multiply(distribution.parallelism(), Long.BYTES));
        }
        if (size > MAX_FRAME_BYTES || size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("execution telemetry summary exceeds 48 KiB");
        }
        ByteBuffer buffer = ByteBuffer.allocate((int) size);
        buffer.putInt(MAGIC).putInt(VERSION).putInt((int) size);
        buffer.put(coverageId(summary.resourceCoverage(), false));
        buffer.put(coverageId(summary.partitionCoverage(), true));
        buffer.putLong(summary.managedPeakReservedBytes());
        buffer.putLong(summary.managedAcquiredBytes());
        buffer.putLong(summary.pressureSpillCount());
        buffer.putLong(summary.spillRunCount());
        buffer.putLong(summary.spillLogicalBytesWritten());
        buffer.putLong(summary.spillPeakReservedBytes());
        buffer.putInt(summary.partitionDistributions().size());
        for (PartitionDistribution distribution : summary.partitionDistributions()) {
            int bitmapLength = bitmapBytes(distribution.parallelism());
            buffer.putInt(distribution.logicalStage());
            buffer.put(roleId(distribution.role()));
            buffer.put(coverageId(distribution.coverage(), true));
            buffer.putInt(distribution.parallelism());
            buffer.putInt(bitmapLength);
            putBitmap(buffer, distribution.borrowedPresenceBitmap(), bitmapLength);
            for (long rows : distribution.borrowedRowCounts()) buffer.putLong(rows);
        }
        int bodyLength = buffer.position();
        buffer.put(digest(buffer.array(), bodyLength));
        return buffer.array();
    }

    public static ExecutionTelemetrySummary decode(byte[] encoded) {
        return decode(encoded, NO_RESERVATION);
    }

    public static ExecutionTelemetrySummary decode(byte[] encoded, LongConsumer reservation) {
        if (encoded == null || encoded.length < HEADER_BYTES + FIXED_BODY_BYTES + CHECKSUM_BYTES
                || encoded.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("invalid execution telemetry summary length");
        }
        if (reservation == null) throw new IllegalArgumentException("reservation is required");
        int bodyLength = encoded.length - CHECKSUM_BYTES;
        byte[] expected = digest(encoded, bodyLength);
        if (!checksumEquals(expected, encoded, bodyLength)) {
            throw new IllegalArgumentException("execution telemetry summary checksum mismatch");
        }
        ByteBuffer buffer = ByteBuffer.wrap(encoded, 0, bodyLength);
        if (buffer.getInt() != MAGIC || buffer.getInt() != VERSION
                || buffer.getInt() != encoded.length) {
            throw new IllegalArgumentException("unsupported execution telemetry summary");
        }
        TelemetryCoverage resourceCoverage = coverage(buffer.get(), false);
        TelemetryCoverage partitionCoverage = coverage(buffer.get(), true);
        long peak = buffer.getLong();
        long acquired = buffer.getLong();
        long pressure = buffer.getLong();
        long runs = buffer.getLong();
        long written = buffer.getLong();
        long spillPeak = buffer.getLong();
        int distributionCount = buffer.getInt();
        if (distributionCount < 0
                || distributionCount > ExecutionTelemetrySummary.MAX_DISTRIBUTIONS) {
            throw new IllegalArgumentException("invalid partition distribution count");
        }
        long slots = scanDistributions(buffer.duplicate(), distributionCount);
        reservation.accept(decodedResidentBytes(distributionCount, slots));
        List<PartitionDistribution> distributions = new ArrayList<>(distributionCount);
        for (int i = 0; i < distributionCount; i++) {
            requireRemaining(buffer, DISTRIBUTION_HEADER_BYTES);
            int logicalStage = buffer.getInt();
            PartitionRole role = role(buffer.get());
            TelemetryCoverage distributionCoverage = coverage(buffer.get(), true);
            if (distributionCoverage == TelemetryCoverage.NOT_APPLICABLE) {
                throw new IllegalArgumentException("distribution cannot be not-applicable");
            }
            int parallelism = buffer.getInt();
            int bitmapLength = buffer.getInt();
            if (logicalStage < 0 || parallelism <= 1
                    || bitmapLength != bitmapBytes(parallelism)) {
                throw new IllegalArgumentException("invalid partition distribution header");
            }
            long variableBytes = add(bitmapLength, multiply(parallelism, Long.BYTES));
            if (variableBytes > buffer.remaining()) {
                throw new IllegalArgumentException("truncated partition distribution");
            }
            boolean[] present = readBitmap(buffer, parallelism, bitmapLength);
            long[] rowCounts = new long[parallelism];
            for (int p = 0; p < parallelism; p++) rowCounts[p] = buffer.getLong();
            distributions.add(PartitionDistribution.decoded(logicalStage, role, parallelism,
                    distributionCoverage, present, rowCounts));
        }
        if (buffer.hasRemaining()) {
            throw new IllegalArgumentException("execution telemetry summary has trailing bytes");
        }
        return ExecutionTelemetrySummary.decoded(resourceCoverage, peak, acquired, pressure,
                runs, written, spillPeak, partitionCoverage, distributions);
    }

    private static long scanDistributions(ByteBuffer buffer, int distributionCount) {
        long slots = 0L;
        for (int i = 0; i < distributionCount; i++) {
            requireRemaining(buffer, DISTRIBUTION_HEADER_BYTES);
            int logicalStage = buffer.getInt();
            role(buffer.get());
            TelemetryCoverage distributionCoverage = coverage(buffer.get(), true);
            int parallelism = buffer.getInt();
            int bitmapLength = buffer.getInt();
            if (logicalStage < 0 || distributionCoverage == TelemetryCoverage.NOT_APPLICABLE
                    || parallelism <= 1 || bitmapLength != bitmapBytes(parallelism)) {
                throw new IllegalArgumentException("invalid partition distribution header");
            }
            slots = add(slots, parallelism);
            if (slots > ExecutionTelemetrySummary.MAX_PARTITION_SLOTS) {
                throw new IllegalArgumentException("too many partition slots");
            }
            long variableBytes = add(bitmapLength, multiply(parallelism, Long.BYTES));
            if (variableBytes > buffer.remaining()) {
                throw new IllegalArgumentException("truncated partition distribution");
            }
            buffer.position(buffer.position() + (int) variableBytes);
        }
        if (buffer.hasRemaining()) {
            throw new IllegalArgumentException("execution telemetry summary has trailing bytes");
        }
        return slots;
    }

    private static long decodedResidentBytes(int distributions, long slots) {
        long bytes = add(256L, multiply(distributions, 160L));
        bytes = add(bytes, multiply(slots, 16L));
        if (bytes > MAX_MATERIALIZED_RESIDENT_BYTES) {
            throw new IllegalArgumentException("execution telemetry resident estimate exceeds limit");
        }
        return bytes;
    }

    private static int bitmapBytes(int parallelism) {
        if (parallelism < 0 || parallelism > ExecutionTelemetrySummary.MAX_PARTITION_SLOTS) {
            throw new IllegalArgumentException("invalid partition parallelism");
        }
        return (parallelism + 7) / 8;
    }

    private static void putBitmap(ByteBuffer buffer, boolean[] present, int length) {
        for (int byteIndex = 0; byteIndex < length; byteIndex++) {
            int value = 0;
            int base = byteIndex << 3;
            int end = Math.min(base + 8, present.length);
            for (int i = base; i < end; i++) {
                if (present[i]) value |= 1 << (i - base);
            }
            buffer.put((byte) value);
        }
    }

    private static boolean[] readBitmap(ByteBuffer buffer, int parallelism, int length) {
        boolean[] present = new boolean[parallelism];
        for (int byteIndex = 0; byteIndex < length; byteIndex++) {
            int value = buffer.get() & 0xff;
            int base = byteIndex << 3;
            int end = Math.min(base + 8, parallelism);
            for (int i = base; i < end; i++) {
                present[i] = (value & (1 << (i - base))) != 0;
            }
            if (end - base < 8 && (value >>> (end - base)) != 0) {
                throw new IllegalArgumentException("partition bitmap has non-zero padding");
            }
        }
        return present;
    }

    private static boolean checksumEquals(byte[] expected, byte[] encoded, int offset) {
        int difference = 0;
        for (int i = 0; i < expected.length; i++) {
            difference |= expected[i] ^ encoded[offset + i];
        }
        return difference == 0;
    }

    private static byte coverageId(TelemetryCoverage coverage, boolean partition) {
        switch (coverage) {
            case COMPLETE: return 1;
            case PARTIAL: return 2;
            case UNAVAILABLE: return 3;
            case NOT_APPLICABLE:
                if (partition) return 4;
                break;
            default:
                break;
        }
        throw new IllegalArgumentException("invalid summary coverage");
    }

    private static TelemetryCoverage coverage(byte id, boolean partition) {
        switch (id) {
            case 1: return TelemetryCoverage.COMPLETE;
            case 2: return TelemetryCoverage.PARTIAL;
            case 3: return TelemetryCoverage.UNAVAILABLE;
            case 4:
                if (partition) return TelemetryCoverage.NOT_APPLICABLE;
                break;
            default:
                break;
        }
        throw new IllegalArgumentException("unknown summary coverage");
    }

    private static byte roleId(PartitionRole role) {
        if (role == PartitionRole.KEYED_OP) return 1;
        if (role == PartitionRole.JOIN_CONSUMER) return 2;
        throw new IllegalArgumentException("unknown partition role");
    }

    private static PartitionRole role(byte id) {
        if (id == 1) return PartitionRole.KEYED_OP;
        if (id == 2) return PartitionRole.JOIN_CONSUMER;
        throw new IllegalArgumentException("unknown partition role");
    }

    private static void requireRemaining(ByteBuffer buffer, int bytes) {
        if (buffer.remaining() < bytes) {
            throw new IllegalArgumentException("truncated execution telemetry summary");
        }
    }

    private static long multiply(long left, long right) {
        if (left < 0L || right < 0L || (left != 0L && right > Long.MAX_VALUE / left)) {
            throw new IllegalArgumentException("execution telemetry summary size overflow");
        }
        return left * right;
    }

    private static long add(long left, long right) {
        if (left < 0L || right < 0L || right > Long.MAX_VALUE - left) {
            throw new IllegalArgumentException("execution telemetry summary size overflow");
        }
        return left + right;
    }

    private static byte[] digest(byte[] bytes, int length) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(bytes, 0, length);
            return digest.digest();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }
}
