package io.rowweave.plan;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.function.LongConsumer;

/** Canonical fixed-width wire representation for one stage telemetry value. */
public final class StageExecutionTelemetryCodec {
    public static final int MAX_FRAME_BYTES = 256;
    private static final int MAGIC = 0x52575431; // RWT1
    private static final byte VERSION = 1;
    private static final int BODY_BYTES = 4 + 1 + 1 + 6 * 8;
    private static final int FRAME_BYTES = BODY_BYTES + 32;
    public static final long MAX_MATERIALIZED_RESIDENT_BYTES = 128L;
    private static final LongConsumer NO_RESERVATION = value -> { };

    private StageExecutionTelemetryCodec() { }

    public static byte[] encode(StageExecutionTelemetry telemetry) {
        if (telemetry == null) throw new IllegalArgumentException("telemetry is required");
        ByteBuffer buffer = ByteBuffer.allocate(FRAME_BYTES);
        buffer.putInt(MAGIC).put(VERSION).put(coverageId(telemetry.coverage()));
        buffer.putLong(telemetry.managedPeakReservedBytes());
        buffer.putLong(telemetry.managedAcquiredBytes());
        buffer.putLong(telemetry.pressureSpillCount());
        buffer.putLong(telemetry.spillRunCount());
        buffer.putLong(telemetry.spillLogicalBytesWritten());
        buffer.putLong(telemetry.spillPeakReservedBytes());
        buffer.put(digest(buffer.array(), BODY_BYTES));
        return buffer.array();
    }

    public static StageExecutionTelemetry decode(byte[] encoded) {
        return decode(encoded, NO_RESERVATION);
    }

    public static StageExecutionTelemetry decode(byte[] encoded, LongConsumer reservation) {
        if (encoded == null || encoded.length != FRAME_BYTES
                || encoded.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("invalid stage telemetry frame length");
        }
        if (reservation == null) throw new IllegalArgumentException("reservation is required");
        reservation.accept(MAX_MATERIALIZED_RESIDENT_BYTES);
        byte[] expected = digest(encoded, BODY_BYTES);
        if (!checksumEquals(expected, encoded, BODY_BYTES)) {
            throw new IllegalArgumentException("stage telemetry checksum mismatch");
        }
        ByteBuffer buffer = ByteBuffer.wrap(encoded, 0, BODY_BYTES);
        if (buffer.getInt() != MAGIC || buffer.get() != VERSION) {
            throw new IllegalArgumentException("unsupported stage telemetry frame");
        }
        TelemetryCoverage coverage = coverage(buffer.get());
        long peak = buffer.getLong();
        long acquired = buffer.getLong();
        long pressure = buffer.getLong();
        long runs = buffer.getLong();
        long written = buffer.getLong();
        long spillPeak = buffer.getLong();
        if (coverage == TelemetryCoverage.COMPLETE) {
            return StageExecutionTelemetry.complete(
                    peak, acquired, pressure, runs, written, spillPeak);
        }
        if (peak != 0 || acquired != 0 || pressure != 0 || runs != 0
                || written != 0 || spillPeak != 0) {
            throw new IllegalArgumentException("non-complete telemetry must contain zero values");
        }
        return coverage == TelemetryCoverage.NOT_EXECUTED
                ? StageExecutionTelemetry.notExecuted()
                : StageExecutionTelemetry.unavailable();
    }

    private static boolean checksumEquals(byte[] expected, byte[] encoded, int offset) {
        int difference = 0;
        for (int i = 0; i < expected.length; i++) {
            difference |= expected[i] ^ encoded[offset + i];
        }
        return difference == 0;
    }

    private static byte coverageId(TelemetryCoverage coverage) {
        switch (coverage) {
            case COMPLETE: return 1;
            case NOT_EXECUTED: return 2;
            case UNAVAILABLE: return 3;
            default: throw new IllegalArgumentException("invalid stage telemetry coverage");
        }
    }

    private static TelemetryCoverage coverage(byte id) {
        switch (id) {
            case 1: return TelemetryCoverage.COMPLETE;
            case 2: return TelemetryCoverage.NOT_EXECUTED;
            case 3: return TelemetryCoverage.UNAVAILABLE;
            default: throw new IllegalArgumentException("unknown stage telemetry coverage");
        }
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
