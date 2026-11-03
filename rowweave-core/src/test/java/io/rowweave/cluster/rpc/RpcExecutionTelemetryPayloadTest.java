package io.rowweave.cluster.rpc;

import io.rowweave.plan.ExecutionTelemetrySummaryCodec;
import io.rowweave.plan.TelemetryCoverage;
import io.rowweave.plan.StageExecutionTelemetry;
import io.rowweave.plan.StageExecutionTelemetryCodec;
import io.rowweave.run.RunAttemptFence;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** typed telemetry payload union and immutable public boundary. */
class RpcExecutionTelemetryPayloadTest {

    private static final EnumSet<RpcMessage.Type> PAYLOAD_TYPES = EnumSet.of(
            RpcMessage.Type.ASSIGN_STAGE,
            RpcMessage.Type.ASSIGNMENT_ACCEPTED,
            RpcMessage.Type.RECONCILE_DURABLE_STAGE,
            RpcMessage.Type.MANIFEST_MEMBER_INSTALLED,
            RpcMessage.Type.PUBLISH_MANIFEST_GROUP,
            RpcMessage.Type.MANIFEST_OUTCOME,
            RpcMessage.Type.BATCH_SINK_OUTCOME,
            RpcMessage.Type.STAGE_REPORT,
            RpcMessage.Type.DURABLE_STAGE_RECONCILED,
            RpcMessage.Type.JOB_RESULT,
            RpcMessage.Type.RUN_SNAPSHOT,
            RpcMessage.Type.EXECUTE_PROVIDER_GROUP,
            RpcMessage.Type.PROVIDER_GROUP_RESULT);

    @Test
    void commitPayloadIsAnExplicitClosedUnion() {
        for (RpcMessage.Type type : RpcMessage.Type.values()) {
            RpcMessage raw = rawPayload(type, new byte[]{1});
            if (PAYLOAD_TYPES.contains(type)) {
                assertDoesNotThrow(() -> RpcCodec.encode(raw), type.name());
            } else {
                assertThrows(IllegalArgumentException.class,
                        () -> RpcCodec.encode(raw), type.name());
            }
        }
    }

    @Test
    void typedTelemetryFactoriesCopyInAndPublicAccessorsCopyOut() throws Exception {
        byte[] bytes = StageExecutionTelemetryCodec.encode(
                StageExecutionTelemetry.complete(1, 2, 3, 4, 5, 6));
        RpcMessage report = RpcMessage.stageReport(
                "worker", 7L, true, 9L, 8L, null, "incarnation",
                new RunAttemptFence("run", "attempt", 1L), bytes);
        byte[] borrowed = report.borrowedCommitPayload();

        bytes[0] ^= 1;
        assertArrayEquals(StageExecutionTelemetryCodec.encode(
                StageExecutionTelemetry.complete(1, 2, 3, 4, 5, 6)),
                report.executionTelemetryBytes());
        byte[] publicTyped = report.executionTelemetryBytes();
        byte[] publicCompat = report.commitPayload();
        assertNotSame(borrowed, publicTyped);
        assertNotSame(borrowed, publicCompat);
        publicTyped[1] ^= 1;
        publicCompat[2] ^= 1;
        assertSame(borrowed, report.borrowedCommitPayload());

        RpcMessage secured = report.withSecurityMetadata(RpcSecurityMetadata.of(
                repeat('a', 64), repeat('b', 64)));
        assertSame(borrowed, secured.borrowedCommitPayload(),
                "security wrapper must share the already-private immutable backing");
        RpcMessage decoded = RpcCodec.decode(new DataInputStream(
                new ByteArrayInputStream(RpcCodec.encode(secured))));
        assertArrayEquals(report.executionTelemetryBytes(),
                decoded.executionTelemetryBytes());
    }

    @Test
    void telemetryPayloadHasTypeSpecificBoundsAndNullCompatibility() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> RpcMessage.stageReport(
                "worker", 7L, true, 0L, 0L, null, null, null,
                new byte[StageExecutionTelemetryCodec.MAX_FRAME_BYTES + 1]));
        assertThrows(IllegalArgumentException.class, () -> RpcMessage.jobResult(
                true, 1L, null,
                new byte[ExecutionTelemetrySummaryCodec.MAX_FRAME_BYTES + 1]));

        RpcMessage stage = roundTrip(RpcMessage.stageReport(
                "worker", 7L, true, 0L, 0L, null));
        RpcMessage job = roundTrip(RpcMessage.jobResult(true, 1L, null));
        RpcMessage snapshot = roundTrip(RpcMessage.runSnapshot(new byte[]{3}));
        assertNull(stage.executionTelemetryBytes());
        assertNull(job.executionTelemetryBytes());
        assertNull(snapshot.executionTelemetryBytes());
        assertThrows(IllegalStateException.class,
                () -> RpcMessage.ack().executionTelemetryBytes());
    }

    @Test
    void corruptJobResultAndRunSnapshotTelemetryDegradeWithoutChangingBaseFacts() {
        RpcMessage job = RpcMessage.jobResult(true, 41L, null, new byte[]{1, 2, 3});
        RpcMessage snapshot = RpcMessage.runSnapshot(new byte[]{9}, new byte[]{4, 5, 6});

        assertEquals(41L, job.rowCount());
        assertEquals(TelemetryCoverage.UNAVAILABLE,
                job.executionTelemetrySummary().resourceCoverage());
        assertArrayEquals(new byte[]{9}, snapshot.planGraphBytes());
        assertEquals(TelemetryCoverage.UNAVAILABLE,
                snapshot.executionTelemetrySummary().partitionCoverage());
    }

    @Test
    void oldPayloadKindsKeepTheirExistingNonCloningCompatibilityBoundary() {
        byte[] legacy = {1, 2, 3};
        RpcMessage old = rawPayload(RpcMessage.Type.MANIFEST_OUTCOME, legacy);

        assertSame(legacy, old.commitPayload());
        assertSame(legacy, old.borrowedCommitPayload());
    }

    @Test
    void stageAssignmentTokenAccessorCannotBorrowTelemetryPayloadBacking() {
        byte[] token = {1, 2, 3};
        assertSame(token, rawPayload(RpcMessage.Type.ASSIGN_STAGE, token)
                .stageAssignmentTokenBytes());
        assertSame(token, rawPayload(RpcMessage.Type.ASSIGNMENT_ACCEPTED, token)
                .stageAssignmentTokenBytes());
        assertSame(token, rawPayload(RpcMessage.Type.RECONCILE_DURABLE_STAGE, token)
                .stageAssignmentTokenBytes());

        for (RpcMessage.Type type : Arrays.asList(
                RpcMessage.Type.STAGE_REPORT,
                RpcMessage.Type.DURABLE_STAGE_RECONCILED,
                RpcMessage.Type.JOB_RESULT,
                RpcMessage.Type.RUN_SNAPSHOT)) {
            RpcMessage telemetry = rawPayload(type, new byte[]{4, 5, 6});
            assertThrows(IllegalStateException.class,
                    telemetry::stageAssignmentTokenBytes, type.name());
        }
    }

    @Test
    void sevenPreviouslyLegalCommitPayloadFramesRemainByteForByteGolden() {
        byte[] payload = {0x11, 0x22, 0x33};
        RpcMessage[] messages = {
                rawPayload(RpcMessage.Type.ASSIGN_STAGE, payload),
                rawPayload(RpcMessage.Type.ASSIGNMENT_ACCEPTED, payload),
                rawPayload(RpcMessage.Type.RECONCILE_DURABLE_STAGE, payload),
                rawPayload(RpcMessage.Type.MANIFEST_MEMBER_INSTALLED, payload),
                rawPayload(RpcMessage.Type.PUBLISH_MANIFEST_GROUP, payload),
                rawPayload(RpcMessage.Type.MANIFEST_OUTCOME, payload),
                rawPayload(RpcMessage.Type.BATCH_SINK_OUTCOME, payload)
        };
        String[] golden = {
                "0000008000000003ffffffffffffffff0000000000000000ffffffff0000000000000000ffffffff000000000000000000ffffffff0000000000000000ffffffffffffffffffffffffffffffffffffffffffffffff000000000000000000000003112233ffffffffffffffffffffffffffffffffffffffff0000000000000000ffffffff",
                "0000008000000017ffffffffffffffff0000000000000000ffffffff0000000000000000ffffffff000000000000000000ffffffff0000000000000000ffffffffffffffffffffffffffffffffffffffffffffffff000000000000000000000003112233ffffffffffffffffffffffffffffffffffffffff0000000000000000ffffffff",
                "0000008000000019ffffffffffffffff0000000000000000ffffffff0000000000000000ffffffff000000000000000000ffffffff0000000000000000ffffffffffffffffffffffffffffffffffffffffffffffff000000000000000000000003112233ffffffffffffffffffffffffffffffffffffffff0000000000000000ffffffff",
                "000000800000000fffffffffffffffff0000000000000000ffffffff0000000000000000ffffffff000000000000000000ffffffff0000000000000000ffffffffffffffffffffffffffffffffffffffffffffffff000000000000000000000003112233ffffffffffffffffffffffffffffffffffffffff0000000000000000ffffffff",
                "0000008000000010ffffffffffffffff0000000000000000ffffffff0000000000000000ffffffff000000000000000000ffffffff0000000000000000ffffffffffffffffffffffffffffffffffffffffffffffff000000000000000000000003112233ffffffffffffffffffffffffffffffffffffffff0000000000000000ffffffff",
                "0000008000000012ffffffffffffffff0000000000000000ffffffff0000000000000000ffffffff000000000000000000ffffffff0000000000000000ffffffffffffffffffffffffffffffffffffffffffffffff000000000000000000000003112233ffffffffffffffffffffffffffffffffffffffff0000000000000000ffffffff",
                "000000800000001cffffffffffffffff0000000000000000ffffffff0000000000000000ffffffff000000000000000000ffffffff0000000000000000ffffffffffffffffffffffffffffffffffffffffffffffff000000000000000000000003112233ffffffffffffffffffffffffffffffffffffffff0000000000000000ffffffff"
        };

        assertAll(
                () -> assertEquals(golden[0], hex(RpcCodec.encode(messages[0]))),
                () -> assertEquals(golden[1], hex(RpcCodec.encode(messages[1]))),
                () -> assertEquals(golden[2], hex(RpcCodec.encode(messages[2]))),
                () -> assertEquals(golden[3], hex(RpcCodec.encode(messages[3]))),
                () -> assertEquals(golden[4], hex(RpcCodec.encode(messages[4]))),
                () -> assertEquals(golden[5], hex(RpcCodec.encode(messages[5]))),
                () -> assertEquals(golden[6], hex(RpcCodec.encode(messages[6]))));
    }

    @Test
    void decoderRejectsAnIllegalPayloadUnionEvenWhenTheWireWasOtherwiseValid() {
        byte[] frame = RpcCodec.encode(rawPayload(
                RpcMessage.Type.MANIFEST_OUTCOME, new byte[]{1, 2, 3}));
        java.nio.ByteBuffer.wrap(frame, Integer.BYTES, Integer.BYTES)
                .putInt(RpcMessage.Type.ACK.ordinal());

        assertThrows(IllegalArgumentException.class, () -> RpcCodec.decode(
                new DataInputStream(new ByteArrayInputStream(frame))));
    }

    @Test
    void telemetryFieldLimitsRejectOversizedPayloadBeforeResidentAllocation() throws Exception {
        assertOversizedTelemetryRejectedBeforeReservation(
                RpcMessage.Type.STAGE_REPORT,
                StageExecutionTelemetryCodec.MAX_FRAME_BYTES + 1);
        assertOversizedTelemetryRejectedBeforeReservation(
                RpcMessage.Type.RUN_SNAPSHOT,
                ExecutionTelemetrySummaryCodec.MAX_FRAME_BYTES + 1);
    }

    @Test
    void normalOneMibAndLargeEightyMibFramesStreamWithoutPayloadClones() throws Exception {
        int normalLimit = 1024 * 1024;
        int largeLimit = 80 * 1024 * 1024;
        RpcAdmissionConfig admission = RpcAdmissionConfig.forTesting(
                2, 2, 5_000L, normalLimit, largeLimit, largeLimit);

        RpcMessage normalEmpty = rawPayload(RpcMessage.Type.MANIFEST_OUTCOME, null);
        int normalOverhead = RpcCodec.encode(normalEmpty).length - Integer.BYTES;
        byte[] normalPayload = new byte[normalLimit - normalOverhead];
        RpcMessage normal = rawPayload(RpcMessage.Type.MANIFEST_OUTCOME, normalPayload);
        CountingOutputStream normalOut = new CountingOutputStream();
        encodeUnderExactLease(normal, normalOut, admission);
        assertEquals(normalLimit + Integer.BYTES, normalOut.bytes);
        assertSame(normalPayload, normal.borrowedCommitPayload());

        RpcMessage largeEmpty = RpcMessage.submitRun(new byte[0]);
        int largeOverhead = RpcCodec.encode(largeEmpty).length - Integer.BYTES;
        byte[] largePayload = new byte[largeLimit - largeOverhead];
        RpcMessage large = RpcMessage.submitRun(largePayload);
        RpcMessage secured = large.withSecurityMetadata(RpcSecurityMetadata.of(
                repeat('c', 64), repeat('d', 64)));
        assertSame(largePayload, secured.planGraphBytes());
        CountingOutputStream largeOut = new CountingOutputStream();
        RpcAdmissionConfig securedAdmission = RpcAdmissionConfig.forTesting(
                2, 2, 5_000L, normalLimit,
                largeLimit + RpcCodec.SECURE_SUFFIX_BYTES,
                largeLimit + RpcCodec.SECURE_SUFFIX_BYTES);
        ControlResidentBudget largeResident = ControlResidentBudget.forTesting(
                secured.encodeResidentBytes());
        ControlResidentLease first = largeResident.openLease();
        ControlResidentLease.OwnerToken firstOwner = first.ownerToken();
        ControlResidentLease rejected = largeResident.openLease();
        try {
            first.reserve(firstOwner, secured.encodeResidentBytes());
            assertEquals(secured.encodeResidentBytes(),
                    largeResident.snapshot().usedBytes());
            assertThrows(ControlResidentBudgetExceededException.class,
                    () -> rejected.reserve(rejected.ownerToken(),
                            secured.encodeResidentBytes()));
            RpcCodec.encodeTo(secured, largeOut, securedAdmission);
        } finally {
            rejected.release(rejected.ownerToken());
            first.release(firstOwner);
        }
        assertEquals(0L, largeResident.snapshot().usedBytes());
        assertEquals((long) largeLimit + RpcCodec.SECURE_SUFFIX_BYTES + Integer.BYTES,
                largeOut.bytes);
    }

    @Test
    void concurrentTelemetryEncodeUsesStablePrivateBacking() throws Exception {
        RpcMessage report = RpcMessage.jobResult(true, 9L, null,
                new byte[ExecutionTelemetrySummaryCodec.MAX_FRAME_BYTES])
                .withSecurityMetadata(RpcSecurityMetadata.of(
                        repeat('c', 64), repeat('d', 64)));
        long oneCharge = report.encodeResidentBytes();
        ControlResidentBudget resident = ControlResidentBudget.forTesting(oneCharge * 2L);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch bothReserved = new CountDownLatch(2);
        CountDownLatch encode = new CountDownLatch(1);
        AtomicReference<Long> first = new AtomicReference<>();
        AtomicReference<Long> second = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread one = leasedEncoder(start, bothReserved, encode, resident,
                report, first, failure);
        Thread two = leasedEncoder(start, bothReserved, encode, resident,
                report, second, failure);
        one.start();
        two.start();
        start.countDown();
        assertTrue(bothReserved.await(5, TimeUnit.SECONDS));
        assertEquals(oneCharge * 2L, resident.snapshot().usedBytes(),
                "two concurrent 48KiB telemetry encodes must remain explicitly charged");
        encode.countDown();
        one.join(TimeUnit.SECONDS.toMillis(5));
        two.join(TimeUnit.SECONDS.toMillis(5));

        if (failure.get() != null) throw new AssertionError(failure.get());
        assertEquals(first.get(), second.get());
        assertEquals(0L, resident.snapshot().usedBytes());
    }

    private static RpcMessage roundTrip(RpcMessage message) throws Exception {
        return RpcCodec.decode(new DataInputStream(
                new ByteArrayInputStream(RpcCodec.encode(message))));
    }

    private static void assertOversizedTelemetryRejectedBeforeReservation(
            RpcMessage.Type telemetryType, int payloadBytes) throws Exception {
        byte[] frame = RpcCodec.encode(rawPayload(
                RpcMessage.Type.MANIFEST_OUTCOME, new byte[payloadBytes]));
        java.nio.ByteBuffer.wrap(frame, Integer.BYTES, Integer.BYTES)
                .putInt(telemetryType.ordinal());
        ControlResidentBudget resident = ControlResidentBudget.forTesting(1024L * 1024L);
        RpcAdmissionConfig admission = RpcAdmissionConfig.forTesting(
                2, 2, 5_000L, 1024 * 1024, 1024 * 1024, 2L * 1024 * 1024);
        RpcFrameBudget wire = new RpcFrameBudget(2L * 1024 * 1024);

        IOException rejected = assertThrows(IOException.class, () -> RpcCodec.decodeRequest(
                new DataInputStream(new ByteArrayInputStream(frame)), admission, wire,
                RpcRequestPolicy.GENERAL, resident));

        assertEquals(0L, resident.snapshot().usedBytes(), rejected.getMessage());
        assertEquals(0L, wire.used(), "rejected frame reservation must be released");
    }

    private static RpcMessage rawPayload(RpcMessage.Type type, byte[] payload) {
        return new RpcMessage(type, null, null, 0, 0, null, 0L, null,
                false, 0L, null, 0L, -1L, -1, null, null,
                null, 0L, payload, null, null, null, null, null);
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static String hex(byte[] bytes) {
        StringBuilder value = new StringBuilder(bytes.length * 2);
        for (byte current : bytes) {
            value.append(String.format("%02x", current & 0xff));
        }
        return value.toString();
    }

    private static void encodeUnderExactLease(RpcMessage message, OutputStream target,
                                              RpcAdmissionConfig admission) throws Exception {
        ControlResidentBudget resident = ControlResidentBudget.forTesting(
                message.encodeResidentBytes());
        ControlResidentLease lease = resident.openLease();
        ControlResidentLease.OwnerToken owner = lease.ownerToken();
        try {
            lease.reserve(owner, message.encodeResidentBytes());
            RpcCodec.encodeTo(message, target, admission);
        } finally {
            lease.release(owner);
        }
        assertEquals(0L, resident.snapshot().usedBytes());
    }

    private static Thread leasedEncoder(CountDownLatch start,
                                        CountDownLatch bothReserved,
                                        CountDownLatch encode,
                                        ControlResidentBudget resident,
                                        RpcMessage message,
                                        AtomicReference<Long> target,
                                        AtomicReference<Throwable> failure) {
        return new Thread(() -> {
            ControlResidentLease lease = null;
            ControlResidentLease.OwnerToken owner = null;
            try {
                start.await();
                lease = resident.openLease();
                owner = lease.ownerToken();
                lease.reserve(owner, message.encodeResidentBytes());
                bothReserved.countDown();
                assertTrue(encode.await(5, TimeUnit.SECONDS));
                CountingOutputStream output = new CountingOutputStream();
                RpcCodec.encodeTo(message, output);
                target.set(output.bytes);
            } catch (Throwable error) {
                failure.compareAndSet(null, error);
            } finally {
                if (lease != null) lease.release(owner);
            }
        });
    }

    private static final class CountingOutputStream extends OutputStream {
        long bytes;

        @Override
        public void write(int value) { bytes++; }

        @Override
        public void write(byte[] values, int offset, int length) { bytes += length; }
    }
}
