package io.rowweave.cluster.rpc;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RpcResidentAdmissionTest {

    @Test
    void decodedFieldReservationLivesThroughHandlerAndReleasesAfterReturn() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ControlResidentBudget resident = ControlResidentBudget.forTesting(1024L);
        try (RpcServer server = server(request -> {
            entered.countDown();
            try {
                assertTrue(release.await(2, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            return RpcMessage.ack();
        }, resident);
             Socket socket = new Socket("127.0.0.1", server.port())) {
            socket.getOutputStream().write(RpcCodec.encode(
                    RpcMessage.submitJob(new byte[64])));
            socket.getOutputStream().flush();
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            assertEquals(64L, server.residentBytes(), "decoded plan byte[] must stay charged");
            release.countDown();
            assertEquals(RpcMessage.Type.ACK,
                    RpcCodec.decode(new DataInputStream(socket.getInputStream())).type());
            await(() -> server.residentBytes() == 0L);
        } finally {
            release.countDown();
        }
    }

    @Test
    void residentExhaustionReturnsStableFailureBeforeHandlerAndLeavesNoCharge() throws Exception {
        java.util.concurrent.atomic.AtomicInteger handled = new java.util.concurrent.atomic.AtomicInteger();
        ControlResidentBudget resident = ControlResidentBudget.forTesting(63L);
        try (RpcServer server = server(request -> {
            handled.incrementAndGet();
            return RpcMessage.ack();
        }, resident);
             Socket socket = new Socket("127.0.0.1", server.port())) {
            socket.setSoTimeout(2_000);
            socket.getOutputStream().write(RpcCodec.encode(
                    RpcMessage.submitJob(new byte[64])));
            socket.getOutputStream().flush();

            RpcMessage failure = RpcCodec.decode(new DataInputStream(socket.getInputStream()));
            assertEquals(RpcMessage.Type.JOB_RESULT, failure.type());
            assertTrue(failure.errorMessage().startsWith(
                    ControlResidentBudgetExceededException.CODE));
            assertEquals(0, handled.get());
            await(() -> server.residentBytes() == 0L);
        }
    }

    @Test
    void materializationReservationFailureReturnsStableCodeAndRunsNoLaterSideEffect() throws Exception {
        java.util.concurrent.atomic.AtomicInteger sideEffects = new java.util.concurrent.atomic.AtomicInteger();
        ControlResidentBudget resident = ControlResidentBudget.forTesting(64L);
        try (RpcServer server = server(request -> {
            request.reserveResidentBytes(1L);
            sideEffects.incrementAndGet();
            return RpcMessage.ack();
        }, resident);
             Socket socket = new Socket("127.0.0.1", server.port())) {
            socket.setSoTimeout(2_000);
            socket.getOutputStream().write(RpcCodec.encode(
                    RpcMessage.submitJob(new byte[64])));
            socket.getOutputStream().flush();

            RpcMessage failure = RpcCodec.decode(new DataInputStream(socket.getInputStream()));
            assertEquals(RpcMessage.Type.JOB_RESULT, failure.type());
            assertTrue(failure.errorMessage().startsWith(
                    ControlResidentBudgetExceededException.CODE));
            assertEquals(0, sideEffects.get());
            await(() -> server.residentBytes() == 0L);
        }
    }

    @Test
    void malformedUtf8ReleasesRawAndMaterializationReservations() throws Exception {
        ControlResidentBudget resident = ControlResidentBudget.forTesting(1024L);
        byte[] frame = RpcCodec.encode(RpcMessage.heartbeat("a"));
        // first String starts after [length][type]; replace its single UTF-8 byte with malformed 0xff.
        frame[12] = (byte) 0xff;
        RpcAdmissionConfig admission = RpcAdmissionConfig.forTesting(2, 2, 5_000L,
                512, 1024, 2048);
        RpcFrameBudget wire = new RpcFrameBudget(2048L);
        try {
            RpcCodec.decodeRequest(new DataInputStream(new ByteArrayInputStream(frame)),
                    admission, wire, RpcRequestPolicy.GENERAL, resident);
        } catch (java.io.IOException expected) {
            // expected
        }
        assertEquals(0L, resident.snapshot().usedBytes());
        assertEquals(0L, wire.used());
    }

    @Test
    void clientReservesExternalSubmitPayloadBeforeWritingAnyRequestBytes() throws Exception {
        java.util.concurrent.atomic.AtomicInteger handled = new java.util.concurrent.atomic.AtomicInteger();
        ControlResidentBudget serverBudget = ControlResidentBudget.forTesting(1024L);
        ControlResidentBudget clientBudget = ControlResidentBudget.forTesting(63L);
        try (RpcServer server = server(request -> {
            handled.incrementAndGet();
            return RpcMessage.ack();
        }, serverBudget);
             RpcClient client = new RpcClient("127.0.0.1", server.port(),
                     2_000, 2_000, ControlSecurityConfig.fromSystemProperties(), clientBudget)) {
            RuntimeException rejected = assertThrows(RuntimeException.class,
                    () -> client.call(RpcMessage.submitJob(new byte[64])));
            assertTrue(hasCause(rejected, ControlResidentBudgetExceededException.class));
            Thread.sleep(50L);
            assertEquals(0, handled.get());
            assertEquals(0L, clientBudget.snapshot().usedBytes());
        }
    }

    @Test
    void decodedResponseStaysChargedUntilScopedConsumerReturns() throws Exception {
        byte[] snapshot = new byte[512];
        ControlResidentBudget serverBudget = ControlResidentBudget.forTesting(8_192L);
        ControlResidentBudget clientBudget = ControlResidentBudget.forTesting(8_192L);
        CountDownLatch consuming = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (RpcServer server = server(request -> RpcMessage.runSnapshot(snapshot), serverBudget);
             RpcClient client = new RpcClient("127.0.0.1", server.port(),
                     2_000, 2_000, ControlSecurityConfig.fromSystemProperties(), clientBudget)) {
            FutureTask<Integer> call = new FutureTask<>(() -> client.callScoped(
                    RpcMessage.queryRun("scoped-response"), response -> {
                        consuming.countDown();
                        try {
                            assertTrue(release.await(2, TimeUnit.SECONDS));
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException(interrupted);
                        }
                        return response.planGraphBytes().length;
                    }));
            Thread caller = new Thread(call, "rpc-scoped-response-test");
            caller.start();
            assertTrue(consuming.await(2, TimeUnit.SECONDS));
            assertTrue(clientBudget.snapshot().usedBytes() >= snapshot.length,
                    "decoded response must remain resident-charged while consumed");
            release.countDown();
            assertEquals(snapshot.length, call.get(2, TimeUnit.SECONDS));
            assertEquals(0L, clientBudget.snapshot().usedBytes());
        } finally {
            release.countDown();
        }
    }

    @Test
    void responseProducerMustReserveBeforeAllocatingPayload() throws Exception {
        java.util.concurrent.atomic.AtomicInteger allocations = new java.util.concurrent.atomic.AtomicInteger();
        ControlResidentBudget resident = ControlResidentBudget.forTesting(256L);
        try (RpcServer server = server(request -> {
            request.reserveResponseResidentBytes(512L);
            allocations.incrementAndGet();
            return RpcMessage.runSnapshot(new byte[512]);
        }, resident);
             Socket socket = new Socket("127.0.0.1", server.port())) {
            socket.setSoTimeout(2_000);
            socket.getOutputStream().write(RpcCodec.encode(RpcMessage.queryRun("response-budget")));
            socket.getOutputStream().flush();

            RpcMessage failure = RpcCodec.decode(new DataInputStream(socket.getInputStream()));
            assertEquals(RpcMessage.Type.JOB_RESULT, failure.type());
            assertTrue(failure.errorMessage().startsWith(
                    ControlResidentBudgetExceededException.CODE));
            assertEquals(0, allocations.get());
            await(() -> server.residentBytes() == 0L);
        }
    }

    @Test
    void producerReservationCreditsRetainedPayloadInsteadOfDoubleChargingIt() throws Exception {
        byte[] telemetry = new byte[40 * 1024];
        ControlResidentBudget resident = ControlResidentBudget.forTesting(60L * 1024L);
        RpcAdmissionConfig admission = RpcAdmissionConfig.forTesting(
                4, 4, 5_000L, 64 * 1024, 64 * 1024, 128 * 1024L);
        try (RpcServer server = new RpcServer(request -> {
            request.reserveResponseResidentBytes(48L * 1024L);
            return RpcMessage.jobResult(true, 73L, null, telemetry);
        }, 0, ControlSecurityConfig.fromSystemProperties(), admission,
                RpcRequestPolicy.GENERAL, resident);
             Socket socket = new Socket("127.0.0.1", server.port())) {
            socket.setSoTimeout(2_000);
            socket.getOutputStream().write(RpcCodec.encode(RpcMessage.heartbeat("credit")));
            socket.getOutputStream().flush();

            RpcMessage response = RpcCodec.decode(
                    new DataInputStream(socket.getInputStream()));

            assertEquals(RpcMessage.Type.JOB_RESULT, response.type());
            assertTrue(response.success(), "advisory payload accounting cannot change success");
            assertEquals(73L, response.rowCount());
            await(() -> server.residentBytes() == 0L);
        }
    }

    @Test
    void maximumTelemetryResponseStaysChargedThroughScopedDecodeAndReleasesOnFailure()
            throws Exception {
        byte[] telemetry = new byte[io.rowweave.plan.ExecutionTelemetrySummaryCodec.MAX_FRAME_BYTES];
        ControlResidentBudget serverBudget = ControlResidentBudget.forTesting(256L * 1024L);
        ControlResidentBudget clientBudget = ControlResidentBudget.forTesting(256L * 1024L);
        RpcAdmissionConfig admission = RpcAdmissionConfig.forTesting(
                4, 4, 5_000L, 64 * 1024, 64 * 1024, 128 * 1024L);
        try (RpcServer server = new RpcServer(request -> {
            request.reserveResponseResidentBytes(telemetry.length);
            return RpcMessage.jobResult(true, 7L, null, telemetry);
        }, 0, ControlSecurityConfig.fromSystemProperties(), admission,
                RpcRequestPolicy.GENERAL, serverBudget);
             RpcClient client = new RpcClient("127.0.0.1", server.port(),
                     2_000, 2_000, ControlSecurityConfig.fromSystemProperties(),
                     clientBudget)) {
            assertThrows(RuntimeException.class, () -> client.callScoped(
                    RpcMessage.heartbeat("telemetry-client"), response -> {
                        assertTrue(clientBudget.snapshot().usedBytes() >= telemetry.length);
                        throw new IllegalStateException("consumer failed");
                    }));
            assertEquals(0L, clientBudget.snapshot().usedBytes());
            await(() -> server.residentBytes() == 0L);
        }
    }

    @Test
    void decodedSummaryBudgetFailureOnlyDowngradesAdvisoryCoverage() throws Exception {
        byte[] telemetry = io.rowweave.plan.ExecutionTelemetrySummaryCodec.encode(
                io.rowweave.plan.ExecutionTelemetrySummary.unavailable());
        ControlResidentBudget serverBudget = ControlResidentBudget.forTesting(8_192L);
        ControlResidentBudget clientBudget = ControlResidentBudget.forTesting(220L);
        try (RpcServer server = server(request ->
                     RpcMessage.jobResult(true, 79L, null, telemetry), serverBudget);
             RpcClient client = new RpcClient("127.0.0.1", server.port(),
                     2_000, 2_000, ControlSecurityConfig.fromSystemProperties(),
                     clientBudget)) {
            long rows = client.callScoped(RpcMessage.heartbeat("r"), response -> {
                assertTrue(response.success());
                assertEquals(io.rowweave.plan.TelemetryCoverage.UNAVAILABLE,
                        response.executionTelemetrySummary().resourceCoverage());
                return response.rowCount();
            });

            assertEquals(79L, rows);
            assertEquals(0L, clientBudget.snapshot().usedBytes());
        }
    }

    @Test
    void peerDisconnectDuringResponseReleasesProducerAndEncoderLease() throws Exception {
        CountDownLatch produced = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ControlResidentBudget resident = ControlResidentBudget.forTesting(8_192L);
        RpcServer server = server(request -> {
            request.reserveResponseResidentBytes(512L);
            produced.countDown();
            try {
                assertTrue(release.await(2, TimeUnit.SECONDS));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(interrupted);
            }
            return RpcMessage.runSnapshot(new byte[512]);
        }, resident);
        Socket socket = new Socket("127.0.0.1", server.port());
        try {
            socket.getOutputStream().write(RpcCodec.encode(RpcMessage.queryRun("disconnect")));
            socket.getOutputStream().flush();
            assertTrue(produced.await(2, TimeUnit.SECONDS));
            socket.close();
            release.countDown();
            await(() -> server.residentBytes() == 0L);
        } finally {
            release.countDown();
            socket.close();
            server.close();
        }
    }

    private static RpcServer server(java.util.function.Function<RpcMessage, RpcMessage> handler,
                                    ControlResidentBudget resident) {
        RpcAdmissionConfig admission = RpcAdmissionConfig.forTesting(
                4, 4, 5_000L, 1024, 2048, 4096);
        return new RpcServer(handler, 0, ControlSecurityConfig.fromSystemProperties(),
                admission, RpcRequestPolicy.GENERAL, resident);
    }

    private static void await(java.util.function.BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.yield();
        assertTrue(condition.getAsBoolean(), "condition did not converge");
    }

    private static boolean hasCause(Throwable failure, Class<? extends Throwable> type) {
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (type.isInstance(current)) return true;
        }
        return false;
    }
}

