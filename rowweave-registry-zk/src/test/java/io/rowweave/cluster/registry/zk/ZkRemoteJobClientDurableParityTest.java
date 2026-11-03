package io.rowweave.cluster.registry.zk;

import io.rowweave.QualityGateDecision;
import io.rowweave.cluster.Endpoint;
import io.rowweave.cluster.MasterElection;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.cluster.rpc.RpcServer;
import io.rowweave.commit.BatchSinkOutcome;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.RunCancellationResult;
import io.rowweave.run.RunExecutionEvent;
import io.rowweave.run.RunExecutionEventPage;
import io.rowweave.run.RunExecutionEventPageCodec;
import io.rowweave.run.RunStatus;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** public ZK HA durable-client parity and failover semantics. */
class ZkRemoteJobClientDurableParityTest {
    private static final String RUN_ID = "zk-client-parity";
    private static final String REQUEST_ID = "stable-cancel-request";
    private static final String SUBMIT_TIMEOUT_KEY = "rowweave.cluster.submitTimeoutMs";

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void durableSurfaceMatchesTheRealClientContract() throws Exception {
        assertAll(
                () -> assertEquals(RunExecutionEventPage.class,
                        publicMethod("queryRunEvents", String.class, long.class, int.class)
                                .getReturnType()),
                () -> assertEquals(RunCancellationResult.class,
                        publicMethod("cancelRun", String.class, String.class)
                                .getReturnType()),
                () -> assertEquals(BatchSinkOutcome.class,
                        publicMethod("queryBatchSinkOutcome", String.class)
                                .getReturnType()),
                () -> assertEquals(QualityGateDecision.class,
                        publicMethod("queryQualityGateDecision", String.class)
                                .getReturnType()));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void oneClientReresolvesAllDurableOperationsAcrossRealLeaderFailover()
            throws Exception {
        requireDurableSurface();
        AtomicInteger firstRequests = new AtomicInteger();
        AtomicInteger secondRequests = new AtomicInteger();
        try (TestingServer zk = new TestingServer(true);
             ZkServiceRegistry firstRegistry = new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry secondRegistry = new ZkServiceRegistry(zk.getConnectString());
             RpcServer firstServer = new RpcServer(
                     request -> leaderResponse(request, true, firstRequests));
             RpcServer secondServer = new RpcServer(
                     request -> leaderResponse(request, false, secondRequests));
             MasterElection firstElection = new MasterElection(firstRegistry,
                     new Endpoint("127.0.0.1", firstServer.port()));
             MasterElection secondElection = new MasterElection(secondRegistry,
                     new Endpoint("127.0.0.1", secondServer.port()))) {
            CountDownLatch secondGained = new CountDownLatch(1);
            secondElection.addLeadershipListener(active -> {
                if (active) secondGained.countDown();
            });
            firstElection.start();
            assertTrue(firstElection.isLeader());
            secondElection.start();
            assertFalse(secondElection.isLeader());

            try (ZkRemoteJobClient client = new ZkRemoteJobClient(zk.getConnectString())) {
                assertEquals(1L, queryEvents(client, RUN_ID, 0L, 1)
                        .events().get(0).sequence());
                assertEquals(RunCancellationResult.Disposition.ACCEPTED,
                        cancel(client, RUN_ID, REQUEST_ID).disposition());
                assertEquals(BatchSinkOutcome.State.PREPARING,
                        querySink(client, RUN_ID).state());
                assertNull(queryQuality(client, RUN_ID));
                assertEquals(4, firstRequests.get());

                firstElection.close();
                assertTrue(secondGained.await(10L, TimeUnit.SECONDS));
                assertTrue(secondElection.isLeader());

                assertEquals(2L, queryEvents(client, RUN_ID, 0L, 1)
                        .events().get(0).sequence());
                assertEquals(RunCancellationResult.Disposition.ALREADY_REQUESTED,
                        cancel(client, RUN_ID, REQUEST_ID).disposition());
                BatchSinkOutcome committed = querySink(client, RUN_ID);
                assertEquals(BatchSinkOutcome.State.COMMITTED, committed.state());
                assertEquals(Long.valueOf(7L), committed.generation());
                assertNull(queryQuality(client, RUN_ID));
                assertEquals(4, secondRequests.get());
                assertEquals(4, firstRequests.get(),
                        "post-failover calls must not use the old endpoint");
            }
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void invalidArgumentsFailBeforeAnyLeaderCanBeLocated() throws Exception {
        requireDurableSurface();
        try (TestingServer zk = new TestingServer(true);
             ZkRemoteJobClient client = new ZkRemoteJobClient(zk.getConnectString())) {
            assertThrows(IllegalArgumentException.class,
                    () -> queryEvents(client, "", 0L, 1));
            assertThrows(IllegalArgumentException.class,
                    () -> queryEvents(client, RUN_ID, -1L, 1));
            assertThrows(IllegalArgumentException.class,
                    () -> queryEvents(client, RUN_ID, 0L, 0));
            assertThrows(IllegalArgumentException.class,
                    () -> queryEvents(client, RUN_ID, 0L, 257));
            assertThrows(IllegalArgumentException.class,
                    () -> cancel(client, "", REQUEST_ID));
            assertThrows(IllegalArgumentException.class,
                    () -> cancel(client, RUN_ID, ""));
            assertThrows(IllegalArgumentException.class,
                    () -> cancel(client, RUN_ID, "bad\0request"));
            assertThrows(IllegalArgumentException.class,
                    () -> cancel(client, RUN_ID, repeat('x', 129)));
            assertThrows(IllegalArgumentException.class,
                    () -> querySink(client, ""));
            assertThrows(IllegalArgumentException.class,
                    () -> queryQuality(client, ""));
        }
    }

    @Test
    @Timeout(value = 45, unit = TimeUnit.SECONDS)
    void malformedResponsesFailClosedAndAuthorityErrorsRemainNonActionable()
            throws Exception {
        requireDurableSurface();
        AtomicReference<Function<RpcMessage, RpcMessage>> behavior =
                new AtomicReference<>();
        try (TestingServer zk = new TestingServer(true);
             ZkServiceRegistry registry = new ZkServiceRegistry(zk.getConnectString());
             RpcServer server = new RpcServer(request -> behavior.get().apply(request));
             MasterElection election = new MasterElection(registry,
                     new Endpoint("127.0.0.1", server.port()))) {
            election.start();
            assertTrue(election.isLeader());
            try (ZkRemoteJobClient client = new ZkRemoteJobClient(zk.getConnectString())) {
                assertEventFailure(client, behavior, request -> RpcMessage.ack());
                assertEventFailure(client, behavior, request -> RpcMessage.runEventsPage(
                        "other-run", validPage(0L, 1)));
                assertEventFailure(client, behavior, request ->
                        RpcMessage.runEventsPageFailure(request.runId(), "UNAVAILABLE"));
                assertEventFailure(client, behavior, request -> RpcMessage.runEventsPage(
                        request.runId(), new byte[]{1, 2, 3}));
                assertEventFailure(client, behavior, request -> RpcMessage.runEventsPage(
                        request.runId(), validPage(1L, 2)));
                assertEventFailure(client, behavior, request -> RpcMessage.runEventsPage(
                        request.runId(), pageWithTwoEvents()));

                assertCancelFailure(client, behavior, request -> RpcMessage.ack());
                assertCancelFailure(client, behavior, request ->
                        RpcMessage.runCancellationResult(new RunCancellationResult(
                                "other-run", RunCancellationResult.Disposition.NOT_FOUND,
                                null, 0L)));
                assertCancelFailure(client, behavior, request ->
                        RpcMessage.runCancellationFailure(request.runId(), "UNAVAILABLE"));
                RpcMessage invalidDisposition = corrupt(
                        validCancellation(RUN_ID), "commitOutcome", "INVALID");
                assertEquals("INVALID", invalidDisposition.runCancellationDisposition());
                assertCancelFailure(client, behavior, request -> invalidDisposition);
                RpcMessage invalidStatus = corrupt(
                        validCancellation(RUN_ID), "commitDomainId", "INVALID");
                assertEquals("INVALID", invalidStatus.runCancellationStatus());
                assertCancelFailure(client, behavior, request -> invalidStatus);
                RpcMessage invalidRevision = corrupt(
                        validCancellation(RUN_ID), "rowCount", -1L);
                assertEquals(-1L, invalidRevision.runCancellationRevision());
                assertCancelFailure(client, behavior, request -> invalidRevision);

                assertSinkFailure(client, behavior, request -> RpcMessage.ack());
                assertSinkFailure(client, behavior, request -> RpcMessage.batchSinkOutcome(
                        "other-run", "NOT_FOUND", 0L, null, null));
                assertSinkFailure(client, behavior, request -> RpcMessage.batchSinkOutcome(
                        request.runId(), "INVALID", 0L, null, "INVALID_STATE"));
                assertSinkFailure(client, behavior, request -> RpcMessage.batchSinkOutcome(
                        request.runId(), "COMMITTED", 1L, new byte[]{1},
                        "MANIFEST_VISIBLE_VERIFIED"));
                assertSinkFailure(client, behavior, request -> RpcMessage.batchSinkOutcome(
                        request.runId(), "COMMITTED", 1L, new byte[32], "invalid-detail"));
                assertSinkFailure(client, behavior, request -> RpcMessage.batchSinkOutcome(
                        request.runId(), "NOT_FOUND", 1L, new byte[32], "EVIDENCE"));
                assertSinkFailure(client, behavior, request -> RpcMessage.batchSinkOutcome(
                        request.runId(), "COMMITTED", -1L, new byte[32],
                        "MANIFEST_VISIBLE_VERIFIED"));

                behavior.set(request -> RpcMessage.qualityGateDecisionNotFound(
                        request.runId()));
                assertNull(queryQuality(client, RUN_ID));
                assertQualityFailure(client, behavior, request -> RpcMessage.ack());
                assertQualityFailure(client, behavior, request ->
                        RpcMessage.qualityGateDecision(
                                "other-run", new byte[]{1}));
                assertQualityFailure(client, behavior, request ->
                        RpcMessage.qualityGateDecisionFailure(
                                request.runId(), "UNAVAILABLE"));
                assertQualityFailure(client, behavior, request ->
                        RpcMessage.qualityGateDecision(
                                request.runId(), new byte[]{1, 2, 3}));

                assertAuthorityError(client, behavior, "OUTCOME_AUTHORITY_UNAVAILABLE");
                assertAuthorityError(client, behavior, "OUTCOME_PROJECTION_FAILED");
            }
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void timedOutCancellationDoesNotReplayAcrossLeaderUntilCallerRetries()
            throws Exception {
        requireDurableSurface();
        String previousTimeout = System.getProperty(SUBMIT_TIMEOUT_KEY);
        CountDownLatch firstReceived = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch secondGained = new CountDownLatch(1);
        AtomicInteger firstRequests = new AtomicInteger();
        AtomicInteger secondRequests = new AtomicInteger();
        ExecutorService caller = Executors.newSingleThreadExecutor();
        System.setProperty(SUBMIT_TIMEOUT_KEY, "5000");
        try (TestingServer zk = new TestingServer(true);
             ZkServiceRegistry firstRegistry = new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry secondRegistry = new ZkServiceRegistry(zk.getConnectString());
             RpcServer firstServer = new RpcServer(request -> {
                 assertCancelRequest(request);
                 firstRequests.incrementAndGet();
                 firstReceived.countDown();
                 try {
                     if (!releaseFirst.await(20L, TimeUnit.SECONDS)) {
                         throw new AssertionError("blocked leader handler was not released");
                     }
                 } catch (InterruptedException interrupted) {
                     Thread.currentThread().interrupt();
                     throw new RuntimeException(interrupted);
                 }
                 return RpcMessage.runCancellationResult(new RunCancellationResult(
                         RUN_ID, RunCancellationResult.Disposition.ACCEPTED,
                         RunStatus.CANCELING, 1L));
             });
             RpcServer secondServer = new RpcServer(request -> {
                 assertCancelRequest(request);
                 secondRequests.incrementAndGet();
                 return RpcMessage.runCancellationResult(new RunCancellationResult(
                         RUN_ID, RunCancellationResult.Disposition.ALREADY_REQUESTED,
                         RunStatus.CANCELING, 2L));
             });
             MasterElection firstElection = new MasterElection(firstRegistry,
                     new Endpoint("127.0.0.1", firstServer.port()));
             MasterElection secondElection = new MasterElection(secondRegistry,
                     new Endpoint("127.0.0.1", secondServer.port()))) {
            secondElection.addLeadershipListener(active -> {
                if (active) secondGained.countDown();
            });
            firstElection.start();
            secondElection.start();
            assertTrue(firstElection.isLeader());
            assertFalse(secondElection.isLeader());

            try (ZkRemoteJobClient client = new ZkRemoteJobClient(zk.getConnectString())) {
                Future<RunCancellationResult> inFlight = caller.submit(
                        () -> cancel(client, RUN_ID, REQUEST_ID));
                assertTrue(firstReceived.await(10L, TimeUnit.SECONDS));
                assertFalse(inFlight.isDone());

                firstElection.close();
                assertTrue(secondGained.await(10L, TimeUnit.SECONDS));
                assertTrue(secondElection.isLeader());
                assertFalse(inFlight.isDone(),
                        "the original A socket must still be awaiting its own response");

                ExecutionException timedOut = assertThrows(ExecutionException.class,
                        () -> inFlight.get(10L, TimeUnit.SECONDS));
                assertTrue(hasCause(timedOut, SocketTimeoutException.class),
                        "unknown outcome must come from the configured socket read timeout");
                assertEquals(1, firstRequests.get());
                assertEquals(0, secondRequests.get(),
                        "one explicit call must not replay itself on the new leader");

                releaseFirst.countDown();
                RunCancellationResult replay = cancel(client, RUN_ID, REQUEST_ID);
                assertEquals(RunCancellationResult.Disposition.ALREADY_REQUESTED,
                        replay.disposition());
                assertEquals(1, firstRequests.get());
                assertEquals(1, secondRequests.get());
            }
        } finally {
            releaseFirst.countDown();
            caller.shutdownNow();
            try {
                assertTrue(caller.awaitTermination(5L, TimeUnit.SECONDS),
                        "cancellation caller must terminate within the cleanup budget");
            } finally {
                if (previousTimeout == null) System.clearProperty(SUBMIT_TIMEOUT_KEY);
                else System.setProperty(SUBMIT_TIMEOUT_KEY, previousTimeout);
            }
        }
    }

    private static RpcMessage leaderResponse(
            RpcMessage request, boolean first, AtomicInteger requests) {
        requests.incrementAndGet();
        switch (request.type()) {
            case QUERY_RUN_EVENTS:
                assertEquals(RUN_ID, request.runId());
                assertEquals(0L, request.eventAfterSequence());
                assertEquals(1, request.eventLimit());
                return RpcMessage.runEventsPage(
                        RUN_ID, validPage(0L, first ? 1L : 2L));
            case CANCEL_RUN:
                assertCancelRequest(request);
                return RpcMessage.runCancellationResult(new RunCancellationResult(
                        RUN_ID, first
                        ? RunCancellationResult.Disposition.ACCEPTED
                        : RunCancellationResult.Disposition.ALREADY_REQUESTED,
                        RunStatus.CANCELING, first ? 5L : 6L));
            case QUERY_BATCH_SINK_OUTCOME:
                assertEquals(RUN_ID, request.runId());
                if (first) {
                    return RpcMessage.batchSinkOutcome(
                            RUN_ID, "PREPARING", 0L, null, "LOCATORS_PENDING");
                }
                byte[] identity = new byte[32];
                identity[0] = 7;
                return RpcMessage.batchSinkOutcome(
                        RUN_ID, "COMMITTED", 7L, identity,
                        "MANIFEST_VISIBLE_VERIFIED");
            case QUERY_QUALITY_GATE_DECISION:
                assertEquals(RUN_ID, request.runId());
                return RpcMessage.qualityGateDecisionNotFound(RUN_ID);
            default:
                throw new AssertionError("unexpected request " + request.type());
        }
    }

    private static void assertEventFailure(
            ZkRemoteJobClient client,
            AtomicReference<Function<RpcMessage, RpcMessage>> behavior,
            Function<RpcMessage, RpcMessage> response) {
        behavior.set(response);
        assertThrows(RuntimeException.class,
                () -> queryEvents(client, RUN_ID, 0L, 1));
    }

    private static void assertCancelFailure(
            ZkRemoteJobClient client,
            AtomicReference<Function<RpcMessage, RpcMessage>> behavior,
            Function<RpcMessage, RpcMessage> response) {
        behavior.set(response);
        assertThrows(RuntimeException.class,
                () -> cancel(client, RUN_ID, REQUEST_ID));
    }

    private static void assertSinkFailure(
            ZkRemoteJobClient client,
            AtomicReference<Function<RpcMessage, RpcMessage>> behavior,
            Function<RpcMessage, RpcMessage> response) {
        behavior.set(response);
        assertThrows(RuntimeException.class, () -> querySink(client, RUN_ID));
    }

    private static void assertQualityFailure(
            ZkRemoteJobClient client,
            AtomicReference<Function<RpcMessage, RpcMessage>> behavior,
            Function<RpcMessage, RpcMessage> response) {
        behavior.set(response);
        assertThrows(RuntimeException.class, () -> queryQuality(client, RUN_ID));
    }

    private static void assertAuthorityError(
            ZkRemoteJobClient client,
            AtomicReference<Function<RpcMessage, RpcMessage>> behavior,
            String detail) {
        behavior.set(request -> RpcMessage.batchSinkOutcome(
                request.runId(), "CORRUPT_OR_CONFLICT", 0L, null, detail));
        BatchSinkOutcome outcome = querySink(client, RUN_ID);
        assertEquals(BatchSinkOutcome.State.CORRUPT_OR_CONFLICT, outcome.state());
        assertEquals(detail, outcome.detailCode());
    }

    private static RpcMessage validCancellation(String runId) {
        return RpcMessage.runCancellationResult(new RunCancellationResult(
                runId, RunCancellationResult.Disposition.ACCEPTED,
                RunStatus.CANCELING, 1L));
    }

    private static RpcMessage corrupt(RpcMessage message, String field, Object value) {
        try {
            Field target = RpcMessage.class.getDeclaredField(field);
            target.setAccessible(true);
            target.set(message, value);
            return message;
        } catch (ReflectiveOperationException failure) {
            throw new AssertionError("failed to build malformed RPC fixture", failure);
        }
    }

    private static void assertCancelRequest(RpcMessage request) {
        assertEquals(RpcMessage.Type.CANCEL_RUN, request.type());
        assertEquals(RUN_ID, request.runId());
        assertEquals(REQUEST_ID, request.cancelRequestId());
    }

    private static byte[] validPage(long afterSequence, long sequence) {
        return RunExecutionEventPageCodec.encode(RunExecutionEventPage.found(
                afterSequence, Collections.singletonList(event(sequence)), false));
    }

    private static byte[] pageWithTwoEvents() {
        return RunExecutionEventPageCodec.encode(RunExecutionEventPage.found(
                0L, Arrays.asList(event(1L), event(2L)), false));
    }

    private static RunExecutionEvent event(long sequence) {
        return RunExecutionEvent.of(sequence, RunExecutionEvent.Type.ATTEMPT_ADVANCED,
                RunStatus.RUNNING, RunStatus.RUNNING,
                AttemptStatus.DISPATCHING, AttemptStatus.RUNNING,
                1000L + sequence, RunExecutionEvent.DiagnosticCategory.NONE,
                "0001020304050607");
    }

    private static void requireDurableSurface() {
        publicMethod("queryRunEvents", String.class, long.class, int.class);
        publicMethod("cancelRun", String.class, String.class);
        publicMethod("queryBatchSinkOutcome", String.class);
        publicMethod("queryQualityGateDecision", String.class);
    }

    private static Method publicMethod(String name, Class<?>... arguments) {
        try {
            return ZkRemoteJobClient.class.getMethod(name, arguments);
        } catch (NoSuchMethodException missing) {
            throw new AssertionError("missing ZK durable client method " + name, missing);
        }
    }

    private static RunExecutionEventPage queryEvents(
            ZkRemoteJobClient client, String runId, long afterSequence, int limit) {
        return (RunExecutionEventPage) invoke(client,
                publicMethod("queryRunEvents", String.class, long.class, int.class),
                runId, afterSequence, limit);
    }

    private static RunCancellationResult cancel(
            ZkRemoteJobClient client, String runId, String requestId) {
        return (RunCancellationResult) invoke(client,
                publicMethod("cancelRun", String.class, String.class), runId, requestId);
    }

    private static BatchSinkOutcome querySink(
            ZkRemoteJobClient client, String runId) {
        return (BatchSinkOutcome) invoke(client,
                publicMethod("queryBatchSinkOutcome", String.class), runId);
    }

    private static QualityGateDecision queryQuality(
            ZkRemoteJobClient client, String runId) {
        return (QualityGateDecision) invoke(client,
                publicMethod("queryQualityGateDecision", String.class), runId);
    }

    private static Object invoke(Object target, Method method, Object... arguments) {
        try {
            return method.invoke(target, arguments);
        } catch (InvocationTargetException wrapped) {
            Throwable cause = wrapped.getCause();
            if (cause instanceof RuntimeException) throw (RuntimeException) cause;
            if (cause instanceof Error) throw (Error) cause;
            throw new RuntimeException(cause);
        } catch (IllegalAccessException inaccessible) {
            throw new AssertionError(inaccessible);
        }
    }

    private static boolean hasCause(Throwable failure, Class<? extends Throwable> type) {
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (type.isInstance(current)) return true;
        }
        return false;
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
