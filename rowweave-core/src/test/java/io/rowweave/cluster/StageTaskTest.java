package io.rowweave.cluster;

import io.rowweave.CollectingSink;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.Source;
import io.rowweave.ManifestFileSink;
import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.TelemetryCoverage;
import io.rowweave.sched.net.ShuffleEndpoint;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.InetSocketAddress;
import java.io.IOException;
import java.nio.file.Path;
import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class StageTaskTest {

    @TempDir
    Path tempDir;

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType), new Field("v", DataType.IntegerType));
    private static final int WINDOW = 64;

    private static DataRow row(String k, int v) {
        DataRow r = new DataRow(2);
        r.set(0, k);
        r.set(1, v);
        return r;
    }

    private static Source input(List<DataRow> rows) {
        return new Source() {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() { return new ArrayList<>(rows).iterator(); }
        };
    }

    @Test
    void progressScheduleFailureShutsCandidateDownAndReturnsTheOriginalFailure() {
        AtomicBoolean shutdown = new AtomicBoolean();
        IllegalStateException scheduleFailure = new IllegalStateException("schedule failed");
        ScheduledThreadPoolExecutor candidate = new ScheduledThreadPoolExecutor(1) {
            @Override
            public ScheduledFuture<?> scheduleAtFixedRate(
                    Runnable command, long initialDelay, long period, TimeUnit unit) {
                throw scheduleFailure;
            }

            @Override
            public List<Runnable> shutdownNow() {
                shutdown.set(true);
                return super.shutdownNow();
            }
        };
        StageTask task = new StageTask(
                StageCommand.consumer(META, WINDOW, new CollectingSink(META)),
                StageTask.PortListener.NONE, () -> candidate);
        task.setProgressListener((in, out, running, first) -> { });

        StageTask.StageResult result = task.run();

        assertEquals(StageTask.StopReason.FAILED, result.reason());
        assertSame(scheduleFailure, result.failure());
        assertTrue(shutdown.get(), "unpublished progress executor must be rolled back");
        assertTrue(candidate.isShutdown());
    }

    @Test
    void publishedProgressShutdownFailureIsReturnedInsteadOfEscapingRun() {
        RuntimeException shutdownFailure = new RuntimeException("progress shutdown failed");
        ScheduledThreadPoolExecutor candidate = new ScheduledThreadPoolExecutor(1) {
            @Override
            public List<Runnable> shutdownNow() {
                super.shutdownNow();
                throw shutdownFailure;
            }
        };
        PlanGraph identity = new PlanGraph(Collections.singletonList(
                PlanNode.source(META, Collections.emptyList())));
        StageCommand command = StageCommand.sourceStage(
                META, META, WINDOW, input(Collections.emptyList()), identity,
                new ShuffleEndpoint("ignored", 1));
        StageTask task = new StageTask(command, StageTask.PortListener.NONE,
                () -> candidate, new StageTask.SetupFactory() {
                    @Override
                    public io.rowweave.sched.Channel endpoint(
                            ShuffleEndpoint endpoint, DataSchema meta,
                            java.util.function.Supplier<io.rowweave.sched.Channel> productionFactory) {
                        return probeChannel(() -> { });
                    }
                });
        task.setProgressListener((in, out, running, first) -> { });

        StageTask.StageResult result = assertDoesNotThrow(task::run);

        assertEquals(StageTask.StopReason.FAILED, result.reason());
        assertSame(shutdownFailure, result.failure());
        assertEquals(TelemetryCoverage.COMPLETE, result.telemetry().coverage());
        assertTrue(candidate.isShutdown());
    }

    @Test
    void publishedProgressShutdownFailureIsSuppressedUnderStagePrimary() {
        RuntimeException stageFailure = new RuntimeException("stage setup failed");
        RuntimeException shutdownFailure = new RuntimeException("progress shutdown failed");
        ScheduledThreadPoolExecutor candidate = new ScheduledThreadPoolExecutor(1) {
            @Override
            public List<Runnable> shutdownNow() {
                super.shutdownNow();
                throw shutdownFailure;
            }
        };
        PlanGraph identity = new PlanGraph(Collections.singletonList(
                PlanNode.source(META, Collections.emptyList())));
        StageCommand command = StageCommand.sourceStage(
                META, META, WINDOW, input(Collections.emptyList()), identity,
                new ShuffleEndpoint("ignored", 1));
        StageTask task = new StageTask(command, StageTask.PortListener.NONE,
                () -> candidate, new StageTask.SetupFactory() {
                    @Override
                    public io.rowweave.sched.Channel endpoint(
                            ShuffleEndpoint endpoint, DataSchema meta,
                            java.util.function.Supplier<io.rowweave.sched.Channel> productionFactory) {
                        throw stageFailure;
                    }
                });
        task.setProgressListener((in, out, running, first) -> { });

        StageTask.StageResult result = task.run();

        assertSame(stageFailure, result.failure());
        assertArrayEquals(new Throwable[]{shutdownFailure}, stageFailure.getSuppressed());
        assertEquals(TelemetryCoverage.COMPLETE, result.telemetry().coverage());
    }

    @Test
    void producerInputOpenFailureClosesAnAlreadyConnectedSink() throws Exception {
        RuntimeException inputFailure = new RuntimeException("input open failed");
        Source failingInput = new Source() {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() { throw inputFailure; }
        };
        ExecutorService peerExecutor = Executors.newSingleThreadExecutor();
        try (ServerSocket server = new ServerSocket(0)) {
            Future<Boolean> peerSawEof = peerExecutor.submit(() -> {
                try (Socket peer = server.accept()) {
                    peer.setSoTimeout(2_000);
                    try {
                        while (peer.getInputStream().read() != -1) { }
                        return true;
                    } catch (SocketTimeoutException leakedConnection) {
                        return false;
                    }
                }
            });
            StageCommand command = StageCommand.producer(
                    META, WINDOW, failingInput, "127.0.0.1", server.getLocalPort());

            StageTask.StageResult result =
                    new StageTask(command, StageTask.PortListener.NONE).run();

        assertEquals(StageTask.StopReason.FAILED, result.reason());
        assertSame(inputFailure, result.failure());
        assertEquals(TelemetryCoverage.COMPLETE, result.telemetry().coverage());
        assertEquals(0L, result.telemetry().managedAcquiredBytes());
            assertTrue(peerSawEof.get(5, TimeUnit.SECONDS),
                    "input setup failure must close the sink that connected first");
        } finally {
            peerExecutor.shutdownNow();
        }
    }

    @Test
    void secondJoinInboundSetupFailureClosesBothListeningSockets() {
        AtomicInteger leftPort = new AtomicInteger(-1);
        AtomicInteger rightPort = new AtomicInteger(-1);
        RuntimeException portFailure = new RuntimeException("second port report failed");
        StageCommand command = StageCommand.joinConsumer(
                META, META, META, WINDOW, 1, 1,
                new String[]{"k"}, new String[]{"k"}, "INNER",
                new io.rowweave.sched.net.ShuffleEndpoint("127.0.0.1", 1));
        StageTask task = new StageTask(command, (port, edge) -> {
            if (edge == 0) {
                leftPort.set(port);
            } else {
                rightPort.set(port);
                throw portFailure;
            }
        });

        StageTask.StageResult result = task.run();

        assertEquals(StageTask.StopReason.FAILED, result.reason());
        assertSame(portFailure, result.failure());
        assertTrue(leftPort.get() > 0 && rightPort.get() > 0);
        assertPortClosed(leftPort.get());
        assertPortClosed(rightPort.get());
    }

    @Test
    void legacyPortReportFailureReleasesPublishedLocalInboundReference() throws Exception {
        RuntimeException portFailure = new RuntimeException("port report failed");
        String localKey = "legacy-local-inbound";
        io.rowweave.sched.LocalShuffleRegistry registry =
                new io.rowweave.sched.LocalShuffleRegistry();
        try {
            PlanGraph identity = new PlanGraph(Collections.singletonList(
                    PlanNode.source(META, Collections.emptyList())));
            StageCommand command = StageCommand.keyedOp(
                            META, WINDOW, 2, identity,
                            new ShuffleEndpoint("127.0.0.1", 1))
                    .withLocalInbound(1, localKey);
            StageTask task = new StageTask(command, (port, edge) -> {
                throw portFailure;
            });
            task.setLocalShuffle(registry);

            StageTask.StageResult result = task.run();

            assertSame(portFailure, result.failure());
            assertEquals(0, activeLocalChannels(registry),
                    "failed setup must not retain the published inbound channel");
            assertThrows(RuntimeException.class, () -> registry.await(localKey, 1));
        } finally {
            registry.close();
        }
    }

    @Test
    void muxRegistrationFailureReleasesEarlierPublishedLocalInboundReference() throws Exception {
        String localKey = "mux-local-inbound";
        java.util.UUID targetId = java.util.UUID.randomUUID();
        io.rowweave.sched.LocalShuffleRegistry local =
                new io.rowweave.sched.LocalShuffleRegistry();
        io.rowweave.sched.net.DataInboundRegistry data =
                new io.rowweave.sched.net.DataInboundRegistry();
        data.close();
        try {
            PlanGraph identity = new PlanGraph(Collections.singletonList(
                    PlanNode.source(META, Collections.emptyList())));
            StageCommand command = StageCommand.keyedOp(
                            META, WINDOW, 2, identity,
                            new ShuffleEndpoint("127.0.0.1", 1))
                    .withLocalInbound(1, localKey)
                    .withInboundTargetId(targetId);
            StageTask task = new StageTask(command, StageTask.PortListener.NONE);
            task.setLocalShuffle(local);
            task.setDataInbound(data, null, "stage-test");

            StageTask.StageResult result = task.run();

            assertEquals(StageTask.StopReason.FAILED, result.reason());
            assertEquals(0, activeLocalChannels(local),
                    "second registry failure must roll back the first registry owner");
            assertThrows(RuntimeException.class, () -> local.await(localKey, 1));
        } finally {
            local.close();
        }
    }

    @Test
    void secondLegacyFanoutEndpointFailureClosesTheFirstConnection() throws Exception {
        int unavailablePort;
        try (ServerSocket reservation = new ServerSocket(0)) {
            unavailablePort = reservation.getLocalPort();
        }
        PlanGraph identity = new PlanGraph(Collections.singletonList(
                PlanNode.source(META, Collections.emptyList())));
        ExecutorService peerExecutor = Executors.newSingleThreadExecutor();
        try (ServerSocket firstEndpoint = new ServerSocket(0)) {
            Future<Boolean> peerSawEof = peerExecutor.submit(() -> {
                try (Socket peer = firstEndpoint.accept()) {
                    peer.setSoTimeout(2_000);
                    try {
                        while (peer.getInputStream().read() != -1) { }
                        return true;
                    } catch (SocketTimeoutException leakedConnection) {
                        return false;
                    }
                }
            });
            StageCommand command = StageCommand.sourceStageFanout(
                    META, META, WINDOW, input(Collections.emptyList()), identity,
                    Arrays.asList(
                            new ShuffleEndpoint("127.0.0.1", firstEndpoint.getLocalPort()),
                            new ShuffleEndpoint("127.0.0.1", unavailablePort)),
                    new String[]{"k"}, false);

            StageTask.StageResult result =
                    new StageTask(command, StageTask.PortListener.NONE).run();

            assertEquals(StageTask.StopReason.FAILED, result.reason());
            assertTrue(peerSawEof.get(5, TimeUnit.SECONDS),
                    "fan-out setup failure must close the endpoint opened first");
        } finally {
            peerExecutor.shutdownNow();
        }
    }

    @Test
    void keyedFanoutSetupFailureClosesItsInboundListener() throws Exception {
        int unavailablePort;
        try (ServerSocket reservation = new ServerSocket(0)) {
            unavailablePort = reservation.getLocalPort();
        }
        PlanGraph identity = new PlanGraph(Collections.singletonList(
                PlanNode.source(META, Collections.emptyList())));
        AtomicInteger inboundPort = new AtomicInteger(-1);
        StageCommand command = StageCommand.keyedOp(
                META, WINDOW, 1, identity,
                new ShuffleEndpoint("127.0.0.1", unavailablePort));

        StageTask.StageResult result = new StageTask(command,
                (port, edge) -> inboundPort.set(port)).run();

        assertEquals(StageTask.StopReason.FAILED, result.reason());
        assertTrue(inboundPort.get() > 0);
        assertPortClosed(inboundPort.get());
    }

    @Test
    void sinkManifestSetupFailureClosesItsInboundListener() {
        PlanGraph identity = new PlanGraph(Collections.singletonList(
                PlanNode.source(META, Collections.emptyList())));
        ManifestFileSink output = new ManifestFileSink(META,
                tempDir.resolve("manifest-domain").toString(), "rows");
        output.bindManifestAttempt("already-bound", 1L);
        StageCommand command = StageCommand.sink(META, WINDOW, 1, identity, output)
                .withManifestCommitContext("conflicting-attempt", 2L);
        AtomicInteger inboundPort = new AtomicInteger(-1);

        StageTask.StageResult result = new StageTask(command,
                (port, edge) -> inboundPort.set(port)).run();

        assertEquals(StageTask.StopReason.FAILED, result.reason());
        assertTrue(result.failure().getMessage().contains("already bound"));
        assertTrue(inboundPort.get() > 0);
        assertPortClosed(inboundPort.get());
        output.rollbackCommit();
    }

    @Test
    void sinkCancellationWaitsForInFlightCollectBeforeAbortingOutput()
            throws Exception {
        CountDownLatch collectEntered = new CountDownLatch(1);
        CountDownLatch releaseCollect = new CountDownLatch(1);
        CountDownLatch abortEntered = new CountDownLatch(1);
        Sink output = new Sink() {
            @Override public DataSchema getSchema() { return META; }
            @Override public void collect(DataRow row) {
                collectEntered.countDown();
                try {
                    releaseCollect.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
            }
            @Override public void close() { }
            @Override public void abort() { abortEntered.countDown(); }
        };
        PlanGraph identity = new PlanGraph(Collections.singletonList(
                PlanNode.source(META, Collections.emptyList())));
        String localKey = "sink-cancellation-collect";
        io.rowweave.sched.LocalShuffleRegistry registry =
                new io.rowweave.sched.LocalShuffleRegistry();
        StageTask sink = new StageTask(
                StageCommand.sink(META, WINDOW, 1, identity, output)
                        .withLocalInbound(1, localKey),
                StageTask.PortListener.NONE);
        sink.setLocalShuffle(registry);
        ExecutorService threads = Executors.newFixedThreadPool(2);
        io.rowweave.sched.LocalChannelSink producer = null;
        try {
            Future<StageTask.StageResult> running = threads.submit(sink::run);
            io.rowweave.sched.Channel shared = registry.await(localKey, 10_000);
            producer = new io.rowweave.sched.LocalChannelSink(
                    (io.rowweave.sched.MultiProducerChannel) shared);
            producer.send(io.rowweave.sched.stream.StreamEvent.record(row("race", 1)));
            assertTrue(collectEntered.await(10, TimeUnit.SECONDS));
            Future<?> stopping = threads.submit(
                    () -> sink.stop(StageTask.StopReason.CANCELLED));
            assertFalse(abortEntered.await(200, TimeUnit.MILLISECONDS),
                    "abort must not run concurrently with collect");
            releaseCollect.countDown();
            stopping.get(10, TimeUnit.SECONDS);
            assertTrue(abortEntered.await(10, TimeUnit.SECONDS));
            assertEquals(StageTask.StopReason.CANCELLED,
                    running.get(10, TimeUnit.SECONDS).reason());
        } finally {
            releaseCollect.countDown();
            if (producer != null) {
                producer.close();
            }
            registry.close();
            threads.shutdownNow();
        }
    }

    @Test
    void joinMemorySetupFailureClosesBothInboundListenersAndKeepsPrimary() {
        RuntimeException memoryFailure = new RuntimeException("join memory setup failed");
        AtomicInteger leftPort = new AtomicInteger(-1);
        AtomicInteger rightPort = new AtomicInteger(-1);
        StageCommand command = StageCommand.joinConsumer(
                META, META, META, WINDOW, 1, 1,
                new String[]{"k"}, new String[]{"k"}, "INNER",
                new ShuffleEndpoint("127.0.0.1", 1));
        StageTask task = new StageTask(command, (port, edge) -> {
            if (edge == 0) leftPort.set(port); else rightPort.set(port);
        }, new StageTask.SetupFactory() {
            @Override
            public io.rowweave.mem.ExecutionMemory executionMemory(
                    boolean workspaceNeeded,
                    io.rowweave.mem.MemoryOwnerRequirement... requirements) {
                assertTrue(workspaceNeeded);
                assertEquals(1, requirements.length);
                assertEquals(2, requirements[0].childArity(),
                        "binary JOIN must be admitted as one two-leaf owner");
                throw memoryFailure;
            }
        });

        StageTask.StageResult result = task.run();

        assertEquals(StageTask.StopReason.FAILED, result.reason());
        assertSame(memoryFailure, result.failure());
        assertEquals(TelemetryCoverage.COMPLETE, result.telemetry().coverage());
        assertPortClosed(leftPort.get());
        assertPortClosed(rightPort.get());
    }

    @Test
    void commandNotStartedHasExplicitNotExecutedTelemetry() {
        AtomicInteger abortCalls = new AtomicInteger();
        AtomicInteger inputOpenCalls = new AtomicInteger();
        AtomicInteger portCalls = new AtomicInteger();
        Source unopened = new Source() {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() {
                inputOpenCalls.incrementAndGet();
                return Collections.emptyIterator();
            }
        };
        Sink output = new Sink() {
            @Override public DataSchema getSchema() { return META; }
            @Override public void collect(DataRow row) { }
            @Override public void close() { }
            @Override public void abort() { abortCalls.incrementAndGet(); }
        };
        PlanGraph identity = new PlanGraph(Collections.singletonList(
                PlanNode.source(META, Collections.emptyList())));
        List<StageCommand> commands = Arrays.asList(
                StageCommand.consumer(META, WINDOW, output),
                StageCommand.producer(META, WINDOW, unopened, "ignored", 1),
                StageCommand.router(META, WINDOW, unopened, new String[]{"k"},
                        Collections.singletonList(new ShuffleEndpoint("ignored", 1))),
                StageCommand.keyedOp(META, WINDOW, 1, identity,
                        new ShuffleEndpoint("ignored", 1)),
                StageCommand.sink(META, WINDOW, 1, identity, output),
                StageCommand.wholeGraph(identity),
                StageCommand.joinConsumer(META, META, META, WINDOW, 1, 1,
                        new String[]{"k"}, new String[]{"k"}, "INNER",
                        new ShuffleEndpoint("ignored", 1)),
                StageCommand.sourceStage(META, META, WINDOW, unopened, identity,
                        new ShuffleEndpoint("ignored", 1)));

        for (StageCommand command : commands) {
            StageTask task = new StageTask(command,
                    (port, edge) -> portCalls.incrementAndGet());
            task.stop(StageTask.StopReason.CANCELLED);

            StageTask.StageResult result = task.run();

            assertEquals(StageTask.StopReason.CANCELLED, result.reason(), command.role().name());
            assertEquals(TelemetryCoverage.NOT_EXECUTED,
                    result.telemetry().coverage(), command.role().name());
        }
        assertEquals(0, abortCalls.get(), "queued cancellation must not enter the role body");
        assertEquals(0, inputOpenCalls.get(), "queued cancellation must not open Source");
        assertEquals(0, portCalls.get(), "queued cancellation must not publish a port");
    }

    @Test
    void sourceStageReturnsItsManagedBreakerTelemetry() {
        io.rowweave.ops.ParallelGroupByAgg aggregate =
                io.rowweave.ops.ParallelGroupByAgg.of(META, "k").count();
        DataSchema grouped = aggregate.getOutputSchema();
        PlanGraph fragment = new PlanGraph(Arrays.asList(
                PlanNode.source(META, Collections.emptyList()),
                PlanNode.unary("groupByCount", grouped, 0)
                        .listParam("keys", Collections.singletonList("k"))
                        .build()));
        StageCommand command = StageCommand.sourceStage(
                META, grouped, WINDOW,
                input(Arrays.asList(row("a", 1), row("a", 2), row("b", 3))),
                fragment, new ShuffleEndpoint("ignored", 1));
        StageTask task = new StageTask(command, StageTask.PortListener.NONE,
                new StageTask.SetupFactory() {
                    @Override
                    public io.rowweave.sched.Channel endpoint(
                            ShuffleEndpoint endpoint, DataSchema meta,
                            java.util.function.Supplier<io.rowweave.sched.Channel> productionFactory) {
                        return probeChannel(() -> { });
                    }
                });

        StageTask.StageResult result = task.run();

        assertEquals(StageTask.StopReason.EOS, result.reason());
        assertEquals(2L, result.rows());
        assertEquals(TelemetryCoverage.COMPLETE, result.telemetry().coverage());
        assertTrue(result.telemetry().managedAcquiredBytes() > 0);
        assertTrue(result.telemetry().managedPeakReservedBytes() > 0);
    }

    @Test
    void blockingBufferSetupFailureClosesMemoryAndEveryEndpoint() throws Exception {
        RuntimeException bufferFailure = new RuntimeException("blocking buffer setup failed");
        AtomicReference<io.rowweave.mem.ExecutionMemory> capturedMemory = new AtomicReference<>();
        PlanGraph identity = new PlanGraph(Collections.singletonList(
                PlanNode.source(META, Collections.emptyList())));
        ExecutorService peers = Executors.newFixedThreadPool(2);
        try (ServerSocket first = new ServerSocket(0);
             ServerSocket second = new ServerSocket(0)) {
            Future<Boolean> firstEof = peers.submit(() -> readUntilEof(first));
            Future<Boolean> secondEof = peers.submit(() -> readUntilEof(second));
            ShuffleTarget firstTarget = ShuffleTarget.socket(
                    new ShuffleEndpoint("127.0.0.1", first.getLocalPort()));
            ShuffleTarget secondTarget = ShuffleTarget.socket(
                    new ShuffleEndpoint("127.0.0.1", second.getLocalPort()));
            StageCommand command = StageCommand.sourceStage(
                            META, META, WINDOW, input(Collections.emptyList()), identity,
                            new ShuffleEndpoint("127.0.0.1", first.getLocalPort()))
                    .withBroadcastGroups(Arrays.asList(
                            new StageCommand.BroadcastGroup(
                                    Collections.singletonList(firstTarget), new String[]{"k"}, false, true),
                            new StageCommand.BroadcastGroup(
                                    Collections.singletonList(secondTarget), new String[]{"k"}, false, false)));
            StageTask task = new StageTask(command, StageTask.PortListener.NONE,
                    new StageTask.SetupFactory() {
                        @Override
                        public io.rowweave.mem.ExecutionMemory executionMemory(
                                boolean workspaceNeeded,
                                io.rowweave.mem.MemoryOwnerRequirement... requirements) {
                            assertTrue(workspaceNeeded);
                            assertEquals(0, requirements.length,
                                    "blocking fanout is disk-only and must not consume an owner guarantee");
                            io.rowweave.mem.ExecutionMemory memory =
                                    io.rowweave.mem.ExecutionMemory.forJob(
                                            workspaceNeeded, requirements);
                            capturedMemory.set(memory);
                            assertTrue(memory.spillRootDir().isDirectory());
                            return memory;
                        }

                        @Override
                        public io.rowweave.sched.MaterializedRowBuffer materializedBuffer(
                                DataSchema meta, io.rowweave.mem.spill.SpillStore spillStore) {
                            throw bufferFailure;
                        }
                    });

            StageTask.StageResult result = task.run();

            assertEquals(StageTask.StopReason.FAILED, result.reason());
            assertSame(bufferFailure, result.failure());
            assertNotNull(capturedMemory.get());
            assertFalse(capturedMemory.get().spillRootDir().exists(),
                    "failed blocking setup must delete its spill workspace");
            assertTrue(firstEof.get(5, TimeUnit.SECONDS));
            assertTrue(secondEof.get(5, TimeUnit.SECONDS));
        } finally {
            peers.shutdownNow();
        }
    }

    @Test
    void blockingFanoutFreeGroupEosDoesNotDisableCancellationOfParkedGroup() throws Exception {
        CountDownLatch parkedSend = new CountDownLatch(1);
        CountDownLatch releaseParked = new CountDownLatch(1);
        CountDownLatch freeEos = new CountDownLatch(1);
        AtomicInteger parkedFails = new AtomicInteger();
        AtomicInteger freeFails = new AtomicInteger();
        io.rowweave.sched.Channel parked = new io.rowweave.sched.Channel() {
            @Override public void send(io.rowweave.sched.stream.StreamEvent event) {
                parkedSend.countDown();
                try {
                    releaseParked.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("parked send interrupted", interrupted);
                }
            }
            @Override public io.rowweave.sched.stream.StreamEvent poll() { return null; }
            @Override public void close() { }
            @Override public void fail(Throwable cause) {
                parkedFails.incrementAndGet();
                releaseParked.countDown();
            }
        };
        io.rowweave.sched.Channel free = new io.rowweave.sched.Channel() {
            @Override public void send(io.rowweave.sched.stream.StreamEvent event) { }
            @Override public io.rowweave.sched.stream.StreamEvent poll() { return null; }
            @Override public void close() { freeEos.countDown(); }
            @Override public void fail(Throwable cause) { freeFails.incrementAndGet(); }
        };
        PlanGraph identity = new PlanGraph(Collections.singletonList(
                PlanNode.source(META, Collections.emptyList())));
        StageCommand command = StageCommand.sourceStage(
                        META, META, WINDOW, input(Collections.singletonList(row("a", 1))),
                        identity, new ShuffleEndpoint("ignored", 1))
                .withBroadcastGroups(Arrays.asList(
                        new StageCommand.BroadcastGroup(
                                Collections.singletonList(ShuffleTarget.socket(
                                        new ShuffleEndpoint("ignored", 1))),
                                new String[]{"k"}, false, true),
                        new StageCommand.BroadcastGroup(
                                Collections.singletonList(ShuffleTarget.socket(
                                        new ShuffleEndpoint("ignored", 2))),
                                new String[]{"k"}, false, false)));
        StageTask task = new StageTask(command, StageTask.PortListener.NONE,
                new StageTask.SetupFactory() {
                    @Override
                    public io.rowweave.sched.Channel endpoint(
                            ShuffleEndpoint endpoint, DataSchema meta,
                            java.util.function.Supplier<io.rowweave.sched.Channel> productionFactory) {
                        return endpoint.port() == 1 ? parked : free;
                    }
                });
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<StageTask.StageResult> running = executor.submit(task::run);
            assertTrue(parkedSend.await(10, TimeUnit.SECONDS));
            assertTrue(freeEos.await(10, TimeUnit.SECONDS),
                    "independent free replay group must still publish its EOS");

            task.stop(StageTask.StopReason.CANCELLED);
            StageTask.StageResult result = running.get(2, TimeUnit.SECONDS);

            assertEquals(StageTask.StopReason.CANCELLED, result.reason());
            assertEquals(1, parkedFails.get());
            assertEquals(0, freeFails.get(),
                    "the already-closed free group owner must be retired before cancellation");
        } finally {
            releaseParked.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void blockingFanoutCancellationWinsBeforeNonFinalGroupEosPermit() throws Exception {
        CountDownLatch victimSendStarted = new CountDownLatch(1);
        CountDownLatch holdSendStarted = new CountDownLatch(1);
        CountDownLatch releaseVictimSend = new CountDownLatch(1);
        CountDownLatch releaseHoldSend = new CountDownLatch(1);
        CountDownLatch stopCleanupEntered = new CountDownLatch(1);
        CountDownLatch releaseStopCleanup = new CountDownLatch(1);
        CountDownLatch victimFailed = new CountDownLatch(1);
        AtomicInteger victimCloses = new AtomicInteger();
        AtomicInteger victimFails = new AtomicInteger();
        AtomicInteger holdCloses = new AtomicInteger();
        AtomicInteger holdFails = new AtomicInteger();
        io.rowweave.sched.Channel victim = new io.rowweave.sched.Channel() {
            @Override public void send(io.rowweave.sched.stream.StreamEvent event) {
                victimSendStarted.countDown();
                try {
                    releaseVictimSend.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("victim send interrupted", interrupted);
                }
            }
            @Override public io.rowweave.sched.stream.StreamEvent poll() { return null; }
            @Override public void close() { victimCloses.incrementAndGet(); }
            @Override public void fail(Throwable cause) {
                victimFails.incrementAndGet();
                victimFailed.countDown();
                releaseVictimSend.countDown();
            }
        };
        io.rowweave.sched.Channel hold = new io.rowweave.sched.Channel() {
            @Override public void send(io.rowweave.sched.stream.StreamEvent event) {
                holdSendStarted.countDown();
                try {
                    releaseHoldSend.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("hold send interrupted", interrupted);
                }
            }
            @Override public io.rowweave.sched.stream.StreamEvent poll() { return null; }
            @Override public void close() { holdCloses.incrementAndGet(); }
            @Override public void fail(Throwable cause) {
                holdFails.incrementAndGet();
                stopCleanupEntered.countDown();
                try {
                    releaseStopCleanup.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("stop cleanup interrupted", interrupted);
                } finally {
                    releaseHoldSend.countDown();
                }
            }
        };
        PlanGraph identity = new PlanGraph(Collections.singletonList(
                PlanNode.source(META, Collections.emptyList())));
        StageCommand command = StageCommand.sourceStage(
                        META, META, WINDOW, input(Collections.singletonList(row("a", 1))),
                        identity, new ShuffleEndpoint("ignored", 1))
                .withBroadcastGroups(Arrays.asList(
                        new StageCommand.BroadcastGroup(
                                Collections.singletonList(ShuffleTarget.socket(
                                        new ShuffleEndpoint("ignored", 1))),
                                new String[]{"k"}, false, true),
                        new StageCommand.BroadcastGroup(
                                Collections.singletonList(ShuffleTarget.socket(
                                        new ShuffleEndpoint("ignored", 2))),
                                new String[]{"k"}, false, false)));
        StageTask task = new StageTask(command, StageTask.PortListener.NONE,
                new StageTask.SetupFactory() {
                    @Override
                    public io.rowweave.sched.Channel endpoint(
                            ShuffleEndpoint endpoint, DataSchema meta,
                            java.util.function.Supplier<io.rowweave.sched.Channel> productionFactory) {
                        return endpoint.port() == 1 ? victim : hold;
                    }
                });
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<StageTask.StageResult> running = executor.submit(task::run);
            assertTrue(victimSendStarted.await(10, TimeUnit.SECONDS));
            assertTrue(holdSendStarted.await(10, TimeUnit.SECONDS));

            Future<?> stopping = executor.submit(
                    () -> task.stop(StageTask.StopReason.CANCELLED));
            assertTrue(stopCleanupEntered.await(10, TimeUnit.SECONDS),
                    "stop must mark CANCELLED before its newest owner cleanup blocks");

            releaseVictimSend.countDown();
            assertTrue(victimFailed.await(10, TimeUnit.SECONDS),
                    "the non-final replay must reject EOS and enter failure cleanup");
            assertEquals(0, victimCloses.get(),
                    "cancel won the shared permit lock, so a late non-final EOS is forbidden");

            releaseStopCleanup.countDown();
            stopping.get(10, TimeUnit.SECONDS);
            StageTask.StageResult result = running.get(10, TimeUnit.SECONDS);

            assertEquals(StageTask.StopReason.CANCELLED, result.reason());
            assertEquals(0, victimCloses.get());
            assertEquals(0, holdCloses.get());
            assertEquals(1, victimFails.get());
            assertEquals(1, holdFails.get());
        } finally {
            releaseVictimSend.countDown();
            releaseStopCleanup.countDown();
            releaseHoldSend.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void blockingFanoutPermitWinnerRemainsCancellableDuringBlockingClose() throws Exception {
        CountDownLatch closeStarted = new CountDownLatch(1);
        CountDownLatch releaseClose = new CountDownLatch(1);
        CountDownLatch holdSendStarted = new CountDownLatch(1);
        CountDownLatch releaseHoldSend = new CountDownLatch(1);
        AtomicInteger closingCloses = new AtomicInteger();
        AtomicInteger closingFails = new AtomicInteger();
        AtomicInteger holdCloses = new AtomicInteger();
        AtomicInteger holdFails = new AtomicInteger();
        io.rowweave.sched.Channel closing = new io.rowweave.sched.Channel() {
            @Override public void send(io.rowweave.sched.stream.StreamEvent event) { }
            @Override public io.rowweave.sched.stream.StreamEvent poll() { return null; }
            @Override public void close() {
                closingCloses.incrementAndGet();
                closeStarted.countDown();
                try {
                    releaseClose.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("graceful close interrupted", interrupted);
                }
            }
            @Override public void fail(Throwable cause) {
                closingFails.incrementAndGet();
                releaseClose.countDown();
            }
        };
        io.rowweave.sched.Channel hold = new io.rowweave.sched.Channel() {
            @Override public void send(io.rowweave.sched.stream.StreamEvent event) {
                holdSendStarted.countDown();
                try {
                    releaseHoldSend.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("hold send interrupted", interrupted);
                }
            }
            @Override public io.rowweave.sched.stream.StreamEvent poll() { return null; }
            @Override public void close() { holdCloses.incrementAndGet(); }
            @Override public void fail(Throwable cause) {
                holdFails.incrementAndGet();
                releaseHoldSend.countDown();
            }
        };
        PlanGraph identity = new PlanGraph(Collections.singletonList(
                PlanNode.source(META, Collections.emptyList())));
        StageCommand command = StageCommand.sourceStage(
                        META, META, WINDOW, input(Collections.singletonList(row("a", 1))),
                        identity, new ShuffleEndpoint("ignored", 1))
                .withBroadcastGroups(Arrays.asList(
                        new StageCommand.BroadcastGroup(
                                Collections.singletonList(ShuffleTarget.socket(
                                        new ShuffleEndpoint("ignored", 1))),
                                new String[]{"k"}, false, true),
                        new StageCommand.BroadcastGroup(
                                Collections.singletonList(ShuffleTarget.socket(
                                        new ShuffleEndpoint("ignored", 2))),
                                new String[]{"k"}, false, false)));
        StageTask task = new StageTask(command, StageTask.PortListener.NONE,
                new StageTask.SetupFactory() {
                    @Override
                    public io.rowweave.sched.Channel endpoint(
                            ShuffleEndpoint endpoint, DataSchema meta,
                            java.util.function.Supplier<io.rowweave.sched.Channel> productionFactory) {
                        return endpoint.port() == 1 ? closing : hold;
                    }
                });
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<StageTask.StageResult> running = executor.submit(task::run);
            assertTrue(holdSendStarted.await(10, TimeUnit.SECONDS));
            assertTrue(closeStarted.await(10, TimeUnit.SECONDS),
                    "non-final group must enter its graceful close after winning the permit");

            task.stop(StageTask.StopReason.CANCELLED);
            StageTask.StageResult result = running.get(10, TimeUnit.SECONDS);

            assertEquals(StageTask.StopReason.CANCELLED, result.reason());
            assertEquals(1, closingCloses.get());
            assertEquals(1, closingFails.get(),
                    "owner stays cancellable until graceful close actually returns");
            assertEquals(0, holdCloses.get());
            assertEquals(1, holdFails.get());
        } finally {
            releaseClose.countDown();
            releaseHoldSend.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void fanoutRollbackIsLifoAndCleanupFailuresStaySuppressed() {
        RuntimeException setupFailure = new RuntimeException("third endpoint failed");
        RuntimeException firstCleanup = new RuntimeException("first cleanup failed");
        RuntimeException secondCleanup = new RuntimeException("second cleanup failed");
        AtomicInteger opens = new AtomicInteger();
        List<String> rollbackOrder = new ArrayList<>();
        PlanGraph identity = new PlanGraph(Collections.singletonList(
                PlanNode.source(META, Collections.emptyList())));
        StageCommand command = StageCommand.sourceStageFanout(
                META, META, WINDOW, input(Collections.emptyList()), identity,
                Arrays.asList(
                        new ShuffleEndpoint("ignored", 1),
                        new ShuffleEndpoint("ignored", 2),
                        new ShuffleEndpoint("ignored", 3)),
                new String[]{"k"}, false);
        StageTask task = new StageTask(command, StageTask.PortListener.NONE,
                new StageTask.SetupFactory() {
                    @Override
                    public io.rowweave.sched.Channel endpoint(
                            ShuffleEndpoint endpoint, DataSchema meta,
                            java.util.function.Supplier<io.rowweave.sched.Channel> productionFactory) {
                        int index = opens.incrementAndGet();
                        if (index == 3) throw setupFailure;
                        String name = index == 1 ? "first" : "second";
                        RuntimeException cleanup = index == 1 ? firstCleanup : secondCleanup;
                        return new io.rowweave.sched.Channel() {
                            @Override public void send(io.rowweave.sched.stream.StreamEvent event) { }
                            @Override public io.rowweave.sched.stream.StreamEvent poll() { return null; }
                            @Override public void close() { }
                            @Override public void fail(Throwable cause) {
                                rollbackOrder.add(name);
                                throw cleanup;
                            }
                        };
                    }
                });

        StageTask.StageResult result = task.run();

        assertEquals(StageTask.StopReason.FAILED, result.reason());
        assertSame(setupFailure, result.failure());
        assertEquals(Arrays.asList("second", "first"), rollbackOrder);
        assertArrayEquals(new Throwable[]{secondCleanup, firstCleanup},
                setupFailure.getSuppressed());
    }

    @Test
    void targetFanoutEndpointFailureRollsBackOpenedTargets() {
        RuntimeException setupFailure = new RuntimeException("target endpoint failed");
        AtomicInteger opens = new AtomicInteger();
        List<Integer> rolledBack = new ArrayList<>();
        PlanGraph identity = new PlanGraph(Collections.singletonList(
                PlanNode.source(META, Collections.emptyList())));
        StageCommand command = StageCommand.sourceStageFanout(
                        META, META, WINDOW, input(Collections.emptyList()), identity,
                        Collections.emptyList(), new String[]{"k"}, false)
                .withDownTargets(Arrays.asList(
                        ShuffleTarget.socket(new ShuffleEndpoint("ignored", 1)),
                        ShuffleTarget.socket(new ShuffleEndpoint("ignored", 2)),
                        ShuffleTarget.socket(new ShuffleEndpoint("ignored", 3))));
        StageTask task = new StageTask(command, StageTask.PortListener.NONE,
                new StageTask.SetupFactory() {
                    @Override
                    public io.rowweave.sched.Channel endpoint(
                            ShuffleEndpoint endpoint, DataSchema meta,
                            java.util.function.Supplier<io.rowweave.sched.Channel> productionFactory) {
                        int index = opens.incrementAndGet();
                        if (index == 3) throw setupFailure;
                        return probeChannel(() -> rolledBack.add(index));
                    }
                });

        StageTask.StageResult result = task.run();

        assertSame(setupFailure, result.failure());
        assertEquals(Arrays.asList(2, 1), rolledBack);
    }

    @Test
    void partitionerSetupFailureRollsBackEveryOpenedEndpoint() {
        RuntimeException partitionerFailure = new RuntimeException("partitioner setup failed");
        AtomicInteger opens = new AtomicInteger();
        List<Integer> rolledBack = new ArrayList<>();
        PlanGraph identity = new PlanGraph(Collections.singletonList(
                PlanNode.source(META, Collections.emptyList())));
        StageCommand command = StageCommand.sourceStageFanout(
                META, META, WINDOW, input(Collections.emptyList()), identity,
                Arrays.asList(
                        new ShuffleEndpoint("ignored", 1),
                        new ShuffleEndpoint("ignored", 2)),
                new String[]{"k"}, false);
        StageTask task = new StageTask(command, StageTask.PortListener.NONE,
                new StageTask.SetupFactory() {
                    @Override
                    public io.rowweave.sched.Channel endpoint(
                            ShuffleEndpoint endpoint, DataSchema meta,
                            java.util.function.Supplier<io.rowweave.sched.Channel> productionFactory) {
                        int index = opens.incrementAndGet();
                        return probeChannel(() -> rolledBack.add(index));
                    }

                    @Override
                    public io.rowweave.ops.Partitioner partitioner(
                            DataSchema meta, int partitions, String[] keys, boolean normalized) {
                        throw partitionerFailure;
                    }
                });

        StageTask.StageResult result = task.run();

        assertSame(partitionerFailure, result.failure());
        assertEquals(Arrays.asList(2, 1), rolledBack);
    }

    @Test
    void sourceBodyFailureFailsEveryFanoutChannelInLifoOrderAndKeepsPrimary() {
        RuntimeException bodyFailure = new RuntimeException("source read failed");
        RuntimeException secondCleanupFailure = new RuntimeException("second channel fail failed");
        Source failingDuringRead = new Source() {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() {
                return new Iterator<DataRow>() {
                    @Override public boolean hasNext() { throw bodyFailure; }
                    @Override public DataRow next() { throw new AssertionError("next must not be called"); }
                };
            }
        };
        List<String> cleanupOrder = new ArrayList<>();
        AtomicInteger opens = new AtomicInteger();
        PlanGraph identity = new PlanGraph(Collections.singletonList(
                PlanNode.source(META, Collections.emptyList())));
        StageCommand command = StageCommand.sourceStageFanout(
                META, META, WINDOW, failingDuringRead, identity,
                Arrays.asList(
                        new ShuffleEndpoint("ignored", 1),
                        new ShuffleEndpoint("ignored", 2)),
                new String[]{"k"}, false);
        StageTask task = new StageTask(command, StageTask.PortListener.NONE,
                new StageTask.SetupFactory() {
                    @Override
                    public io.rowweave.sched.Channel endpoint(
                            ShuffleEndpoint endpoint, DataSchema meta,
                            java.util.function.Supplier<io.rowweave.sched.Channel> productionFactory) {
                        int index = opens.incrementAndGet();
                        return new io.rowweave.sched.Channel() {
                            @Override public void send(io.rowweave.sched.stream.StreamEvent event) { }
                            @Override public io.rowweave.sched.stream.StreamEvent poll() { return null; }
                            @Override public void close() { }
                            @Override public void fail(Throwable cause) {
                                cleanupOrder.add(index == 1 ? "first" : "second");
                                if (index == 2) throw secondCleanupFailure;
                            }
                        };
                    }
                });

        StageTask.StageResult result = task.run();

        assertEquals(StageTask.StopReason.FAILED, result.reason());
        assertSame(bodyFailure, result.failure().getCause());
        assertEquals(Arrays.asList("second", "first"), cleanupOrder);
        assertArrayEquals(new Throwable[]{secondCleanupFailure}, result.failure().getSuppressed());
    }

    @Test
    void routerEndpointSetupFailureRollsBackEveryChannelInLifoOrderAndKeepsPrimary() {
        RuntimeException setupFailure = new RuntimeException("third router endpoint failed");
        RuntimeException firstCleanupFailure = new RuntimeException("first router cleanup failed");
        AtomicInteger opens = new AtomicInteger();
        List<String> cleanupOrder = new ArrayList<>();
        StageCommand command = StageCommand.router(
                META, WINDOW, input(Collections.emptyList()), new String[]{"k"},
                Arrays.asList(
                        new ShuffleEndpoint("ignored", 1),
                        new ShuffleEndpoint("ignored", 2),
                        new ShuffleEndpoint("ignored", 3)));
        StageTask task = new StageTask(command, StageTask.PortListener.NONE,
                new StageTask.SetupFactory() {
                    @Override
                    public io.rowweave.sched.Channel endpoint(
                            ShuffleEndpoint endpoint, DataSchema meta,
                            java.util.function.Supplier<io.rowweave.sched.Channel> productionFactory) {
                        int index = opens.incrementAndGet();
                        if (index == 3) throw setupFailure;
                        return new io.rowweave.sched.Channel() {
                            @Override public void send(io.rowweave.sched.stream.StreamEvent event) { }
                            @Override public io.rowweave.sched.stream.StreamEvent poll() { return null; }
                            @Override public void close() { }
                            @Override public void fail(Throwable cause) {
                                cleanupOrder.add(index == 1 ? "first" : "second");
                                if (index == 1) throw firstCleanupFailure;
                            }
                        };
                    }
                });

        StageTask.StageResult result = task.run();

        assertEquals(StageTask.StopReason.FAILED, result.reason());
        assertSame(setupFailure, result.failure());
        assertEquals(Arrays.asList("second", "first"), cleanupOrder);
        assertArrayEquals(new Throwable[]{firstCleanupFailure}, setupFailure.getSuppressed());
    }

    @Test
    void routerBodyFailureKeepsPrimaryWhenInputCloseAlsoFails() {
        RuntimeException sendFailure = new RuntimeException("router send failed");
        RuntimeException inputCloseFailure = new RuntimeException("input close failed");
        class FailingCloseInput implements Source, AutoCloseable {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() {
                return Collections.singletonList(row("a", 1)).iterator();
            }
            @Override public void close() { throw inputCloseFailure; }
        }
        StageCommand command = StageCommand.router(
                META, WINDOW, new FailingCloseInput(), new String[]{"k"},
                Collections.singletonList(new ShuffleEndpoint("ignored", 1)));
        StageTask task = new StageTask(command, StageTask.PortListener.NONE,
                new StageTask.SetupFactory() {
                    @Override
                    public io.rowweave.sched.Channel endpoint(
                            ShuffleEndpoint endpoint, DataSchema meta,
                            java.util.function.Supplier<io.rowweave.sched.Channel> productionFactory) {
                        return new io.rowweave.sched.Channel() {
                            @Override public void send(io.rowweave.sched.stream.StreamEvent event) {
                                throw sendFailure;
                            }
                            @Override public io.rowweave.sched.stream.StreamEvent poll() { return null; }
                            @Override public void close() { }
                            @Override public void fail(Throwable cause) { }
                        };
                    }
                });

        StageTask.StageResult result = task.run();

        assertEquals(StageTask.StopReason.FAILED, result.reason());
        assertSame(sendFailure, result.failure());
        assertEquals(1, sendFailure.getSuppressed().length);
        assertSame(inputCloseFailure, sendFailure.getSuppressed()[0].getCause());
    }

    private static io.rowweave.sched.Channel probeChannel(Runnable failAction) {
        return new io.rowweave.sched.Channel() {
            @Override public void send(io.rowweave.sched.stream.StreamEvent event) { }
            @Override public io.rowweave.sched.stream.StreamEvent poll() { return null; }
            @Override public void close() { }
            @Override public void fail(Throwable cause) { failAction.run(); }
        };
    }

    private static boolean readUntilEof(ServerSocket server) throws IOException {
        try (Socket peer = server.accept()) {
            peer.setSoTimeout(2_000);
            try {
                while (peer.getInputStream().read() != -1) { }
                return true;
            } catch (SocketTimeoutException leakedConnection) {
                return false;
            }
        }
    }

    private static int activeLocalChannels(io.rowweave.sched.LocalShuffleRegistry registry)
            throws Exception {
        Method method = io.rowweave.sched.LocalShuffleRegistry.class
                .getDeclaredMethod("activeChannelCount");
        method.setAccessible(true);
        return (Integer) method.invoke(registry);
    }

    private static void assertPortClosed(int port) {
        try (Socket probe = new Socket()) {
            probe.connect(new InetSocketAddress("127.0.0.1", port), 300);
            fail("listening socket leaked on port " + port);
        } catch (IOException expected) {
            // Connection refusal proves that the failed setup no longer owns the listener.
        }
    }

    @Test
    void cooperativeCancelUnblocksParkedStage() throws Exception {
        CollectingSink out = new CollectingSink(META);
        StageCommand consumerCmd = StageCommand.consumer(META, WINDOW, out);
        CountDownLatch portReady = new CountDownLatch(1);
        StageTask consumer = new StageTask(consumerCmd, (port, edge) -> portReady.countDown());

        StageTask.StageResult[] result = new StageTask.StageResult[1];
        Thread ct = new Thread(() -> result[0] = consumer.run(), "consumer-park");
        ct.start();
        assertTrue(portReady.await(10, TimeUnit.SECONDS), "the consumer binds its port back");

        consumer.stop(StageTask.StopReason.CANCELLED);
        ct.join(10_000);
        assertFalse(ct.isAlive(), "after cancellation the stage thread exits promptly (no hang)");
        assertEquals(StageTask.StopReason.CANCELLED, result[0].reason(), "stop reason = CANCELLED");
        assertEquals(TelemetryCoverage.COMPLETE, result[0].telemetry().coverage());
        assertEquals(0L, result[0].telemetry().managedAcquiredBytes());
    }

    @Test
    void consumerCancelAttemptsOutputAbortExactlyOnceWhenAbortThrows() throws Exception {
        AtomicInteger abortCalls = new AtomicInteger();
        RuntimeException abortFailure = new RuntimeException("abort failed after side effect");
        AtomicReference<StageTask> taskRef = new AtomicReference<>();
        Sink out = new Sink() {
            @Override public DataSchema getSchema() { return META; }
            @Override public void collect(DataRow row) {
                taskRef.get().stop(StageTask.StopReason.CANCELLED);
            }
            @Override public void close() { }
            @Override public void abort() {
                abortCalls.incrementAndGet();
                throw abortFailure;
            }
        };
        CountDownLatch portReady = new CountDownLatch(1);
        AtomicInteger port = new AtomicInteger(-1);
        StageTask consumer = new StageTask(
                StageCommand.consumer(META, WINDOW, out),
                (boundPort, edge) -> {
                    port.set(boundPort);
                    portReady.countDown();
                });
        taskRef.set(consumer);

        StageTask.StageResult[] result = new StageTask.StageResult[1];
        Thread thread = new Thread(() -> result[0] = consumer.run(), "consumer-abort-once");
        thread.start();
        assertTrue(portReady.await(10, TimeUnit.SECONDS), "consumer bind must complete");

        StageTask producer = new StageTask(
                StageCommand.producer(META, WINDOW,
                        input(Collections.singletonList(row("cancel", 1))),
                        "127.0.0.1", port.get()),
                StageTask.PortListener.NONE);
        producer.run();
        thread.join(10_000);

        assertFalse(thread.isAlive(), "cancel must unblock the consumer");
        assertEquals(1, abortCalls.get(), "abort side effect must never be repeated");
        assertEquals(StageTask.StopReason.CANCELLED, result[0].reason());
        assertSame(abortFailure, result[0].failure());
    }

    @Test
    void runningCancellationKeepsLifoCleanupFailuresObservable() {
        RuntimeException channelFailure = new RuntimeException("channel fail cleanup");
        RuntimeException inputCloseFailure = new RuntimeException("input close cleanup");
        List<String> cleanupOrder = new ArrayList<>();
        AtomicReference<StageTask> taskRef = new AtomicReference<>();
        class FailingCloseInput implements Source, AutoCloseable {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() {
                return new Iterator<DataRow>() {
                    @Override public boolean hasNext() { return true; }
                    @Override public DataRow next() { return row("cancel", 1); }
                };
            }
            @Override public void close() {
                cleanupOrder.add("input");
                throw inputCloseFailure;
            }
        }
        PlanGraph identity = new PlanGraph(Collections.singletonList(
                PlanNode.source(META, Collections.emptyList())));
        StageCommand command = StageCommand.sourceStage(
                META, META, WINDOW, new FailingCloseInput(), identity,
                new ShuffleEndpoint("ignored", 1));
        StageTask task = new StageTask(command, StageTask.PortListener.NONE,
                new StageTask.SetupFactory() {
                    private final AtomicBoolean cancelled = new AtomicBoolean();

                    @Override
                    public io.rowweave.sched.Channel endpoint(
                            ShuffleEndpoint endpoint, DataSchema meta,
                            java.util.function.Supplier<io.rowweave.sched.Channel> productionFactory) {
                        return new io.rowweave.sched.Channel() {
                            @Override public void send(io.rowweave.sched.stream.StreamEvent event) {
                                if (cancelled.compareAndSet(false, true)) {
                                    taskRef.get().stop(StageTask.StopReason.CANCELLED);
                                }
                            }
                            @Override public io.rowweave.sched.stream.StreamEvent poll() { return null; }
                            @Override public void close() { }
                            @Override public void fail(Throwable cause) {
                                cleanupOrder.add("channel");
                                throw channelFailure;
                            }
                        };
                    }
                });
        taskRef.set(task);

        StageTask.StageResult result = task.run();

        assertEquals(StageTask.StopReason.CANCELLED, result.reason());
        assertEquals(Arrays.asList("channel", "input"), cleanupOrder,
                "newest fanout owner must be attempted before its source");
        assertEquals("stage cancelled", result.failure().getMessage());
        assertEquals(2, result.failure().getSuppressed().length);
        assertSame(channelFailure, result.failure().getSuppressed()[0]);
        assertSame(inputCloseFailure,
                result.failure().getSuppressed()[1].getCause());
    }

    @Test
    void terminalResultWaitsForConcurrentCancellationCleanupToFinish() throws Exception {
        RuntimeException inputCloseFailure = new RuntimeException("late input close cleanup");
        CountDownLatch closeStarted = new CountDownLatch(1);
        CountDownLatch releaseClose = new CountDownLatch(1);
        AtomicReference<StageTask> taskRef = new AtomicReference<>();
        AtomicBoolean stopStarted = new AtomicBoolean();
        class BlockingCloseInput implements Source, AutoCloseable {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() {
                return new Iterator<DataRow>() {
                    @Override public boolean hasNext() { return true; }
                    @Override public DataRow next() { return row("cancel", 1); }
                };
            }
            @Override public void close() throws Exception {
                closeStarted.countDown();
                assertTrue(releaseClose.await(10, TimeUnit.SECONDS));
                throw inputCloseFailure;
            }
        }
        StageCommand command = StageCommand.router(
                META, WINDOW, new BlockingCloseInput(), new String[]{"k"},
                Collections.singletonList(new ShuffleEndpoint("ignored", 1)));
        StageTask task = new StageTask(command, StageTask.PortListener.NONE,
                new StageTask.SetupFactory() {
                    @Override
                    public io.rowweave.sched.Channel endpoint(
                            ShuffleEndpoint endpoint, DataSchema meta,
                            java.util.function.Supplier<io.rowweave.sched.Channel> productionFactory) {
                        return new io.rowweave.sched.Channel() {
                            @Override public void send(io.rowweave.sched.stream.StreamEvent event) {
                                if (stopStarted.compareAndSet(false, true)) {
                                    new Thread(() -> taskRef.get().stop(
                                            StageTask.StopReason.CANCELLED),
                                            "concurrent-stage-stop").start();
                                }
                            }
                            @Override public io.rowweave.sched.stream.StreamEvent poll() { return null; }
                            @Override public void close() { }
                            @Override public void fail(Throwable cause) { }
                        };
                    }
                });
        taskRef.set(task);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<StageTask.StageResult> running = executor.submit(task::run);
            assertTrue(closeStarted.await(10, TimeUnit.SECONDS));

            assertThrows(TimeoutException.class,
                    () -> running.get(100, TimeUnit.MILLISECONDS),
                    "terminal result must wait until cancellation cleanup facts are frozen");
            releaseClose.countDown();

            StageTask.StageResult result = running.get(10, TimeUnit.SECONDS);
            assertEquals(StageTask.StopReason.CANCELLED, result.reason());
            assertEquals("stage cancelled", result.failure().getMessage());
            assertEquals(1, result.failure().getSuppressed().length);
            assertSame(inputCloseFailure,
                    result.failure().getSuppressed()[0].getCause());
        } finally {
            releaseClose.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void routerCancellationOwnsLateEndpointAndNeverPublishesNormalEos() {
        AtomicInteger inputCloses = new AtomicInteger();
        AtomicInteger channelFails = new AtomicInteger();
        AtomicInteger channelCloses = new AtomicInteger();
        AtomicReference<StageTask> taskRef = new AtomicReference<>();
        class OneRowInput implements Source, AutoCloseable {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() {
                return Collections.singletonList(row("cancel", 1)).iterator();
            }
            @Override public void close() { inputCloses.incrementAndGet(); }
        }
        StageTask task = new StageTask(
                StageCommand.router(META, WINDOW, new OneRowInput(), new String[]{"k"},
                        Collections.singletonList(new ShuffleEndpoint("ignored", 1))),
                StageTask.PortListener.NONE,
                new StageTask.SetupFactory() {
                    @Override
                    public io.rowweave.sched.Channel endpoint(
                            ShuffleEndpoint endpoint, DataSchema meta,
                            java.util.function.Supplier<io.rowweave.sched.Channel> productionFactory) {
                        return new io.rowweave.sched.Channel() {
                            @Override public void send(io.rowweave.sched.stream.StreamEvent event) {
                                taskRef.get().stop(StageTask.StopReason.CANCELLED);
                                taskRef.get().stop(StageTask.StopReason.CANCELLED);
                            }
                            @Override public io.rowweave.sched.stream.StreamEvent poll() { return null; }
                            @Override public void close() { channelCloses.incrementAndGet(); }
                            @Override public void fail(Throwable cause) { channelFails.incrementAndGet(); }
                        };
                    }
                });
        taskRef.set(task);

        StageTask.StageResult result = task.run();

        assertEquals(StageTask.StopReason.CANCELLED, result.reason());
        assertEquals(1, channelFails.get(), "cancel must poison each outbound owner exactly once");
        assertEquals(0, channelCloses.get(), "cancelled truncated output must never be advertised as EOS");
        assertEquals(1, inputCloses.get(), "repeated stop and role finally share one input cleanup owner");
    }

    @Test
    void cancellationDeduplicatesSharedCleanupThrowableAcrossOwners() {
        RuntimeException sharedCleanup = new RuntimeException("shared channel cleanup");
        AtomicInteger channelFails = new AtomicInteger();
        AtomicReference<StageTask> taskRef = new AtomicReference<>();
        AtomicBoolean stopOnce = new AtomicBoolean();
        StageTask task = new StageTask(
                StageCommand.router(META, WINDOW,
                        input(Collections.singletonList(row("cancel", 1))), new String[]{"k"},
                        Arrays.asList(new ShuffleEndpoint("ignored", 1),
                                new ShuffleEndpoint("ignored", 2))),
                StageTask.PortListener.NONE,
                new StageTask.SetupFactory() {
                    @Override
                    public io.rowweave.sched.Channel endpoint(
                            ShuffleEndpoint endpoint, DataSchema meta,
                            java.util.function.Supplier<io.rowweave.sched.Channel> productionFactory) {
                        return new io.rowweave.sched.Channel() {
                            @Override public void send(
                                    io.rowweave.sched.stream.StreamEvent event) {
                                if (stopOnce.compareAndSet(false, true)) {
                                    taskRef.get().stop(StageTask.StopReason.CANCELLED);
                                }
                            }
                            @Override public io.rowweave.sched.stream.StreamEvent poll() { return null; }
                            @Override public void close() { }
                            @Override public void fail(Throwable cause) {
                                channelFails.incrementAndGet();
                                throw sharedCleanup;
                            }
                        };
                    }
                });
        taskRef.set(task);

        StageTask.StageResult result = task.run();

        assertEquals(StageTask.StopReason.CANCELLED, result.reason());
        assertEquals(2, channelFails.get(), "every owner cleanup must still be attempted");
        assertEquals(1, countIdentity(result.failure(), sharedCleanup),
                "the same cleanup Throwable identity must appear only once in the final graph");
    }

    private static int countIdentity(Throwable root, Throwable target) {
        if (root == null) return 0;
        int count = root == target ? 1 : 0;
        count += countIdentity(root.getCause(), target);
        for (Throwable suppressed : root.getSuppressed()) {
            count += countIdentity(suppressed, target);
        }
        return count;
    }

    @Test
    void producerCancellationPoisonsOutboundExactlyOnceWithoutEos() {
        AtomicInteger inputCloses = new AtomicInteger();
        AtomicInteger channelFails = new AtomicInteger();
        AtomicInteger channelCloses = new AtomicInteger();
        AtomicReference<StageTask> taskRef = new AtomicReference<>();
        class OneRowInput implements Source, AutoCloseable {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() {
                return Collections.singletonList(row("cancel", 1)).iterator();
            }
            @Override public void close() { inputCloses.incrementAndGet(); }
        }
        StageTask task = new StageTask(
                StageCommand.producer(META, WINDOW, new OneRowInput(), "ignored", 1),
                StageTask.PortListener.NONE,
                new StageTask.SetupFactory() {
                    @Override
                    public io.rowweave.sched.Channel endpoint(
                            ShuffleEndpoint endpoint, DataSchema meta,
                            java.util.function.Supplier<io.rowweave.sched.Channel> productionFactory) {
                        return new io.rowweave.sched.Channel() {
                            @Override public void send(io.rowweave.sched.stream.StreamEvent event) {
                                taskRef.get().stop(StageTask.StopReason.CANCELLED);
                                taskRef.get().stop(StageTask.StopReason.CANCELLED);
                            }
                            @Override public io.rowweave.sched.stream.StreamEvent poll() { return null; }
                            @Override public void close() { channelCloses.incrementAndGet(); }
                            @Override public void fail(Throwable cause) { channelFails.incrementAndGet(); }
                        };
                    }
                });
        taskRef.set(task);

        StageTask.StageResult result = task.run();

        assertEquals(StageTask.StopReason.CANCELLED, result.reason());
        assertEquals(1, channelFails.get());
        assertEquals(0, channelCloses.get(), "producer cancellation must not publish truncated EOS");
        assertEquals(1, inputCloses.get());
    }

    @Test
    void cancellationIsReplayedOntoEndpointReturnedAfterStop() throws Exception {
        CountDownLatch endpointConstructionStarted = new CountDownLatch(1);
        CountDownLatch releaseEndpoint = new CountDownLatch(1);
        AtomicInteger sends = new AtomicInteger();
        AtomicInteger fails = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        AtomicInteger inputCloses = new AtomicInteger();
        class CloseTrackedInput implements Source, AutoCloseable {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() {
                return new Iterator<DataRow>() {
                    @Override public boolean hasNext() { return true; }
                    @Override public DataRow next() { return row("must-not-send", 1); }
                };
            }
            @Override public void close() { inputCloses.incrementAndGet(); }
        }
        StageTask task = new StageTask(
                StageCommand.router(META, WINDOW, new CloseTrackedInput(), new String[]{"k"},
                        Collections.singletonList(new ShuffleEndpoint("ignored", 1))),
                StageTask.PortListener.NONE,
                new StageTask.SetupFactory() {
                    @Override
                    public io.rowweave.sched.Channel endpoint(
                            ShuffleEndpoint endpoint, DataSchema meta,
                            java.util.function.Supplier<io.rowweave.sched.Channel> productionFactory) {
                        endpointConstructionStarted.countDown();
                        try {
                            assertTrue(releaseEndpoint.await(10, TimeUnit.SECONDS));
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(interrupted);
                        }
                        return new io.rowweave.sched.Channel() {
                            @Override public void send(io.rowweave.sched.stream.StreamEvent event) {
                                sends.incrementAndGet();
                            }
                            @Override public io.rowweave.sched.stream.StreamEvent poll() { return null; }
                            @Override public void close() { closes.incrementAndGet(); }
                            @Override public void fail(Throwable cause) { fails.incrementAndGet(); }
                        };
                    }
                });
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<StageTask.StageResult> running = executor.submit(task::run);
            assertTrue(endpointConstructionStarted.await(10, TimeUnit.SECONDS));

            task.stop(StageTask.StopReason.CANCELLED);
            releaseEndpoint.countDown();
            StageTask.StageResult result = running.get(10, TimeUnit.SECONDS);

            assertEquals(StageTask.StopReason.CANCELLED, result.reason());
            assertEquals(1, fails.get(), "late owner publication must replay the accepted cancel");
            assertEquals(0, closes.get());
            assertEquals(0, sends.get(), "cancelled owner must never enter the role body");
            assertEquals(0, inputCloses.get(), "lazy Source was never opened before endpoint construction");
        } finally {
            releaseEndpoint.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void wholeGraphCancellationBeforeOutputTerminalAbortsWithoutPublishing() {
        AtomicInteger closes = new AtomicInteger();
        AtomicInteger aborts = new AtomicInteger();
        AtomicReference<StageTask> taskRef = new AtomicReference<>();
        Sink output = new Sink() {
            @Override public DataSchema getSchema() { return META; }
            @Override public void collect(DataRow row) {
                taskRef.get().stop(StageTask.StopReason.CANCELLED);
            }
            @Override public void close() { closes.incrementAndGet(); }
            @Override public void abort() { aborts.incrementAndGet(); }
        };
        PlanGraph graph = new PlanGraph(Arrays.asList(
                PlanNode.source(META, Collections.singletonList(row("cancel", 1))),
                PlanNode.sink(META, 0, output).build()));
        StageTask task = new StageTask(
                StageCommand.wholeGraph(graph), StageTask.PortListener.NONE);
        taskRef.set(task);

        StageTask.StageResult result = task.run();

        assertEquals(StageTask.StopReason.CANCELLED, result.reason());
        assertEquals(0, closes.get(), "cancel must win before the irreversible output terminal action");
        assertEquals(1, aborts.get());
    }

    @Test
    void wholeGraphCancellationRetainsAbortFailureEvidence() {
        RuntimeException abortFailure = new RuntimeException("whole graph abort failed");
        AtomicReference<StageTask> taskRef = new AtomicReference<>();
        Sink output = new Sink() {
            @Override public DataSchema getSchema() { return META; }
            @Override public void collect(DataRow row) {
                taskRef.get().stop(StageTask.StopReason.CANCELLED);
            }
            @Override public void close() { }
            @Override public void abort() { throw abortFailure; }
        };
        PlanGraph graph = new PlanGraph(Arrays.asList(
                PlanNode.source(META, Collections.singletonList(row("cancel", 1))),
                PlanNode.sink(META, 0, output).build()));
        StageTask task = new StageTask(
                StageCommand.wholeGraph(graph), StageTask.PortListener.NONE);
        taskRef.set(task);

        StageTask.StageResult result = task.run();

        assertEquals(StageTask.StopReason.CANCELLED, result.reason());
        assertEquals("stage cancelled", result.failure().getMessage());
        assertTrue(containsIdentity(result.failure(), abortFailure),
                "cancellation evidence must retain the exact abort failure instance");
    }

    private static boolean containsIdentity(Throwable root, Throwable target) {
        if (root == target) return true;
        if (root == null) return false;
        if (containsIdentity(root.getCause(), target)) return true;
        for (Throwable suppressed : root.getSuppressed()) {
            if (containsIdentity(suppressed, target)) return true;
        }
        return false;
    }

    @Test
    void wholeGraphLateCancellationCannotReclassifyPublishedOutput() {
        AtomicInteger closes = new AtomicInteger();
        AtomicInteger aborts = new AtomicInteger();
        AtomicReference<StageTask> taskRef = new AtomicReference<>();
        Sink output = new Sink() {
            @Override public DataSchema getSchema() { return META; }
            @Override public void collect(DataRow row) { }
            @Override public void close() {
                closes.incrementAndGet();
                taskRef.get().stop(StageTask.StopReason.CANCELLED);
            }
            @Override public void abort() { aborts.incrementAndGet(); }
        };
        PlanGraph graph = new PlanGraph(Arrays.asList(
                PlanNode.source(META, Collections.singletonList(row("done", 1))),
                PlanNode.sink(META, 0, output).build()));
        StageTask task = new StageTask(
                StageCommand.wholeGraph(graph), StageTask.PortListener.NONE);
        taskRef.set(task);

        StageTask.StageResult result = task.run();

        assertEquals(StageTask.StopReason.EOS, result.reason());
        assertEquals(1, closes.get());
        assertEquals(0, aborts.get(), "late cancellation loses after output terminal claim");
    }

    @Test
    void wholeGraphMultiOutputLateCancellationCannotSplitTheCommitGroup() {
        AtomicReference<StageTask> taskRef = new AtomicReference<>();
        AtomicInteger firstPrepares = new AtomicInteger();
        AtomicInteger firstCommits = new AtomicInteger();
        AtomicInteger firstRollbacks = new AtomicInteger();
        AtomicInteger firstFinalizes = new AtomicInteger();
        AtomicInteger secondPrepares = new AtomicInteger();
        AtomicInteger secondCommits = new AtomicInteger();
        AtomicInteger secondRollbacks = new AtomicInteger();
        AtomicInteger secondFinalizes = new AtomicInteger();
        Sink first = coordinatedOutput(firstPrepares, firstCommits,
                firstRollbacks, firstFinalizes,
                () -> taskRef.get().stop(StageTask.StopReason.CANCELLED));
        Sink second = coordinatedOutput(secondPrepares, secondCommits,
                secondRollbacks, secondFinalizes, () -> { });
        PlanGraph graph = new PlanGraph(Arrays.asList(
                PlanNode.source(META, Collections.singletonList(row("done", 1))),
                PlanNode.sink(META, 0, first).build(),
                PlanNode.sink(META, 0, second).build()));
        StageTask task = new StageTask(
                StageCommand.wholeGraph(graph), StageTask.PortListener.NONE);
        taskRef.set(task);

        StageTask.StageResult result = task.run();

        assertEquals(StageTask.StopReason.EOS, result.reason());
        assertEquals(1, firstPrepares.get());
        assertEquals(1, secondPrepares.get());
        assertEquals(1, firstCommits.get());
        assertEquals(1, secondCommits.get(),
                "cancel after the group terminal claim must not leave a partial publication");
        assertEquals(0, firstRollbacks.get());
        assertEquals(0, secondRollbacks.get());
        assertEquals(1, firstFinalizes.get());
        assertEquals(1, secondFinalizes.get());
    }

    private static Sink coordinatedOutput(
            AtomicInteger prepares, AtomicInteger commits,
            AtomicInteger rollbacks, AtomicInteger finalizes,
            Runnable onCommit) {
        return new Sink() {
            @Override public DataSchema getSchema() { return META; }
            @Override public void collect(DataRow row) { }
            @Override public boolean supportsCoordinatedCommit() { return true; }
            @Override public void prepareCommit() { prepares.incrementAndGet(); }
            @Override public void commitPrepared() {
                commits.incrementAndGet();
                onCommit.run();
            }
            @Override public void rollbackCommit() { rollbacks.incrementAndGet(); }
            @Override public void finalizeCommit() { finalizes.incrementAndGet(); }
        };
    }

    @Test
    void wholeGraphStopActivelyInterruptsAndJoinsItsPipeline() throws Exception {
        CountDownLatch readStarted = new CountDownLatch(1);
        CountDownLatch neverReleased = new CountDownLatch(1);
        AtomicInteger inputCloses = new AtomicInteger();
        AtomicInteger outputCloses = new AtomicInteger();
        AtomicInteger outputAborts = new AtomicInteger();
        class BlockingInput implements Source, AutoCloseable {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() {
                return new Iterator<DataRow>() {
                    @Override public boolean hasNext() {
                        readStarted.countDown();
                        try {
                            neverReleased.await();
                            return false;
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException("source interrupted", interrupted);
                        }
                    }
                    @Override public DataRow next() { throw new AssertionError("unreachable"); }
                };
            }
            @Override public void close() { inputCloses.incrementAndGet(); }
        }
        Sink output = new Sink() {
            @Override public DataSchema getSchema() { return META; }
            @Override public void collect(DataRow row) { }
            @Override public void close() { outputCloses.incrementAndGet(); }
            @Override public void abort() { outputAborts.incrementAndGet(); }
        };
        PlanGraph graph = new PlanGraph(Arrays.asList(
                PlanNode.sourceFromInputs(META, Collections.singletonList(new BlockingInput())),
                PlanNode.sink(META, 0, output).build()));
        StageTask task = new StageTask(
                StageCommand.wholeGraph(graph), StageTask.PortListener.NONE);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<StageTask.StageResult> running = executor.submit(task::run);
            assertTrue(readStarted.await(10, TimeUnit.SECONDS));

            task.stop(StageTask.StopReason.CANCELLED);
            StageTask.StageResult result = running.get(10, TimeUnit.SECONDS);

            assertEquals(StageTask.StopReason.CANCELLED, result.reason());
            assertEquals(1, inputCloses.get());
            assertEquals(0, outputCloses.get());
            assertEquals(1, outputAborts.get());
        } finally {
            neverReleased.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void wholeGraphStopClosesInputThatIgnoresThreadInterrupt() throws Exception {
        CountDownLatch readStarted = new CountDownLatch(1);
        CountDownLatch releaseRead = new CountDownLatch(1);
        AtomicInteger inputCloses = new AtomicInteger();
        AtomicInteger outputCloses = new AtomicInteger();
        AtomicInteger outputAborts = new AtomicInteger();
        class CloseOnlyInput implements Source, AutoCloseable {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() {
                return new Iterator<DataRow>() {
                    @Override public boolean hasNext() {
                        readStarted.countDown();
                        while (releaseRead.getCount() > 0) {
                            try {
                                releaseRead.await();
                            } catch (InterruptedException ignored) {
                                // Simulate a legal blocking connector whose read is released only by close().
                            }
                        }
                        return false;
                    }
                    @Override public DataRow next() { throw new AssertionError("unreachable"); }
                };
            }
            @Override public void close() {
                inputCloses.incrementAndGet();
                releaseRead.countDown();
            }
        }
        Sink output = new Sink() {
            @Override public DataSchema getSchema() { return META; }
            @Override public void collect(DataRow row) { }
            @Override public void close() { outputCloses.incrementAndGet(); }
            @Override public void abort() { outputAborts.incrementAndGet(); }
        };
        PlanGraph graph = new PlanGraph(Arrays.asList(
                PlanNode.sourceFromInputs(META, Collections.singletonList(new CloseOnlyInput())),
                PlanNode.sink(META, 0, output).build()));
        StageTask task = new StageTask(
                StageCommand.wholeGraph(graph), StageTask.PortListener.NONE);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<StageTask.StageResult> running = executor.submit(task::run);
            assertTrue(readStarted.await(10, TimeUnit.SECONDS));

            task.stop(StageTask.StopReason.CANCELLED);
            StageTask.StageResult result = running.get(2, TimeUnit.SECONDS);

            assertEquals(StageTask.StopReason.CANCELLED, result.reason());
            assertEquals(1, inputCloses.get());
            assertEquals(0, outputCloses.get());
            assertEquals(1, outputAborts.get());
        } finally {
            releaseRead.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void producerToConsumerInThreadBitIdentical() throws Exception {
        List<DataRow> src = new ArrayList<>();
        for (int i = 0; i < 500; i++) src.add(row("k" + (i % 7), i));

        List<DataRow> got = Collections.synchronizedList(new ArrayList<>());
        Sink out = new Sink() {
            @Override public DataSchema getSchema() { return META; }
            @Override public void collect(DataRow r) { got.add(r); }
            @Override public void close() { }
            @Override public void abort() { }
        };

        StageCommand consumerCmd = StageCommand.consumer(META, WINDOW, out);
        AtomicInteger boundPort = new AtomicInteger(-1);
        CountDownLatch portReady = new CountDownLatch(1);
        StageTask consumer = new StageTask(consumerCmd, (port, edge) -> {
            boundPort.set(port);
            portReady.countDown();
        });

        StageTask.StageResult[] consumerResult = new StageTask.StageResult[1];
        Thread ct = new Thread(() -> consumerResult[0] = consumer.run(), "consumer");
        ct.start();

        assertTrue(portReady.await(10, TimeUnit.SECONDS), "the consumer should report its port through the callback");
        int port = boundPort.get();
        assertTrue(port > 0, "the port is valid");

        StageCommand producerCmd = StageCommand.producer(META, WINDOW, input(src), "127.0.0.1", port);
        StageTask producer = new StageTask(producerCmd, StageTask.PortListener.NONE);
        StageTask.StageResult producerResult = producer.run();

        ct.join(10_000);
        assertFalse(ct.isAlive(), "the consumer thread should complete");

        assertEquals(StageTask.StopReason.EOS, producerResult.reason(), "the producer ends with a normal EOS");
        assertEquals(StageTask.StopReason.EOS, consumerResult[0].reason(), "the consumer ends with a normal EOS");
        assertEquals(TelemetryCoverage.COMPLETE, producerResult.telemetry().coverage());
        assertEquals(0L, producerResult.telemetry().managedAcquiredBytes());
        assertEquals(TelemetryCoverage.COMPLETE, consumerResult[0].telemetry().coverage());
        assertEquals(0L, consumerResult[0].telemetry().managedAcquiredBytes());
        assertEquals(src.size(), got.size(), "the consumer receives every row");
        for (int i = 0; i < src.size(); i++) {
            assertEquals(src.get(i).get(0), got.get(i).get(0), "row " + i + " k");
            assertEquals(src.get(i).get(1), got.get(i).get(1), "row " + i + " v");
        }
    }
}

