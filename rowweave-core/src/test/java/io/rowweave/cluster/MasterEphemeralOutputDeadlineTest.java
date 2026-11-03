package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.cluster.rpc.EphemeralSubmitTimeoutPolicy;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.cluster.rpc.RpcServer;
import io.rowweave.plan.ObjectTrustContext;
import io.rowweave.cluster.rpc.RpcTransport;
import io.rowweave.cluster.rpc.RpcTransports;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** the execution deadline also fences compensatable Sink publication. */
class MasterEphemeralOutputDeadlineTest {
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType));

    @Test
    @Timeout(value = 12, unit = TimeUnit.SECONDS)
    void earlyPreparedSinkSurvivesWhileAnotherSinkStillComputes(
            @TempDir Path temp) throws Exception {
        String retentionKey = "rowweave.worker.preparedOutputRetentionMs";
        String priorRetention = System.setProperty(retentionKey, "1200");
        Path first = temp.resolve("early.tsv");
        Path second = temp.resolve("late.tsv");
        File spill = temp.resolve("lease-worker").toFile();
        assertTrue(spill.mkdirs());
        try (MasterServer master = new MasterServer(10_000L);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "lease-worker", 8, "127.0.0.1", master.port(), spill);
             RealClusterJobClient client = new RealClusterJobClient(
                     "127.0.0.1", master.port())) {
            DataRow row = new DataRow(1);
            row.set(0, 17);
            PlanGraph graph = new PlanGraph(Arrays.asList(
                    PlanNode.source(META, Collections.singletonList(row)),
                    PlanNode.unary("partitionByHash", META, 0)
                            .listParam("keys", Collections.singletonList("id")).build(),
                    PlanNode.sink(META, 1, new FileSink(META, first.toString())).build(),
                    PlanNode.sink(META, 1, new SlowPrepareFileOutput(
                            META, second.toString(), 3_500L)).build()), 1);

            assertEquals(2L, client.submitToSink(graph),
                    "the first prepared Sink must survive a longer sibling stage");
            assertEquals(1, FileSink.read(META, first.toString()).size());
            assertEquals(1, FileSink.read(META, second.toString()).size());
            assertEquals(0, worker.preparedOutputActiveCount());
        } finally {
            restore(retentionKey, priorRetention);
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void deadlineDuringOrdinaryMultiOutputCommitRestoresEveryOldValue(
            @TempDir Path temp) throws Exception {
        String timeoutKey = "rowweave.cluster.submitTimeoutMs";
        String cleanupKey = "rowweave.cluster.submitCleanupGraceMs";
        String previousTimeout = System.setProperty(timeoutKey, "1000");
        String previousCleanup = System.setProperty(cleanupKey, "3000");
        Path first = temp.resolve("first.tsv");
        Path second = temp.resolve("second.tsv");
        Path delayedCommit = temp.resolve("delayed-commit.marker");
        byte[] firstOld = "first-old\n".getBytes(StandardCharsets.UTF_8);
        byte[] secondOld = "second-old\n".getBytes(StandardCharsets.UTF_8);
        Files.write(first, firstOld);
        Files.write(second, secondOld);
        File spill = temp.resolve("worker").toFile();
        assertTrue(spill.mkdirs());
        try (MasterServer master = new MasterServer(10_000L);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "output-deadline-worker", 8, "127.0.0.1",
                     master.port(), spill);
             RealClusterJobClient client = new RealClusterJobClient(
                     "127.0.0.1", master.port())) {
            long started = System.nanoTime();
            RuntimeException failure = assertThrows(RuntimeException.class,
                    () -> client.submitToSink(graph(first, second, delayedCommit)));
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - started);

            assertTrue(failure.getMessage().contains(
                    "CLUSTER_JOB_EXECUTION_TIMEOUT"), failure.getMessage());
            assertTrue(elapsedMs >= 800L && elapsedMs < 5_500L,
                    "commit and compensation must share the request deadlines: "
                            + elapsedMs + "ms");
            assertTrue(Files.exists(delayedCommit),
                    "the test must prove the second Sink entered commitPrepared");
            assertArrayEquals(firstOld, Files.readAllBytes(first));
            assertArrayEquals(secondOld, Files.readAllBytes(second));
            assertEquals(0, worker.preparedOutputActiveCount());
            try (java.util.stream.Stream<Path> files = Files.list(temp)) {
                assertFalse(files.anyMatch(path -> path.getFileName().toString()
                        .startsWith(".rowweave-output-")));
            }
        } finally {
            restore(timeoutKey, previousTimeout);
            restore(cleanupKey, previousCleanup);
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void clientDisconnectIsUnknownAndDoesNotCancelTheServerDeadline(
            @TempDir Path temp) throws Exception {
        Path first = temp.resolve("disconnect-first.tsv");
        Path second = temp.resolve("disconnect-second.tsv");
        Path delayedCommit = temp.resolve("disconnect-commit.marker");
        byte[] firstOld = "disconnect-first-old\n".getBytes(StandardCharsets.UTF_8);
        byte[] secondOld = "disconnect-second-old\n".getBytes(StandardCharsets.UTF_8);
        Files.write(first, firstOld);
        Files.write(second, secondOld);
        File spill = temp.resolve("disconnect-worker").toFile();
        assertTrue(spill.mkdirs());
        try (MasterServer master = new MasterServer(10_000L);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "disconnect-output-worker", 8, "127.0.0.1",
                     master.port(), spill)) {
            RuntimeException disconnected;
            try (RpcTransport transport = RpcTransports.connect(
                    "127.0.0.1", master.port(), 500, 150)) {
                RpcMessage request = RpcMessage.submitJob(
                        graph(first, second, delayedCommit).toBytes(),
                        "disconnect-attempt",
                        EphemeralSubmitTimeoutPolicy.Budget.of(1_000L, 3_000L));
                disconnected = assertThrows(RuntimeException.class,
                        () -> transport.call(request));
            }
            assertFalse(causeMessages(disconnected).contains(
                    "CLUSTER_JOB_EXECUTION_TIMEOUT"),
                    "a disconnected Client cannot claim the server terminal outcome");
            awaitExists(delayedCommit, 5_000L);
            awaitPreparedOutputs(worker, 0, 5_000L);

            assertArrayEquals(firstOld, Files.readAllBytes(first));
            assertArrayEquals(secondOld, Files.readAllBytes(second));

            String timeoutKey = "rowweave.cluster.submitTimeoutMs";
            String cleanupKey = "rowweave.cluster.submitCleanupGraceMs";
            String previousTimeout = System.setProperty(timeoutKey, "5000");
            String previousCleanup = System.setProperty(cleanupKey, "1000");
            try (RealClusterJobClient client = new RealClusterJobClient(
                    "127.0.0.1", master.port())) {
                assertEquals(1L, client.submitToSink(healthyGraph()),
                        "the disconnected submit must release stage and slot ownership");
            } finally {
                restore(timeoutKey, previousTimeout);
                restore(cleanupKey, previousCleanup);
            }
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void anotherJobsBlockedCancellationCannotExtendPreparedOutputSubmitDeadline(
            @TempDir Path temp) throws Exception {
        java.util.concurrent.CountDownLatch bothSinksAssigned =
                new java.util.concurrent.CountDownLatch(2);
        java.util.concurrent.CountDownLatch cancellationEntered =
                new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch firstRenewal =
                new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.atomic.AtomicReference<Thread> preparedJobThread =
                new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.CountDownLatch releaseCancellation =
                new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.atomic.AtomicInteger preparedSinks =
                new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.ExecutorService submits =
                java.util.concurrent.Executors.newFixedThreadPool(2);
        try (MasterServer master = new MasterServer(60_000L);
             RpcServer worker = new RpcServer(request -> {
                 if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                     StageCommand command = StageCommandCodecV2.preflight(
                             request.stageCommandBlob(), ignored -> { }).materialize(
                             ObjectTrustContext.localTrusted(getClass().getClassLoader()),
                             ignored -> { });
                     boolean completed = command.role() != StageCommand.Role.SINK;
                     if (command.commitGroupId() != null
                             && command.role() == StageCommand.Role.SINK) {
                         completed = preparedSinks.incrementAndGet() == 1;
                         bothSinksAssigned.countDown();
                     }
                     if (completed) {
                         master.handler().apply(RpcMessage.stageReport(
                                 request.workerId(), request.stageId(), true, 1L, 1L, null,
                                 request.incarnationId(), request.runAttemptFence()));
                     }
                 } else if (request.type() == RpcMessage.Type.CANCEL_STAGE) {
                     cancellationEntered.countDown();
                     try {
                         releaseCancellation.await(10L, TimeUnit.SECONDS);
                     } catch (InterruptedException interrupted) {
                         Thread.currentThread().interrupt();
                     }
                 } else if (request.type() == RpcMessage.Type.RENEW_PREPARED_OUTPUT) {
                     firstRenewal.countDown();
                     return null;
                 }
                 return RpcMessage.ack();
             })) {
            master.handler().apply(RpcMessage.registerWorker(
                    "shared-control-worker", "127.0.0.1", worker.port(), 16, -1,
                    "shared-control-incarnation"));
            long started = System.nanoTime();
            java.util.concurrent.Future<RpcMessage> preparedJob = submits.submit(() -> {
                    preparedJobThread.set(Thread.currentThread());
                    return master.handler().apply(RpcMessage.submitJob(
                            graph(temp.resolve("a.tsv"), temp.resolve("b.tsv"),
                                    temp.resolve("unused.marker")).toBytes(),
                            "prepared-job", EphemeralSubmitTimeoutPolicy.Budget.of(3_000L, 1_000L)));
            });
            java.util.concurrent.Future<RpcMessage> cancelJob = null;
            try {
                assertTrue(bothSinksAssigned.await(2L, TimeUnit.SECONDS));
                assertTrue(firstRenewal.await(2L, TimeUnit.SECONDS),
                        "prove Master accepted the first prepared sink and entered renewal");
                // A separate Job has a longer cleanup budget on the same real socket cache.
                cancelJob = submits.submit(() -> master.handler().apply(RpcMessage.submitJob(
                        healthyGraph().toBytes(), "blocked-cancel-job",
                        EphemeralSubmitTimeoutPolicy.Budget.of(1_000L, 10_000L))));
                assertTrue(cancellationEntered.await(2L, TimeUnit.SECONDS));
                long sampleUntil = System.nanoTime() + TimeUnit.SECONDS.toNanos(1L);
                while (System.nanoTime() < sampleUntil) {
                    Thread owner = preparedJobThread.get();
                    if (owner.getState() == Thread.State.BLOCKED) {
                        System.out.println("CONTENTION blocked stack="
                                + Arrays.toString(owner.getStackTrace()));
                        break;
                    }
                    Thread.sleep(10L);
                }
                RpcMessage terminal = preparedJob.get(5_500L, TimeUnit.MILLISECONDS);
                long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
                System.out.println("CONTENTION terminal=" + terminal.errorMessage());
                assertFalse(terminal.success());
                assertTrue(terminal.errorMessage().startsWith("CLUSTER_JOB_"),
                        terminal.errorMessage());
                assertTrue(elapsedMs < 5_000L,
                        "a sibling Job's control lock cannot replace this Job's deadline: "
                                + elapsedMs + "ms");
            } finally {
                releaseCancellation.countDown();
                if (cancelJob != null) {
                    RpcMessage otherTerminal = cancelJob.get(5L, TimeUnit.SECONDS);
                    assertFalse(otherTerminal.success());
                }
                submits.shutdownNow();
                assertTrue(submits.awaitTermination(5L, TimeUnit.SECONDS));
            }
        } finally {
            releaseCancellation.countDown();
            submits.shutdownNow();
        }
    }

    private static PlanGraph graph(Path first, Path second, Path marker) {
        DataRow row = new DataRow(1);
        row.set(0, 7);
        PlanNode source = PlanNode.source(META, Collections.singletonList(row));
        PlanNode partition = PlanNode.unary("partitionByHash", META, 0)
                .listParam("keys", Collections.singletonList("id"))
                .build();
        PlanNode firstSink = PlanNode.sink(
                META, 1, new FileSink(META, first.toString())).build();
        PlanNode secondSink = PlanNode.sink(
                META, 1, new DelayedCommitFileOutput(
                        META, second.toString(), marker.toString(), 1_500L)).build();
        return new PlanGraph(Arrays.asList(
                source, partition, firstSink, secondSink), 1);
    }

    private static PlanGraph healthyGraph() {
        DataRow row = new DataRow(1);
        row.set(0, 11);
        return new PlanGraph(Arrays.asList(
                PlanNode.source(META, Collections.singletonList(row)),
                PlanNode.unary("partitionByHash", META, 0)
                        .listParam("keys", Collections.singletonList("id"))
                        .build(),
                PlanNode.sink(META, 1, new DiscardSink(META)).build()), 1);
    }

    private static void awaitExists(Path path, long timeoutMs) throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (!Files.exists(path) && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        assertTrue(Files.exists(path), "expected marker was not created: " + path);
    }

    private static void awaitPreparedOutputs(
            WorkerServer worker, int expected, long timeoutMs) throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (worker.preparedOutputActiveCount() != expected
                && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        assertEquals(expected, worker.preparedOutputActiveCount());
    }

    private static String causeMessages(Throwable failure) {
        StringBuilder messages = new StringBuilder();
        for (Throwable current = failure; current != null;
             current = current.getCause()) {
            if (current.getMessage() != null) {
                messages.append(current.getMessage()).append('\n');
            }
        }
        return messages.toString();
    }

    private static void restore(String key, String previous) {
        if (previous == null) System.clearProperty(key);
        else System.setProperty(key, previous);
    }

    private static final class DelayedCommitFileOutput implements Sink {
        private static final long serialVersionUID = 1L;
        private final FileSink delegate;
        private final String marker;
        private final long delayMs;

        private DelayedCommitFileOutput(
                DataSchema meta, String target, String marker, long delayMs) {
            this.delegate = new FileSink(meta, target);
            this.marker = marker;
            this.delayMs = delayMs;
        }

        @Override public DataSchema getSchema() { return delegate.getSchema(); }
        @Override public void collect(DataRow row) { delegate.collect(row); }
        @Override public void close() { delegate.close(); }
        @Override public void abort() { delegate.abort(); }
        @Override public boolean supportsCoordinatedCommit() { return true; }
        @Override public void prepareCommit() { delegate.prepareCommit(); }
        @Override public void commitPrepared() {
            delegate.commitPrepared();
            try {
                Files.write(Paths.get(marker), new byte[]{1});
                Thread.sleep(delayMs);
            } catch (java.io.IOException failure) {
                throw new java.io.UncheckedIOException(failure);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(interrupted);
            }
        }
        @Override public void rollbackCommit() { delegate.rollbackCommit(); }
        @Override public void finalizeCommit() { delegate.finalizeCommit(); }
        @Override public boolean isWorkerWritable() { return true; }
        @Override public String exclusiveDestinationId() {
            return delegate.exclusiveDestinationId();
        }
    }

    private static final class SlowPrepareFileOutput implements Sink {
        private static final long serialVersionUID = 1L;
        private final FileSink delegate;
        private final long delayMs;

        private SlowPrepareFileOutput(DataSchema meta, String target, long delayMs) {
            this.delegate = new FileSink(meta, target);
            this.delayMs = delayMs;
        }

        @Override public DataSchema getSchema() { return delegate.getSchema(); }
        @Override public void collect(DataRow row) { delegate.collect(row); }
        @Override public void close() { delegate.close(); }
        @Override public void abort() { delegate.abort(); }
        @Override public boolean supportsCoordinatedCommit() { return true; }
        @Override public void prepareCommit() {
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(interrupted);
            }
            delegate.prepareCommit();
        }
        @Override public void commitPrepared() { delegate.commitPrepared(); }
        @Override public void rollbackCommit() { delegate.rollbackCommit(); }
        @Override public void finalizeCommit() { delegate.finalizeCommit(); }
        @Override public boolean isWorkerWritable() { return true; }
        @Override public String exclusiveDestinationId() {
            return delegate.exclusiveDestinationId();
        }
    }
}
