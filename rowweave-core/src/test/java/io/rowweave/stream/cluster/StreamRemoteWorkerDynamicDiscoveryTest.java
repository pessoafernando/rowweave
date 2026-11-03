package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.StreamSession;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceDiscoverySpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.contract.StreamSourceAssignment;
import io.rowweave.stream.contract.StreamSourceSplit;
import io.rowweave.stream.runtime.DynamicDurableStreamSourceProvider;
import io.rowweave.stream.runtime.EagerStreamSinkProvider;
import io.rowweave.stream.runtime.StreamSourceEnumeratorOpenRequest;
import io.rowweave.stream.runtime.StreamSourceOpenRequest;
import io.rowweave.stream.runtime.StreamSourceReader;
import io.rowweave.stream.spi.StreamDiscoveryCursor;
import io.rowweave.stream.spi.StreamSourceEnumerator;
import io.rowweave.stream.spi.StreamSplitChange;
import io.rowweave.stream.spi.StreamSplitDiscovery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Dynamic enumeration executes on a bound Worker through the control plane. */
class StreamRemoteWorkerDynamicDiscoveryTest {

    @TempDir Path temporary;

    @Test
    void manualWorkerRejectsSignedArtifactBeforeOpeningEnumerator() {
        StreamSession signed = session();
        signed.setDeploymentSetId(repeat('d'));
        StreamTestGraph graph = StreamTestGraph.from(
                signed.toPlanArtifact());
        CapturingProvider provider = new CapturingProvider();
        Map<String, io.rowweave.stream.runtime.DurableStreamSourceProvider>
                sources = new LinkedHashMap<>();
        sources.put(provider.providerKey(), provider);
        List<String> capabilities = new ArrayList<>(
                graph.product.executionCapabilities());
        capabilities.remove(PlanArtifact.FEATURE_STREAM_CONNECTOR_DEPLOYMENT_V1);
        capabilities.remove(PlanArtifact.DEPLOYMENT_FEATURE_PREFIX
                + repeat('d'));
        StreamWorkerCapabilityProfile profile =
                new StreamWorkerCapabilityProfile(
                        "manual-worker", "manual-incarnation", 2, 1,
                        capabilities, 8, 32, 1024 * 1024,
                        64L * 1024 * 1024, 16,
                        1024, 1024, 64L * 1024 * 1024,
                        64L * 1024 * 1024, 256);
        try (StreamRemoteWorkerServer server =
                     StreamRemoteWorkerServer.inProcess(
                             profile, sources,
                             Collections.<String, EagerStreamSinkProvider>
                                     emptyMap(),
                             reference -> { throw new AssertionError(); },
                             temporary)) {
            StreamRemoteWorkerWire.Response response =
                    server.dispatchInProcess(
                            StreamRemoteWorkerWire.Request.discoverSource(
                                    command(profile, graph),
                                    PlanArtifactCodec.encode(graph.artifact),
                                    "source", new byte[0], 2));
            assertEquals(StreamRemoteWorkerWire.ErrorCode.ADMISSION_REJECTED,
                    response.errorCode);
            assertEquals(null, provider.request);
        }
    }

    @Test
    void endpointDiscoversWithAttemptFenceAndProviderLimits() {
        StreamTestGraph graph = StreamTestGraph.from(session().toPlanArtifact());
        CapturingProvider provider = new CapturingProvider();
        Map<String, io.rowweave.stream.runtime.DurableStreamSourceProvider>
                sources = new LinkedHashMap<>();
        sources.put(provider.providerKey(), provider);
        StreamWorkerCapabilityProfile profile = profile(graph);
        try (StreamRemoteWorkerServer server =
                     StreamRemoteWorkerServer.inProcess(
                             profile, sources,
                             Collections.<String, EagerStreamSinkProvider>
                                     emptyMap(),
                             reference -> { throw new AssertionError(); },
                             temporary)) {
            RemoteStreamWorkerEndpoint endpoint =
                    RemoteStreamWorkerEndpoint.inProcess(server);
            StreamSplitDiscovery discovery = endpoint.discoverSource(
                    command(profile, graph),
                    PlanArtifactCodec.encode(graph.artifact), "source",
                    StreamDiscoveryCursor.initial(), 2,
                    new StreamSourceDiscoverySpec(
                            10L, 100L, 4, 4, 2, 16, 16));
            StreamSplitDiscovery replay = endpoint.discoverSource(
                    command(profile, graph),
                    PlanArtifactCodec.encode(graph.artifact), "source",
                    StreamDiscoveryCursor.initial(), 2,
                    new StreamSourceDiscoverySpec(
                            10L, 100L, 4, 4, 2, 16, 16));

            assertEquals(1L, discovery.discoveryEpoch());
            assertEquals(1, discovery.changes().size());
            assertEquals(repeat('c'), discovery.changes().get(0).splitId());
            assertEquals("source", provider.request.sourceOperatorUid());
            assertEquals(2, provider.limit);
            assertEquals(discovery.discoveryEpoch(), replay.discoveryEpoch());
            assertEquals(discovery.nextCursor(), replay.nextCursor());
            assertEquals(1, provider.discoveries);
        }
    }

    @Test
    void endpointRetriesTheExactDiscoveryAfterItsFirstAckIsLost() {
        StreamTestGraph graph = StreamTestGraph.from(session().toPlanArtifact());
        CapturingProvider provider = new CapturingProvider();
        Map<String, io.rowweave.stream.runtime.DurableStreamSourceProvider>
                sources = new LinkedHashMap<>();
        sources.put(provider.providerKey(), provider);
        StreamWorkerCapabilityProfile profile = profile(graph);
        try (StreamRemoteWorkerServer server =
                     StreamRemoteWorkerServer.inProcess(
                             profile, sources,
                             Collections.<String, EagerStreamSinkProvider>
                                     emptyMap(),
                             reference -> { throw new AssertionError(); },
                             temporary)) {
            AtomicBoolean loseFirstAck = new AtomicBoolean(true);
            RemoteStreamWorkerEndpoint endpoint =
                    RemoteStreamWorkerEndpoint.fromTransport(
                            "lossy-in-process", 0, request -> {
                                StreamRemoteWorkerWire.Response response =
                                        server.dispatchInProcess(request);
                                if (request.type == StreamRemoteWorkerWire.Type
                                        .DISCOVER_SOURCE
                                        && loseFirstAck.compareAndSet(
                                        true, false)) {
                                    throw new IllegalStateException(
                                            "simulated response loss");
                                }
                                return response;
                            });

            StreamSplitDiscovery discovery = endpoint.discoverSource(
                    command(profile, graph),
                    PlanArtifactCodec.encode(graph.artifact), "source",
                    StreamDiscoveryCursor.initial(), 2,
                    new StreamSourceDiscoverySpec(
                            10L, 100L, 4, 4, 2, 16, 16));

            assertEquals(1L, discovery.discoveryEpoch());
            assertEquals(1, provider.discoveries,
                    "the retry must replay the Worker result, not the provider");
        }
    }

    @Test
    void blockingEnumeratorTimesOutWithoutWedgingControlOrGrowingThreads()
            throws Exception {
        StreamTestGraph graph = StreamTestGraph.from(session().toPlanArtifact());
        BlockingProvider provider = new BlockingProvider();
        Map<String, io.rowweave.stream.runtime.DurableStreamSourceProvider>
                sources = new LinkedHashMap<>();
        sources.put(provider.providerKey(), provider);
        StreamWorkerCapabilityProfile profile = profile(graph);
        StreamRemoteWorkerServer server = StreamRemoteWorkerServer.inProcess(
                profile, sources,
                Collections.<String, EagerStreamSinkProvider>emptyMap(),
                reference -> { throw new AssertionError(); }, temporary);
        ExecutorService controlCalls = Executors.newFixedThreadPool(2);
        try {
            RemoteStreamWorkerEndpoint endpoint =
                    RemoteStreamWorkerEndpoint.inProcess(server);
            StreamSourceDiscoverySpec limits = new StreamSourceDiscoverySpec(
                    10L, 100L, 4, 4, 2, 16, 16);

            assertThrows(IllegalStateException.class,
                    () -> endpoint.discoverSource(command(profile, graph),
                            PlanArtifactCodec.encode(graph.artifact), "source",
                            StreamDiscoveryCursor.initial(), 2, limits));
            assertTrue(provider.entered.await(1L, TimeUnit.SECONDS));

            long retryStarted = System.nanoTime();
            assertThrows(IllegalStateException.class,
                    () -> endpoint.discoverSource(command(profile, graph),
                            PlanArtifactCodec.encode(graph.artifact), "source",
                            StreamDiscoveryCursor.initial(), 2, limits));
            assertTrue(TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - retryStarted) < 500L,
                    "a poisoned discovery facility must reject promptly");
            assertEquals(1, provider.discoveries.get(),
                    "a timed-out enumerator must not gain replacement lanes");
            assertTrue(countThreads("rowweave-stream-worker-source-discovery-")
                    <= 1L);

            Future<?> reachable = controlCalls.submit(endpoint::requireReachable);
            reachable.get(1L, TimeUnit.SECONDS);
            Future<?> closed = controlCalls.submit(server::close);
            closed.get(1L, TimeUnit.SECONDS);
        } finally {
            provider.release.countDown();
            server.close();
            controlCalls.shutdownNow();
        }
    }

    @Test
    void lostAssignmentAckReplaysWithoutOpeningTheSplitTwice()
            throws Exception {
        StreamTestGraph graph = StreamTestGraph.from(session().toPlanArtifact());
        AssignmentProvider source = new AssignmentProvider();
        RecordingProviderSink sink = new RecordingProviderSink();
        Map<String, io.rowweave.stream.runtime.DurableStreamSourceProvider>
                sources = Collections.singletonMap(source.providerKey(), source);
        Map<String, EagerStreamSinkProvider> sinks =
                Collections.singletonMap(sink.providerKey(), sink);
        StreamWorkerCapabilityProfile profile = profile(graph);
        try (StreamRemoteWorkerServer server =
                     StreamRemoteWorkerServer.inProcess(
                             profile, sources, sinks,
                             reference -> { throw new AssertionError(); },
                             temporary)) {
            AtomicBoolean loseFirstAck = new AtomicBoolean(true);
            RemoteStreamWorkerEndpoint endpoint =
                    RemoteStreamWorkerEndpoint.fromTransport(
                            "lossy-assignment", 0, request -> {
                                StreamRemoteWorkerWire.Response response =
                                        server.dispatchInProcess(request);
                                if (request.type == StreamRemoteWorkerWire.Type
                                        .ASSIGN_SOURCE_SPLIT
                                        && loseFirstAck.compareAndSet(
                                        true, false)) {
                                    throw new IllegalStateException(
                                            "simulated assignment ACK loss");
                                }
                                return response;
                            });
            Map<String, RemoteStreamWorkerEndpoint> workers =
                    Collections.singletonMap(profile.workerId(), endpoint);
            StreamWorkerPlacement placement = new StreamWorkerPlacement(
                    java.util.Arrays.asList(
                            new StreamWorkerPlacement.Assignment(
                                    0, "source", 0, profile.workerId(),
                                    profile.workerIncarnation()),
                            new StreamWorkerPlacement.Assignment(
                                    1, "sink", 0, profile.workerId(),
                                    profile.workerIncarnation())));
            byte[] start = command(profile, graph);
            StreamGenericTaskDeployment.deploy(graph.product,
                    PlanArtifactCodec.encode(graph.artifact), placement,
                    workers, graph.maximumEvents(), null,
                    assignment -> "source".equals(
                            assignment.identity().operatorUid())
                            ? new byte[0] : null,
                    ignored -> start);

            StreamSourceSplit split = new StreamSourceSplit(repeat('c'),
                    "position-v1", new byte[] {3});
            StreamSourceAssignment assignment = new StreamSourceAssignment(
                    "run", "attempt", "source", split.splitId(), 1L, 0,
                    profile.workerIncarnation(), 1L);
            endpoint.assignSourceSplit(progressCommand(profile, graph),
                    "source", 0,
                    StreamSourceTaskAssignmentCodec.encode(
                            assignment, split));

            assertTrue(sink.received.await(2L, TimeUnit.SECONDS));
            assertEquals(1, source.readerOpens.get(),
                    "an exact assignment retry must not reopen its split");
            endpoint.cancel(cancelCommand(profile, graph));
        }
    }

    @Test
    void timedOutRevocationReplayClearsOldAssignmentBeforeReplacement()
            throws Exception {
        StreamTestGraph graph = StreamTestGraph.from(session().toPlanArtifact());
        AssignmentProvider source = new AssignmentProvider(true);
        RecordingProviderSink sink = new RecordingProviderSink();
        StreamWorkerCapabilityProfile profile = profile(graph);
        try (StreamRemoteWorkerServer server = StreamRemoteWorkerServer.inProcess(
                profile, Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink),
                reference -> { throw new AssertionError(); }, temporary)) {
            RemoteStreamWorkerEndpoint endpoint =
                    RemoteStreamWorkerEndpoint.inProcess(server);
            Map<String, RemoteStreamWorkerEndpoint> workers =
                    Collections.singletonMap(profile.workerId(), endpoint);
            StreamWorkerPlacement placement = new StreamWorkerPlacement(
                    java.util.Arrays.asList(
                            new StreamWorkerPlacement.Assignment(
                                    0, "source", 0, profile.workerId(),
                                    profile.workerIncarnation()),
                            new StreamWorkerPlacement.Assignment(
                                    1, "sink", 0, profile.workerId(),
                                    profile.workerIncarnation())));
            StreamGenericTaskDeployment.deploy(graph.product,
                    PlanArtifactCodec.encode(graph.artifact), placement,
                    workers, graph.maximumEvents(), null,
                    assignment -> "source".equals(
                            assignment.identity().operatorUid())
                            ? new byte[0] : null,
                    ignored -> command(profile, graph));
            StreamSourceSplit first = new StreamSourceSplit(repeat('c'),
                    "position-v1", new byte[] {3});
            StreamSourceAssignment firstAssignment =
                    new StreamSourceAssignment("run", "attempt", "source",
                            first.splitId(), 1L, 0,
                            profile.workerIncarnation(), 1L);
            byte[] firstFrame = StreamSourceTaskAssignmentCodec.encode(
                    firstAssignment, first);
            endpoint.assignSourceSplit(progressCommand(profile, graph, 2L),
                    "source", 0, firstFrame);
            assertTrue(sink.received.await(2L, TimeUnit.SECONDS));

            try {
                assertThrows(IllegalStateException.class,
                        () -> endpoint.revokeSourceSplit(
                                progressCommand(profile, graph, 3L),
                                "source", 0, firstFrame, 100L));
                assertTrue(source.closeEntered.await(1L, TimeUnit.SECONDS));
            } finally {
                source.releaseClose.countDown();
            }
            endpoint.revokeSourceSplit(progressCommand(profile, graph, 3L),
                    "source", 0, firstFrame, 100L);
            StreamSourceSplit replacement = new StreamSourceSplit(repeat('d'),
                    "position-v1", new byte[] {4});
            StreamSourceAssignment replacementAssignment =
                    new StreamSourceAssignment("run", "attempt", "source",
                            replacement.splitId(), 1L, 0,
                            profile.workerIncarnation(), 1L);
            endpoint.assignSourceSplit(progressCommand(profile, graph, 4L),
                    "source", 0, StreamSourceTaskAssignmentCodec.encode(
                            replacementAssignment, replacement));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2L);
            while (source.readerOpens.get() < 2
                    && System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }
            assertEquals(2, source.readerOpens.get());
            endpoint.cancel(cancelCommand(profile, graph, 5L));
        }
    }

    private static StreamSession session() {
        StreamSession session = RowWeave.createStreamSession("dynamic-rpc");
        session.setParallelism(1);
        session.setMaxParallelism(8);
        session.setResourceProfile(new StreamResourceProfile(
                8, 1_024, 4_096, 0L, 0, 0,
                65_536L, 8, 32, 0L));
        session.fromSource(StreamSourceSpec.dynamicExactOrdered(
                new StreamSchema(Collections.singletonList(
                        new StreamField("id", "id", "id",
                                DataType.LongType, false))),
                "dynamic", repeat('a'), new byte[] {1},
                new StreamSourceDiscoverySpec(
                        10L, 100L, 4, 4, 2, 16, 16))).uid("source")
                .sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                        "sink", repeat('b'), new byte[] {2}));
        return session;
    }

    private static StreamWorkerCapabilityProfile profile(
            StreamTestGraph graph) {
        return new StreamWorkerCapabilityProfile(
                "worker", "worker-incarnation", 2, 1,
                graph.product.executionCapabilities(), 8, 32,
                1024 * 1024, 64L * 1024 * 1024, 16,
                1024, 1024, 64L * 1024 * 1024,
                64L * 1024 * 1024, 256);
    }

    private static byte[] command(StreamWorkerCapabilityProfile profile,
            StreamTestGraph graph) {
        return StreamControlMessageCodec.encode(new StreamControlMessage(
                StreamControlMessage.Kind.START, 1L, "master",
                "run", "attempt", 1L, 1L,
                profile.workerIncarnation(), 1L, graph.commandDigest,
                ByteBuffer.allocate(8).putLong(1L).array()));
    }

    private static byte[] progressCommand(
            StreamWorkerCapabilityProfile profile, StreamTestGraph graph) {
        return progressCommand(profile, graph, 2L);
    }

    private static byte[] progressCommand(
            StreamWorkerCapabilityProfile profile, StreamTestGraph graph,
            long sequence) {
        return StreamControlMessageCodec.encode(new StreamControlMessage(
                StreamControlMessage.Kind.TASK_PROGRESS, 1L, "master",
                "run", "attempt", 1L, 1L,
                profile.workerIncarnation(), sequence, graph.commandDigest,
                new byte[0]));
    }

    private static byte[] cancelCommand(
            StreamWorkerCapabilityProfile profile, StreamTestGraph graph) {
        return cancelCommand(profile, graph, 3L);
    }

    private static byte[] cancelCommand(
            StreamWorkerCapabilityProfile profile, StreamTestGraph graph,
            long sequence) {
        return StreamControlMessageCodec.encode(new StreamControlMessage(
                StreamControlMessage.Kind.CANCEL, 1L, "master",
                "run", "attempt", 1L, 1L,
                profile.workerIncarnation(), sequence, graph.commandDigest,
                new byte[0]));
    }

    private static final class CapturingProvider
            implements DynamicDurableStreamSourceProvider {
        private StreamSourceEnumeratorOpenRequest request;
        private int limit;
        private int discoveries;
        @Override public String providerKey() { return "dynamic"; }
        @Override public String contractDigest() { return repeat('a'); }
        @Override public StreamSourceReader open(StreamSourceOpenRequest request) {
            throw new UnsupportedOperationException();
        }
        @Override public StreamSourceEnumerator openEnumerator(
                StreamSourceEnumeratorOpenRequest request) {
            this.request = request;
            return new StreamSourceEnumerator() {
                @Override public StreamSplitDiscovery discover(
                        StreamDiscoveryCursor cursor, int limit) {
                    discoveries++;
                    CapturingProvider.this.limit = limit;
                    return new StreamSplitDiscovery(1L,
                            StreamDiscoveryCursor.of(new byte[] {1}),
                            Collections.singletonList(StreamSplitChange.add(
                                    new StreamSplitSpec(repeat('c'),
                                            "position-v1", new byte[] {3}),
                                    1L)));
                }
                @Override public void close() { }
            };
        }
    }

    private static final class BlockingProvider
            implements DynamicDurableStreamSourceProvider {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicInteger discoveries = new AtomicInteger();

        @Override public String providerKey() { return "dynamic"; }
        @Override public String contractDigest() { return repeat('a'); }
        @Override public StreamSourceReader open(StreamSourceOpenRequest request) {
            throw new UnsupportedOperationException();
        }
        @Override public StreamSourceEnumerator openEnumerator(
                StreamSourceEnumeratorOpenRequest request) {
            return new StreamSourceEnumerator() {
                @Override public StreamSplitDiscovery discover(
                        StreamDiscoveryCursor cursor, int limit) {
                    discoveries.incrementAndGet();
                    entered.countDown();
                    boolean interrupted = false;
                    try {
                        while (true) {
                            try {
                                release.await();
                                return new StreamSplitDiscovery(
                                        1L, cursor, Collections.emptyList());
                            } catch (InterruptedException ignored) {
                                interrupted = true;
                            }
                        }
                    } finally {
                        if (interrupted) Thread.currentThread().interrupt();
                    }
                }
                @Override public void close() { release.countDown(); }
            };
        }
    }

    private static final class AssignmentProvider
            implements DynamicDurableStreamSourceProvider {
        private final AtomicInteger readerOpens = new AtomicInteger();
        private final boolean blockClose;
        private final CountDownLatch closeEntered = new CountDownLatch(1);
        private final CountDownLatch releaseClose = new CountDownLatch(1);

        private AssignmentProvider() { this(false); }
        private AssignmentProvider(boolean blockClose) {
            this.blockClose = blockClose;
        }
        @Override public String providerKey() { return "dynamic"; }
        @Override public String contractDigest() { return repeat('a'); }
        @Override public StreamSourceEnumerator openEnumerator(
                StreamSourceEnumeratorOpenRequest request) {
            throw new UnsupportedOperationException();
        }
        @Override public StreamSourceReader open(
                StreamSourceOpenRequest request) {
            readerOpens.incrementAndGet();
            return new StreamSourceReader() {
                private final Semaphore wakeups = new Semaphore(0);
                private boolean emitted;
                @Override public io.rowweave.stream.runtime.StreamSourceRecord
                next() throws Exception {
                    if (!emitted) {
                        emitted = true;
                        return io.rowweave.stream.runtime.StreamSourceRecord
                                .of(request.split().descriptorFrame(),
                                        new byte[] {1});
                    }
                    wakeups.acquire();
                    return null;
                }
                @Override public byte[] checkpointPositionFrame() {
                    return new byte[] {(byte) (emitted ? 1 : 0)};
                }
                @Override public void wakeUp() { wakeups.release(); }
                @Override public void cancel() { wakeups.release(); }
                @Override public void close() throws Exception {
                    if (blockClose && request.split().splitId().equals(
                            repeat('c'))) {
                        closeEntered.countDown();
                        releaseClose.await();
                    }
                    wakeups.release();
                }
            };
        }
    }

    private static final class RecordingProviderSink
            implements EagerStreamSinkProvider {
        private final CountDownLatch received = new CountDownLatch(1);
        @Override public String providerKey() { return "sink"; }
        @Override public String contractDigest() { return repeat('b'); }
        @Override public io.rowweave.stream.runtime.EagerStreamSinkWriter open(
                io.rowweave.stream.runtime.StreamSinkOpenRequest request) {
            return new io.rowweave.stream.runtime.EagerStreamSinkWriter() {
                @Override public void write(byte[] recordFrame) {
                    received.countDown();
                }
                @Override public void cancel() { }
                @Override public void close() { }
            };
        }
    }

    private static long countThreads(String prefix) {
        long count = 0L;
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.isAlive() && thread.getName().startsWith(prefix)) {
                count++;
            }
        }
        return count;
    }

    private static String repeat(char value) {
        char[] result = new char[64];
        java.util.Arrays.fill(result, value);
        return new String(result);
    }
}
