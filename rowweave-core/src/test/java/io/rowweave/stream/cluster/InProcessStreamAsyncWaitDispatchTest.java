package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataStream;
import io.rowweave.DataRow;
import io.rowweave.StreamAsyncFunction;
import io.rowweave.StreamAsyncRequest;
import io.rowweave.StreamCheckpointHandle;
import io.rowweave.StreamCollector;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamRestoreRequest;
import io.rowweave.StreamSavepointHandle;
import io.rowweave.StreamSavepointRequest;
import io.rowweave.StreamSession;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.mem.RowSerializer;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.stream.api.StreamAsyncOrdering;
import io.rowweave.stream.api.StreamAsyncReplaySpec;
import io.rowweave.stream.api.StreamAsyncResourceSpec;
import io.rowweave.stream.api.StreamAsyncWaitSpec;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamEmissionLimit;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamRecoveryPointId;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.runtime.DurableStreamSourceProvider;
import io.rowweave.stream.runtime.EagerStreamSinkProvider;
import io.rowweave.stream.runtime.EagerStreamSinkWriter;
import io.rowweave.stream.runtime.StreamSinkOpenRequest;
import io.rowweave.stream.runtime.StreamSourceOpenRequest;
import io.rowweave.stream.runtime.StreamSourceReader;
import io.rowweave.stream.runtime.StreamSourceRecord;
import io.rowweave.stream.runtime.StreamSourceEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** public async wait executes through Local shared roles. */
class InProcessStreamAsyncWaitDispatchTest {

    @Test
    void orderedLookupRunsAndCheckpointsThroughMasterWorkerRoles(
            @TempDir Path root) throws Exception {
        AsyncSource source = new AsyncSource();
        AsyncSink sink = new AsyncSink();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root, Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink),
                ignored -> new Lookup(), Collections.singletonList(
                StreamAsyncWaitProductAdmissionTest.function()
                        .deploymentSetId()))) {
            StreamJobHandle job = session(cluster).submit(
                    StreamSubmissionId.of("async-wait-submit"));
            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(5_000L));
            assertTrue(sink.received.await(5L, TimeUnit.SECONDS));
            assertEquals(java.util.Arrays.asList(1L, 2L, 3L), sink.ids());

            StreamCheckpointHandle checkpoint = job.triggerCheckpoint(
                    StreamCommandId.of("async-wait-checkpoint"));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    checkpoint.awaitCompletion(5_000L));
            assertEquals(1L, job.progress().completedCheckpoints());
            job.cancel(StreamCommandId.of("async-wait-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    @Test
    void chainedAsyncWaitsContributeIndependentCheckpointMembers(
            @TempDir Path root) throws Exception {
        AsyncSource source = new AsyncSource();
        AsyncSink sink = new AsyncSink();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root, Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink),
                ignored -> new Lookup(), Collections.singletonList(
                StreamAsyncWaitProductAdmissionTest.function()
                        .deploymentSetId()))) {
            StreamJobHandle job = session(cluster, 2).submit(
                    StreamSubmissionId.of("async-wait-chain-submit"));
            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(5_000L));
            assertTrue(sink.received.await(5L, TimeUnit.SECONDS));
            assertEquals(java.util.Arrays.asList(1L, 2L, 3L), sink.ids());

            StreamCheckpointHandle checkpoint = job.triggerCheckpoint(
                    StreamCommandId.of("async-wait-chain-checkpoint"));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    checkpoint.awaitCompletion(5_000L));
            job.cancel(StreamCommandId.of("async-wait-chain-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    @Test
    void savepointRestoresInflightIdentityWithoutReinvokingCompletedResponses(
            @TempDir Path root) throws Exception {
        AsyncSource source = new AsyncSource();
        AsyncSink sink = new AsyncSink();
        ReplayLookup lookup = new ReplayLookup();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root, Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink),
                ignored -> lookup, Collections.singletonList(
                StreamAsyncWaitProductAdmissionTest.function()
                        .deploymentSetId()))) {
            StreamSession definition = session(cluster, 1, 30_000L);
            PlanArtifact artifact = definition.toPlanArtifact();
            StreamJobHandle original = definition.submit(
                    StreamSubmissionId.of("async-inflight-submit"));
            assertEquals(StreamJobStatus.RUNNING,
                    original.awaitRunning(5_000L));
            assertTrue(lookup.allInvoked.await(5L, TimeUnit.SECONDS));

            StreamSavepointHandle savepoint = original.stopWithSavepoint(
                    StreamCommandId.of("async-inflight-savepoint"),
                    new StreamSavepointRequest(
                            "file-savepoint-v1", new byte[0]));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    savepoint.awaitCompletion(5_000L));
            assertEquals(StreamJobStatus.STOPPED_WITH_SAVEPOINT,
                    original.awaitTermination(5_000L));

            StreamJobHandle restored = cluster.restore(
                    StreamSubmissionId.of("async-inflight-restored"),
                    artifact, new StreamRestoreRequest(original.id(),
                            StreamRecoveryPointId.savepoint(
                                    savepoint.savepointId()), 1, 128));
            assertEquals(StreamJobStatus.RUNNING,
                    restored.awaitRunning(5_000L));
            assertTrue(sink.received.await(5L, TimeUnit.SECONDS));
            assertEquals(java.util.Arrays.asList(1L, 2L, 3L), sink.ids());
            assertEquals(2, lookup.firstInvocations.get());
            assertEquals(1L, lookup.firstIdentities.stream()
                    .distinct().count());
            assertEquals(1, lookup.invocations(2L));
            assertEquals(1, lookup.invocations(3L));
            restored.cancel(StreamCommandId.of("async-inflight-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    @Test
    void liveRescaleRedistributesInflightRequestsWithoutChangingIdentity(
            @TempDir Path root) throws Exception {
        AsyncSource source = new AsyncSource();
        AsyncSink sink = new AsyncSink();
        ReplayLookup lookup = new ReplayLookup();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root, Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink),
                ignored -> lookup, Collections.singletonList(
                StreamAsyncWaitProductAdmissionTest.function()
                        .deploymentSetId()))) {
            StreamJobHandle job = session(cluster, 1, 30_000L).submit(
                    StreamSubmissionId.of("async-inflight-live-rescale"));
            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(5_000L));
            assertTrue(lookup.allInvoked.await(5L, TimeUnit.SECONDS));

            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.rescale(StreamCommandId.of(
                            "async-inflight-scale-to-two"), 2)
                            .awaitCompletion(10_000L));
            assertTrue(sink.received.await(5L, TimeUnit.SECONDS),
                    () -> "status=" + job.status()
                            + ", values=" + sink.ids()
                            + ", invoked=" + lookup.invokedIds
                            + ", identities=" + lookup.firstIdentities);
            List<Long> values = sink.ids();
            Collections.sort(values);
            assertEquals(java.util.Arrays.asList(1L, 2L, 3L), values);
            assertEquals(2, lookup.firstInvocations.get());
            assertEquals(1L, lookup.firstIdentities.stream()
                    .distinct().count());
            assertEquals(1, lookup.invocations(2L));
            assertEquals(1, lookup.invocations(3L));
            job.cancel(StreamCommandId.of(
                    "async-inflight-live-rescale-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    @Test
    void liveRescalePreservesControlsQueuedBehindInflightRequests(
            @TempDir Path root) throws Exception {
        ControlAsyncSource source = new ControlAsyncSource();
        AsyncSink sink = new AsyncSink();
        ReplayLookup lookup = new ReplayLookup();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root, Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink),
                ignored -> lookup, Collections.singletonList(
                StreamAsyncWaitProductAdmissionTest.function()
                        .deploymentSetId()))) {
            StreamJobHandle job = session(cluster, 1, 30_000L).submit(
                    StreamSubmissionId.of("async-control-live-rescale"));
            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(5_000L));
            assertTrue(lookup.allInvoked.await(5L, TimeUnit.SECONDS));
            assertTrue(source.controlsEmitted.await(5L, TimeUnit.SECONDS));

            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.rescale(StreamCommandId.of(
                            "async-control-scale-to-two"), 2)
                            .awaitCompletion(10_000L));
            assertTrue(sink.received.await(5L, TimeUnit.SECONDS));
            List<Long> values = sink.ids();
            Collections.sort(values);
            assertEquals(java.util.Arrays.asList(1L, 2L, 3L), values);
            assertEquals(StreamJobStatus.RUNNING, job.status());
            job.cancel(StreamCommandId.of("async-control-live-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    private static StreamSession session(InProcessStreamCluster cluster) {
        return session(cluster, 1);
    }

    private static StreamSession session(
            InProcessStreamCluster cluster, int asyncWaits) {
        return session(cluster, asyncWaits, 1_000L);
    }

    private static StreamSession session(InProcessStreamCluster cluster,
            int asyncWaits, long timeoutMillis) {
        // Replay tests deliberately hold the first invocation until cutover;
        // its timeout must outlive a loaded reactor's rescale operation.
        StreamSession session = RowWeave.createStreamSession(
                "async-wait", "async-wait", cluster);
        session.setResourceProfile(new StreamResourceProfile(
                16, 4_096, 16_384, 131_072L, 0, 0,
                131_072L, 16, 32, 0L));
        DataStream stream = session.fromSource(StreamSourceSpec.exactOrdered(
                        StreamAsyncWaitProductAdmissionTest.inputSchema(),
                        "source-v1",
                        StreamAsyncWaitProductAdmissionTest.digest("source"),
                        new byte[] {1}, Collections.singletonList(
                        new StreamSplitSpec(
                                StreamAsyncWaitProductAdmissionTest
                                        .digest("split"),
                                "sequence-v1", new byte[] {2}))))
                .uid("source");
        for (int index = 0; index < asyncWaits; index++) {
            stream = stream.asyncWait(StreamAsyncWaitSpec.of(
                        StreamAsyncWaitProductAdmissionTest.function(),
                        StreamAsyncWaitProductAdmissionTest.outputSchema(),
                        Collections.singletonList("id"),
                        StreamAsyncOrdering.ORDERED,
                        StreamAsyncReplaySpec.identityKeyed(
                                "request-id-v1",
                                StreamAsyncWaitProductAdmissionTest
                                        .digest("replay")),
                        StreamAsyncResourceSpec.of(4, 32_768L,
                                timeoutMillis, 3, 10L, 100L,
                                StreamEmissionLimit.of(1, 4_096L))))
                    .uid(asyncWaits == 1 ? "lookup"
                            : "lookup-" + (index + 1));
        }
        stream.sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                        "sink-v1",
                        StreamAsyncWaitProductAdmissionTest.digest("sink"),
                        new byte[] {3}));
        return session;
    }

    private static final class Lookup implements StreamAsyncFunction {
        @Override public byte[] createRequest(DataRow input) {
            return ByteBuffer.allocate(Long.BYTES)
                    .putLong(input.getLong(0)).array();
        }
        @Override public byte[] invoke(StreamAsyncRequest request)
                throws Exception {
            long id = ByteBuffer.wrap(request.requestFrame()).getLong();
            Thread.sleep((4L - id) * 10L);
            return request.requestFrame();
        }
        @Override public void complete(DataRow input, byte[] responseFrame,
                StreamCollector output) {
            DataRow row = new DataRow(2);
            row.set(0, input.getLong(0));
            row.set(1, "customer-" + input.getLong(0));
            output.collect(row);
        }
    }

    private static final class ReplayLookup implements StreamAsyncFunction {
        private final CountDownLatch allInvoked = new CountDownLatch(3);
        private final AtomicInteger firstInvocations = new AtomicInteger();
        private final CopyOnWriteArrayList<String> firstIdentities =
                new CopyOnWriteArrayList<>();
        private final CopyOnWriteArrayList<Long> invokedIds =
                new CopyOnWriteArrayList<>();

        @Override public byte[] createRequest(DataRow input) {
            return ByteBuffer.allocate(Long.BYTES)
                    .putLong(input.getLong(0)).array();
        }

        @Override public byte[] invoke(StreamAsyncRequest request)
                throws Exception {
            long id = ByteBuffer.wrap(request.requestFrame()).getLong();
            invokedIds.add(id);
            allInvoked.countDown();
            if (id == 1L) {
                firstIdentities.add(request.operationIdentity());
                if (firstInvocations.incrementAndGet() == 1) {
                    new CountDownLatch(1).await();
                }
            }
            return request.requestFrame();
        }

        @Override public void complete(DataRow input, byte[] responseFrame,
                StreamCollector output) {
            DataRow row = new DataRow(2);
            row.set(0, input.getLong(0));
            row.set(1, "customer-" + input.getLong(0));
            output.collect(row);
        }

        private int invocations(long id) {
            int count = 0;
            for (Long current : invokedIds) if (current == id) count++;
            return count;
        }
    }

    private static final class AsyncSource
            implements DurableStreamSourceProvider {
        @Override public String providerKey() { return "source-v1"; }
        @Override public String contractDigest() {
            return StreamAsyncWaitProductAdmissionTest.digest("source");
        }
        @Override public StreamSourceReader open(StreamSourceOpenRequest request) {
            RowSerializer serializer = new RowSerializer(
                    PortablePlanCompiler.toDataSchema(
                            StreamAsyncWaitProductAdmissionTest.inputSchema()
                                    .toPortableSchema()));
            return new StreamSourceReader() {
                private final Semaphore wakeups = new Semaphore(0);
                private int next = request.hasRestoredPosition()
                        ? ByteBuffer.wrap(request.restoredPositionFrame())
                        .getInt() : 0;
                @Override public StreamSourceRecord next() throws Exception {
                    if (next < 3) {
                        DataRow row = new DataRow(1);
                        row.set(0, (long) ++next);
                        return StreamSourceRecord.of(serializer.serialize(row),
                                ByteBuffer.allocate(4).putInt(next).array());
                    }
                    wakeups.acquire();
                    return null;
                }
                @Override public byte[] checkpointPositionFrame() {
                    return ByteBuffer.allocate(4).putInt(next).array();
                }
                @Override public void wakeUp() { wakeups.release(); }
                @Override public void cancel() { wakeups.release(); }
                @Override public void close() { wakeups.release(); }
            };
        }
    }

    private static final class ControlAsyncSource
            implements DurableStreamSourceProvider {
        private final CountDownLatch controlsEmitted = new CountDownLatch(1);

        @Override public String providerKey() { return "source-v1"; }

        @Override public String contractDigest() {
            return StreamAsyncWaitProductAdmissionTest.digest("source");
        }

        @Override public StreamSourceReader open(
                StreamSourceOpenRequest request) {
            RowSerializer serializer = new RowSerializer(
                    PortablePlanCompiler.toDataSchema(
                            StreamAsyncWaitProductAdmissionTest.inputSchema()
                                    .toPortableSchema()));
            return new StreamSourceReader() {
                private final Semaphore wakeups = new Semaphore(0);
                private int next = request.hasRestoredPosition()
                        ? ByteBuffer.wrap(request.restoredPositionFrame())
                        .getInt() : 0;
                private int control = request.hasRestoredPosition() ? 1 : 0;

                @Override public StreamSourceRecord next() {
                    throw new AssertionError(
                            "control source must use nextEvent");
                }

                @Override public StreamSourceEvent nextEvent()
                        throws Exception {
                    if (next < 3) {
                        DataRow row = new DataRow(1);
                        row.set(0, (long) ++next);
                        return StreamSourceEvent.record(
                                StreamSourceRecord.of(
                                serializer.serialize(row),
                                ByteBuffer.allocate(4).putInt(next).array()));
                    }
                    if (control++ == 0) {
                        controlsEmitted.countDown();
                        return StreamSourceEvent.watermark(200L);
                    }
                    wakeups.acquire();
                    return null;
                }

                @Override public byte[] checkpointPositionFrame() {
                    return ByteBuffer.allocate(4).putInt(next).array();
                }

                @Override public void wakeUp() { wakeups.release(); }

                @Override public void cancel() { wakeups.release(); }

                @Override public void close() { wakeups.release(); }
            };
        }
    }

    private static final class AsyncSink implements EagerStreamSinkProvider {
        private final CountDownLatch received = new CountDownLatch(3);
        private final List<Long> values = new CopyOnWriteArrayList<>();
        @Override public String providerKey() { return "sink-v1"; }
        @Override public String contractDigest() {
            return StreamAsyncWaitProductAdmissionTest.digest("sink");
        }
        @Override public EagerStreamSinkWriter open(StreamSinkOpenRequest request) {
            RowSerializer serializer = new RowSerializer(
                    PortablePlanCompiler.toDataSchema(
                            StreamAsyncWaitProductAdmissionTest.outputSchema()
                                    .toPortableSchema()));
            return new EagerStreamSinkWriter() {
                @Override public synchronized void write(byte[] frame) {
                    values.add(serializer.deserialize(frame).getLong(0));
                    received.countDown();
                }
                @Override public void cancel() { }
                @Override public void close() { }
            };
        }
        private synchronized List<Long> ids() {
            return new ArrayList<>(values);
        }
    }
}
