package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.DataStream;
import io.rowweave.MapFunction;
import io.rowweave.DataRow;
import io.rowweave.StreamSession;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.runtime.EagerStreamSinkProvider;
import io.rowweave.stream.runtime.EagerStreamSinkWriter;
import io.rowweave.stream.runtime.DurableStreamSourceProvider;
import io.rowweave.stream.runtime.StreamSourceOpenRequest;
import io.rowweave.stream.runtime.StreamSourceReader;
import io.rowweave.stream.runtime.StreamSourceRecord;
import io.rowweave.stream.runtime.StreamSinkOpenRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Generic task command recovery and legacy role-shaped RPC rejection. */
class StreamRemoteWorkerCommandRecoveryTest {

    @Test
    @Timeout(5)
    void stalledControlConnectionCannotHeadOfLineBlockWorkerProfile()
            throws Exception {
        StreamWorkerCapabilityProfile profile = profile(
                "incarnation-concurrent-control");
        try (StreamRemoteWorkerServer server = new StreamRemoteWorkerServer(
                profile, Collections.emptyMap(), Collections.emptyMap());
             Socket stalled = new Socket()) {
            stalled.connect(new InetSocketAddress(
                    "127.0.0.1", server.port()), 5_000);
            Thread.sleep(100L);

            long started = System.nanoTime();
            RemoteStreamWorkerEndpoint endpoint =
                    RemoteStreamWorkerEndpoint.connect(
                            "127.0.0.1", server.port());
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - started);

            assertEquals(profile.workerId(), endpoint.profile().workerId());
            assertEquals(profile.workerIncarnation(),
                    endpoint.profile().workerIncarnation());
            assertTrue(elapsedMillis < 1_000L,
                    "one partial frame must not stall the Worker control plane");
        }
    }

    @Test
    void legacyRoleShapedStartsCannotBypassTheOperatorTaskLifecycle()
            throws Exception {
        PlanArtifact artifact = artifact();
        StreamTestGraph admitted = StreamTestGraph.from(artifact);
        StreamWorkerCapabilityProfile profile = profile("incarnation-1");
        try (StreamRemoteWorkerServer server = new StreamRemoteWorkerServer(
                profile, Collections.emptyMap(), Collections.singletonMap(
                "sink-v1", new NoOpSinkProvider()))) {
            byte[] command = command(StreamControlMessage.Kind.START,
                    1L, "master-1", profile.workerIncarnation(),
                    1L, admitted.commandDigest);
            byte[] artifactFrame = PlanArtifactCodec.encode(artifact);
            Map<String, Integer> fanout = new HashMap<>();
            fanout.put("sink", 19_001);
            fanout.put("sink-2", 19_002);
            java.util.List<StreamRemoteWorkerWire.Request> legacy =
                    Arrays.asList(
                            StreamRemoteWorkerWire.Request.startSink(
                                    command, artifactFrame,
                                    admitted.maximumEvents(), "sink"),
                            StreamRemoteWorkerWire.Request.startSinkTask(
                                    command, artifactFrame,
                                    admitted.maximumEvents(), "sink"),
                            StreamRemoteWorkerWire.Request.startSource(
                                    command, artifactFrame, 19_001,
                                    admitted.maximumEvents(), "sink"),
                            StreamRemoteWorkerWire.Request.startSourceFanout(
                                    command, artifactFrame, fanout,
                                    admitted.maximumEvents()));
            for (StreamRemoteWorkerWire.Request request : legacy) {
                StreamRemoteWorkerWire.Response response =
                        rpc(server.port(), request);
                assertEquals(StreamRemoteWorkerWire.ResponseKind.ERROR,
                        response.kind);
                assertEquals(StreamRemoteWorkerWire.ErrorCode.ADMISSION_REJECTED,
                        response.errorCode);
            }
        }
    }

    @Test
    void genericOperatorPrepareBindsItsInputLaneWithoutSinkShapeInference()
            throws Exception {
        PlanArtifact artifact = artifact();
        StreamTestGraph admitted = StreamTestGraph.from(artifact);
        StreamWorkerCapabilityProfile profile = profile("incarnation-generic");
        try (StreamRemoteWorkerServer server = new StreamRemoteWorkerServer(
                profile, Collections.emptyMap(), Collections.singletonMap(
                "sink-v1", new NoOpSinkProvider()))) {
            byte[] command = command(StreamControlMessage.Kind.START,
                    3L, "master-3", profile.workerIncarnation(),
                    1L, admitted.commandDigest);
            StreamRemoteWorkerWire.Response response = rpc(server.port(),
                    StreamRemoteWorkerWire.Request.prepareOperatorTask(
                            command, PlanArtifactCodec.encode(artifact),
                            admitted.maximumEvents(), "sink", 0,
                            Collections.singletonList(StreamTaskLaneSpec.input(
                                    "source#0->sink#0", 4_096, 16_384L)),
                            Collections.emptyList()));

            assertEquals(StreamRemoteWorkerWire.ResponseKind.TASK_ENDPOINTS,
                    response.kind);
            assertEquals(1, response.taskEndpoints.size());
            assertTrue(response.taskEndpoints.get(0).port() > 0);
            assertEquals("source#0->sink#0",
                    response.taskEndpoints.get(0).laneId());
        }
    }

    @Test
    void sameOperatorUidIsIsolatedByRunAndCancelScope() throws Exception {
        PlanArtifact artifact = artifact();
        StreamTestGraph admitted = StreamTestGraph.from(artifact);
        StreamWorkerCapabilityProfile profile = profile(
                "incarnation-multi-run", 2, 2, 131_072L, 4);
        try (StreamRemoteWorkerServer server = new StreamRemoteWorkerServer(
                profile, Collections.emptyMap(), Collections.singletonMap(
                "sink-v1", new NoOpSinkProvider()))) {
            byte[] runA = command(StreamControlMessage.Kind.START,
                    3L, "master-a", "run-a", profile.workerIncarnation(),
                    1L, admitted.commandDigest);
            byte[] runB = command(StreamControlMessage.Kind.START,
                    4L, "master-b", "run-b", profile.workerIncarnation(),
                    1L, admitted.commandDigest);
            StreamRemoteWorkerWire.Request first =
                    prepareSink(runA, artifact, admitted);
            StreamRemoteWorkerWire.Request second =
                    prepareSink(runB, artifact, admitted);
            assertEquals(StreamRemoteWorkerWire.ResponseKind.TASK_ENDPOINTS,
                    rpc(server.port(), first).kind);
            assertEquals(StreamRemoteWorkerWire.ResponseKind.TASK_ENDPOINTS,
                    rpc(server.port(), second).kind);

            byte[] staleRunA = command(StreamControlMessage.Kind.START,
                    2L, "master-stale", "run-a",
                    profile.workerIncarnation(), 1L,
                    admitted.commandDigest);
            StreamRemoteWorkerWire.Response stalePrepare = rpc(server.port(),
                    StreamRemoteWorkerWire.Request.prepareOperatorTask(
                            staleRunA, PlanArtifactCodec.encode(artifact),
                            admitted.maximumEvents(), "source", 0,
                            Collections.emptyList(),
                            Collections.emptyList()));
            assertEquals(StreamRemoteWorkerWire.ResponseKind.ERROR,
                    stalePrepare.kind);
            assertEquals(StreamRemoteWorkerWire.ErrorCode.STALE_FENCE,
                    stalePrepare.errorCode);

            byte[] cancelA = command(StreamControlMessage.Kind.CANCEL,
                    3L, "master-a", "run-a", profile.workerIncarnation(),
                    2L, admitted.commandDigest);
            assertEquals(StreamRemoteWorkerWire.ResponseKind.ACK,
                    rpc(server.port(),
                            StreamRemoteWorkerWire.Request.cancel(cancelA)).kind);
            StreamRemoteWorkerWire.Response runBStatus = rpc(server.port(),
                    StreamRemoteWorkerWire.Request.status(
                            runB, "sink", 0));
            assertEquals(StreamRemoteWorkerWire.ResponseKind.TASK_STATUS,
                    runBStatus.kind);
            assertTrue(!runBStatus.taskFailed);
            assertEquals(StreamRemoteWorkerWire.ResponseKind.ERROR,
                    rpc(server.port(), StreamRemoteWorkerWire.Request.status(
                            runA, "sink", 0)).kind);
        }
    }

    @Test
    void twoRunningJobsWithSameTaskIdentitiesDoNotCrossCancel()
            throws Exception {
        PlanArtifact artifact = artifact();
        StreamTestGraph admitted = StreamTestGraph.from(artifact);
        StreamWorkerCapabilityProfile profile = profile(
                "incarnation-live-runs", 4, 4, 262_144L, 16);
        IsolatedBlockingSource source = new IsolatedBlockingSource();
        try (StreamRemoteWorkerServer server = new StreamRemoteWorkerServer(
                profile, Collections.singletonMap("source-v1", source),
                Collections.singletonMap("sink-v1",
                        new NoOpSinkProvider()))) {
            RemoteStreamWorkerEndpoint endpoint =
                    RemoteStreamWorkerEndpoint.connect(
                            "127.0.0.1", server.port());
            Map<String, RemoteStreamWorkerEndpoint> workers =
                    Collections.singletonMap(profile.workerId(), endpoint);
            StreamWorkerPlacement placement = new StreamWorkerPlacement(
                    Arrays.asList(
                            new StreamWorkerPlacement.Assignment(
                                    0, "source", 0, profile.workerId(),
                                    profile.workerIncarnation()),
                            new StreamWorkerPlacement.Assignment(
                                    1, "sink", 0, profile.workerId(),
                                    profile.workerIncarnation())));
            byte[] artifactFrame = PlanArtifactCodec.encode(artifact);
            byte[] runA = command(StreamControlMessage.Kind.START,
                    3L, "master-a", "run-live-a",
                    profile.workerIncarnation(), 1L,
                    admitted.commandDigest);
            byte[] runB = command(StreamControlMessage.Kind.START,
                    2L, "master-b", "run-live-b",
                    profile.workerIncarnation(), 1L,
                    admitted.commandDigest);
            StreamGenericTaskDeployment.deploy(admitted.product,
                    artifactFrame, placement, workers,
                    admitted.maximumEvents(), ignored -> runA);
            StreamGenericTaskDeployment.deploy(admitted.product,
                    artifactFrame, placement, workers,
                    admitted.maximumEvents(), ignored -> runB);
            assertTrue(source.awaitOpened(2, 5, TimeUnit.SECONDS));

            endpoint.cancel(command(StreamControlMessage.Kind.CANCEL,
                    3L, "master-a", "run-live-a",
                    profile.workerIncarnation(), 2L,
                    admitted.commandDigest));
            assertTrue(!endpoint.taskFailed(runB));
            assertEquals(1, source.activeReaders());

            endpoint.cancel(command(StreamControlMessage.Kind.CANCEL,
                    2L, "master-b", "run-live-b",
                    profile.workerIncarnation(), 2L,
                    admitted.commandDigest));
            assertTrue(source.awaitClosed(2, 5, TimeUnit.SECONDS));
        }
    }

    @Test
    void preparedTasksLeaseCapacityAcrossRuns() throws Exception {
        PlanArtifact artifact = artifact();
        StreamTestGraph admitted = StreamTestGraph.from(artifact);
        StreamWorkerCapabilityProfile profile = profile(
                "incarnation-one-slot", 1, 1, 65_536L, 2);
        try (StreamRemoteWorkerServer server = new StreamRemoteWorkerServer(
                profile, Collections.emptyMap(), Collections.singletonMap(
                "sink-v1", new NoOpSinkProvider()))) {
            byte[] runA = command(StreamControlMessage.Kind.START,
                    3L, "master-a", "run-a", profile.workerIncarnation(),
                    1L, admitted.commandDigest);
            byte[] runB = command(StreamControlMessage.Kind.START,
                    4L, "master-b", "run-b", profile.workerIncarnation(),
                    1L, admitted.commandDigest);
            assertEquals(StreamRemoteWorkerWire.ResponseKind.TASK_ENDPOINTS,
                    rpc(server.port(), prepareSink(
                            runA, artifact, admitted)).kind);
            StreamRemoteWorkerWire.Response rejected = rpc(server.port(),
                    prepareSink(runB, artifact, admitted));
            assertEquals(StreamRemoteWorkerWire.ResponseKind.ERROR,
                    rejected.kind);
            assertEquals(StreamRemoteWorkerWire.ErrorCode.ADMISSION_REJECTED,
                    rejected.errorCode);
            byte[] cancelA = command(StreamControlMessage.Kind.CANCEL,
                    3L, "master-a", "run-a", profile.workerIncarnation(),
                    2L, admitted.commandDigest);
            assertEquals(StreamRemoteWorkerWire.ResponseKind.ACK,
                    rpc(server.port(), StreamRemoteWorkerWire.Request.cancel(
                            cancelA)).kind);
            assertEquals(StreamRemoteWorkerWire.ResponseKind.TASK_ENDPOINTS,
                    rpc(server.port(), prepareSink(
                            runB, artifact, admitted)).kind);
        }
    }

    private static StreamRemoteWorkerWire.Request prepareSink(
            byte[] command, PlanArtifact artifact,
            StreamTestGraph admitted) {
        return StreamRemoteWorkerWire.Request.prepareOperatorTask(
                command, PlanArtifactCodec.encode(artifact),
                admitted.maximumEvents(), "sink", 0,
                Collections.singletonList(StreamTaskLaneSpec.input(
                        "source#0->sink#0", 4_096, 4_096L)),
                Collections.emptyList());
    }

    @Test
    void genericDeploymentRunsSourceAndSinkOnDifferentWorkers()
            throws Exception {
        PlanArtifact artifact = artifact();
        StreamTestGraph admitted = StreamTestGraph.from(artifact);
        CountDownLatch received = new CountDownLatch(1);
        StreamWorkerCapabilityProfile sourceProfile = profile("inc-source");
        StreamWorkerCapabilityProfile sinkProfile = new StreamWorkerCapabilityProfile(
                "worker-sink", "inc-sink", 2, 1,
                Arrays.asList(PortableStreamPlan.STREAM_CAPABILITY,
                        PortableStreamPlan.NODE_EXECUTION_CONTRACT_CAPABILITY,
                        StreamWorkerCapabilityProfile.CONTROL_CAPABILITY,
                        StreamWorkerCapabilityProfile.DATA_CAPABILITY,
                        StreamWorkerCapabilityProfile
                                .CHECKPOINT_TRANSFER_CAPABILITY),
                1, 1, 4_096, 65_536L, 4);
        try (StreamRemoteWorkerServer sourceServer =
                     new StreamRemoteWorkerServer(sourceProfile,
                             Collections.singletonMap("source-v1",
                                     new OneRecordSource()),
                             Collections.emptyMap());
             StreamRemoteWorkerServer sinkServer =
                     new StreamRemoteWorkerServer(sinkProfile,
                             Collections.emptyMap(),
                             Collections.singletonMap("sink-v1",
                                     new LatchingSink(received)))) {
            Map<String, RemoteStreamWorkerEndpoint> workers = new HashMap<>();
            RemoteStreamWorkerEndpoint sourceEndpoint =
                    RemoteStreamWorkerEndpoint.connect(
                            "127.0.0.1", sourceServer.port());
            RemoteStreamWorkerEndpoint sinkEndpoint =
                    RemoteStreamWorkerEndpoint.connect(
                            "127.0.0.1", sinkServer.port());
            workers.put("worker-a", sourceEndpoint);
            workers.put("worker-b", sinkEndpoint);
            StreamWorkerPlacement placement = new StreamWorkerPlacement(
                    Arrays.asList(
                            new StreamWorkerPlacement.Assignment(
                                    0, "source", 0, "worker-a", "inc-source"),
                            new StreamWorkerPlacement.Assignment(
                                    1, "sink", 0, "worker-b", "inc-sink")));
            byte[] sourceCommand = command(
                    StreamControlMessage.Kind.START, 5L, "master-5",
                    "inc-source", 1L, admitted.commandDigest);
            byte[] sinkCommand = command(
                    StreamControlMessage.Kind.START, 5L, "master-5",
                    "inc-sink", 1L, admitted.commandDigest);
            StreamGenericTaskDeployment.deploy(admitted.product,
                    PlanArtifactCodec.encode(artifact), placement, workers,
                    admitted.maximumEvents(), assignment ->
                            "source".equals(assignment.operatorUid())
                                    ? sourceCommand : sinkCommand);
            assertTrue(received.await(5, TimeUnit.SECONDS));
            StreamTaskProgress sourceProgress = sourceEndpoint.taskProgress(
                    sourceCommand, "source", 0);
            StreamTaskProgress sinkProgress = sinkEndpoint.taskProgress(
                    sinkCommand, "sink", 0);
            assertEquals(1L, sourceProgress.recordsRead());
            assertEquals(1L, sinkProgress.recordsWritten());
            assertEquals(1L, sourceProgress.recordsWritten());
            assertEquals(1L, sinkProgress.recordsRead());
            assertTrue(sourceProgress.complete());
            assertTrue(sinkProgress.complete());
        }
    }

    @Test
    void genericDeploymentRunsTwoComputeNodesOnOneWorker()
            throws Exception {
        PlanArtifact artifact = twoMapArtifact();
        StreamTestGraph admitted = StreamTestGraph.from(artifact);
        String deploymentCapability = PlanArtifact.DEPLOYMENT_FEATURE_PREFIX
                + sha256("generic-deployment-set");
        java.util.List<String> capabilities = Arrays.asList(
                PortableStreamPlan.STREAM_CAPABILITY,
                PortableStreamPlan.NODE_EXECUTION_CONTRACT_CAPABILITY,
                StreamWorkerCapabilityProfile.CONTROL_CAPABILITY,
                StreamWorkerCapabilityProfile.DATA_CAPABILITY,
                StreamWorkerCapabilityProfile.CHECKPOINT_TRANSFER_CAPABILITY,
                deploymentCapability);
        StreamWorkerCapabilityProfile sourceProfile = new StreamWorkerCapabilityProfile(
                "generic-source", "generic-inc-source", 2, 1, capabilities,
                1, 4, 4_096, 1_048_576L, 16);
        StreamWorkerCapabilityProfile computeProfile = new StreamWorkerCapabilityProfile(
                "generic-compute", "generic-inc-compute", 2, 1, capabilities,
                2, 8, 4_096, 1_048_576L, 32);
        StreamWorkerCapabilityProfile sinkProfile = new StreamWorkerCapabilityProfile(
                "generic-sink", "generic-inc-sink", 2, 1, capabilities,
                1, 4, 4_096, 1_048_576L, 16);
        try (StreamRemoteWorkerServer sourceServer =
                     new StreamRemoteWorkerServer(sourceProfile,
                             Collections.singletonMap("source-v1",
                                     new BlockingSource()),
                             Collections.emptyMap());
             StreamRemoteWorkerServer computeServer =
                     new StreamRemoteWorkerServer(computeProfile,
                             Collections.emptyMap(), Collections.emptyMap(),
                             reference -> new IdentityMap());
             StreamRemoteWorkerServer sinkServer =
                     new StreamRemoteWorkerServer(sinkProfile,
                             Collections.emptyMap(),
                             Collections.singletonMap("sink-v1",
                                     new NoOpSinkProvider()))) {
            Map<String, RemoteStreamWorkerEndpoint> workers = new HashMap<>();
            workers.put("generic-source", RemoteStreamWorkerEndpoint.connect(
                    "127.0.0.1", sourceServer.port()));
            workers.put("generic-compute", RemoteStreamWorkerEndpoint.connect(
                    "127.0.0.1", computeServer.port()));
            workers.put("generic-sink", RemoteStreamWorkerEndpoint.connect(
                    "127.0.0.1", sinkServer.port()));
            StreamWorkerPlacement placement = new StreamWorkerPlacement(
                    Arrays.asList(
                            new StreamWorkerPlacement.Assignment(
                                    0, "source", 0, "generic-source",
                                    "generic-inc-source"),
                            new StreamWorkerPlacement.Assignment(
                                    1, "map-a", 0, "generic-compute",
                                    "generic-inc-compute"),
                            new StreamWorkerPlacement.Assignment(
                                    2, "map-b", 0, "generic-compute",
                                    "generic-inc-compute"),
                            new StreamWorkerPlacement.Assignment(
                                    3, "sink", 0, "generic-sink",
                                    "generic-inc-sink")));
            java.util.List<StreamTaskIdentity> started =
                    StreamGenericTaskDeployment.deploy(admitted.product,
                            PlanArtifactCodec.encode(artifact), placement,
                            workers, admitted.maximumEvents(), assignment ->
                            command(StreamControlMessage.Kind.START, 7L,
                                    "generic-master-7",
                                    assignment.workerIncarnation(), 1L,
                                    admitted.commandDigest));
            assertEquals(4, started.size());
            workers.get("generic-source").cancel(command(
                    StreamControlMessage.Kind.CANCEL, 7L, "generic-master-7",
                    "generic-inc-source", 2L, admitted.commandDigest));
            workers.get("generic-compute").cancel(command(
                    StreamControlMessage.Kind.CANCEL, 7L, "generic-master-7",
                    "generic-inc-compute", 2L, admitted.commandDigest));
            workers.get("generic-sink").cancel(command(
                    StreamControlMessage.Kind.CANCEL, 7L, "generic-master-7",
                    "generic-inc-sink", 2L, admitted.commandDigest));
        }
    }

    private static StreamRemoteWorkerWire.Response rpc(int port,
            StreamRemoteWorkerWire.Request request) throws Exception {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", port), 5_000);
            DataOutputStream output = new DataOutputStream(
                    new BufferedOutputStream(socket.getOutputStream()));
            DataInputStream input = new DataInputStream(
                    new BufferedInputStream(socket.getInputStream()));
            StreamRemoteWorkerWire.writeRequest(output, request);
            return StreamRemoteWorkerWire.readResponse(input);
        }
    }

    private static byte[] command(StreamControlMessage.Kind kind,
            long term, String master, String workerIncarnation,
            long sequence, String digest) {
        return command(kind, term, master, "run-command-recovery",
                workerIncarnation, sequence, digest);
    }

    private static byte[] command(StreamControlMessage.Kind kind,
            long term, String master, String runId,
            String workerIncarnation, long sequence, String digest) {
        return StreamControlMessageCodec.encode(new StreamControlMessage(
                kind, term, master, runId, "attempt-1",
                1L, 1L, workerIncarnation, sequence, digest,
                kind == StreamControlMessage.Kind.START
                        ? ByteBuffer.allocate(8).putLong(1L).array()
                        : new byte[0]));
    }

    private static StreamWorkerCapabilityProfile profile(String incarnation) {
        return profile(incarnation, 1, 1, 65_536L, 4);
    }

    private static StreamWorkerCapabilityProfile profile(
            String incarnation, int slots, int channels,
            long residentBytes, int threads) {
        return new StreamWorkerCapabilityProfile(
                "worker-sink", incarnation, 2, 1,
                Arrays.asList(PortableStreamPlan.STREAM_CAPABILITY,
                        PortableStreamPlan.NODE_EXECUTION_CONTRACT_CAPABILITY,
                        StreamWorkerCapabilityProfile.CONTROL_CAPABILITY,
                        StreamWorkerCapabilityProfile.DATA_CAPABILITY,
                        StreamWorkerCapabilityProfile
                                .CHECKPOINT_TRANSFER_CAPABILITY),
                slots, channels, 4_096, residentBytes, threads);
    }

    private static PlanArtifact artifact() {
        StreamSession session = RowWeave.createStreamSession("command-recovery");
        session.setResourceProfile(new StreamResourceProfile(
                8, 1_024, 4_096, 0L, 0, 0, 0L, 4, 16, 0L));
        StreamSchema schema = new StreamSchema(Collections.singletonList(
                new StreamField("id", "id", "id",
                        DataType.StringType, false)));
        session.fromSource(StreamSourceSpec.exactOrdered(
                schema, "source-v1", sha256("source-contract"),
                new byte[] {1}, Collections.singletonList(
                new StreamSplitSpec(sha256("split"),
                        "sequence-v1", new byte[] {2}))))
                .uid("source")
                .sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                        "sink-v1", sha256("sink-contract"),
                        new byte[] {3}));
        return session.toPlanArtifact();
    }

    private static PlanArtifact twoMapArtifact() {
        StreamSession session = RowWeave.createStreamSession("generic-two-map");
        session.setResourceProfile(new StreamResourceProfile(
                8, 1_024, 4_096, 0L, 0, 0, 0L, 8, 32, 0L));
        StreamSchema schema = new StreamSchema(Collections.singletonList(
                new StreamField("value", "value", "value",
                        DataType.StringType, false)));
        FunctionDeploymentReference mapper = new FunctionDeploymentReference(
                sha256("generic-deployment-set"), "identity-map",
                sha256("generic-map-jar"), "java.lang.String",
                FunctionAbi.MAP_V1, "generic-map-config-v1",
                sha256("identity-map"), "identity-map".getBytes(
                        StandardCharsets.UTF_8),
                new FunctionResourceProfile(1_024L, 0L, 1));
        DataStream source = session.fromSource(StreamSourceSpec.exactOrdered(
                schema, "source-v1", sha256("source-contract"),
                new byte[] {1}, Collections.singletonList(
                new StreamSplitSpec(sha256("generic-split"),
                        "sequence-v1", new byte[] {2})))).uid("source");
        DataStream first = source.map(mapper, schema).uid("map-a");
        first.map(mapper, schema).uid("map-b")
                .sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                        "sink-v1", sha256("sink-contract"),
                        new byte[] {3}));
        return session.toPlanArtifact();
    }

    private static String sha256(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                    value.getBytes(StandardCharsets.UTF_8));
            char[] alphabet = "0123456789abcdef".toCharArray();
            char[] result = new char[digest.length * 2];
            for (int index = 0; index < digest.length; index++) {
                int current = digest[index] & 0xff;
                result[index * 2] = alphabet[current >>> 4];
                result[index * 2 + 1] = alphabet[current & 0x0f];
            }
            return new String(result);
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static final class NoOpSinkProvider
            implements EagerStreamSinkProvider {
        @Override public String providerKey() { return "sink-v1"; }
        @Override public String contractDigest() { return sha256("sink-contract"); }
        @Override public EagerStreamSinkWriter open(StreamSinkOpenRequest request) {
            return new EagerStreamSinkWriter() {
                @Override public void write(byte[] recordFrame) { }
                @Override public void cancel() { }
                @Override public void close() { }
            };
        }
    }

    private static final class OneRecordSource
            implements DurableStreamSourceProvider {
        @Override public String providerKey() { return "source-v1"; }
        @Override public String contractDigest() { return sha256("source-contract"); }
        @Override public StreamSourceReader open(StreamSourceOpenRequest request) {
            return new StreamSourceReader() {
                private boolean emitted;
                @Override public StreamSourceRecord next() {
                    if (emitted) return null;
                    emitted = true;
                    return StreamSourceRecord.of(new byte[] {1}, new byte[] {1});
                }
                @Override public void cancel() { }
                @Override public void close() { }
            };
        }
    }

    private static final class BlockingSource
            implements DurableStreamSourceProvider {
        private final CountDownLatch cancelled = new CountDownLatch(1);
        @Override public String providerKey() { return "source-v1"; }
        @Override public String contractDigest() { return sha256("source-contract"); }
        @Override public StreamSourceReader open(StreamSourceOpenRequest request) {
            return new StreamSourceReader() {
                @Override public StreamSourceRecord next() throws Exception {
                    cancelled.await();
                    return null;
                }
                @Override public void cancel() { cancelled.countDown(); }
                @Override public void close() { cancelled.countDown(); }
            };
        }
    }

    private static final class IsolatedBlockingSource
            implements DurableStreamSourceProvider {
        private final java.util.concurrent.atomic.AtomicInteger opened =
                new java.util.concurrent.atomic.AtomicInteger();
        private final java.util.concurrent.atomic.AtomicInteger active =
                new java.util.concurrent.atomic.AtomicInteger();
        private final java.util.concurrent.atomic.AtomicInteger closed =
                new java.util.concurrent.atomic.AtomicInteger();

        @Override public String providerKey() { return "source-v1"; }
        @Override public String contractDigest() {
            return sha256("source-contract");
        }

        @Override public StreamSourceReader open(
                StreamSourceOpenRequest request) {
            opened.incrementAndGet();
            active.incrementAndGet();
            CountDownLatch released = new CountDownLatch(1);
            java.util.concurrent.atomic.AtomicBoolean finished =
                    new java.util.concurrent.atomic.AtomicBoolean();
            return new StreamSourceReader() {
                @Override public StreamSourceRecord next() throws Exception {
                    released.await();
                    return null;
                }

                @Override public void cancel() {
                    finish();
                }

                @Override public void close() {
                    finish();
                }

                private void finish() {
                    if (finished.compareAndSet(false, true)) {
                        active.decrementAndGet();
                        closed.incrementAndGet();
                    }
                    released.countDown();
                }
            };
        }

        int activeReaders() { return active.get(); }

        boolean awaitOpened(int expected, long timeout, TimeUnit unit)
                throws InterruptedException {
            return awaitAtLeast(opened, expected, timeout, unit);
        }

        boolean awaitClosed(int expected, long timeout, TimeUnit unit)
                throws InterruptedException {
            return awaitAtLeast(closed, expected, timeout, unit);
        }

        private static boolean awaitAtLeast(
                java.util.concurrent.atomic.AtomicInteger value,
                int expected, long timeout, TimeUnit unit)
                throws InterruptedException {
            long deadline = System.nanoTime() + unit.toNanos(timeout);
            while (value.get() < expected
                    && System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }
            return value.get() >= expected;
        }
    }

    private static final class IdentityMap implements MapFunction {
        @Override public DataRow map(DataRow row) { return row; }
    }

    private static final class LatchingSink implements EagerStreamSinkProvider {
        private final CountDownLatch received;
        private LatchingSink(CountDownLatch received) { this.received = received; }
        @Override public String providerKey() { return "sink-v1"; }
        @Override public String contractDigest() { return sha256("sink-contract"); }
        @Override public EagerStreamSinkWriter open(StreamSinkOpenRequest request) {
            return new EagerStreamSinkWriter() {
                @Override public void write(byte[] recordFrame) { received.countDown(); }
                @Override public void cancel() { }
                @Override public void close() { }
            };
        }
    }
}
