package io.rowweave.stream.cluster;

import io.rowweave.DataRow;
import io.rowweave.StreamAsyncFunction;
import io.rowweave.StreamAsyncRequest;
import io.rowweave.StreamCollector;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.mem.RowSerializer;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.runtime.DurableStreamSourceProvider;
import io.rowweave.stream.runtime.EagerStreamSinkProvider;
import io.rowweave.stream.runtime.EagerStreamSinkWriter;
import io.rowweave.stream.runtime.StreamSinkOpenRequest;
import io.rowweave.stream.runtime.StreamSourceOpenRequest;
import io.rowweave.stream.runtime.StreamSourceReader;
import io.rowweave.stream.runtime.StreamSourceRecord;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;

/** Independent Worker JVM fixture for async-wait admission. */
public final class StreamProductAsyncWaitWorkerProcessFixture {
    static final String SOURCE_PROVIDER = "async-wait-source-v1";
    static final String SINK_PROVIDER = "async-wait-sink-v1";
    static final String SOURCE_CONTRACT =
            StreamAsyncWaitProductAdmissionTest.digest("cluster-source");
    static final String SINK_CONTRACT =
            StreamAsyncWaitProductAdmissionTest.digest("cluster-sink");
    private static final Object SINK_LOCK = new Object();

    private StreamProductAsyncWaitWorkerProcessFixture() { }

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 2) {
            throw new IllegalArgumentException(
                    "role and exchange directory required");
        }
        String role = arguments[0];
        Path exchange = Paths.get(arguments[1]);
        List<String> capabilities = new ArrayList<>(Arrays.asList(
                PortableStreamPlan.STREAM_CAPABILITY,
                PortableStreamPlan.NODE_EXECUTION_CONTRACT_CAPABILITY,
                PortableStreamPlan.ASYNC_WAIT_CAPABILITY,
                StreamWorkerCapabilityProfile.CONTROL_CAPABILITY,
                StreamWorkerCapabilityProfile.DATA_CAPABILITY,
                StreamWorkerCapabilityProfile
                        .CHECKPOINT_TRANSFER_CAPABILITY,
                PlanArtifact.DEPLOYMENT_FEATURE_PREFIX
                        + StreamAsyncWaitProductAdmissionTest.function()
                        .deploymentSetId()));
        StreamWorkerCapabilityProfile profile =
                new StreamWorkerCapabilityProfile(
                        "async-wait-worker-" + role,
                        "async-wait-incarnation-" + role,
                        2, 1, capabilities, 8, 16,
                        8_192, 8_388_608L, 32);
        DurableStreamSourceProvider source = new Source();
        EagerStreamSinkProvider sink = new Sink(exchange);
        try (StreamRemoteWorkerServer server = new StreamRemoteWorkerServer(
                profile,
                "source".equals(role)
                        ? Collections.singletonMap(
                        source.providerKey(), source)
                        : Collections.emptyMap(),
                "sink".equals(role)
                        ? Collections.singletonMap(sink.providerKey(), sink)
                        : Collections.emptyMap(),
                ignored -> new Lookup(),
                exchange.resolve("async-wait-checkpoints"))) {
            Files.write(exchange.resolve("async-wait-" + role + ".port"),
                    Integer.toString(server.port()).getBytes(
                            StandardCharsets.US_ASCII));
            while (!Files.exists(exchange.resolve("async-wait-shutdown"))) {
                Thread.sleep(5L);
            }
        }
    }

    private static final class Source
            implements DurableStreamSourceProvider {
        @Override public String providerKey() { return SOURCE_PROVIDER; }
        @Override public String contractDigest() { return SOURCE_CONTRACT; }

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
                                position());
                    }
                    wakeups.acquire();
                    return null;
                }

                @Override public byte[] checkpointPositionFrame() {
                    return position();
                }

                private byte[] position() {
                    return ByteBuffer.allocate(Integer.BYTES)
                            .putInt(next).array();
                }

                @Override public void wakeUp() { wakeups.release(); }
                @Override public void cancel() { wakeups.release(); }
                @Override public void close() { wakeups.release(); }
            };
        }
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

    private static final class Sink implements EagerStreamSinkProvider {
        private final Path result;

        private Sink(Path exchange) {
            this.result = exchange.resolve("async-wait-result");
        }

        @Override public String providerKey() { return SINK_PROVIDER; }
        @Override public String contractDigest() { return SINK_CONTRACT; }

        @Override public EagerStreamSinkWriter open(
                StreamSinkOpenRequest request) {
            RowSerializer serializer = new RowSerializer(
                    PortablePlanCompiler.toDataSchema(
                            StreamAsyncWaitProductAdmissionTest.outputSchema()
                                    .toPortableSchema()));
            return new EagerStreamSinkWriter() {
                @Override public void write(byte[] frame) throws Exception {
                    DataRow row = serializer.deserialize(frame);
                    String line = row.getLong(0) + ":" + row.getString(1)
                            + '\n';
                    synchronized (SINK_LOCK) {
                        Files.write(result, line.getBytes(StandardCharsets.UTF_8),
                                StandardOpenOption.CREATE,
                                StandardOpenOption.APPEND);
                    }
                }

                @Override public void cancel() { }
                @Override public void close() { }
            };
        }
    }
}
