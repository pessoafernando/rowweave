package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataStream;
import io.rowweave.DataType;
import io.rowweave.DataRow;
import io.rowweave.StreamJobClient;
import io.rowweave.StreamCheckpointHandle;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamSession;
import io.rowweave.mem.RowSerializer;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.PortableDataType;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamCheckpointConfig;
import io.rowweave.stream.api.StreamCalcSpec;
import io.rowweave.stream.api.StreamEmissionLimit;
import io.rowweave.stream.api.StreamEventTimeSpec;
import io.rowweave.stream.api.StreamExpression;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.api.StreamTimeMode;
import io.rowweave.test.ForkedJvm;
import io.rowweave.stream.contract.CheckpointEvidence;
import io.rowweave.stream.runtime.FileStreamSavepointStore;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** public stateless functions execute in the source Worker JVM. */
@Tag("multiprocess")
class StreamProductStatelessDagDispatchTest {

    @Test
    void publicClusterPreservesDuplicateSelfUnionEdges(
            @TempDir Path exchange) throws Exception {
        Process source = launch("source", exchange);
        Process sink = launch("self-union-sink", exchange);
        try {
            awaitFile(exchange.resolve("stateless-source.port"), 5_000L);
            awaitFile(exchange.resolve(
                    "stateless-self-union-sink.port"), 5_000L);
            RemoteStreamSourceSinkRunFactory runs =
                    new RemoteStreamSourceSinkRunFactory(
                            exchange.resolve("self-union-run-authority"), 1L,
                            "self-union-master-incarnation",
                            Arrays.asList(endpoint(exchange, "source"),
                                    endpoint(exchange, "self-union-sink")),
                            exchange.resolve("stateless-checkpoint-store"));
            try (StreamProductJobMasterServer master =
                         new StreamProductJobMasterServer(
                                 exchange.resolve("self-union-job-authority"),
                                 runs);
                 StreamJobClient client = RowWeave.createRealClusterStreamJobClient(
                         "127.0.0.1", master.port())) {
                StreamJobHandle job = selfUnionSession(client).submit(
                        StreamSubmissionId.of("self-union-submit"));
                assertEquals(StreamJobStatus.RUNNING,
                        job.awaitRunning(5_000L));
                awaitFile(exchange.resolve(
                        "stateless-self-union-result"), 10_000L);
                assertEquals("2000:999000", text(exchange.resolve(
                        "stateless-self-union-result")));
                StreamCheckpointHandle checkpoint = job.triggerCheckpoint(
                        StreamCommandId.of("self-union-checkpoint"));
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        checkpoint.awaitCompletion(5_000L),
                        () -> log(exchange, "source") + '\n'
                                + log(exchange, "self-union-sink"));
                assertEquals(1L, job.progress().completedCheckpoints());
                job.cancel(StreamCommandId.of("cancel-self-union"))
                        .awaitCompletion(5_000L);
            }
        } finally {
            Files.write(exchange.resolve("stateless-shutdown"), new byte[] {1});
            assertExit(source, exchange, "source");
            assertExit(sink, exchange, "self-union-sink");
        }
    }

    @Test
    void publicClusterExecutesTwoSourceUnionThroughOneInputGate(
            @TempDir Path exchange) throws Exception {
        Process source = launch("union-source", exchange);
        Process sink = launch("union-sink", exchange);
        try {
            awaitFile(exchange.resolve("stateless-union-source.port"), 5_000L);
            awaitFile(exchange.resolve("stateless-union-sink.port"), 5_000L);
            RemoteStreamSourceSinkRunFactory runs =
                    new RemoteStreamSourceSinkRunFactory(
                            exchange.resolve("union-run-authority"), 1L,
                            "union-master-incarnation",
                            Arrays.asList(endpoint(exchange, "union-source"),
                                    endpoint(exchange, "union-sink")),
                            exchange.resolve("stateless-checkpoint-store"));
            try (StreamProductJobMasterServer master =
                         new StreamProductJobMasterServer(
                                 exchange.resolve("union-job-authority"), runs);
                 StreamJobClient client = RowWeave.createRealClusterStreamJobClient(
                         "127.0.0.1", master.port())) {
                StreamJobHandle job = unionSession(client).submit(
                        StreamSubmissionId.of("union-runtime-submit"));
                assertEquals(StreamJobStatus.RUNNING,
                        job.awaitRunning(5_000L));
                awaitFile(exchange.resolve("stateless-union-result"), 10_000L);
                assertEquals("500:124750", text(
                        exchange.resolve("stateless-union-result")));
                StreamCheckpointHandle checkpoint = job.triggerCheckpoint(
                        StreamCommandId.of("union-checkpoint"));
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        checkpoint.awaitCompletion(5_000L));
                assertEquals(1L, job.progress().completedCheckpoints());
                job.cancel(StreamCommandId.of("cancel-union-runtime"))
                        .awaitCompletion(5_000L);
            }
        } finally {
            Files.write(exchange.resolve("stateless-shutdown"), new byte[] {1});
            assertExit(source, exchange, "union-source");
            assertExit(sink, exchange, "union-sink");
        }
    }

    @Test
    void publicClusterPersistsEveryPhysicalLaneInUnalignedUnionCut(
            @TempDir Path exchange) throws Exception {
        Process source = launch("union-source", exchange);
        Process sink = launch("union-sink", exchange);
        try {
            awaitFile(exchange.resolve("stateless-union-source.port"), 5_000L);
            awaitFile(exchange.resolve("stateless-union-sink.port"), 5_000L);
            Path checkpoints = exchange.resolve(
                    "stateless-unaligned-checkpoint-store");
            RemoteStreamSourceSinkRunFactory runs =
                    new RemoteStreamSourceSinkRunFactory(
                            exchange.resolve("unaligned-union-run-authority"),
                            1L, "unaligned-union-master-incarnation",
                            Arrays.asList(endpoint(exchange, "union-source"),
                                    endpoint(exchange, "union-sink")),
                            checkpoints);
            try (StreamProductJobMasterServer master =
                         new StreamProductJobMasterServer(
                                 exchange.resolve(
                                         "unaligned-union-job-authority"),
                                 runs);
                 StreamJobClient client = RowWeave.createRealClusterStreamJobClient(
                         "127.0.0.1", master.port())) {
                StreamSession session = unionSession(client);
                session.setCheckpointConfig(
                        StreamCheckpointConfig.manualOnlyUnaligned(
                                5_000L, 2));
                StreamJobHandle job = session.submit(
                        StreamSubmissionId.of(
                                "unaligned-union-runtime-submit"));
                assertEquals(StreamJobStatus.RUNNING,
                        job.awaitRunning(5_000L));
                awaitFile(exchange.resolve("stateless-union-result"), 10_000L);
                StreamCheckpointHandle checkpoint = job.triggerCheckpoint(
                        StreamCommandId.of("unaligned-union-checkpoint"));
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        checkpoint.awaitCompletion(5_000L));
                assertEquals(3L, new FileStreamSavepointStore(checkpoints)
                        .get(checkpoint.checkpointId()).checkpointManifest()
                        .taskReceipts().stream()
                        .flatMap(receipt -> receipt.evidence().stream())
                        .filter(evidence -> evidence.kind()
                                == CheckpointEvidence.Kind.CHANNEL_STATE)
                        .count());
                job.cancel(StreamCommandId.of(
                                "cancel-unaligned-union-runtime"))
                        .awaitCompletion(5_000L);
            }
        } finally {
            Files.write(exchange.resolve("stateless-shutdown"),
                    new byte[] {1});
            assertExit(source, exchange, "union-source");
            assertExit(sink, exchange, "union-sink");
        }
    }

    @Test
    void publicClusterExecutesAtomicGraphAndCheckpointsAssignedEventTime(
            @TempDir Path exchange) throws Exception {
        Process source = launch("source", exchange);
        Process sink = launch("sink", exchange);
        try {
            awaitFile(exchange.resolve("stateless-source.port"), 5_000L);
            awaitFile(exchange.resolve("stateless-sink.port"), 5_000L);
            RemoteStreamSourceSinkRunFactory runs =
                    new RemoteStreamSourceSinkRunFactory(
                            exchange.resolve("atomic-run-authority"), 1L,
                            "atomic-master-incarnation",
                            Arrays.asList(endpoint(exchange, "source"),
                                    endpoint(exchange, "sink")),
                            exchange.resolve("stateless-checkpoint-store"));
            try (StreamProductJobMasterServer master =
                         new StreamProductJobMasterServer(
                                 exchange.resolve("atomic-job-authority"), runs);
                 StreamJobClient client = RowWeave.createRealClusterStreamJobClient(
                         "127.0.0.1", master.port())) {
                StreamJobHandle job = atomicSession(client).submit(
                        StreamSubmissionId.of("atomic-stateless-submit"));

                assertEquals(StreamJobStatus.RUNNING,
                        job.awaitRunning(5_000L));
                awaitFile(exchange.resolve("stateless-result"), 10_000L);
                assertEquals(expectedAtomicResult(), text(
                        exchange.resolve("stateless-result")));
                StreamCheckpointHandle checkpoint = job.triggerCheckpoint(
                        StreamCommandId.of("atomic-checkpoint"));
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        checkpoint.awaitCompletion(5_000L));
                assertEquals(1L, job.progress().completedCheckpoints());
                job.cancel(StreamCommandId.of("cancel-atomic-stateless"))
                        .awaitCompletion(5_000L);
            }
        } finally {
            Files.write(exchange.resolve("stateless-shutdown"), new byte[] {1});
            assertExit(source, exchange, "source");
            assertExit(sink, exchange, "sink");
        }
    }

    @Test
    void publicClusterPathExecutesFilterThenMapWithoutMaterializingRows(
            @TempDir Path exchange) throws Exception {
        Process source = launch("source", exchange);
        Process sink = launch("sink", exchange);
        try {
            awaitFile(exchange.resolve("stateless-source.port"), 5_000L);
            awaitFile(exchange.resolve("stateless-sink.port"), 5_000L);
            RemoteStreamSourceSinkRunFactory runs =
                    new RemoteStreamSourceSinkRunFactory(
                            exchange.resolve("stateless-run-authority"), 1L,
                            "stateless-master-incarnation",
                            Arrays.asList(endpoint(exchange, "source"),
                                    endpoint(exchange, "sink")));
            try (StreamProductJobMasterServer master =
                         new StreamProductJobMasterServer(
                                 exchange.resolve("stateless-job-authority"), runs);
                 StreamJobClient client = RowWeave.createRealClusterStreamJobClient(
                         "127.0.0.1", master.port())) {
                StreamJobHandle job = session(client).submit(
                        StreamSubmissionId.of("stateless-dag-submit"));

                assertEquals(StreamJobStatus.RUNNING,
                        job.awaitRunning(5_000L));
                awaitFile(exchange.resolve("stateless-result"), 10_000L);
                assertEquals(expectedResult(), text(
                        exchange.resolve("stateless-result")));

                job.cancel(StreamCommandId.of("cancel-stateless-dag"))
                        .awaitCompletion(5_000L);
                assertEquals(StreamJobStatus.CANCELED,
                        job.awaitTermination(5_000L));
            }
        } finally {
            Files.write(exchange.resolve("stateless-shutdown"), new byte[] {1});
            boolean sourceExited = source.waitFor(5L, TimeUnit.SECONDS);
            boolean sinkExited = sink.waitFor(5L, TimeUnit.SECONDS);
            source.destroyForcibly();
            sink.destroyForcibly();
            assertTrue(sourceExited, () -> log(exchange, "source"));
            assertTrue(sinkExited, () -> log(exchange, "sink"));
        }
        assertEquals(0, source.exitValue(), () -> log(exchange, "source"));
        assertEquals(0, sink.exitValue(), () -> log(exchange, "sink"));
    }

    @Test
    void sharedStatelessSubgraphRunsOnceBeforeTwoRemoteSinks(
            @TempDir Path exchange) throws Exception {
        Process source = launch("source", exchange);
        Process sinkA = launch("sink-a", exchange);
        Process sinkB = launch("sink-b", exchange);
        try {
            awaitFile(exchange.resolve("stateless-source.port"), 5_000L);
            awaitFile(exchange.resolve("stateless-sink-a.port"), 5_000L);
            awaitFile(exchange.resolve("stateless-sink-b.port"), 5_000L);
            RemoteStreamSourceSinkRunFactory runs =
                    new RemoteStreamSourceSinkRunFactory(
                            exchange.resolve("fanout-run-authority"), 1L,
                            "fanout-master-incarnation",
                            Arrays.asList(endpoint(exchange, "source"),
                                    endpoint(exchange, "sink-a"),
                                    endpoint(exchange, "sink-b")));
            try (StreamProductJobMasterServer master =
                         new StreamProductJobMasterServer(
                                 exchange.resolve("fanout-job-authority"), runs);
                 StreamJobClient client = RowWeave.createRealClusterStreamJobClient(
                         "127.0.0.1", master.port())) {
                StreamJobHandle job = fanoutSession(client).submit(
                        StreamSubmissionId.of("stateless-fanout-submit"));

                assertEquals(StreamJobStatus.RUNNING,
                        job.awaitRunning(5_000L));
                awaitFile(exchange.resolve("stateless-result-a"), 10_000L);
                awaitFile(exchange.resolve("stateless-result-b"), 10_000L);
                assertEquals(expectedResult(), text(
                        exchange.resolve("stateless-result-a")));
                assertEquals(expectedResult(), text(
                        exchange.resolve("stateless-result-b")));
                assertEquals("500", text(exchange.resolve("map-invocations")));

                job.cancel(StreamCommandId.of("cancel-stateless-fanout"))
                        .awaitCompletion(5_000L);
            }
        } finally {
            Files.write(exchange.resolve("stateless-shutdown"), new byte[] {1});
            assertExit(source, exchange, "source");
            assertExit(sinkA, exchange, "sink-a");
            assertExit(sinkB, exchange, "sink-b");
        }
    }

    private static StreamSession session(StreamJobClient client) {
        StreamSession session = RowWeave.createStreamSession(
                "stateless-dag", "stateless-dag", client);
        session.setResourceProfile(new StreamResourceProfile(
                8, 1_024, 4_096, 0L, 0, 0, 0L, 4, 16, 0L));
        DataStream source = session.fromSource(source()).uid("source");
        DataStream filtered = source.filter(
                StreamProductStatelessWorkerProcessFixture.function(
                "filter-even", FunctionAbi.FILTER_V1, "filter-even"))
                .uid("filter");
        filtered.map(StreamProductStatelessWorkerProcessFixture.function(
                        "map-times-ten", FunctionAbi.MAP_V1,
                        "map-times-ten"), longSchema())
                .uid("map")
                .sinkTo("sink", sink());
        return session;
    }

    private static StreamSession fanoutSession(StreamJobClient client) {
        StreamSession session = RowWeave.createStreamSession(
                "stateless-fanout", "stateless-fanout", client);
        session.setResourceProfile(new StreamResourceProfile(
                8, 1_024, 4_096, 0L, 0, 0, 0L, 4, 16, 0L));
        DataStream mapped = session.fromSource(source()).uid("source")
                .filter(StreamProductStatelessWorkerProcessFixture.function(
                        "filter-even", FunctionAbi.FILTER_V1, "filter-even"))
                .uid("filter")
                .map(StreamProductStatelessWorkerProcessFixture.function(
                        "map-times-ten", FunctionAbi.MAP_V1,
                        "map-times-ten"), longSchema())
                .uid("map");
        mapped.sinkTo("sink-a", sink(
                StreamProductStatelessWorkerProcessFixture.SINK_PROVIDER_A));
        mapped.sinkTo("sink-b", sink(
                StreamProductStatelessWorkerProcessFixture.SINK_PROVIDER_B));
        return session;
    }

    private static StreamSession atomicSession(StreamJobClient client) {
        StreamSession session = RowWeave.createStreamSession(
                "atomic-stateless", "atomic-stateless", client);
        session.setTimeMode(StreamTimeMode.EVENT_TIME);
        session.setResourceProfile(new StreamResourceProfile(
                8, 1_024, 4_096, 0L, 1, 0,
                1_048_576L, 4, 16, 0L));
        StreamExpression value = StreamExpression.input(
                "value", PortableDataType.INTEGER, false);
        StreamCalcSpec timestamped = StreamCalcSpec.of(
                StreamExpression.call(
                        StreamExpression.Function.GREATER_OR_EQUAL, value,
                        StreamExpression.integerLiteral(500)),
                Arrays.asList(StreamCalcSpec.projection("value", value),
                        StreamCalcSpec.projection("event-time",
                                StreamExpression.call(StreamExpression.Function
                                                .EPOCH_MILLIS_TO_TIMESTAMP,
                                        StreamExpression.call(StreamExpression
                                                        .Function.CAST_LONG,
                                                value)))));
        StreamCalcSpec valueOnly = StreamCalcSpec.of(null,
                Collections.singletonList(StreamCalcSpec.projection(
                        "value", value)));
        session.fromSource(source()).uid("source")
                .calc(timestamped, timestampedSchema()).uid("add-time")
                .assignEventTime(StreamEventTimeSpec.boundedOutOfOrderness(
                        StreamExpression.input("event-time",
                                PortableDataType.TIMESTAMP, false),
                        10L, 30_000L,
                        StreamEventTimeSpec.InvalidTimestampPolicy.FAIL))
                .uid("assign-time")
                .calc(valueOnly, integerSchema()).uid("drop-time")
                .flatMap(StreamProductStatelessWorkerProcessFixture.function(
                                "flat-map-even", FunctionAbi.FLAT_MAP_V1,
                                "flat-map-even"),
                        longSchema(), StreamEmissionLimit.of(2, 64L))
                .uid("flat-map")
                .sinkTo("sink", sink());
        return session;
    }

    private static StreamSession unionSession(StreamJobClient client) {
        StreamSession session = RowWeave.createStreamSession(
                "union-runtime", "union-runtime", client);
        session.setResourceProfile(new StreamResourceProfile(
                8, 1_024, 4_096, 0L, 0, 0,
                1_048_576L, 5, 16, 0L));
        DataStream left = session.fromSource(unionSource(
                StreamProductStatelessWorkerProcessFixture.UNION_SOURCE_LEFT,
                "union-left")).uid("left");
        DataStream right = session.fromSource(unionSource(
                StreamProductStatelessWorkerProcessFixture.UNION_SOURCE_RIGHT,
                "union-right")).uid("right");
        left.union(right).uid("union").sinkTo("sink",
                StreamSinkSpec.eagerAtLeastOnce(
                        StreamProductStatelessWorkerProcessFixture
                                .UNION_SINK_PROVIDER,
                        StreamProductStatelessWorkerProcessFixture.SINK_CONTRACT,
                        new byte[] {3}));
        return session;
    }

    private static StreamSession selfUnionSession(StreamJobClient client) {
        StreamSession session = RowWeave.createStreamSession(
                "self-union-runtime", "self-union-runtime", client);
        session.setResourceProfile(new StreamResourceProfile(
                8, 1_024, 4_096, 0L, 0, 0,
                1_048_576L, 4, 16, 0L));
        DataStream source = session.fromSource(source()).uid("source");
        source.union(source).uid("union").sinkTo("sink",
                StreamSinkSpec.eagerAtLeastOnce(
                        StreamProductStatelessWorkerProcessFixture
                                .SELF_UNION_SINK_PROVIDER,
                        StreamProductStatelessWorkerProcessFixture.SINK_CONTRACT,
                        new byte[] {3}));
        return session;
    }

    private static StreamSourceSpec unionSource(
            String providerKey, String split) {
        return StreamSourceSpec.exactOrdered(integerSchema(), providerKey,
                StreamProductStatelessWorkerProcessFixture.SOURCE_CONTRACT,
                new byte[] {1}, Collections.singletonList(new StreamSplitSpec(
                        StreamProductStatelessWorkerProcessFixture.digestText(
                                split), "sequence-v1", new byte[] {2})));
    }

    private static StreamSourceSpec source() {
        return StreamSourceSpec.exactOrdered(integerSchema(),
                StreamProductStatelessWorkerProcessFixture.SOURCE_PROVIDER,
                StreamProductStatelessWorkerProcessFixture.SOURCE_CONTRACT,
                new byte[] {1}, Collections.singletonList(new StreamSplitSpec(
                        StreamProductStatelessWorkerProcessFixture.digestText("split"),
                        "sequence-v1", new byte[] {2})));
    }

    private static StreamSinkSpec sink() {
        return sink(StreamProductStatelessWorkerProcessFixture.SINK_PROVIDER);
    }

    private static StreamSinkSpec sink(String providerKey) {
        return StreamSinkSpec.eagerAtLeastOnce(
                providerKey,
                StreamProductStatelessWorkerProcessFixture.SINK_CONTRACT,
                new byte[] {3});
    }

    private static StreamSchema integerSchema() {
        return schema(DataType.IntegerType);
    }

    private static StreamSchema longSchema() {
        return schema(DataType.LongType);
    }

    private static StreamSchema timestampedSchema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("value", "value", "value",
                        DataType.IntegerType, false),
                new StreamField("event-time", "eventTime", "eventTime",
                        DataType.TimestampType, false)));
    }

    private static StreamSchema schema(DataType type) {
        return new StreamSchema(Collections.singletonList(
                new StreamField("value", "value", "value", type, false)));
    }

    private static String expectedResult() {
        RowSerializer serializer = new RowSerializer(
                StreamProductStatelessWorkerProcessFixture.longMeta());
        java.security.MessageDigest digest =
                StreamProductStatelessWorkerProcessFixture.digest();
        for (int value = 0; value
                < StreamProductStatelessWorkerProcessFixture.RECORDS; value += 2) {
            DataRow row = new DataRow(1);
            row.set(0, value * 10L);
            digest.update(serializer.serialize(row));
        }
        return (StreamProductStatelessWorkerProcessFixture.RECORDS / 2)
                + ":" + StreamProductStatelessWorkerProcessFixture.hex(
                digest.digest());
    }

    private static String expectedAtomicResult() {
        RowSerializer serializer = new RowSerializer(
                StreamProductStatelessWorkerProcessFixture.longMeta());
        java.security.MessageDigest digest =
                StreamProductStatelessWorkerProcessFixture.digest();
        int count = 0;
        for (int value = 500; value
                < StreamProductStatelessWorkerProcessFixture.RECORDS; value++) {
            if ((value & 1) != 0) continue;
            DataRow first = new DataRow(1);
            first.set(0, (long) value);
            digest.update(serializer.serialize(first));
            DataRow second = new DataRow(1);
            second.set(0, value * 10L);
            digest.update(serializer.serialize(second));
            count += 2;
        }
        return count + ":" + StreamProductStatelessWorkerProcessFixture.hex(
                digest.digest());
    }

    private static Process launch(String role, Path exchange) throws Exception {
        return new ProcessBuilder(ForkedJvm.command(
                StreamProductStatelessWorkerProcessFixture.class.getName(),
                role, exchange.toString()))
                .redirectErrorStream(true)
                .redirectOutput(exchange.resolve(
                        "stateless-" + role + ".log").toFile())
                .start();
    }

    private static void assertExit(
            Process process, Path exchange, String role) throws Exception {
        boolean exited = process.waitFor(5L, TimeUnit.SECONDS);
        process.destroyForcibly();
        assertTrue(exited, () -> log(exchange, role));
        assertEquals(0, process.exitValue(), () -> log(exchange, role));
    }

    private static RemoteStreamWorkerEndpoint endpoint(
            Path exchange, String role) throws Exception {
        return RemoteStreamWorkerEndpoint.connect("127.0.0.1",
                Integer.parseInt(text(exchange.resolve(
                        "stateless-" + role + ".port"))));
    }

    private static void awaitFile(Path file, long timeoutMillis)
            throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while ((!Files.exists(file) || Files.size(file) == 0L)
                && System.nanoTime() < deadline) {
            Thread.sleep(5L);
        }
        assertTrue(Files.exists(file) && Files.size(file) > 0L,
                "process did not publish " + file);
    }

    private static String text(Path file) throws Exception {
        return new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
    }

    private static String log(Path exchange, String role) {
        try {
            return text(exchange.resolve(
                    "stateless-" + role + ".log"));
        } catch (Exception ignored) {
            return "missing process log";
        }
    }
}
