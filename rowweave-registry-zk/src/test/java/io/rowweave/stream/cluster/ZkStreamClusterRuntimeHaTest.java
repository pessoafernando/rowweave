package io.rowweave.stream.cluster;

import io.rowweave.StreamClusterRuntime;
import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.DataRow;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamJobClient;
import io.rowweave.StreamSession;
import io.rowweave.cluster.registry.zk.ZkStreamJobClient;
import io.rowweave.cluster.registry.zk.ZkServiceRegistry;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.stream.api.StreamClusterConfig;
import io.rowweave.stream.api.StreamControlException;
import io.rowweave.stream.api.StreamCheckpointConfig;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamDeliveryGuarantee;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamJobId;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamDataRowCodec;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.runtime.StreamCheckpointAuthorityProvider;
import io.rowweave.stream.runtime.StreamCheckpointCatalogProvider;
import io.rowweave.stream.spi.StreamSourceContext;
import io.rowweave.stream.spi.StreamSourceProvider;
import io.rowweave.stream.spi.StreamSourceReader;
import io.rowweave.stream.spi.StreamSourceRecord;
import io.rowweave.stream.spi.StreamTransactionStatus;
import io.rowweave.stream.spi.StreamTransactionalSinkContext;
import io.rowweave.stream.spi.StreamTransactionalSinkProvider;
import io.rowweave.stream.spi.StreamTransactionalSinkWriter;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** product bootstrap wires ZK election and shared authority. */
class ZkStreamClusterRuntimeHaTest {
    private static final String SOURCE = "zk-ha-source-v1";
    private static final String SINK = "zk-ha-sink-v1";
    private static final String SOURCE_CONTRACT = digest("zk-ha-source");
    private static final String SINK_CONTRACT = digest("zk-ha-sink");
    private static final StreamSchema SCHEMA = new StreamSchema(
            Collections.singletonList(new StreamField(
                    "value", "value", "value", DataType.StringType, false)));

    @Test
    void twoMasterProcessesFailOverThroughProductBootstrap(@TempDir Path root)
            throws Exception {
        try (TestingServer zk = new TestingServer(true)) {
            Path artifacts = root.resolve("shared-artifacts");
            StreamClusterRuntime first = start(
                    root.resolve("first"), artifacts, zk.getConnectString());
            try (StreamClusterRuntime second = start(
                         root.resolve("second"), artifacts,
                         zk.getConnectString());
                 ZkServiceRegistry observer =
                         new ZkServiceRegistry(zk.getConnectString());
                 StreamJobClient client = new ZkStreamJobClient(
                         zk.getConnectString(),
                         ControlSecurityConfig.fromProperties(
                                 new Properties()))) {
                assertTrue(Files.isDirectory(
                        artifacts.resolve("artifacts-v1")));
                assertTrue(Files.isDirectory(
                        artifacts.resolve("stream-checkpoints")),
                        "HA checkpoint payloads must not remain under a local Master root");
                assertTrue(observer instanceof
                        StreamCheckpointAuthorityProvider,
                        "HA registry must expose shared checkpoint authority");
                assertTrue(observer instanceof
                        StreamCheckpointCatalogProvider,
                        "HA registry must expose shared member/savepoint catalog");
                assertEquals(3L,
                        ((StreamClusterRunStateStoreProvider) observer)
                                .streamClusterRunStateStore()
                                .nextCoordinatorTerm(),
                        "both process runtimes must reserve shared terms");
                assertMissing(client, "first-zk-leader");
                first.close();
                first = null;
                assertMissingEventually(client, "replacement-zk-leader");
            } finally {
                if (first != null) first.close();
            }
        }
    }

    @Test
    void checkpointedJobContinuesAfterLeaderMovesToDisjointMasterRoot(
            @TempDir Path root) throws Exception {
        try (TestingServer zk = new TestingServer(true)) {
            Path artifacts = root.resolve("shared-artifacts");
            ExactSource source = new ExactSource();
            ExactSink sink = new ExactSink();
            StreamClusterRuntime worker = startWorker(root.resolve("worker"),
                    zk.getConnectString(), source, sink);
            StreamClusterRuntime first = start(
                    root.resolve("master-a"), artifacts,
                    zk.getConnectString());
            try (StreamClusterRuntime second = start(
                         root.resolve("master-b"), artifacts,
                         zk.getConnectString());
                 ZkServiceRegistry observer =
                         new ZkServiceRegistry(zk.getConnectString());
                 StreamJobClient client = new ZkStreamJobClient(
                         zk.getConnectString(),
                         ControlSecurityConfig.fromProperties(
                                 new Properties()))) {
                StreamJobHandle job = session(client).submit(
                        StreamSubmissionId.of("zk-ha-submit"));
                job.awaitRunning(10_000L);
                source.awaitPrefix(10_000L);
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        job.triggerCheckpoint(StreamCommandId.of("zk-cut-1"))
                                .awaitCompletion(10_000L));

                first.close();
                first = null;
                awaitLeader(observer, second.masterEndpoint().port(),
                        10_000L);
                StreamJobHandle replacement = awaitRunning(
                        client, job.id(), 30_000L);
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        replacement.triggerCheckpoint(
                                StreamCommandId.of("zk-cut-2"))
                                .awaitCompletion(10_000L));
                replacement.cancel(StreamCommandId.of("zk-ha-cancel"))
                        .awaitCompletion(10_000L);
                assertEquals(2, sink.commitCount());
            } finally {
                if (first != null) first.close();
                worker.close();
            }
        }
    }

    private static StreamClusterRuntime start(Path state, Path artifacts,
            String connect) {
        Properties properties = new Properties();
        properties.setProperty("rowweave.stream.master.enable", "true");
        properties.setProperty("rowweave.stream.state.dir", state.toString());
        properties.setProperty("rowweave.stream.master.artifact.dir",
                artifacts.toString());
        properties.setProperty("rowweave.registry", "zk");
        properties.setProperty("rowweave.zk.connectString", connect);
        return StreamClusterRuntime.start(
                StreamClusterConfig.fromProperties(properties),
                Collections.emptyList(), Collections.emptyList(),
                Collections.emptyList(), Collections.emptyList());
    }

    private static StreamClusterRuntime startWorker(Path state,
            String connect, ExactSource source, ExactSink sink) {
        Properties properties = new Properties();
        properties.setProperty("rowweave.stream.worker.enable", "true");
        properties.setProperty("rowweave.stream.worker.id", "zk-ha-worker");
        properties.setProperty("rowweave.stream.state.dir", state.toString());
        properties.setProperty("rowweave.registry", "zk");
        properties.setProperty("rowweave.zk.connectString", connect);
        return StreamClusterRuntime.start(
                StreamClusterConfig.fromProperties(properties),
                Collections.singletonList(source), Collections.emptyList(),
                Collections.singletonList(sink), Collections.emptyList());
    }

    private static StreamSession session(StreamJobClient client) {
        StreamSession session = RowWeave.createStreamSession(
                "zk-ha", "zk-ha", client);
        session.setDeliveryGuarantee(StreamDeliveryGuarantee.EXACTLY_ONCE);
        session.setCheckpointConfig(StreamCheckpointConfig.periodic(
                60_000L, 10_000L, 2));
        session.setResourceProfile(new StreamResourceProfile(
                8, 4_096, 8_192, 0L, 0, 0,
                32_768L, 8, 32, 0L));
        session.fromSource(StreamSourceSpec.exactOrdered(
                        SCHEMA, SOURCE, SOURCE_CONTRACT, new byte[] {1},
                        Collections.singletonList(new StreamSplitSpec(
                                digest("zk-ha-split"), "int32-v1",
                                new byte[] {2}))))
                .uid("source")
                .sinkTo("sink", StreamSinkSpec.checkpointTransactional(
                        SINK, SINK_CONTRACT, new byte[] {3}));
        return session;
    }

    private static StreamJobHandle awaitRunning(StreamJobClient client,
            StreamJobId jobId, long timeoutMillis) throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        Throwable last = null;
        while (System.nanoTime() < deadline) {
            try {
                StreamJobHandle handle = client.attach(jobId);
                StreamJobStatus status = handle.awaitRunning(Math.max(1L,
                        TimeUnit.NANOSECONDS.toMillis(
                                deadline - System.nanoTime())));
                if (status == StreamJobStatus.RUNNING) return handle;
            } catch (RuntimeException unavailable) {
                last = unavailable;
                Thread.sleep(20L);
            }
        }
        throw new IllegalStateException(
                "replacement Stream Master did not resume the job", last);
    }

    private static void awaitLeader(ZkServiceRegistry registry,
            int expectedPort, long timeoutMillis) throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            io.rowweave.cluster.registry.RegistryWorker active =
                    registry.worker(StreamClusterRoles
                            .MASTER_LEADER_ROLE_ID);
            if (active != null && active.port() == expectedPort) return;
            Thread.sleep(20L);
        }
        throw new IllegalStateException(
                "standby Stream Master did not acquire the ZK role");
    }

    private static void assertMissingEventually(
            StreamJobClient client, String jobId) throws Exception {
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (true) {
            try {
                assertMissing(client, jobId);
                return;
            } catch (AssertionError unavailable) {
                if (System.nanoTime() >= deadline) throw unavailable;
                Thread.sleep(20L);
            }
        }
    }

    private static void assertMissing(StreamJobClient client, String jobId) {
        StreamControlException missing = assertThrows(
                StreamControlException.class,
                () -> client.attach(StreamJobId.of(jobId)));
        assertEquals(StreamControlException.Code.JOB_NOT_FOUND,
                missing.code());
    }

    private static final class ExactSource implements StreamSourceProvider {
        private final Semaphore prefix = new Semaphore(0);

        @Override public String providerKey() { return SOURCE; }
        @Override public String contractDigest() { return SOURCE_CONTRACT; }

        @Override public StreamSourceReader open(StreamSourceContext context) {
            StreamDataRowCodec codec = new StreamDataRowCodec(SCHEMA);
            return new StreamSourceReader() {
                private final Semaphore wakeups = new Semaphore(0);
                private volatile boolean canceled;
                private int next = context.hasRestoredPosition()
                        ? ByteBuffer.wrap(
                                context.restoredPositionFrame()).getInt() : 0;

                @Override public StreamSourceRecord next() throws Exception {
                    if (next < 2) {
                        DataRow row = new DataRow(1);
                        row.set(0, next == 0 ? "one" : "two");
                        next++;
                        if (next == 2) prefix.release();
                        return StreamSourceRecord.of(
                                codec.encode(row), position(next));
                    }
                    while (!canceled) {
                        if (wakeups.tryAcquire(20L, TimeUnit.MILLISECONDS)) {
                            return null;
                        }
                    }
                    return null;
                }

                @Override public byte[] checkpointPositionFrame() {
                    return position(next);
                }
                @Override public void wakeUp() { wakeups.release(); }
                @Override public void cancel() {
                    canceled = true;
                    wakeups.release();
                }
                @Override public void close() { cancel(); }
            };
        }

        void awaitPrefix(long timeoutMillis) throws InterruptedException {
            if (!prefix.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("source prefix timed out");
            }
        }

        private static byte[] position(int value) {
            return ByteBuffer.allocate(4).putInt(value).array();
        }
    }

    private static final class ExactSink
            implements StreamTransactionalSinkProvider {
        private final Map<String, Transaction> transactions =
                new LinkedHashMap<>();
        private int commits;

        @Override public String providerKey() { return SINK; }
        @Override public String contractDigest() { return SINK_CONTRACT; }

        @Override public StreamTransactionalSinkWriter open(
                StreamTransactionalSinkContext context) {
            StreamDataRowCodec codec = new StreamDataRowCodec(SCHEMA);
            return new StreamTransactionalSinkWriter() {
                private final List<String> pending = new ArrayList<>();
                @Override public void write(byte[] frame) {
                    pending.add(codec.decode(frame).getString(0));
                }
                @Override public byte[] prepare(String identity) {
                    synchronized (transactions) {
                        transactions.putIfAbsent(identity,
                                new Transaction(new ArrayList<>(pending)));
                    }
                    return digest(identity).getBytes(
                            StandardCharsets.US_ASCII);
                }
                @Override public StreamTransactionStatus query(
                        String identity) {
                    synchronized (transactions) {
                        Transaction value = transactions.get(identity);
                        return value == null ? StreamTransactionStatus.UNKNOWN
                                : value.status;
                    }
                }
                @Override public void commit(String identity) {
                    synchronized (transactions) {
                        Transaction value = transactions.get(identity);
                        if (value.status != StreamTransactionStatus.VISIBLE) {
                            value.status = StreamTransactionStatus.VISIBLE;
                            commits++;
                        }
                    }
                }
                @Override public void finalizeTransaction(String identity) { }
                @Override public void abort(String identity) {
                    synchronized (transactions) {
                        Transaction value = transactions.get(identity);
                        if (value != null) {
                            value.status = StreamTransactionStatus.ABORTED;
                        }
                    }
                }
                @Override public void close() { }
            };
        }

        int commitCount() {
            synchronized (transactions) { return commits; }
        }
    }

    private static final class Transaction {
        private final List<String> rows;
        private StreamTransactionStatus status =
                StreamTransactionStatus.PREPARED;
        private Transaction(List<String> rows) { this.rows = rows; }
    }

    private static String digest(String value) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256").digest(
                    value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(64);
            for (byte one : bytes) {
                result.append(Character.forDigit((one >>> 4) & 0x0f, 16));
                result.append(Character.forDigit(one & 0x0f, 16));
            }
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
