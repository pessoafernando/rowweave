package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.DataRow;
import io.rowweave.StreamClusterRuntime;
import io.rowweave.MapFunction;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.stream.api.StreamClusterConfig;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamDataRowCodec;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.spi.StreamDiscoveryCursor;
import io.rowweave.stream.spi.StreamDynamicSourceProvider;
import io.rowweave.stream.spi.StreamFunctionProvider;
import io.rowweave.stream.spi.StreamSourceContext;
import io.rowweave.stream.spi.StreamSourceEnumerator;
import io.rowweave.stream.spi.StreamSourceEnumeratorContext;
import io.rowweave.stream.spi.StreamSourceReader;
import io.rowweave.stream.spi.StreamSourceRecord;
import io.rowweave.stream.spi.StreamSplitChange;
import io.rowweave.stream.spi.StreamSplitDiscovery;
import io.rowweave.stream.spi.StreamTransactionStatus;
import io.rowweave.stream.spi.StreamTransactionalSinkContext;
import io.rowweave.stream.spi.StreamTransactionalSinkProvider;
import io.rowweave.stream.spi.StreamTransactionalSinkWriter;

import java.nio.ByteBuffer;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.ZooKeeper;

/** Independent product role used by the ZK + mTLS Stream exit gate. */
public final class StreamZkMtlsProcessFixture {
    public static final String SOURCE = "zk-mtls-process-source-v1";
    public static final String SINK = "zk-mtls-process-sink-v1";
    public static final String SOURCE_CONTRACT = digest("zk-mtls-process-source");
    public static final String SINK_CONTRACT = digest("zk-mtls-process-sink");
    public static final String RELAY_DEPLOYMENT = digest(
            "zk-mtls-process-relay-deployment");
    public static final StreamSchema SCHEMA = new StreamSchema(
            Collections.singletonList(new StreamField(
                    "value", "value", "value", DataType.StringType, false)));

    private static final String PASSWORD = "changeit";

    private StreamZkMtlsProcessFixture() { }

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 7) {
            throw new IllegalArgumentException(
                    "role, exchange, ZK, host, key store, trust store and authz are required");
        }
        String role = arguments[0];
        Path exchange = Paths.get(arguments[1]);
        Properties properties = properties(role, exchange, arguments);
        StreamDynamicSourceProvider source = "source".equals(role)
                ? new ExactSource() : null;
        StreamTransactionalSinkProvider sink = "sink".equals(role)
                ? new FileTransactionalSink(exchange.resolve("sink-store"))
                : null;
        StreamFunctionProvider relay = "relay".equals(role)
                ? new RelayFunctions() : null;
        try (StreamClusterRuntime runtime = StreamClusterRuntime.start(
                StreamClusterConfig.fromProperties(properties),
                source == null ? Collections.emptyList()
                        : Collections.singletonList(source),
                Collections.emptyList(),
                sink == null ? Collections.emptyList()
                        : Collections.singletonList(sink),
                relay == null ? Collections.emptyList()
                        : Collections.singletonList(relay))) {
            int port = role.startsWith("master-")
                    ? runtime.masterEndpoint().port()
                    : runtime.workerEndpoints().get(0).port();
            Files.write(exchange.resolve(role + ".ready"),
                    Integer.toString(port).getBytes(StandardCharsets.US_ASCII));
            while (!Files.exists(exchange.resolve("stop-all"))
                    && !Files.exists(exchange.resolve("stop-" + role))) {
                Path expire = exchange.resolve("expire-" + role);
                if (Files.exists(expire)) {
                    Files.delete(expire);
                    expireRegistrySession(runtime);
                    Files.write(exchange.resolve("expired-" + role),
                            new byte[] {1});
                }
                Thread.sleep(10L);
            }
        }
    }

    private static void expireRegistrySession(StreamClusterRuntime runtime)
            throws Exception {
        Field registryField = StreamClusterRuntime.class.getDeclaredField(
                "workerRegistry");
        registryField.setAccessible(true);
        Object registry = registryField.get(runtime);
        Field clientField = registry.getClass().getDeclaredField("client");
        clientField.setAccessible(true);
        CuratorFramework client = (CuratorFramework) clientField.get(registry);
        CountDownLatch lost = new CountDownLatch(1);
        client.getConnectionStateListenable().addListener((ignored, state) -> {
            if (state == ConnectionState.LOST) lost.countDown();
        });
        ZooKeeper zooKeeper = client.getZookeeperClient().getZooKeeper();
        zooKeeper.getTestable().injectSessionExpiration();
        if (!lost.await(10L, TimeUnit.SECONDS)) {
            throw new IllegalStateException(
                    "source registry did not observe an expired ZK session");
        }
    }

    public static FunctionDeploymentReference relayFunction() {
        byte[] configuration = "relay".getBytes(StandardCharsets.UTF_8);
        return new FunctionDeploymentReference(
                RELAY_DEPLOYMENT, "relay", digest("relay-jar"),
                RelayMap.class.getName(), FunctionAbi.MAP_V1,
                "config-v1", digest("relay"), configuration,
                new FunctionResourceProfile(1_024L, 0L, 1));
    }

    private static final class RelayFunctions
            implements StreamFunctionProvider {
        @Override public String deploymentSetId() {
            return RELAY_DEPLOYMENT;
        }

        @Override public Serializable resolve(
                FunctionDeploymentReference reference) {
            if (!relayFunction().equals(reference)) {
                throw new IllegalArgumentException(
                        "unknown relay function");
            }
            return new RelayMap();
        }
    }

    public static final class RelayMap implements MapFunction {
        @Override public DataRow map(DataRow value) {
            return value.copy();
        }
    }

    private static Properties properties(String role, Path exchange,
            String[] arguments) {
        Properties properties = new Properties();
        boolean master = role.startsWith("master-");
        properties.setProperty("rowweave.stream.master.enable",
                Boolean.toString(master));
        properties.setProperty("rowweave.stream.worker.enable",
                Boolean.toString(!master));
        if (!master) {
            properties.setProperty("rowweave.stream.worker.id",
                    "zk-mtls-" + role);
        }
        properties.setProperty("rowweave.stream.state.dir",
                exchange.resolve(role + "-state").toString());
        properties.setProperty("rowweave.registry", "zk");
        properties.setProperty("rowweave.zk.connectString", arguments[2]);
        if (master) {
            properties.setProperty("rowweave.stream.master.artifact.dir",
                    exchange.resolve("shared-artifacts").toString());
        }
        properties.setProperty("rowweave.control.securityProfile", "MTLS");
        properties.setProperty("rowweave.control.host", arguments[3]);
        properties.setProperty("rowweave.control.tls.keyStore", arguments[4]);
        properties.setProperty("rowweave.control.tls.keyStorePassword", PASSWORD);
        properties.setProperty("rowweave.control.tls.trustStore", arguments[5]);
        properties.setProperty("rowweave.control.tls.trustStorePassword", PASSWORD);
        properties.setProperty("rowweave.control.tls.authzFile", arguments[6]);
        return properties;
    }

    private static final class ExactSource
            implements StreamDynamicSourceProvider {
        @Override public String providerKey() { return SOURCE; }
        @Override public String contractDigest() { return SOURCE_CONTRACT; }

        @Override public StreamSourceEnumerator openEnumerator(
                StreamSourceEnumeratorContext context) {
            return new StreamSourceEnumerator() {
                @Override public StreamSplitDiscovery discover(
                        StreamDiscoveryCursor cursor, int limit) {
                    if (cursor.frame().length == 0) {
                        StreamSplitSpec split = new StreamSplitSpec(
                                digest("zk-mtls-process-split"),
                                "int32-v1", new byte[] {2});
                        return new StreamSplitDiscovery(1L,
                                StreamDiscoveryCursor.of(new byte[] {1}),
                                Collections.singletonList(
                                        StreamSplitChange.add(split, 1L)));
                    }
                    return new StreamSplitDiscovery(1L, cursor,
                            Collections.emptyList());
                }

                @Override public void close() { }
            };
        }

        @Override public StreamSourceReader open(StreamSourceContext context) {
            StreamDataRowCodec codec = new StreamDataRowCodec(SCHEMA);
            return new StreamSourceReader() {
                private final Semaphore wakeups = new Semaphore(0);
                private volatile boolean canceled;
                private int next = context.hasRestoredPosition()
                        ? ByteBuffer.wrap(
                                context.restoredPositionFrame()).getInt() : 0;

                @Override public StreamSourceRecord next() throws Exception {
                    if (next == 0) {
                        DataRow row = new DataRow(1);
                        row.set(0, "zk-mtls");
                        next++;
                        return StreamSourceRecord.of(
                                codec.encode(row), position(next));
                    }
                    while (!canceled) {
                        if (wakeups.tryAcquire(20L,
                                java.util.concurrent.TimeUnit.MILLISECONDS)) {
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
    }

    private static final class FileTransactionalSink
            implements StreamTransactionalSinkProvider {
        private final Path root;

        private FileTransactionalSink(Path root) { this.root = root; }

        @Override public String providerKey() { return SINK; }
        @Override public String contractDigest() { return SINK_CONTRACT; }

        @Override public StreamTransactionalSinkWriter open(
                StreamTransactionalSinkContext context) throws Exception {
            Files.createDirectories(root);
            return new StreamTransactionalSinkWriter() {
                private final java.io.ByteArrayOutputStream pending =
                        new java.io.ByteArrayOutputStream();

                @Override public void write(byte[] frame) throws Exception {
                    pending.write(frame);
                    pending.write('\n');
                }

                @Override public byte[] prepare(String identity)
                        throws Exception {
                    Path prepared = file(identity, ".prepared");
                    if (!Files.exists(prepared)) {
                        Files.write(prepared, pending.toByteArray());
                    }
                    return digest(identity).getBytes(StandardCharsets.US_ASCII);
                }

                @Override public StreamTransactionStatus query(
                        String identity) {
                    if (Files.exists(file(identity, ".visible"))) {
                        return StreamTransactionStatus.VISIBLE;
                    }
                    if (Files.exists(file(identity, ".aborted"))) {
                        return StreamTransactionStatus.ABORTED;
                    }
                    return Files.exists(file(identity, ".prepared"))
                            ? StreamTransactionStatus.PREPARED
                            : StreamTransactionStatus.UNKNOWN;
                }

                @Override public void commit(String identity) throws Exception {
                    Path visible = file(identity, ".visible");
                    if (!Files.exists(visible)) {
                        Files.move(file(identity, ".prepared"), visible);
                    }
                }

                @Override public void finalizeTransaction(String identity) { }

                @Override public void abort(String identity) throws Exception {
                    if (!Files.exists(file(identity, ".visible"))) {
                        Files.deleteIfExists(file(identity, ".prepared"));
                        Files.write(file(identity, ".aborted"), new byte[0]);
                    }
                }

                @Override public void close() { }
            };
        }

        private Path file(String identity, String suffix) {
            return root.resolve(digest(identity) + suffix);
        }
    }

    private static byte[] position(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    public static String digest(String value) {
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
