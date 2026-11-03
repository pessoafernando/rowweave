package io.rowweave.soak;

import io.rowweave.DataType;
import io.rowweave.connector.s3.S3ConnectorConfiguration;
import io.rowweave.connector.s3.S3CredentialCodec;
import io.rowweave.connector.s3.S3Credentials;
import io.rowweave.connector.s3.S3Manifest;
import io.rowweave.connector.s3.S3ManifestPlanner;
import io.rowweave.connector.s3.S3ObjectFormat;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamSchema;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

final class StreamSoakMinio implements AutoCloseable {
    static final String ACCESS_KEY = "rowweave-soak-access";
    static final String SECRET_KEY = "rowweave-soak-secret";
    static final String BUCKET = "rowweave-soak-snapshots";
    static final StreamSchema SCHEMA = new StreamSchema(
            Collections.singletonList(new StreamField(
                    "id", "id", "id", DataType.LongType, false)));

    private final Process process;
    private final Path log;
    private final int port;
    private final S3Client client;
    private final S3ConnectorConfiguration configuration;
    private final byte[] credentials;
    private long nextId = 1L;

    private StreamSoakMinio(Process process, Path log, int port, S3Client client,
            S3ConnectorConfiguration configuration, byte[] credentials) {
        this.process = process;
        this.log = log;
        this.port = port;
        this.client = client;
        this.configuration = configuration;
        this.credentials = credentials;
    }

    static StreamSoakMinio start(Path binary, Path root) throws Exception {
        if (binary == null || !Files.isExecutable(binary)) {
            throw new IllegalArgumentException("MinIO binary is not executable");
        }
        int apiPort = freePort();
        int consolePort = freePort();
        Path data = Files.createDirectories(root.resolve("data"));
        Path log = root.resolve("minio.log");
        ProcessBuilder builder = new ProcessBuilder(binary.toString(),
                "server", "--address", "127.0.0.1:" + apiPort,
                "--console-address", "127.0.0.1:" + consolePort,
                data.toString());
        builder.environment().put("MINIO_ROOT_USER", ACCESS_KEY);
        builder.environment().put("MINIO_ROOT_PASSWORD", SECRET_KEY);
        builder.environment().put("MINIO_BROWSER", "off");
        Process process = builder.redirectErrorStream(true)
                .redirectOutput(log.toFile()).start();
        try {
            awaitReady(process, log, apiPort);
            S3ConnectorConfiguration configuration =
                    S3ConnectorConfiguration.builder()
                            .endpoint(URI.create("http://127.0.0.1:" + apiPort))
                            .region("us-east-1").bucket(BUCKET)
                            .format(S3ObjectFormat.NDJSON_UTF8)
                            .credentialsReference("vault/s3")
                            .maximumObjectBytes(16L * 1024L * 1024L)
                            .maximumRowBytes(64 * 1024)
                            .temporaryDiskBudgetBytes(32L * 1024L * 1024L)
                            .connectTimeoutMillis(2_000)
                            .readTimeoutMillis(10_000).maximumAttempts(3)
                            .pathStyleAccess(true).allowInsecureEndpoint(true)
                            .build();
            S3Client client = S3Client.builder()
                    .endpointOverride(configuration.endpoint())
                    .region(Region.of(configuration.region()))
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                    .httpClientBuilder(UrlConnectionHttpClient.builder())
                    .serviceConfiguration(S3Configuration.builder()
                            .pathStyleAccessEnabled(true).build())
                    .build();
            client.createBucket(CreateBucketRequest.builder()
                    .bucket(BUCKET).build());
            S3Credentials value = S3Credentials.basic(
                    ACCESS_KEY.getBytes(StandardCharsets.UTF_8),
                    SECRET_KEY.getBytes(StandardCharsets.UTF_8));
            byte[] credentials;
            try { credentials = S3CredentialCodec.encode(value); }
            finally { value.destroy(); }
            return new StreamSoakMinio(process, log, apiPort, client,
                    configuration, credentials);
        } catch (Throwable failure) {
            stop(process);
            throw failure;
        }
    }

    synchronized S3Workload putBatch(int rows) throws Exception {
        if (rows < 1 || rows > 100_000) {
            throw new IllegalArgumentException("S3 batch rows invalid");
        }
        String prefix = "batch-" + System.currentTimeMillis() + '-'
                + nextId + '/';
        List<Long> ids = new ArrayList<>(rows);
        StringBuilder body = new StringBuilder(rows * 24);
        for (int index = 0; index < rows; index++) {
            long id = nextId++;
            ids.add(id);
            body.append("{\"id\":").append(id).append("}\n");
        }
        byte[] bytes = body.toString().getBytes(StandardCharsets.UTF_8);
        client.putObject(PutObjectRequest.builder().bucket(BUCKET)
                        .key(prefix + "rows.ndjson")
                        .contentType("application/x-ndjson").build(),
                RequestBody.fromBytes(bytes));
        S3Manifest manifest = S3ManifestPlanner.planPrefix(
                configuration, SCHEMA, prefix, credentials);
        return new S3Workload(configuration, manifest,
                Collections.unmodifiableList(ids), bytes.length);
    }

    byte[] credentialFrame() { return credentials.clone(); }

    Process process() { return process; }
    Path root() { return log.getParent(); }

    @Override public void close() {
        client.close();
        java.util.Arrays.fill(credentials, (byte) 0);
        stop(process);
    }

    static final class S3Workload {
        final S3ConnectorConfiguration configuration;
        final S3Manifest manifest;
        final List<Long> ids;
        final long sourceBytes;

        S3Workload(S3ConnectorConfiguration configuration,
                S3Manifest manifest, List<Long> ids, long sourceBytes) {
            this.configuration = configuration;
            this.manifest = manifest;
            this.ids = ids;
            this.sourceBytes = sourceBytes;
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(
                0, 1, InetAddress.getByName("127.0.0.1"))) {
            return socket.getLocalPort();
        }
    }

    private static void awaitReady(Process process, Path log, int port)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30L);
        URL health = new URL("http://127.0.0.1:" + port
                + "/minio/health/ready");
        while (System.nanoTime() < deadline) {
            if (!process.isAlive()) {
                throw new IllegalStateException("MinIO exited: " + log);
            }
            HttpURLConnection connection = null;
            try {
                connection = (HttpURLConnection) health.openConnection();
                connection.setConnectTimeout(250);
                connection.setReadTimeout(250);
                if (connection.getResponseCode() == 200) return;
            } catch (IOException notReady) {
                // bounded by the deadline
            } finally {
                if (connection != null) connection.disconnect();
            }
            Thread.sleep(50L);
        }
        throw new IllegalStateException("MinIO did not become ready: " + log);
    }

    static void stop(Process process) {
        if (process == null) return;
        process.destroy();
        try {
            if (!process.waitFor(10L, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                process.waitFor(5L, TimeUnit.SECONDS);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
        }
    }
}
